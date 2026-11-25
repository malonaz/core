package agent_service

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	libraryService = "malonaz.test.library.library_service.v1.LibraryService"
	mockService    = "malonaz.test.mock.v1.MockService"
	libraryToolSet = "toolSets/" + libraryService
	mockToolSet    = "toolSets/" + mockService
)

// libraryTool names a tool of the library tool set.
func libraryTool(method string) string { return libraryToolSet + "/tools/" + method }

// newTestReachTarget serves the library and mock services behind an interceptor of their own, with its
// AuthorizationService: anyone may call GetBook; an agent's owner also ListBooks, under a condition reading the
// request; the agent service reads the schemas and carries its callers' calls; no one is granted the mock service.
func newTestReachTarget(t *testing.T) string {
	t.Helper()
	const library = "/" + libraryService + "/"
	configuration := &authenticationpb.Configuration{
		Issuers: []*authenticationpb.Issuer{{
			Id:        ownerTokenIssuer,
			Issuer:    ownerTokenIssuer,
			Audience:  ownerTokenIssuer,
			Keys:      &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(testOwnerTokenKey)},
			GrantsCel: `[Grant{role: "owner", scope: "*"}]`,
		}},
		ServiceAccounts: []*authenticationpb.ServiceAccount{{Id: serviceAccountID, Grants: []*authenticationpb.Grant{{Role: "service", Scope: "*"}, {Role: "carrier", Scope: "*"}}}},
		Roles: []*authenticationpb.Role{
			{Id: "public", Rules: []*authenticationpb.Rule{{Methods: []string{library + "GetBook", "/malonaz.authentication.v2.AuthorizationService/TestMethods"}}}},
			{Id: "owner", Rules: []*authenticationpb.Rule{{Methods: []string{library + "ListBooks"}, ConditionCel: `request.parent.startsWith(principal.subject)`}}},
			{Id: "service", Rules: []*authenticationpb.Rule{{Methods: []string{reflectionFullMethod, library + "*"}}}},
			{Id: "carrier", Rules: []*authenticationpb.Rule{{Methods: []string{"*"}, ConditionCel: `caller.subject != "" && principal.subject != caller.subject`}}},
		},
		PublicRoles: []string{"public"},
	}
	interceptor, err := newTestInterceptor(configuration)
	require.NoError(t, err)
	socketDirectory, err := os.MkdirTemp("", "reach")
	require.NoError(t, err)
	socketPath := filepath.Join(socketDirectory, "grpc.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptor.Unary()), grpc.ChainStreamInterceptor(interceptor.Stream()))
	librarypb.RegisterLibraryServiceServer(server, librarypb.UnimplementedLibraryServiceServer{})
	mockpb.RegisterMockServiceServer(server, mockpb.UnimplementedMockServiceServer{})
	authenticationpb.RegisterAuthorizationServiceServer(server, interceptor.NewAuthorizationService(server))
	reflection.Register(server)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		os.RemoveAll(socketDirectory)
	})
	return "unix:" + socketPath
}

// newTestReachReplica returns a replica routing the library and mock services to a reach target, and the health
// service to a target serving no AuthorizationService.
func newTestReachReplica(t *testing.T) *testReplica {
	t.Helper()
	endpoint := newTestReachTarget(t)
	_, healthEndpoint := newTestToolTarget(t)
	return newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{libraryService: endpoint, mockService: endpoint, healthService: healthEndpoint}
	})
}

// The catalogue lists what its caller could call, as each target answers for the caller's session: an anonymous
// caller here, whom the target lets call GetBook alone, though the agent service itself may call every method.
func TestReach_CatalogueListsWhatTheCallerCouldCall(t *testing.T) {
	service := newTestReachReplica(t)
	client := serverClient(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listToolsResponse, err := client.ListTools(ctx, &agentservicepb.ListToolsRequest{Parent: libraryToolSet})
	require.NoError(t, err)
	require.Equal(t, []string{libraryTool("GetBook")}, toolNames(listToolsResponse.GetTools()))
	_, err = client.GetTool(ctx, &agentservicepb.GetToolRequest{Name: libraryTool("DeleteBook")})
	require.True(t, status.HasCode(err, codes.NotFound), "%v", err)

	// A set none of whose tools the caller could call is not theirs to see.
	_, err = client.GetToolSet(ctx, &agentservicepb.GetToolSetRequest{Name: mockToolSet})
	require.True(t, status.HasCode(err, codes.NotFound), "%v", err)
	_, err = client.ListTools(ctx, &agentservicepb.ListToolsRequest{Parent: mockToolSet})
	require.True(t, status.HasCode(err, codes.NotFound), "%v", err)
	listToolSetsResponse, err := client.ListToolSets(ctx, &agentservicepb.ListToolSetsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"toolSets/genui", healthToolSet, libraryToolSet}, toolSetNames(listToolSetsResponse.GetToolSets()))

	// A target serving no AuthorizationService trims nothing: its own rules decide every call.
	listToolsResponse, err = client.ListTools(ctx, &agentservicepb.ListToolsRequest{Parent: healthToolSet})
	require.NoError(t, err)
	require.Equal(t, []string{healthCheckGrant, healthToolSet + "/tools/List"}, toolNames(listToolsResponse.GetTools()))
}

// A turn offers the model what the agent's owner could call, as each target answers for the owner: GetBook, and
// ListBooks, whose condition only a call decides; never a granted tool the owner could not call.
func TestReach_ToolboxOffersWhatTheOwnerCouldCall(t *testing.T) {
	service := newTestReachReplica(t)
	spec := &agentpb.AgentSpec{Model: testModel, Tools: ungatedTools(libraryToolSet, mockToolSet+"/tools/Echo", healthToolSet)}
	agent := createTestAgentWithSpec(t, service, newTestUser(t), spec)

	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)

	var resourceNames []string
	for _, methodTool := range toolbox.toolNameToMethodTool {
		resourceNames = append(resourceNames, methodTool.resourceName)
	}
	slices.Sort(resourceNames)
	require.Equal(t, []string{healthCheckGrant, healthToolSet + "/tools/List", libraryTool("GetBook"), libraryTool("ListBooks")}, resourceNames)
}
