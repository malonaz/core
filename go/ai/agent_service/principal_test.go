package agent_service

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/authentication"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const (
	agentService        = "malonaz.ai.agent.agent_service.v1.AgentService"
	agentServicePrefix  = "/" + agentService + "/"
	getAgentMethod      = agentServicePrefix + "GetAgent"
	getAgentTool        = "AgentService_GetAgent"
	deleteAgentTool     = "AgentService_DeleteAgent"
	harperAccount       = "harper"
	reflectionMethod    = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
	internalServiceType = authenticationpb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE
)

// testIssuer signs the users' tokens: a user reads and deletes their own agents.
var testIssuer = &authenticationpb.JwtIssuer{
	Id:        "test",
	Issuer:    "test",
	Audience:  "test",
	KeySource: &authenticationpb.JwtIssuer_SymmetricKey{SymmetricKey: "0123456789abcdef0123456789abcdef"},
	MethodToAuthorizationCel: map[string]string{
		agentServicePrefix + "CreateAgent": `request.parent == subject`,
		getAgentMethod:                     `request.name.startsWith(subject + "/")`,
		agentServicePrefix + "DeleteAgent": `request.name.startsWith(subject + "/")`,
		agentServicePrefix + "StreamAgent": `request.name.startsWith(subject + "/")`,
	},
}

// testPermissionConfiguration lets admins make agents act as harper, which reads agents, lets the service alone
// reflect, and denies actors deletions.
var testPermissionConfiguration = &authenticationpb.PermissionConfiguration{
	ServiceAccounts: []*authenticationpb.ServiceAccount{
		{
			Id:          harperAccount,
			Type:        internalServiceType,
			Permissions: []string{getAgentMethod},
			Labels:      map[string]string{"team": "harper"},
			ActAsCel:    `session.jwt_identity.claims["admin"] == true`,
		},
		{Id: serviceAccountID, Type: internalServiceType, Permissions: []string{reflectionMethod}},
	},
	Roles:                      []*authenticationpb.Role{{Id: "unused"}},
	JwtIssuers:                 []*authenticationpb.JwtIssuer{testIssuer},
	DelegationAuthorizationCel: `!method.endsWith("/DeleteAgent")`,
}

// sessionRecorder records the session each call reached its handler with, once authenticated.
type sessionRecorder struct {
	mutex               sync.Mutex
	methodToSessions    map[string][]*authenticationpb.Session
	methodToMetadataKey map[string][]string
}

func (r *sessionRecorder) unary(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := r.record(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, request)
}

func (r *sessionRecorder) stream(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := r.record(stream.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(server, stream)
}

// record records the session and metadata keys a call to a method arrived with.
func (r *sessionRecorder) record(ctx context.Context, method string) error {
	session, err := authentication.GetSession(ctx)
	if err != nil {
		return err
	}
	incomingMetadata, _ := metadata.FromIncomingContext(ctx)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.methodToSessions[method] = append(r.methodToSessions[method], proto.CloneOf(session))
	for key := range incomingMetadata {
		r.methodToMetadataKey[method] = append(r.methodToMetadataKey[method], key)
	}
	return nil
}

// delegatedSessions returns the delegated sessions a method was called with.
func (r *sessionRecorder) delegatedSessions(method string) []*authenticationpb.Session {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var sessions []*authenticationpb.Session
	for _, session := range r.methodToSessions[method] {
		if session.GetDelegation() != nil {
			sessions = append(sessions, session)
		}
	}
	return sessions
}

// authenticatedTestService is a replica authenticating its calls, co-hosting the AgentService as its own tool target.
type authenticatedTestService struct {
	*testReplica
	recorder *sessionRecorder
	client   agentservicepb.AgentServiceClient
	minter   *authentication.JwtMinter
}

func newAuthenticatedTestService(t *testing.T) *authenticatedTestService {
	t.Helper()
	writeConfig := func(name string, config proto.Message) string {
		bytes, err := pbutil.JSONMarshal(config)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), name)
		require.NoError(t, os.WriteFile(path, bytes, 0o600))
		return path
	}
	sessionManager := authentication.NewSessionManager(&authentication.SessionManagerOpts{Secret: "session-secret"})
	jwtOpts := &authentication.JwtAuthenticationInterceptorOpts{Config: writeConfig("jwt.json", &authenticationpb.JwtConfiguration{Issuers: []*authenticationpb.JwtIssuer{testIssuer}})}
	jwtInterceptor, err := authentication.NewJwtAuthenticationInterceptor(context.Background(), jwtOpts, sessionManager)
	require.NoError(t, err)
	permissionOpts := &authentication.PermissionAuthenticationInterceptorOpts{Config: writeConfig("permissions.json", testPermissionConfiguration)}
	permissionInterceptor, err := authentication.NewPermissionAuthenticationInterceptor(permissionOpts, sessionManager)
	require.NoError(t, err)
	recorder := &sessionRecorder{methodToSessions: map[string][]*authenticationpb.Session{}, methodToMetadataKey: map[string][]string{}}
	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			sessionManager.UnaryServerLocalContextInjectorInterceptor(),
			jwtInterceptor.Unary(),
			permissionInterceptor.Unary(),
			recorder.unary,
		),
		grpc.ChainStreamInterceptor(
			sessionManager.StreamServerLocalContextInjectorInterceptor(),
			jwtInterceptor.Stream(),
			permissionInterceptor.Stream(),
			recorder.stream,
		),
	}
	replica := newAuthenticatedTestReplica(t, newFakeAiService(), sessionManager, serverOptions, func(opts *Opts) {
		opts.ToolTargets = map[string]string{agentService: opts.AdvertisedAddress}
	})
	connection, err := grpc.NewClient(replica.opts.AdvertisedAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close() })
	minter, err := authentication.NewJwtMinter(testIssuer)
	require.NoError(t, err)
	return &authenticatedTestService{testReplica: replica, recorder: recorder, client: agentservicepb.NewAgentServiceClient(connection), minter: minter}
}

// as returns a context calling as a user of the test issuer.
func (s *authenticatedTestService) as(t *testing.T, user string, admin bool) context.Context {
	t.Helper()
	token, err := s.minter.MintJwt(authentication.MintJwtOpts{Subject: user, Lifetime: time.Hour, Claims: map[string]any{"admin": admin}})
	require.NoError(t, err)
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

// createAgent creates, through the server, a root agent of a user with ungated AgentService tools.
func (s *authenticatedTestService) createAgent(ctx context.Context, user string, principal *authenticationpb.Identity, tools ...string) (*agentpb.Agent, error) {
	spec := &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel, Tools: ungatedTools(tools...), MaxSubAgentDepth: 1}
	createAgentRequest := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{Title: "test agent", Spec: spec, Principal: principal}}
	return s.client.CreateAgent(ctx, createAgentRequest)
}

func harperIdentity() *authenticationpb.Identity {
	return &authenticationpb.Identity{Identity: &authenticationpb.Identity_ServiceAccountIdentity{
		ServiceAccountIdentity: &authenticationpb.ServiceAccountIdentity{ServiceAccountId: harperAccount, ServiceAccountType: internalServiceType},
	}}
}

// requireRedacted asserts a JWT principal reads as its issuer and subject alone.
func requireRedacted(t *testing.T, agent *agentpb.Agent, user string) {
	t.Helper()
	jwtIdentity := agent.GetPrincipal().GetJwtIdentity()
	require.Equal(t, testIssuer.Id, jwtIdentity.GetIssuerId(), agent.Name)
	require.Equal(t, map[string]any{"sub": user}, jwtIdentity.GetClaims().AsMap(), agent.Name)
}

func TestPrincipal_ToolCallsActAsTheCreator(t *testing.T) {
	service := newAuthenticatedTestService(t)
	alice := newTestUser(t)
	aliceCtx := service.as(t, alice, false)
	agent, err := service.createAgent(aliceCtx, alice, nil, agentService+".GetAgent", agentService+".DeleteAgent")
	require.NoError(t, err)
	otherAgent := createTestAgent(t, service.testReplica)

	// Stored whole, read redacted.
	requireRedacted(t, agent, alice)
	storedAgent, err := service.getAgent(context.Background(), agent.Name)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"sub": alice, "admin": false}, onlyClaims(storedAgent.GetPrincipal().GetJwtIdentity().GetClaims().AsMap(), "sub", "admin"))
	requireRedacted(t, getAgent(t, service.testReplica, agent.Name), alice)
	listAgentsRequest := &agentservicepb.ListAgentsRequest{Parent: alice}
	listAgentsResponse, err := service.ListAgents(context.Background(), listAgentsRequest)
	require.NoError(t, err)
	require.Len(t, listAgentsResponse.Agents, 1)
	requireRedacted(t, listAgentsResponse.Agents[0], alice)
	searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: alice, Query: "test"}
	searchAgentsResponse, err := service.SearchAgents(context.Background(), searchAgentsRequest)
	require.NoError(t, err)
	require.Len(t, searchAgentsResponse.Agents, 1)
	requireRedacted(t, searchAgentsResponse.Agents[0], alice)
	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent.Name}
	streamCtx, cancelStream := context.WithCancel(aliceCtx)
	stream, err := service.client.StreamAgent(streamCtx, streamAgentRequest)
	require.NoError(t, err)
	streamAgentResponse, err := stream.Recv()
	require.NoError(t, err)
	requireRedacted(t, streamAgentResponse.GetSnapshot(), alice)
	cancelStream()

	service.aiService.script(agent.Chat,
		toolCallGeneration(
			newToolCall("call-1", getAgentTool, map[string]any{"tool_call_title": "Read myself", "name": agent.Name}),
			newToolCall("call-2", getAgentTool, map[string]any{"tool_call_title": "Read another user's agent", "name": otherAgent.Name}),
			newToolCall("call-3", deleteAgentTool, map[string]any{"tool_call_title": "Delete myself", "name": agent.Name}),
		),
		textGeneration("done"),
	)
	sendText(t, service.testReplica, agent.Name, "go")
	waitForIdle(t, service.testReplica, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)

	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	require.Nil(t, toolCallIDToToolResult["call-1"].GetError())
	// The target re-checked alice's permissions: she may not read another user's agent.
	require.Equal(t, int32(codes.PermissionDenied), toolCallIDToToolResult["call-2"].GetError().GetCode())
	// Actors may not delete, though alice may.
	require.Equal(t, int32(codes.PermissionDenied), toolCallIDToToolResult["call-3"].GetError().GetCode())

	sessions := service.recorder.delegatedSessions(getAgentMethod)
	require.Len(t, sessions, 1)
	require.Equal(t, alice, sessions[0].GetJwtIdentity().GetClaims().AsMap()["sub"])
	require.Equal(t, agent.Name, sessions[0].GetDelegation().GetActor())
	require.WithinDuration(t, time.Now().Add(service.opts.ToolSessionLifetime), sessions[0].GetExpireTime().AsTime(), time.Minute)
	require.NotContains(t, service.recorder.methodToMetadataKey[getAgentMethod], testServiceAccountHeader)

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err = service.client.DeleteAgent(aliceCtx, deleteAgentRequest)
	require.NoError(t, err)
}

func TestPrincipal_ActingAsAServiceAccount(t *testing.T) {
	service := newAuthenticatedTestService(t)
	user := newTestUser(t)

	_, err := service.createAgent(service.as(t, user, false), user, harperIdentity(), agentService+".GetAgent")
	require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)

	agent, err := service.createAgent(service.as(t, user, true), user, harperIdentity(), agentService+".GetAgent")
	require.NoError(t, err)
	require.True(t, proto.Equal(harperIdentity(), agent.GetPrincipal()))

	service.aiService.script(agent.Chat,
		toolCallGeneration(newToolCall("call-1", getAgentTool, map[string]any{"tool_call_title": "Read myself", "name": agent.Name})),
		textGeneration("done"),
	)
	sendText(t, service.testReplica, agent.Name, "go")
	waitForIdle(t, service.testReplica, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Nil(t, toolResults(service.aiService.messages(agent.Chat))["call-1"].GetError())
	sessions := service.recorder.delegatedSessions(getAgentMethod)
	require.Len(t, sessions, 1)
	require.Equal(t, harperAccount, sessions[0].GetServiceAccountIdentity().GetServiceAccountId())
	require.Equal(t, agent.Name, sessions[0].GetDelegation().GetActor())
	require.Equal(t, map[string]string{"team": "harper"}, sessions[0].Labels)

	// A sub-agent acts as its parent's principal, never another.
	subAgentRequest := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{Spec: agent.Spec, ParentAgent: agent.Name}}
	subAgent, err := service.CreateAgent(context.Background(), subAgentRequest)
	require.NoError(t, err)
	storedSubAgent, err := service.getAgent(context.Background(), subAgent.Name)
	require.NoError(t, err)
	require.True(t, proto.Equal(harperIdentity(), storedSubAgent.GetPrincipal()))
	subAgentRequest.Agent.Principal = harperIdentity()
	_, err = service.CreateAgent(context.Background(), subAgentRequest)
	require.True(t, status.HasCode(err, codes.InvalidArgument), "%v", err)
}

// onlyClaims keeps the named claims, dropping the registered ones a token carries.
func onlyClaims(claims map[string]any, names ...string) map[string]any {
	kept := map[string]any{}
	for name, value := range claims {
		if slices.Contains(names, name) {
			kept[name] = value
		}
	}
	return kept
}

func TestPrincipal_SchemasResolvedAsTheService(t *testing.T) {
	service := newAuthenticatedTestService(t)

	_, err := service.toolSchema(context.Background(), service.opts.AdvertisedAddress)
	require.NoError(t, err)

	// On a signed, expiring session of its own account, never the internal credential every service shares.
	sessions := service.recorder.delegatedSessions(reflectionMethod)
	require.NotEmpty(t, sessions)
	session := sessions[0]
	require.Equal(t, serviceAccountID, session.GetServiceAccountIdentity().GetServiceAccountId())
	require.Equal(t, serviceAccountID, session.GetDelegation().GetActor())
	require.Equal(t, service.opts.ToolSessionLifetime, session.GetExpireTime().AsTime().Sub(session.GetCreateTime().AsTime()))
	service.recorder.mutex.Lock()
	defer service.recorder.mutex.Unlock()
	require.Contains(t, service.recorder.methodToMetadataKey[reflectionMethod], "x-signed-session-bin")
	require.NotContains(t, service.recorder.methodToMetadataKey[reflectionMethod], testServiceAccountHeader)
}
