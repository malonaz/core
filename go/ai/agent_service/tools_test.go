package agent_service

import (
	"context"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/ai"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	healthService = "grpc.health.v1.Health"
	// healthToolSet and healthCheckGrant are the Health service's tool set and its Check tool, as specs grant them.
	healthToolSet    = "grpc-health-v1-health"
	healthCheckGrant = "tools/Health_Check"
	healthCheckTool  = "Health_Check"
)

// fakeHealthServer is a tool target behind the test interceptor: it records its Check requests and answers SERVING,
// NOT_FOUND for the service "missing", after a while for "slow", and never for "block".
type fakeHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	mutex    sync.Mutex
	services []string
	// The session of the last call of each method, reflection included.
	methodToSession map[string]*authenticationpb.Session
}

// recordSession records the session of a call to a method.
func (s *fakeHealthServer) recordSession(ctx context.Context, method string) {
	session, _ := authentication.GetSession(ctx)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.methodToSession == nil {
		s.methodToSession = map[string]*authenticationpb.Session{}
	}
	s.methodToSession[method] = session
}

// session returns the session of the last call to a method.
func (s *fakeHealthServer) session(method string) *authenticationpb.Session {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.methodToSession[method]
}

// serverOptions authenticate every call as the replicas' servers do, and record its session.
func (s *fakeHealthServer) serverOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(testInterceptor.Unary(), func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			s.recordSession(ctx, info.FullMethod)
			return handler(ctx, request)
		}),
		grpc.ChainStreamInterceptor(testInterceptor.Stream(), func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			s.recordSession(stream.Context(), info.FullMethod)
			return handler(server, stream)
		}),
	}
}

func (s *fakeHealthServer) Check(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	s.mutex.Lock()
	s.services = append(s.services, request.Service)
	s.mutex.Unlock()
	switch request.Service {
	case "missing":
		return nil, status.Errorf(codes.NotFound, "service missing is unknown").Err()
	case "slow":
		time.Sleep(300 * time.Millisecond)
	case "block":
		<-ctx.Done()
		return nil, status.FromError(ctx.Err(), "checking").Err()
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (s *fakeHealthServer) checkedServices() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return slices.Clone(s.services)
}

func (s *fakeHealthServer) register(server *grpc.Server) {
	grpc_health_v1.RegisterHealthServer(server, s)
}

// targetSelf points the health tool service at the replica's own server, where the test co-hosts it.
func targetSelf(opts *Opts) {
	opts.ToolTargets = map[string]string{healthService: opts.AdvertisedAddress}
}

// newTestToolTarget serves a fake health service with reflection on its own unix socket and returns its endpoint.
func newTestToolTarget(t *testing.T) (*fakeHealthServer, string) {
	t.Helper()
	socketDirectory, err := os.MkdirTemp("", "tool")
	require.NoError(t, err)
	socketPath := filepath.Join(socketDirectory, "grpc.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	healthServer := &fakeHealthServer{}
	server := grpc.NewServer(healthServer.serverOptions()...)
	healthServer.register(server)
	reflection.Register(server)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		os.RemoveAll(socketDirectory)
	})
	return healthServer, "unix:" + socketPath
}

// lastToolResults returns the tool results of a chat, by tool call id.
func toolResults(messages []*aipb.Message) map[string]*aipb.ToolResult {
	toolCallIDToToolResult := map[string]*aipb.ToolResult{}
	for _, message := range messages {
		for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolResult) {
			toolCallIDToToolResult[block.GetToolResult().GetToolCallId()] = block.GetToolResult()
		}
	}
	return toolCallIDToToolResult
}

func findTool(tools []*aipb.Tool, name string) *aipb.Tool {
	for _, tool := range tools {
		if tool.GetName() == name {
			return tool
		}
	}
	return nil
}

func TestTools_MethodOnMappedTarget(t *testing.T) {
	healthServer, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	agent := createTestAgent(t, service, healthCheckGrant)
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check the health", "service": "library"}),
			newToolCall("call-2", healthCheckTool, map[string]any{"tool_call_title": "Check a missing service", "service": "missing"}),
		),
		textGeneration("all checked"),
	)

	sendText(t, service, agent.Name, "check")

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.ElementsMatch(t, []string{"library", "missing"}, healthServer.checkedServices())
	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	require.Equal(t, "SERVING", toolCallIDToToolResult["call-1"].GetStructuredContent().GetStructValue().GetFields()["status"].GetStringValue())
	require.Equal(t, healthCheckTool, toolCallIDToToolResult["call-1"].GetToolName())
	require.Equal(t, int32(codes.NotFound), toolCallIDToToolResult["call-2"].GetError().GetCode())

	// Every tool, built-ins included, requires a title.
	generateMessageRequest := service.aiService.generateMessageRequests(agent.Chat)[0]
	for _, name := range []string{healthCheckTool, createAgentsToolName, messageAgentToolName, listAgentsToolName, updateLabelsToolName} {
		tool := findTool(generateMessageRequest.GetTools(), name)
		require.NotNil(t, tool, name)
		require.Contains(t, tool.GetJsonSchema().GetRequired(), "tool_call_title", name)
	}
}

func TestTools_ServiceCoHostedOnTheReplica(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createTestAgent(t, service, healthToolSet)
	service.aiService.script(agent.Chat,
		toolCallGeneration(newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check the health", "service": "co-hosted"})),
		textGeneration("checked"),
	)

	sendText(t, service, agent.Name, "check")

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"co-hosted"}, healthServer.checkedServices())
	toolSets := service.aiService.generateMessageRequests(agent.Chat)[0].GetToolSets()
	require.Len(t, toolSets, 1)
	require.Equal(t, healthToolSet, toolSets[0].GetName())
	require.Equal(t, "Health_Discover", toolSets[0].GetDiscoveryTool().GetName())
	// Watch streams: only unary methods are tools.
	var toolNames []string
	for _, tool := range toolSets[0].GetTools() {
		toolNames = append(toolNames, tool.GetName())
	}
	require.ElementsMatch(t, []string{healthCheckTool, "Health_List"}, toolNames)
}

func TestTools_AnsweredCallsAreFedBackVerbatim(t *testing.T) {
	healthServer, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	agent := createTestAgent(t, service, healthToolSet)
	toolCall := newToolCall("call-1", "Health_Discover", map[string]any{"tools": []any{healthCheckTool}})
	toolCall.Result = ai.NewToolResult("Health_Discover", "call-1", "discovered")
	service.aiService.script(agent.Chat, toolCallGeneration(toolCall), textGeneration("done"))

	sendText(t, service, agent.Name, "discover")

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Empty(t, healthServer.checkedServices())
	require.Equal(t, "discovered", toolResults(service.aiService.messages(agent.Chat))["call-1"].GetContent())
}

func TestTools_OtherOrganizationsToolsAreRefused(t *testing.T) {
	service := newTestService(t)
	const refusal = "may be granted only platform tools or its organization's"
	user := newTestUser(t)
	userRn, err := agentpb.ParseUserRn(user)
	require.NoError(t, err)
	for _, grant := range []*agentpb.AgentSpec_ToolGrant{
		toolGrant("organizations/other/tools/Health_Check"),
		{Grant: &agentpb.AgentSpec_ToolGrant_ToolSet{ToolSet: &agentpb.ToolSetSelector{Parent: "organizations/other", Id: healthToolSet}}},
	} {
		spec := &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{grant}}
		createAgentRequest := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{Spec: spec}}
		_, err = service.CreateAgent(context.Background(), createAgentRequest)
		require.True(t, status.HasCode(err, codes.InvalidArgument))
		require.ErrorContains(t, err, refusal)
		createDefinitionRequest := &agentservicepb.CreateDefinitionRequest{
			Parent:     userRn.OrganizationRn().String(),
			Definition: &agentpb.Definition{Title: "foreign", Spec: spec},
		}
		_, err = service.CreateDefinition(context.Background(), createDefinitionRequest)
		require.ErrorContains(t, err, refusal)
	}
}

// A granted set offers its stored tools as they are at each turn, those added since included; a tool calling a method
// of a service with no target is left out; read_only, from the tool's method annotation, runs without approval.
func TestTools_ToolboxReadsStoredTools(t *testing.T) {
	aiService := newFakeAiService()
	added := proto.CloneOf(findTool(aiService.tools, healthCheckGrant))
	added.Name = "tools/Health_Added"
	method, _, err := aipb.Annotations.Method.Get(added)
	require.NoError(t, err)
	method.Method, method.ReadOnly = "grpc.health.v1.Health.Added", true
	require.NoError(t, aipb.Annotations.Method.Set(added, method))
	aiService.tools = append(slices.Clip(aiService.tools), added)
	_, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, aiService, func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	agent := createGatedAgent(t, service, toolGrant(healthToolSet), toolGrant("tools/LibraryService_GetBook"))

	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{healthCheckTool, "Health_List", "Health_Added"}, slices.Collect(maps.Keys(toolbox.toolNameToMethodTool)))
	_, _, awaits := toolbox.await(newToolCall("call-1", "Health_Added", nil))
	require.False(t, awaits)
	_, _, awaits = toolbox.await(healthCheckCall("call-2", "x"))
	require.True(t, awaits)
}

func TestTools_RequestIDsDeriveFromTheCall(t *testing.T) {
	// The replica's own AgentService serves as the tool target: CreateDefinition carries a request id.
	const createDefinitionMethod = "tools/AgentService_CreateDefinition"
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{"malonaz.ai.agent.agent_service.v1.AgentService": opts.AdvertisedAddress}
	})
	agent := createTestAgent(t, service, createDefinitionMethod)
	userRn, err := agentpb.ParseUserRn(userOf(t, agent))
	require.NoError(t, err)
	organization := userRn.OrganizationRn().String()
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	tool := findTool(toolbox.tools, "AgentService_CreateDefinition")
	require.NotNil(t, tool)
	require.NotContains(t, tool.GetJsonSchema().GetProperties(), "request_id")

	toolCall := newToolCall("call-1", "AgentService_CreateDefinition", map[string]any{
		"tool_call_title": "Define",
		"parent":          organization,
		"definition":      map[string]any{"title": "defined", "spec": map[string]any{"model": testModel}},
		"request_id":      "made-up",
	})
	// Run again, as after a crash: the same request id creates the definition once.
	for range 2 {
		require.Nil(t, toolbox.call(context.Background(), toolCall).GetError())
	}
	listDefinitionsRequest := &agentservicepb.ListDefinitionsRequest{Parent: organization}
	listDefinitionsResponse, err := service.ListDefinitions(context.Background(), listDefinitionsRequest)
	require.NoError(t, err)
	require.Len(t, listDefinitionsResponse.Definitions, 1)
}
