package agent_service

import (
	"context"
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

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	healthService     = "grpc.health.v1.Health"
	healthCheckMethod = "grpc.health.v1.Health.Check"
	healthCheckTool   = "Health_Check"
)

// fakeHealthServer is a tool target: it records its Check requests and answers SERVING, NOT_FOUND for the service
// "missing", after a while for "slow", and never for "block".
type fakeHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	mutex    sync.Mutex
	services []string
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

// newTestToolTarget serves a fake health service with reflection on its own unix socket and returns its endpoint.
func newTestToolTarget(t *testing.T) (*fakeHealthServer, string) {
	t.Helper()
	socketDirectory, err := os.MkdirTemp("", "tool")
	require.NoError(t, err)
	socketPath := filepath.Join(socketDirectory, "grpc.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	healthServer := &fakeHealthServer{}
	server := grpc.NewServer()
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
	agent := createTestAgent(t, service, healthCheckMethod)
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check the health", "service": "library"}),
			newToolCall("call-2", healthCheckTool, map[string]any{"tool_call_title": "Check a missing service", "service": "missing"}),
		),
		textGeneration("all checked"),
	)

	sendText(t, service, agent.Name, "check")

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.ElementsMatch(t, []string{"library", "missing"}, healthServer.checkedServices())
	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	require.Equal(t, "SERVING", toolCallIDToToolResult["call-1"].GetStructuredContent().GetStructValue().GetFields()["status"].GetStringValue())
	require.Equal(t, healthCheckTool, toolCallIDToToolResult["call-1"].GetToolName())
	require.Equal(t, int32(codes.NotFound), toolCallIDToToolResult["call-2"].GetError().GetCode())

	// Every tool, built-ins included, requires a title.
	generateMessageRequest := service.aiService.generateMessageRequests()[0]
	for _, name := range []string{healthCheckTool, createAgentsToolName, messageAgentToolName, listAgentsToolName, updateLabelsToolName} {
		tool := findTool(generateMessageRequest.GetTools(), name)
		require.NotNil(t, tool, name)
		require.Contains(t, tool.GetJsonSchema().GetRequired(), "tool_call_title", name)
	}
}

func TestTools_ServiceOnDefaultTarget(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), nil, healthServer.register)
	agent := createTestAgent(t, service, healthService)
	service.aiService.script(agent.Chat,
		toolCallGeneration(newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check the health", "service": "co-hosted"})),
		textGeneration("checked"),
	)

	sendText(t, service, agent.Name, "check")

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"co-hosted"}, healthServer.checkedServices())
	toolSets := service.aiService.generateMessageRequests()[0].GetToolSets()
	require.Len(t, toolSets, 1)
	require.Equal(t, healthService, toolSets[0].GetName())
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
	agent := createTestAgent(t, service, healthService)
	toolCall := newToolCall("call-1", "Health_Discover", map[string]any{"tools": []any{healthCheckTool}})
	toolCall.Result = ai.NewToolResult("Health_Discover", "call-1", "discovered")
	service.aiService.script(agent.Chat, toolCallGeneration(toolCall), textGeneration("done"))

	sendText(t, service, agent.Name, "discover")

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.Empty(t, healthServer.checkedServices())
	require.Equal(t, "discovered", toolResults(service.aiService.messages(agent.Chat))["call-1"].GetContent())
}

func TestTools_UnknownToolsFailTheTurn(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service, "no.such.Service")

	sendText(t, service, agent.Name, "hello")

	agent = waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_FAILED)
	require.Contains(t, agent.Status.GetMessage(), "no.such.Service")
}

func TestTools_TitleIsStrippedFromTheRequest(t *testing.T) {
	_, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	agent := createTestAgent(t, service, healthCheckMethod)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	toolbox, err := service.newToolbox(ctx, agent)
	require.NoError(t, err)
	methodTool := toolbox.toolNameToMethodTool[healthCheckTool]
	require.NotNil(t, methodTool)

	toolCall := newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check", "service": "library"})
	request, err := buildToolCallMessage(methodTool.schemaBuilder, methodTool.method.Input(), toolCall)
	require.NoError(t, err)
	require.Equal(t, "library", request.ProtoReflect().Get(request.ProtoReflect().Descriptor().Fields().ByName("service")).String())
	require.Empty(t, request.ProtoReflect().GetUnknown())
}
