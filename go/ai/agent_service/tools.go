package agent_service

import (
	"context"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"google.golang.org/grpc/codes"
	"github.com/malonaz/core/go/grpc/status"
)

// toolRouter routes tool services to the endpoints serving them.
type toolRouter struct{}

func newToolRouter(serviceToEndpoint map[string]string, defaultEndpoint string, connections *connections) (*toolRouter, error) {
	return &toolRouter{}, nil
}

// toolbox holds the tools of one turn of an agent: the built-in tools and its spec's.
type toolbox struct {
	tools    []*aipb.Tool
	toolSets []*aipb.ToolSet
}

// newToolbox resolves the tools of the agent's turn.
func (s *Service) newToolbox(ctx context.Context, agent *agentpb.Agent) (*toolbox, error) {
	return &toolbox{}, nil
}

// call executes a tool call, never answered by the AiService, and returns its result, an error result if it
// failed, stamped with the call's name and id.
func (b *toolbox) call(ctx context.Context, toolCall *aipb.ToolCall) *aipb.ToolResult {
	return ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), status.Errorf(codes.Unimplemented, "unimplemented").Err())
}
