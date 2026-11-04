package agent_executor

import (
	"context"
	"encoding/json"
	"fmt"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// Tool is a capability an executor gives the agents it runs: its own coding
// tools, or the built-in agent tools.
type Tool interface {
	// Definition is the tool as the model sees it. Its name is unique within an executor.
	Definition() *aipb.Tool
	// Execute runs a call to the tool on behalf of an agent. A returned error is
	// fed back to the model as the call's error result.
	Execute(ctx context.Context, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error)
}

// DecodeArguments unmarshals a tool call's arguments into a Go struct.
func DecodeArguments(toolCall *aipb.ToolCall, arguments any) error {
	bytes, err := toolCall.GetArguments().MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling arguments: %w", err)
	}
	if err := json.Unmarshal(bytes, arguments); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	return nil
}
