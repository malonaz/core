package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/pbutil/pbjson"
)

func TestTurn_InputStartsATurnThatEndsIdle(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("hello back"))

	sendText(t, service, agent.Name, "hello")

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	messages := service.aiService.messages(agent.Chat)
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT}, roles(messages))
	inputs, ok := agentpb.Annotations.Inputs.Get(messages[1])
	require.True(t, ok)
	require.NotEmpty(t, inputs)
}

func TestTurn_DeliveryNamesAMissingSenderAgent(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("noted"))
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	missingAgent := agentRn.UserRn().AgentRn("missing").String()
	message := ai.NewUserMessage(ai.NewTextBlock("hello"))
	agentpb.Annotations.Sender.Set(message, missingAgent)
	createInputRequest := &agentservicepb.CreateInputRequest{Parent: agent.Name, Input: &agentpb.Input{Message: message}}
	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"[Message from agent " + missingAgent + "]", "hello"}, deliveredTexts(t, service, agent.Chat, input.Name))
}

// partialToolCallBlock returns a block streaming a partial tool call with the given arguments.
func partialToolCallBlock(id string, arguments map[string]any) *aipb.Block {
	toolCall := newToolCall(id, "tool", arguments)
	toolCall.Partial = true
	return &aipb.Block{Content: &aipb.Block_PartialToolCall{PartialToolCall: toolCall}}
}

func TestToolCallAnnouncer_WaitsForTheWholeTitle(t *testing.T) {
	for _, test := range []struct {
		name   string
		blocks []*aipb.Block
		// The index of the block completing the title.
		completeIndex int
	}{
		{
			name: "title first",
			blocks: []*aipb.Block{
				partialToolCallBlock("call-1", map[string]any{"tool_call_title": "Run"}),
				partialToolCallBlock("call-1", map[string]any{"tool_call_title": "Run the tests"}),
				partialToolCallBlock("call-1", map[string]any{"tool_call_title": "Run the tests", "command": "plz"}),
				partialToolCallBlock("call-1", map[string]any{"tool_call_title": "Run the tests", "command": "plz test"}),
			},
			completeIndex: 2,
		},
		{
			name: "title after another argument",
			blocks: []*aipb.Block{
				partialToolCallBlock("call-1", map[string]any{"command": "plz"}),
				partialToolCallBlock("call-1", map[string]any{"command": "plz test"}),
				partialToolCallBlock("call-1", map[string]any{"command": "plz test", "tool_call_title": "Run"}),
				partialToolCallBlock("call-1", map[string]any{"command": "plz test", "tool_call_title": "Run the tests"}),
				partialToolCallBlock("call-1", map[string]any{"command": "plz test", "tool_call_title": "Run the tests", "working_directory": "/"}),
			},
			completeIndex: 4,
		},
		{
			name: "title last, completed by the whole call",
			blocks: []*aipb.Block{
				partialToolCallBlock("call-1", map[string]any{"command": "plz test"}),
				partialToolCallBlock("call-1", map[string]any{"command": "plz test", "tool_call_title": "Run"}),
				ai.NewToolCallBlock(newToolCall("call-1", "tool", map[string]any{"command": "plz test", "tool_call_title": "Run the tests"})),
			},
			completeIndex: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			announcer := newToolCallAnnouncer()
			for i, block := range test.blocks {
				toolCall := announcer.complete(block)
				if i != test.completeIndex {
					require.Nil(t, toolCall, "block %d", i)
					continue
				}
				require.NotNil(t, toolCall, "block %d", i)
				title, _ := pbjson.GetTitle(toolCall.GetArguments().AsMap())
				require.Equal(t, "Run the tests", title)
			}
		})
	}
}
