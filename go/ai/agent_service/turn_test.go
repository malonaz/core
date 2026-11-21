package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/uuid"
)

func TestTurn_InputStartsATurnThatEndsIdle(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("hello back"))

	sendText(t, service, agent.Name, "hello")

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	messages := service.aiService.messages(agent.Chat)
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT}, roles(messages))
	// The user's message, delivered as sent, stamped with its sender.
	sender, _ := agentpb.Annotations.Sender.Get(messages[1])
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	require.Equal(t, agentRn.UserRn().String(), sender)
	require.Equal(t, []string{"hello"}, deliveredTexts(t, service, agent.Chat, messages[1].Name))
	require.Empty(t, getAgent(t, service, agent.Name).QueuedMessages)
}

func TestTurn_DeliveryNamesAMissingSenderAgent(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("noted"))
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	missingAgent := agentRn.UserRn().AgentRn("missing").String()
	_, err = service.messageAgent(context.Background(), agent.Name, ai.NewUserMessage(ai.NewTextBlock("hello")), missingAgent, uuid.MustNewV7().String())
	require.NoError(t, err)

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	deliveredMessage := deliveredMessages(t, service, agent.Name)[0]
	require.Equal(t, []string{"[Message from agent " + missingAgent + "]", "hello"}, deliveredTexts(t, service, agent.Chat, deliveredMessage.Name))
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

func TestTurn_MessagesArriveBetweenToolRounds(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	toolCallGeneration := toolCallGeneration(newToolCall("call-1", listAgentsToolName, map[string]any{"tool_call_title": "List"}))
	toolCallGeneration.started = make(chan struct{})
	toolCallGeneration.gate = make(chan struct{})
	service.aiService.script(agent.Chat, toolCallGeneration, textGeneration("both read"))
	sendText(t, service, agent.Name, "first")
	<-toolCallGeneration.started

	// Queued while the first generation runs: hidden from it, delivered after its tool round.
	sendText(t, service, agent.Name, "second")
	close(toolCallGeneration.gate)

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	histories := service.aiService.generateMessageHistories(agent.Chat)
	require.Len(t, histories, 2)
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER}, roles(histories[0]))
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT, aipb.Role_ROLE_TOOL, aipb.Role_ROLE_USER}, roles(histories[1]))
	require.Equal(t, "second", histories[1][4].GetBlocks()[0].GetText())
}
