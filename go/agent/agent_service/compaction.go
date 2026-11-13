package agent_service

import (
	"context"
	"fmt"
	"strings"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/gengo/agent/model"
	"github.com/malonaz/core/go/uuid"
)

const extractionPrompt = `Your context window is about to be reset. Review this conversation and persist anything worth keeping as long-term memories using the create_memory tool: durable facts, preferences, decisions, lessons and unfinished threads. Do not record transient chit-chat. When you are done, reply with a short plain-text summary of this conversation and no tool calls: it will seed your fresh context.`

// maybeCompact rolls the agent's chat over when it outgrows the context
// threshold: extract memories from the old chat, then start a fresh chat
// seeded with the persona, top memories and the tail of the old chat.
// Never runs mid-turn: callers only invoke it when nothing is awaited.
func (s *Service) maybeCompact(ctx context.Context, agent *agentpb.Agent) error {
	response, err := s.aiServiceClient.ListMessages(ctx, &aiservicepb.ListMessagesRequest{
		Parent:   agent.Chat,
		OrderBy:  "create_time desc",
		PageSize: 1000,
	})
	if err != nil {
		return err
	}
	size := 0
	for _, message := range response.Messages {
		for _, block := range message.GetBlocks() {
			size += len(block.GetText()) + len(block.GetThought())
			if result := block.GetToolResult(); result != nil {
				size += len(result.GetContent())
			}
		}
	}
	if size < s.opts.CompactionCharThreshold {
		return nil
	}

	// 1. Extraction turns on the old chat: the model persists memories, then
	// summarizes. The summary seeds the new chat.
	summary := ""
	pending := []*aipb.Message{textMessage(aipb.Role_ROLE_USER, extractionPrompt)}
	for turn := 0; turn < 4; turn++ {
		generateResponse, err := s.aiServiceClient.GenerateMessage(ctx, &aiservicepb.GenerateMessageRequest{
			Parent:    agent.Chat,
			Model:     agent.Model,
			Messages:  pending,
			Tools:     []*aipb.Tool{createMemoryTool()},
			RequestId: uuid.MustNewV7().String(),
		})
		if err != nil {
			return fmt.Errorf("extraction turn: %w", err)
		}
		pending = nil
		generated := generateResponse.GetGeneratedMessage()
		calls := toolCallsOf(generated)
		if len(calls) == 0 {
			summary = messageText(generated)
			break
		}
		var results []*aipb.ToolResult
		for _, call := range calls {
			if call.Name == toolCreateMemory {
				results = append(results, s.executeCreateMemory(ctx, agent.Name, agent.Chat, "", call))
			} else {
				results = append(results, errorToolResult(call, fmt.Errorf("only %s is available now", toolCreateMemory)))
			}
		}
		if _, err := s.appendMessage(ctx, agent.Chat, toolResultMessage(results)); err != nil {
			return err
		}
	}

	// 2. Fresh chat: persona + memory digest + summary + verbatim tail.
	oldChat := agent.Chat
	newChat, err := s.createChat(ctx, agent.Name, fmt.Sprintf("%s (chat %d)", agent.Title, len(agent.Metadata.GetPreviousChats())+2))
	if err != nil {
		return err
	}
	seeds := []*aipb.Message{textMessage(aipb.Role_ROLE_SYSTEM, agent.Instructions)}
	if digest, err := s.memoryDigest(ctx, agent.Name); err != nil {
		return err
	} else if digest != "" {
		seeds = append(seeds, textMessage(aipb.Role_ROLE_USER, "Your long-term memories:\n"+digest))
	}
	if summary != "" {
		seeds = append(seeds, textMessage(aipb.Role_ROLE_USER, "Summary of your previous conversation:\n"+summary))
	}
	seeds = append(seeds, tailMessages(response.Messages, s.opts.CompactionTailMessages)...)
	for _, seed := range seeds {
		if _, err := s.appendMessage(ctx, newChat, seed); err != nil {
			return err
		}
	}

	// 3. Atomic swap.
	if agent.Metadata == nil {
		agent.Metadata = &agentpb.AgentMetadata{}
	}
	agent.Metadata.PreviousChats = append(agent.Metadata.PreviousChats, oldChat)
	agent.Metadata.LastProcessedMessage = ""
	agent.Chat = newChat
	return s.saveAgent(ctx, agent, "chat", "metadata")
}

// memoryDigest renders the freshest memories as a titled list.
func (s *Service) memoryDigest(ctx context.Context, agentName string) (string, error) {
	organizationID, userID, agentID, err := model.ParseAgentName(agentName)
	if err != nil {
		return "", err
	}
	memories, err := s.agentPostgresStore.ListMemories(ctx, organizationID, userID, agentID, false,
		"", "ORDER BY update_time DESC", fmt.Sprintf("LIMIT %d", s.opts.CompactionSeedMemories), nil)
	if err != nil {
		return "", err
	}
	var lines []string
	for _, memoryModel := range memories {
		memory, err := memoryModel.ToPb()
		if err != nil {
			return "", err
		}
		line := fmt.Sprintf("- [%s] %s", memory.Name, memory.Title)
		if memory.Description != "" {
			line += ": " + memory.Description
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}

// tailMessages returns the last n user/assistant text messages (oldest first),
// stripped to text blocks so no dangling tool pairs cross the rollover.
func tailMessages(newestFirst []*aipb.Message, n int) []*aipb.Message {
	var tail []*aipb.Message
	for _, message := range newestFirst {
		if len(tail) == n {
			break
		}
		if message.Role != aipb.Role_ROLE_USER && message.Role != aipb.Role_ROLE_ASSISTANT {
			continue
		}
		text := messageText(message)
		if text == "" {
			continue
		}
		tail = append(tail, textMessage(message.Role, text))
	}
	// Reverse to oldest-first.
	for i, j := 0, len(tail)-1; i < j; i, j = i+1, j-1 {
		tail[i], tail[j] = tail[j], tail[i]
	}
	return tail
}
