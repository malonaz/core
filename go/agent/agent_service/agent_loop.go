package agent_service

import (
	"context"
	"fmt"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/go/uuid"
)

// processNextAgent claims and drives one woken agent. Returns true if work was done.
func (s *Service) processNextAgent(ctx context.Context) bool {
	agent, err := s.claimAgent(ctx)
	if err != nil {
		s.log.Error("claiming agent", "error", err)
		return false
	}
	if agent == nil {
		return false
	}
	if err := s.runAgent(ctx, agent); err != nil {
		// Agents are never terminal: log, go idle, and heal on the next wake.
		s.log.Error("running agent", "agent", agent.Name, "error", err)
		agent.State = agentpb.AgentState_AGENT_STATE_IDLE
		if saveErr := s.saveAgent(ctx, agent, "state"); saveErr != nil {
			s.log.Error("idling agent after error", "agent", agent.Name, "error", saveErr)
		}
	}
	return true
}

// runAgent drives turns on the agent's current chat until it has nothing left
// to do, then idles. A create_task call defers the turn: the agent idles and
// the last awaited task's report wakes it back up.
func (s *Service) runAgent(ctx context.Context, agent *agentpb.Agent) error {
	if agent.Metadata == nil {
		agent.Metadata = &agentpb.AgentMetadata{}
	}
	// A deferred turn is still awaiting children: nothing to do until they
	// resolve (their last resolution re-wakes us).
	if len(agent.Metadata.AwaitedTasks) > 0 {
		agent.State = agentpb.AgentState_AGENT_STATE_IDLE
		return s.saveAgent(ctx, agent, "state")
	}
	if agent.Chat == "" {
		// Woken without a chat (nothing to process).
		agent.State = agentpb.AgentState_AGENT_STATE_IDLE
		return s.saveAgent(ctx, agent, "state")
	}

	// Roll the chat over before starting new work if it has grown too large.
	if err := s.maybeCompact(ctx, agent); err != nil {
		return fmt.Errorf("compacting: %w", err)
	}

	tools := []*aipb.Tool{createTaskTool(), createMemoryTool()}
	for turn := 0; turn < s.opts.MaxTurnsPerClaim; turn++ {
		response, err := s.aiServiceClient.GenerateMessage(ctx, &aiservicepb.GenerateMessageRequest{
			Parent:    agent.Chat,
			Model:     agent.Model,
			Tools:     tools,
			RequestId: uuid.MustNewV7().String(),
		})
		if err != nil {
			return fmt.Errorf("generating message: %w", err)
		}
		agent.Metadata.TurnCount++
		generated := response.GetGeneratedMessage()
		agent.Metadata.LastProcessedMessage = generated.GetName()
		calls := toolCallsOf(generated)

		// No tool calls: the text is the agent's reply; go idle.
		if len(calls) == 0 {
			return s.idleAgent(ctx, agent)
		}

		var results []*aipb.ToolResult
		awaited := map[string]string{}
		for _, call := range calls {
			switch call.Name {
			case toolCreateTask:
				childName, err := s.spawnAgentTask(ctx, agent, call)
				if err != nil {
					results = append(results, errorToolResult(call, err))
					continue
				}
				awaited[call.Id] = childName
			case toolCreateMemory:
				results = append(results, s.executeCreateMemory(ctx, agent.Name, agent.Chat, "", call))
			default:
				results = append(results, errorToolResult(call, fmt.Errorf("unknown tool %q", call.Name)))
			}
		}
		if len(results) > 0 {
			if _, err := s.appendMessage(ctx, agent.Chat, toolResultMessage(results)); err != nil {
				return fmt.Errorf("appending tool results: %w", err)
			}
		}
		if len(awaited) > 0 {
			// Defer the turn: idle until the awaited tasks resolve.
			for id, name := range awaited {
				if agent.Metadata.AwaitedTasks == nil {
					agent.Metadata.AwaitedTasks = map[string]string{}
				}
				agent.Metadata.AwaitedTasks[id] = name
			}
			agent.State = agentpb.AgentState_AGENT_STATE_IDLE
			return s.saveAgent(ctx, agent, "state", "metadata")
		}
		// Heartbeat between turns.
		if err := s.saveAgent(ctx, agent, "metadata"); err != nil {
			return err
		}
	}
	return s.idleAgent(ctx, agent)
}

// idleAgent transitions PROCESSING -> IDLE under the row lock, staying ACTIVE
// if a message raced in while we were processing.
func (s *Service) idleAgent(ctx context.Context, agent *agentpb.Agent) error {
	latest, err := s.latestMessageName(ctx, agent.Chat)
	if err != nil {
		return err
	}
	return s.withLockedAgent(ctx, agent.Name, func(locked *agentpb.Agent) ([]string, error) {
		locked.Metadata = agent.Metadata
		if locked.State == agentpb.AgentState_AGENT_STATE_PROCESSING {
			if latest != "" && latest != agent.Metadata.GetLastProcessedMessage() {
				// A wake raced in: stay queued.
				locked.State = agentpb.AgentState_AGENT_STATE_ACTIVE
			} else {
				locked.State = agentpb.AgentState_AGENT_STATE_IDLE
			}
		}
		return []string{"state", "metadata"}, nil
	})
}

// latestMessageName returns the name of the newest message in a chat.
func (s *Service) latestMessageName(ctx context.Context, chatName string) (string, error) {
	response, err := s.aiServiceClient.ListMessages(ctx, &aiservicepb.ListMessagesRequest{
		Parent:   chatName,
		OrderBy:  "create_time desc",
		PageSize: 1,
	})
	if err != nil {
		return "", err
	}
	if len(response.Messages) == 0 {
		return "", nil
	}
	return response.Messages[0].Name, nil
}

// spawnAgentTask creates a task under the agent from a create_task tool call.
func (s *Service) spawnAgentTask(ctx context.Context, agent *agentpb.Agent, call *aipb.ToolCall) (string, error) {
	goal, err := stringArgument(call, "goal")
	if err != nil || goal == "" {
		return "", fmt.Errorf("create_task requires a goal")
	}
	title, _ := stringArgument(call, "title")
	task := &agentpb.Task{
		Name:  agent.Name + "/tasks/" + newResourceID(),
		Title: title,
		Goal:  goal,
		Model: agent.Model,
		State: agentpb.TaskState_TASK_STATE_PENDING,
		Metadata: &agentpb.TaskMetadata{
			ToolSets:    agent.Metadata.GetToolSets(),
			MaxSubtasks: 8, // Agent-delegated tasks may fan out.
		},
	}
	task.RootTask = task.Name
	if err := s.insertTask(ctx, task); err != nil {
		return "", err
	}
	return task.Name, nil
}
