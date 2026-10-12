package agent_service

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/gengo/agent/model"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/uuid"
)

// processNextTask claims and drives one task. Returns true if work was done.
func (s *Service) processNextTask(ctx context.Context) bool {
	task, err := s.claimTask(ctx)
	if err != nil {
		s.log.Error("claiming task", "error", err)
		return false
	}
	if task == nil {
		return false
	}
	if err := s.runTask(ctx, task); err != nil {
		s.log.Error("running task", "task", task.Name, "error", err)
		s.finishTask(ctx, task, "", fmt.Sprintf("runner error: %v", err))
	}
	return true
}

// runTask drives the task loop: per turn, generate on the backing chat and
// resolve every tool call into a single tool message. No tool calls means the
// final text becomes the report.
func (s *Service) runTask(ctx context.Context, task *agentpb.Task) error {
	agent, err := s.getAgentByName(ctx, taskAgentName(task.Name))
	if err != nil {
		return fmt.Errorf("loading owning agent: %w", err)
	}
	if task.Metadata == nil {
		task.Metadata = &agentpb.TaskMetadata{}
	}

	// First claim: create the backing chat and seed persona + goal.
	var pendingMessages []*aipb.Message
	if task.Chat == "" {
		chatTitle := task.Title
		if chatTitle == "" {
			chatTitle = fmt.Sprintf("task: %.100s", task.Goal)
		}
		chatName, err := s.createChat(ctx, task.Name, chatTitle)
		if err != nil {
			return err
		}
		task.Chat = chatName
		if err := s.saveTask(ctx, task, "chat"); err != nil {
			return err
		}
		pendingMessages = []*aipb.Message{
			textMessage(aipb.Role_ROLE_SYSTEM, taskSystemPrompt(agent)),
			textMessage(aipb.Role_ROLE_USER, task.Goal),
		}
	}

	modelName := task.Model
	if modelName == "" {
		modelName = agent.Model
	}
	tools := []*aipb.Tool{createMemoryTool()}
	if task.Metadata.MaxSubtasks > 0 && int(task.Metadata.Depth) < s.opts.MaxTaskDepth {
		tools = append(tools, createTaskTool())
	}

	for turn := 0; turn < s.opts.MaxTurnsPerClaim; turn++ {
		response, err := s.aiServiceClient.GenerateMessage(ctx, &aiservicepb.GenerateMessageRequest{
			Parent:    task.Chat,
			Model:     modelName,
			Messages:  pendingMessages,
			Tools:     tools,
			RequestId: uuid.MustNewV7().String(),
		})
		if err != nil {
			return fmt.Errorf("generating message: %w", err)
		}
		pendingMessages = nil
		task.Metadata.TurnCount++
		generated := response.GetGeneratedMessage()
		calls := toolCallsOf(generated)

		// No tool calls: the final text is the report.
		if len(calls) == 0 {
			s.finishTask(ctx, task, messageText(generated), "")
			return nil
		}

		// Resolve tool calls. Awaited spawns defer the turn; everything else
		// resolves inline and is appended as a tool message.
		var results []*aipb.ToolResult
		awaited := map[string]string{}
		for _, call := range calls {
			switch call.Name {
			case toolCreateTask:
				childName, err := s.spawnSubtask(ctx, task, call)
				if err != nil {
					results = append(results, errorToolResult(call, err))
					continue
				}
				awaited[call.Id] = childName
			case toolCreateMemory:
				results = append(results, s.executeCreateMemory(ctx, taskAgentName(task.Name), task.Chat, task.Name, call))
			default:
				results = append(results, errorToolResult(call, fmt.Errorf("unknown tool %q", call.Name)))
			}
		}
		if len(results) > 0 {
			if _, err := s.appendMessage(ctx, task.Chat, toolResultMessage(results)); err != nil {
				return fmt.Errorf("appending tool results: %w", err)
			}
		}
		if len(awaited) > 0 {
			// Park: the awaited children's terminal reports resume us.
			if task.Metadata.AwaitedTasks == nil {
				task.Metadata.AwaitedTasks = map[string]string{}
			}
			for id, name := range awaited {
				task.Metadata.AwaitedTasks[id] = name
			}
			task.State = agentpb.TaskState_TASK_STATE_AWAITING_TASKS
			return s.saveTask(ctx, task, "state", "metadata")
		}
		// Heartbeat + bookkeeping between turns.
		if err := s.saveTask(ctx, task, "metadata"); err != nil {
			return err
		}
	}
	// Turn budget exhausted on this claim: re-queue for fairness.
	task.State = agentpb.TaskState_TASK_STATE_PENDING
	return s.saveTask(ctx, task, "state", "metadata")
}

// spawnSubtask creates a child task from a create_task tool call.
func (s *Service) spawnSubtask(ctx context.Context, task *agentpb.Task, call *aipb.ToolCall) (string, error) {
	if int(task.Metadata.Depth) >= s.opts.MaxTaskDepth {
		return "", fmt.Errorf("max task depth (%d) reached", s.opts.MaxTaskDepth)
	}
	if task.Metadata.SubtaskCount >= task.Metadata.MaxSubtasks {
		return "", fmt.Errorf("max subtasks (%d) reached", task.Metadata.MaxSubtasks)
	}
	goal, err := stringArgument(call, "goal")
	if err != nil || goal == "" {
		return "", fmt.Errorf("create_task requires a goal")
	}
	title, _ := stringArgument(call, "title")
	child := &agentpb.Task{
		Name:       task.Name[:strings.LastIndex(task.Name, "/tasks/")] + "/tasks/" + aip.NewSystemGeneratedBase32ResourceID(),
		Title:      title,
		Goal:       goal,
		Model:      task.Model,
		ParentTask: task.Name,
		RootTask:   task.RootTask,
		State:      agentpb.TaskState_TASK_STATE_PENDING,
		Metadata: &agentpb.TaskMetadata{
			ToolSets:    task.Metadata.GetToolSets(),
			MaxSubtasks: task.Metadata.GetMaxSubtasks(),
			Depth:       task.Metadata.GetDepth() + 1,
		},
	}
	if err := s.insertTask(ctx, child); err != nil {
		return "", err
	}
	task.Metadata.SubtaskCount++
	return child.Name, nil
}

// insertTask persists a new task row.
func (s *Service) insertTask(ctx context.Context, task *agentpb.Task) error {
	now := timestamppb.Now()
	task.CreateTime, task.UpdateTime = now, now
	var err error
	if task.Etag, err = aip.ComputeETag(task); err != nil {
		return fmt.Errorf("computing etag: %w", err)
	}
	taskModel, err := model.TaskFromPb(task)
	if err != nil {
		return fmt.Errorf("converting task to model: %w", err)
	}
	if _, err := s.agentPostgresStore.InsertTaskIdempotently(ctx, uuid.MustNewV7().String(), taskModel); err != nil {
		return fmt.Errorf("inserting task: %w", err)
	}
	return nil
}

// executeCreateMemory persists a memory from a create_memory tool call.
func (s *Service) executeCreateMemory(ctx context.Context, agentName, sourceChat, sourceTask string, call *aipb.ToolCall) *aipb.ToolResult {
	memory, err := memoryFromCall(agentName, sourceChat, sourceTask, call)
	if err != nil {
		return errorToolResult(call, err)
	}
	memoryModel, err := model.MemoryFromPb(memory)
	if err != nil {
		return errorToolResult(call, err)
	}
	if _, err := s.agentPostgresStore.InsertMemoryIdempotently(ctx, uuid.MustNewV7().String(), memoryModel); err != nil {
		return errorToolResult(call, err)
	}
	return &aipb.ToolResult{
		ToolName:   call.Name,
		ToolCallId: call.Id,
		Result:     &aipb.ToolResult_Content{Content: fmt.Sprintf("memory created: %s", memory.Name)},
	}
}

// errorToolResult renders an error as a tool result so the model can repair.
func errorToolResult(call *aipb.ToolCall, err error) *aipb.ToolResult {
	return &aipb.ToolResult{
		ToolName:   call.Name,
		ToolCallId: call.Id,
		Result:     &aipb.ToolResult_Content{Content: fmt.Sprintf("ERROR: %v", err)},
	}
}

// taskAgentName extracts the owning agent name from a task name.
func taskAgentName(taskName string) string {
	return taskName[:strings.LastIndex(taskName, "/tasks/")]
}

// taskSystemPrompt renders the system prompt of a task chat.
func taskSystemPrompt(agent *agentpb.Agent) string {
	return fmt.Sprintf(`%s

You are executing a single self-contained task. Work autonomously: you cannot ask questions.
When the work is done, reply with plain text and no tool calls: that final message is your report to whoever created this task. Make it complete and self-contained.`, agent.Instructions)
}
