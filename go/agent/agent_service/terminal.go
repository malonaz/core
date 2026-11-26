package agent_service

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/go/aip"
)

// finishTask transitions a task to its terminal state and notifies whoever
// awaits it: the parent task, the owning agent, or nobody (the creator polls
// the resource).
func (s *Service) finishTask(ctx context.Context, task *agentpb.Task, report, errorMessage string) {
	columns := []string{"state", "metadata"}
	if errorMessage != "" {
		task.State = agentpb.TaskState_TASK_STATE_FAILED
		task.ErrorMessage = errorMessage
		columns = append(columns, "error_message")
	} else {
		task.State = agentpb.TaskState_TASK_STATE_SUCCEEDED
		task.Report = report
		columns = append(columns, "report")
	}
	if err := s.saveTask(ctx, task, columns...); err != nil {
		s.log.Error("saving terminal task", "task", task.Name, "error", err)
		return
	}
	if err := s.notifyTaskTerminal(ctx, task); err != nil {
		s.log.Error("notifying task terminal", "task", task.Name, "error", err)
	}
}

// notifyTaskTerminal writes the task's report back as the awaiting create_task
// tool call's result and resumes the awaiter once nothing else is awaited.
func (s *Service) notifyTaskTerminal(ctx context.Context, task *agentpb.Task) error {
	resultText := task.Report
	if task.State != agentpb.TaskState_TASK_STATE_SUCCEEDED {
		resultText = fmt.Sprintf("TASK %s: %s", task.State, task.ErrorMessage)
	}

	if task.ParentTask != "" {
		return s.resolveAwaitedInTask(ctx, task.ParentTask, task.Name, resultText)
	}
	// No parent task: the owning agent may be awaiting it.
	return s.resolveAwaitedInAgent(ctx, taskAgentName(task.Name), task.Name, resultText)
}

// resolveAwaitedInTask resolves one awaited child on a parent task. The last
// resolution re-queues the parent; the row lock serializes concurrent children.
func (s *Service) resolveAwaitedInTask(ctx context.Context, parentName, childName, resultText string) error {
	// Read outside the lock to find the tool call id and append the result.
	parent, err := s.getTaskByName(ctx, parentName)
	if err != nil {
		return err
	}
	callID := awaitedCallID(parent.GetMetadata().GetAwaitedTasks(), childName)
	if callID == "" {
		return nil // Not awaited (e.g. cancelled tree); nothing to resolve.
	}
	result := &aipb.ToolResult{
		ToolName:   toolCreateTask,
		ToolCallId: callID,
		Result:     &aipb.ToolResult_Content{Content: resultText},
	}
	if _, err := s.appendMessage(ctx, parent.Chat, toolResultMessage([]*aipb.ToolResult{result})); err != nil {
		return fmt.Errorf("appending tool result: %w", err)
	}
	return s.withLockedTask(ctx, parentName, func(parent *agentpb.Task) ([]string, error) {
		if parent.Metadata == nil || parent.Metadata.AwaitedTasks == nil {
			return nil, nil
		}
		delete(parent.Metadata.AwaitedTasks, callID)
		columns := []string{"metadata"}
		if len(parent.Metadata.AwaitedTasks) == 0 && parent.State == agentpb.TaskState_TASK_STATE_AWAITING_TASKS {
			parent.State = agentpb.TaskState_TASK_STATE_PENDING
			columns = append(columns, "state")
		}
		return columns, nil
	})
}

// resolveAwaitedInAgent resolves one awaited task on an agent; the last
// resolution wakes the agent to resume its deferred turn.
func (s *Service) resolveAwaitedInAgent(ctx context.Context, agentName, childName, resultText string) error {
	agent, err := s.getAgentByName(ctx, agentName)
	if err != nil {
		return err
	}
	callID := awaitedCallID(agent.GetMetadata().GetAwaitedTasks(), childName)
	if callID == "" {
		return nil // User-created task: the creator polls the resource.
	}
	result := &aipb.ToolResult{
		ToolName:   toolCreateTask,
		ToolCallId: callID,
		Result:     &aipb.ToolResult_Content{Content: resultText},
	}
	if _, err := s.appendMessage(ctx, agent.Chat, toolResultMessage([]*aipb.ToolResult{result})); err != nil {
		return fmt.Errorf("appending tool result: %w", err)
	}
	return s.withLockedAgent(ctx, agentName, func(agent *agentpb.Agent) ([]string, error) {
		if agent.Metadata == nil || agent.Metadata.AwaitedTasks == nil {
			return nil, nil
		}
		delete(agent.Metadata.AwaitedTasks, callID)
		columns := []string{"metadata"}
		if len(agent.Metadata.AwaitedTasks) == 0 && agent.State == agentpb.AgentState_AGENT_STATE_IDLE {
			agent.State = agentpb.AgentState_AGENT_STATE_ACTIVE
			columns = append(columns, "state")
		}
		return columns, nil
	})
}

// awaitedCallID reverse-looks-up the tool call id awaiting the given child.
func awaitedCallID(awaited map[string]string, childName string) string {
	for callID, name := range awaited {
		if name == childName {
			return callID
		}
	}
	return ""
}

// memoryFromCall builds a Memory resource from a create_memory tool call.
func memoryFromCall(agentName, sourceChat, sourceTask string, call *aipb.ToolCall) (*agentpb.Memory, error) {
	title, err := stringArgument(call, "title")
	if err != nil || title == "" {
		return nil, fmt.Errorf("create_memory requires a title")
	}
	content, err := stringArgument(call, "content")
	if err != nil || content == "" {
		return nil, fmt.Errorf("create_memory requires content")
	}
	description, _ := stringArgument(call, "description")
	now := timestamppb.Now()
	memory := &agentpb.Memory{
		Name:        agentName + "/memories/" + aip.NewSystemGeneratedBase32ResourceID(),
		CreateTime:  now,
		UpdateTime:  now,
		Title:       title,
		Description: description,
		Content:     structpb.NewStringValue(content),
		SourceChat:  sourceChat,
		SourceTask:  sourceTask,
	}
	if memory.Etag, err = aip.ComputeETag(memory); err != nil {
		return nil, err
	}
	return memory, nil
}
