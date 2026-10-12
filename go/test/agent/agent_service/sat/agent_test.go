package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"

	agentservicepb "github.com/malonaz/core/genproto/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider/mock"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

func createAgent(t *testing.T) *agentpb.Agent {
	t.Helper()
	agent, err := agentServiceClient.CreateAgent(ctx, &agentservicepb.CreateAgentRequest{
		Parent: newAgentParent(),
		Agent: &agentpb.Agent{
			Title:        "test agent",
			Instructions: "You are a helpful test agent.",
			Model:        mockModel,
		},
		RequestId: uuid.MustNewV7().String(),
	})
	require.NoError(t, err)
	require.Equal(t, agentpb.AgentState_AGENT_STATE_IDLE, agent.State)
	return agent
}

func assistant(blocks ...*aipb.Block) *aipb.Message {
	return &aipb.Message{Role: aipb.Role_ROLE_ASSISTANT, Blocks: blocks}
}

func textBlock(text string) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_Text{Text: text}}
}

func toolCallBlock(id, name string, arguments map[string]any) *aipb.Block {
	value, err := structpb.NewStruct(arguments)
	if err != nil {
		panic(err)
	}
	return &aipb.Block{Content: &aipb.Block_ToolCall{ToolCall: &aipb.ToolCall{Id: id, Name: name, Arguments: value}}}
}

// scriptedUserMessage builds a USER message carrying the mock provider script.
func scriptedUserMessage(t *testing.T, text string, turns ...*aipb.Message) *aipb.Message {
	t.Helper()
	script, err := mock.Script(turns...)
	require.NoError(t, err)
	message := &aipb.Message{
		Role:        aipb.Role_ROLE_USER,
		Blocks:      []*aipb.Block{textBlock(text)},
		Annotations: map[string]string{mock.ScriptAnnotationKey: script},
	}
	return message
}

func TestAgentCRUD(t *testing.T) {
	t.Parallel()
	agent := createAgent(t)

	fetched, err := agentServiceClient.GetAgent(ctx, &agentservicepb.GetAgentRequest{Name: agent.Name})
	require.NoError(t, err)
	require.Equal(t, agent.Name, fetched.Name)

	list, err := agentServiceClient.ListAgents(ctx, &agentservicepb.ListAgentsRequest{Parent: newAgentParent()})
	require.NoError(t, err)
	require.Empty(t, list.Agents)

	deleted, err := agentServiceClient.DeleteAgent(ctx, &agentservicepb.DeleteAgentRequest{Name: agent.Name})
	require.NoError(t, err)
	require.NotNil(t, deleted.DeleteTime)
}

func TestMemoryCRUD(t *testing.T) {
	t.Parallel()
	agent := createAgent(t)

	memory, err := agentServiceClient.CreateMemory(ctx, &agentservicepb.CreateMemoryRequest{
		Parent: agent.Name,
		Memory: &agentpb.Memory{
			Title:       "user prefers Go",
			Description: "language preference",
			Content:     structpb.NewStringValue("The user writes everything in Go."),
		},
		RequestId: uuid.MustNewV7().String(),
	})
	require.NoError(t, err)

	memory.Title = "user strongly prefers Go"
	updated, err := agentServiceClient.UpdateMemory(ctx, &agentservicepb.UpdateMemoryRequest{
		Memory:     memory,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
	})
	require.NoError(t, err)
	require.Equal(t, "user strongly prefers Go", updated.Title)

	list, err := agentServiceClient.ListMemories(ctx, &agentservicepb.ListMemoriesRequest{Parent: agent.Name})
	require.NoError(t, err)
	require.Len(t, list.Memories, 1)

	_, err = agentServiceClient.DeleteMemory(ctx, &agentservicepb.DeleteMemoryRequest{Name: memory.Name})
	require.NoError(t, err)
}

// TestAgentConversation drives the full loop: a scripted agent persists a
// memory, delegates a task (which fails: tasks see no script), is woken by the
// task's terminal report, and replies.
func TestAgentConversation(t *testing.T) {
	t.Parallel()
	agent := createAgent(t)

	message := scriptedUserMessage(t, "please remember my preference, then delegate some work",
		// Turn 1: persist a memory.
		assistant(toolCallBlock("call-1", "create_memory", map[string]any{
			"title":   "prefers tabs",
			"content": "The user prefers tabs over spaces.",
		})),
		// Turn 2: delegate a task; the turn defers until it terminates.
		assistant(toolCallBlock("call-2", "create_task", map[string]any{
			"title": "star census",
			"goal":  "count the stars in the sky",
		})),
		// Turn 3: resumed with the task's report; reply and idle.
		assistant(textBlock("all done")),
	)
	_, err := agentServiceClient.SendAgentMessage(ctx, &agentservicepb.SendAgentMessageRequest{
		Name:      agent.Name,
		Message:   message,
		RequestId: uuid.MustNewV7().String(),
	})
	require.NoError(t, err)

	// The loop settles: agent IDLE with three turns and nothing awaited.
	waitFor(t, 30_000_000_000, func() bool {
		fetched, err := agentServiceClient.GetAgent(ctx, &agentservicepb.GetAgentRequest{Name: agent.Name})
		require.NoError(t, err)
		return fetched.State == agentpb.AgentState_AGENT_STATE_IDLE &&
			fetched.GetMetadata().GetTurnCount() == 3 &&
			len(fetched.GetMetadata().GetAwaitedTasks()) == 0
	})

	// The memory was persisted.
	memories, err := agentServiceClient.ListMemories(ctx, &agentservicepb.ListMemoriesRequest{Parent: agent.Name})
	require.NoError(t, err)
	require.Len(t, memories.Memories, 1)
	require.Equal(t, "prefers tabs", memories.Memories[0].Title)

	// The delegated task ran and failed (tasks carry no script), which is
	// exactly the terminal report that resumed the agent.
	tasks, err := agentServiceClient.ListTasks(ctx, &agentservicepb.ListTasksRequest{Parent: agent.Name})
	require.NoError(t, err)
	require.Len(t, tasks.Tasks, 1)
	require.Equal(t, agentpb.TaskState_TASK_STATE_FAILED, tasks.Tasks[0].State)
	require.Equal(t, "count the stars in the sky", tasks.Tasks[0].Goal)
	require.Equal(t, "star census", tasks.Tasks[0].Title)
}

func TestTaskCancel(t *testing.T) {
	t.Parallel()
	agent := createAgent(t)

	task, err := agentServiceClient.CreateTask(ctx, &agentservicepb.CreateTaskRequest{
		Parent:    agent.Name,
		Task:      &agentpb.Task{Goal: "some goal"},
		RequestId: uuid.MustNewV7().String(),
	})
	require.NoError(t, err)
	require.Equal(t, agentpb.TaskState_TASK_STATE_PENDING, task.State)
	require.Equal(t, task.Name, task.RootTask)

	cancelled, err := agentServiceClient.CancelTask(ctx, &agentservicepb.CancelTaskRequest{Name: task.Name})
	if err != nil {
		// The runner may have raced us to a terminal state; that is the only
		// acceptable failure.
		grpcrequire.Error(t, codes.FailedPrecondition, err)
		return
	}
	require.Equal(t, agentpb.TaskState_TASK_STATE_CANCELLED, cancelled.State)

	_, err = agentServiceClient.CancelTask(ctx, &agentservicepb.CancelTaskRequest{Name: task.Name})
	grpcrequire.Error(t, codes.FailedPrecondition, err)
}
