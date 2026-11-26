package agent_service

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/agent/agent_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/gengo/agent/model"
	"github.com/malonaz/core/go/grpc/status"
)

// SendAgentMessage appends a user message to the agent's current chat
// (creating it on first contact) and wakes the agent.
func (s *Service) SendAgentMessage(ctx context.Context, request *pb.SendAgentMessageRequest) (*aipb.Message, error) {
	agent, err := s.getAgentByName(ctx, request.Name)
	if err != nil {
		if err == model.ErrAgentNotExist {
			return nil, status.Errorf(codes.NotFound, "agent does not exist").Err()
		}
		return nil, status.FromError(err, "loading agent").Err()
	}
	if agent.State == agentpb.AgentState_AGENT_STATE_SUSPENDED {
		return nil, status.Errorf(codes.FailedPrecondition, "agent is suspended").Err()
	}
	if request.Message.GetRole() != aipb.Role_ROLE_USER {
		return nil, status.Errorf(codes.InvalidArgument, "message must use the USER role").Err()
	}

	// First contact: create the backing chat seeded with the persona.
	if agent.Chat == "" {
		chatName, err := s.createChat(ctx, agent.Name, agent.Title)
		if err != nil {
			return nil, status.FromError(err, "creating chat").Err()
		}
		if _, err := s.appendMessage(ctx, chatName, textMessage(aipb.Role_ROLE_SYSTEM, agent.Instructions)); err != nil {
			return nil, status.FromError(err, "seeding chat").Err()
		}
		agent.Chat = chatName
		if err := s.saveAgent(ctx, agent, "chat"); err != nil {
			return nil, status.FromError(err, "saving agent chat").Err()
		}
	}

	// Append, then wake. The append precedes the state transition so a racing
	// idle transition always either sees the message or yields to our wake.
	message, err := s.appendMessage(ctx, agent.Chat, request.Message)
	if err != nil {
		return nil, status.FromError(err, "appending message").Err()
	}
	err = s.withLockedAgent(ctx, agent.Name, func(locked *agentpb.Agent) ([]string, error) {
		if locked.State != agentpb.AgentState_AGENT_STATE_IDLE {
			return nil, nil // Already queued, processing, or suspended.
		}
		// A deferred turn must not be resumed by a chat message; the awaited
		// tasks' results will wake it, and the message is then in context.
		if len(locked.GetMetadata().GetAwaitedTasks()) > 0 {
			return nil, nil
		}
		locked.State = agentpb.AgentState_AGENT_STATE_ACTIVE
		return []string{"state"}, nil
	})
	if err != nil {
		return nil, status.FromError(err, "waking agent").Err()
	}
	return message, nil
}

// CancelTask cancels a task and every non-terminal task below it in the tree.
func (s *Service) CancelTask(ctx context.Context, request *pb.CancelTaskRequest) (*agentpb.Task, error) {
	task, err := s.getTaskByName(ctx, request.Name)
	if err != nil {
		if err == model.ErrTaskNotExist {
			return nil, status.Errorf(codes.NotFound, "task does not exist").Err()
		}
		return nil, status.FromError(err, "loading task").Err()
	}
	if isTerminalTaskState(task.State) {
		return nil, status.Errorf(codes.FailedPrecondition, "task is already terminal (%s)", task.State).Err()
	}

	organizationID, userID, agentID, taskID, err := model.ParseTaskName(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task name: %v", err).Err()
	}
	// Cascade over the subtree via parent_task links, in one transaction.
	err = s.agentPostgresClient.ExecuteTransaction(ctx, pgx.ReadCommitted, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			WITH RECURSIVE subtree AS (
				SELECT organization_id, user_id, agent_id, task_id,
				       'organizations/' || organization_id || '/users/' || user_id || '/agents/' || agent_id || '/tasks/' || task_id AS name
				FROM agent.task
				WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND task_id = $4
				UNION ALL
				SELECT t.organization_id, t.user_id, t.agent_id, t.task_id,
				       'organizations/' || t.organization_id || '/users/' || t.user_id || '/agents/' || t.agent_id || '/tasks/' || t.task_id
				FROM agent.task t JOIN subtree s ON t.parent_task = s.name
			)
			UPDATE agent.task x SET state = %d, update_time = now(), etag = md5(random()::text)
			FROM subtree s
			WHERE x.organization_id = s.organization_id AND x.user_id = s.user_id
			  AND x.agent_id = s.agent_id AND x.task_id = s.task_id
			  AND x.state IN (%d, %d, %d, %d)`,
			int16(agentpb.TaskState_TASK_STATE_CANCELLED),
			int16(agentpb.TaskState_TASK_STATE_PENDING), int16(agentpb.TaskState_TASK_STATE_RUNNING),
			int16(agentpb.TaskState_TASK_STATE_AWAITING_APPROVAL), int16(agentpb.TaskState_TASK_STATE_AWAITING_TASKS)),
			organizationID, userID, agentID, taskID)
		return err
	})
	if err != nil {
		return nil, status.FromError(err, "cancelling task tree").Err()
	}

	task, err = s.getTaskByName(ctx, request.Name)
	if err != nil {
		return nil, status.FromError(err, "reloading task").Err()
	}
	// Notify an awaiting parent/agent so it is not parked forever.
	if err := s.notifyTaskTerminal(ctx, task); err != nil {
		s.log.Error("notifying cancelled task", "task", task.Name, "error", err)
	}
	return task, nil
}

// ResolveApproval resolves a pending tool-call approval. Server-side tool
// execution beyond the built-ins is not wired up yet, so no task ever parks in
// AWAITING_APPROVAL today.
func (s *Service) ResolveApproval(ctx context.Context, request *pb.ResolveApprovalRequest) (*agentpb.Task, error) {
	task, err := s.getTaskByName(ctx, request.Name)
	if err != nil {
		if err == model.ErrTaskNotExist {
			return nil, status.Errorf(codes.NotFound, "task does not exist").Err()
		}
		return nil, status.FromError(err, "loading task").Err()
	}
	if task.State != agentpb.TaskState_TASK_STATE_AWAITING_APPROVAL {
		return nil, status.Errorf(codes.FailedPrecondition, "task has no pending approvals").Err()
	}
	return nil, status.Errorf(codes.Unimplemented, "approval execution is not implemented yet").Err()
}

// isTerminalTaskState reports whether a task state is terminal.
func isTerminalTaskState(state agentpb.TaskState) bool {
	switch state {
	case agentpb.TaskState_TASK_STATE_SUCCEEDED, agentpb.TaskState_TASK_STATE_FAILED, agentpb.TaskState_TASK_STATE_CANCELLED:
		return true
	}
	return false
}

// CreateAgent wraps the generated method to initialize lifecycle fields.
func (s *Service) CreateAgent(ctx context.Context, request *pb.CreateAgentRequest) (*agentpb.Agent, error) {
	agent, err := s.AgentServiceServer.CreateAgent(ctx, request)
	if err != nil || request.ValidateOnly {
		return agent, err
	}
	agent.State = agentpb.AgentState_AGENT_STATE_IDLE
	if err := s.saveAgent(ctx, agent, "state"); err != nil {
		return nil, status.FromError(err, "initializing agent state").Err()
	}
	return agent, nil
}

// CreateTask wraps the generated method to initialize lifecycle fields:
// a created task is the root of its own tree and enters the queue PENDING.
func (s *Service) CreateTask(ctx context.Context, request *pb.CreateTaskRequest) (*agentpb.Task, error) {
	// The metadata column is NOT NULL: always present, initialized here.
	if request.GetTask() != nil && request.Task.Metadata == nil {
		request.Task.Metadata = &agentpb.TaskMetadata{}
	}
	task, err := s.AgentServiceServer.CreateTask(ctx, request)
	if err != nil || request.ValidateOnly {
		return task, err
	}
	task.State = agentpb.TaskState_TASK_STATE_PENDING
	task.RootTask = task.Name
	if err := s.saveTask(ctx, task, "state", "root_task"); err != nil {
		return nil, status.FromError(err, "initializing task state").Err()
	}
	return task, nil
}
