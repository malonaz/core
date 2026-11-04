package agent_service

import (
	"context"
	"strings"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

var (
	inputNamespace = aip.MustGetUUIDNamespace(&agentpb.Input{})
	// executorLostStatus fails the turn of an agent whose executor went away mid-turn.
	executorLostStatus = &statuspb.Status{Code: int32(codes.Unavailable), Message: "executor lost"}
)

// leaseAgent grants an agent locked by getAgent to an executor locked by FindExecutor, then sends the executor the
// lease and the agent's pending inputs.
func (t *txn) leaseAgent(ctx context.Context, agent *agentpb.Agent, executor string) error {
	executorRn, err := agentpb.ParseExecutorRn(executor)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing executor name: %v", err).Err()
	}
	agent.LeasedExecutor = executor
	agent.State = agentpb.Agent_STATE_IDLE
	if err := t.saveAgent(ctx, agent, newLeaseChangedEvent(executor), newStateChangedEvent(agent.State)); err != nil {
		return err
	}
	if err := t.RefreshExecutorAgentCount(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, t.now); err != nil {
		return status.FromError(err, "refreshing executor agent count").Err()
	}

	lease := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Lease{Lease: proto.CloneOf(agent)}}
	if err := t.send(ctx, executor, lease); err != nil {
		return err
	}
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	inputModels, err := t.ListPendingInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		return status.FromError(err, "listing pending inputs").Err()
	}
	for _, inputModel := range inputModels {
		input, err := inputModel.ToPb()
		if err != nil {
			return status.Errorf(codes.Internal, "converting input from model to pb: %v", err).Err()
		}
		response := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Input{Input: input}}
		if err := t.send(ctx, executor, response); err != nil {
			return err
		}
	}
	return nil
}

// releaseAgent returns an agent locked by getAgent to pending, closing its running turn first, and saves it with any
// other change the caller made. The executor is sent nothing: callers revoke if it may still be running the agent.
func (t *txn) releaseAgent(ctx context.Context, agent *agentpb.Agent) error {
	if agent.State == agentpb.Agent_STATE_RUNNING {
		if err := t.endTurn(ctx, agent, agentpb.Agent_TURN_OUTCOME_FAILED, executorLostStatus); err != nil {
			return err
		}
	}
	leasedExecutor := agent.LeasedExecutor
	var events []*agentpb.Event
	if leasedExecutor != "" {
		agent.LeasedExecutor = ""
		events = append(events, newLeaseChangedEvent(""))
	}
	if agent.State != agentpb.Agent_STATE_PENDING {
		agent.State = agentpb.Agent_STATE_PENDING
		events = append(events, newStateChangedEvent(agent.State))
	}
	if err := t.saveAgent(ctx, agent, events...); err != nil {
		return err
	}
	if leasedExecutor == "" {
		return nil
	}
	executorRn, err := agentpb.ParseExecutorRn(leasedExecutor)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing executor name: %v", err).Err()
	}
	if err := t.RefreshExecutorAgentCount(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, t.now); err != nil {
		return status.FromError(err, "refreshing executor agent count").Err()
	}
	return nil
}

// endTurn closes the running turn of an agent locked by getAgent, and reports it to the agent's parent, if live, as
// an input carrying the turn's final answer.
func (t *txn) endTurn(ctx context.Context, agent *agentpb.Agent, outcome agentpb.Agent_TurnOutcome, turnStatus *statuspb.Status) error {
	// Read before saving: the turn's events end at its last message.
	var reportMessage *aipb.Message
	if agent.ParentAgent != "" {
		var err error
		if reportMessage, err = t.newTurnReportMessage(ctx, agent, outcome, turnStatus); err != nil {
			return err
		}
	}

	agent.LastTurnOutcome = outcome
	agent.Status = nil
	if outcome == agentpb.Agent_TURN_OUTCOME_FAILED {
		agent.Status = turnStatus
	}
	turnEnded := &agentpb.Event{Kind: &agentpb.Event_TurnEnded_{TurnEnded: &agentpb.Event_TurnEnded{
		Outcome: outcome,
		Status:  agent.Status,
	}}}
	events := []*agentpb.Event{turnEnded}
	if agent.State != agentpb.Agent_STATE_IDLE {
		agent.State = agentpb.Agent_STATE_IDLE
		events = append(events, newStateChangedEvent(agent.State))
	}
	if err := t.saveAgent(ctx, agent, events...); err != nil {
		return err
	}
	if reportMessage == nil {
		return nil
	}

	// Locked after the child, per the lock order.
	parentAgent, err := t.getAgent(ctx, agent.ParentAgent)
	if err != nil {
		return status.FromError(err, "getting parent agent").Err()
	}
	if parentAgent.DeleteTime != nil {
		return nil
	}
	// Derived from the turn, so a retried transaction queues the same input.
	requestID := uuid.NewV5(inputNamespace, turnEnded.Name).String()
	_, err = t.queueInput(ctx, parentAgent, requestID, reportMessage)
	return err
}

// newTurnReportMessage builds the input reporting an agent's turn to its parent: the text of the turn's last
// assistant message, or the outcome if it has none.
func (t *txn) newTurnReportMessage(
	ctx context.Context, agent *agentpb.Agent, outcome agentpb.Agent_TurnOutcome, turnStatus *statuspb.Status,
) (*aipb.Message, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	events, err := t.ListTurnEvents(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		return nil, status.FromError(err, "listing turn events").Err()
	}
	var blocks []*aipb.Block
	for i := len(events) - 1; i >= 0 && blocks == nil; i-- {
		message := events[i].GetMessageCreated().GetMessage()
		if message.GetRole() != aipb.Role_ROLE_ASSISTANT {
			continue
		}
		for _, block := range message.Blocks {
			if text, ok := block.Content.(*aipb.Block_Text); ok {
				blocks = append(blocks, ai.NewTextBlock(text.Text))
			}
		}
		// The last assistant message decides, even without text: an earlier one is not the turn's answer.
		break
	}
	if len(blocks) == 0 {
		blocks = []*aipb.Block{ai.NewTextBlock(describeTurnOutcome(outcome, turnStatus))}
	}
	message := ai.NewUserMessage(blocks...)
	agentpb.Annotations.Sender.Set(message, agent.Name)
	return message, nil
}

// describeTurnOutcome words how a turn ended, e.g. "Turn failed: executor lost".
func describeTurnOutcome(outcome agentpb.Agent_TurnOutcome, turnStatus *statuspb.Status) string {
	description := "Turn " + strings.ToLower(strings.TrimPrefix(outcome.String(), "TURN_OUTCOME_"))
	if outcome == agentpb.Agent_TURN_OUTCOME_FAILED && turnStatus.GetMessage() != "" {
		return description + ": " + turnStatus.GetMessage()
	}
	return description + "."
}

func newLeaseChangedEvent(executor string) *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_LeaseChanged_{LeaseChanged: &agentpb.Event_LeaseChanged{Executor: executor}}}
}
