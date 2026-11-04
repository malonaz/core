package agent_service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/agent/model"
	"github.com/malonaz/core/gengo/ai/agent/store"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

// txn is one Postgres transaction over agents. The events and commands it records reach the broker only once
// it commits. Locks are always taken agents first (a child before its parent), executors last.
type txn struct {
	*store.Tx
	service *Service
	now     time.Time
	// The events recorded, per agent, and the commands to send, in order.
	agentEvents []*agentEvents
	commands    []*sessionCommand
}

type agentEvents struct {
	rootAgent string
	events    []*agentpb.Event
}

type sessionCommand struct {
	session  string
	response *agentservicepb.ConnectExecutorResponse
}

// transact runs fn in a transaction, retried from scratch on deadlocks, then publishes what it recorded.
func (s *Service) transact(ctx context.Context, fn func(ctx context.Context, t *txn) error) error {
	var t *txn
	if err := s.agentPostgresStore.Transaction(ctx, func(tx *store.Tx) error {
		t = &txn{Tx: tx, service: s, now: time.Now().UTC()}
		return fn(ctx, t)
	}); err != nil {
		return err
	}
	for _, agentEvents := range t.agentEvents {
		s.broker.publishEvents(ctx, agentEvents.rootAgent, agentEvents.events)
	}
	for _, command := range t.commands {
		s.broker.sendToSession(ctx, command.session, command.response)
	}
	return nil
}

// getAgent reads an agent, tombstones included, and locks it until the transaction ends.
func (t *txn) getAgent(ctx context.Context, name string) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	agentModel, err := t.GetAgentForUpdate(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		if errors.Is(err, model.ErrAgentNotExist) {
			return nil, status.Errorf(codes.NotFound, "agent %s does not exist", name).Err()
		}
		return nil, status.FromError(err, "getting agent").Err()
	}
	agent, err := agentModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
	}
	return agent, nil
}

// saveAgent persists an agent locked by getAgent and appends events to its log, numbered densely after its last.
func (t *txn) saveAgent(ctx context.Context, agent *agentpb.Agent, events ...*agentpb.Event) error {
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	for _, event := range events {
		agent.LastEventSequence++
		event.Name = agentRn.EventRn(strconv.FormatInt(agent.LastEventSequence, 10)).String()
		event.CreateTime = timestamppb.New(t.now)
		event.Sequence = agent.LastEventSequence
	}
	agent.UpdateTime = timestamppb.New(t.now)
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return status.Errorf(codes.Internal, "computing etag: %v", err).Err()
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return status.Errorf(codes.Internal, "converting agent from pb to model: %v", err).Err()
	}
	if err := t.SaveAgent(ctx, agentModel); err != nil {
		return status.FromError(err, "saving agent").Err()
	}
	if len(events) == 0 {
		return nil
	}
	if err := t.InsertEvents(ctx, events); err != nil {
		return status.FromError(err, "inserting events").Err()
	}
	t.agentEvents = append(t.agentEvents, &agentEvents{rootAgent: agent.RootAgent, events: events})
	return nil
}

// queueInput queues a message on an agent locked by getAgent and forwards it to the agent's executor, if any.
// A replayed request id returns the input it queued, recording nothing.
func (t *txn) queueInput(ctx context.Context, agent *agentpb.Agent, requestID string, message *aipb.Message) (*agentpb.Input, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	input := &agentpb.Input{
		Name:       agentRn.InputRn(aip.NewSystemGeneratedBase32ResourceID()).String(),
		CreateTime: timestamppb.New(t.now),
		Message:    message,
	}
	inputModel, err := model.InputFromPb(input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting input from pb to model: %v", err).Err()
	}
	inputModel, inserted, err := t.InsertInput(ctx, requestID, inputModel)
	if err != nil {
		if errors.Is(err, model.ErrInputAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "input already exists").Err()
		}
		return nil, status.FromError(err, "inserting input").Err()
	}
	if input, err = inputModel.ToPb(); err != nil {
		return nil, status.Errorf(codes.Internal, "converting input from model to pb: %v", err).Err()
	}
	if !inserted {
		return input, nil
	}

	inputQueued := &agentpb.Event{Kind: &agentpb.Event_InputQueued_{InputQueued: &agentpb.Event_InputQueued{
		Input:   input.Name,
		Message: input.Message,
	}}}
	if err := t.saveAgent(ctx, agent, inputQueued); err != nil {
		return nil, err
	}
	if agent.LeasedExecutor != "" {
		response := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Input{Input: input}}
		if err := t.send(ctx, agent.LeasedExecutor, response); err != nil {
			return nil, err
		}
	}
	return input, nil
}

// send queues a command for the executor's current session, sent once the transaction commits. A disconnected
// executor gets nothing: it is reconciled when it reconnects.
func (t *txn) send(ctx context.Context, executor string, response *agentservicepb.ConnectExecutorResponse) error {
	executorRn, err := agentpb.ParseExecutorRn(executor)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing executor name: %v", err).Err()
	}
	session, err := t.GetExecutorSession(ctx, executorRn.Organization, executorRn.Executor)
	if err != nil {
		return status.FromError(err, "getting executor session").Err()
	}
	if session != "" {
		t.commands = append(t.commands, &sessionCommand{session: session, response: response})
	}
	return nil
}
