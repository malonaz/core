package agent_service

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/agent/model"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// sessionTaskBufferSize bounds the work the receive loop may queue for the sender before it waits.
const sessionTaskBufferSize = 64

// ConnectExecutor serves one executor session: it registers the executor, re-adopts the agents it still holds,
// then relays its reports up and the service's commands down until the stream ends.
func (s *Service) ConnectExecutor(stream agentservicepb.AgentService_ConnectExecutorServer) error {
	ctx := stream.Context()
	request, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	register := request.GetRegister()
	if register == nil {
		return status.Errorf(codes.FailedPrecondition, "the first message must be register").Err()
	}
	if err := protovalidate.Validate(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	session, err := s.registerExecutor(ctx, stream, register)
	if err != nil {
		return err
	}
	return session.end(ctx, session.run(ctx))
}

// executorSession is one ConnectExecutor stream. Its sender goroutine alone calls stream.Send, and tracks what it
// sent so that commands from the broker and reconciles against Postgres are each sent once.
type executorSession struct {
	service      *Service
	stream       agentservicepb.AgentService_ConnectExecutorServer
	executorRn   *agentpb.ExecutorRn
	executor     string
	id           string
	subscription *subscription[*agentservicepb.ConnectExecutorResponse]
	// Work run by the sender, in order.
	tasks              chan func(context.Context) error
	reconcileRequested atomic.Bool
	// The agents whose lease was sent on this session. Written by the sender only.
	mutex            sync.Mutex
	agentToSentLease map[string]*sentLease
}

type sentLease struct {
	rootAgent string
	// The agent's inputs sent, pruned to the pending ones on each reconcile.
	inputSet map[string]struct{}
}

// registerExecutor upserts the executor bound to a new session, tells it, and re-adopts the agents it still holds.
func (s *Service) registerExecutor(
	ctx context.Context, stream agentservicepb.AgentService_ConnectExecutorServer, register *agentservicepb.ConnectExecutorRequest_Register,
) (*executorSession, error) {
	userRn, err := agentpb.ParseUserRn(register.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	executorRn := userRn.ExecutorRn(register.ExecutorId)
	session := &executorSession{
		service:          s,
		stream:           stream,
		executorRn:       executorRn,
		executor:         executorRn.String(),
		id:               uuid.MustNewV7().String(),
		tasks:            make(chan func(context.Context) error, sessionTaskBufferSize),
		agentToSentLease: map[string]*sentLease{},
	}
	// Subscribed before the session is stored, so no command sent to it is missed.
	session.subscription = s.broker.subscribeSession(session.id)

	now := timestamppb.New(time.Now().UTC())
	executor := proto.CloneOf(register.Executor)
	executor.Name = session.executor
	executor.State = agentpb.Executor_STATE_ONLINE
	executor.CreateTime = now
	executor.UpdateTime = now
	executor.ConnectTime = now
	executor.HeartbeatTime = now
	executorModel, err := model.ExecutorFromPb(executor)
	if err != nil {
		session.subscription.Close()
		return nil, status.Errorf(codes.Internal, "converting executor from pb to model: %v", err).Err()
	}
	if executorModel, err = s.agentPostgresStore.RegisterExecutor(ctx, executorModel, session.id); err != nil {
		session.subscription.Close()
		return nil, status.FromError(err, "registering executor").Err()
	}
	if executor, err = executorModel.ToPb(); err != nil {
		session.subscription.Close()
		return nil, status.Errorf(codes.Internal, "converting executor from model to pb: %v", err).Err()
	}
	registered := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Registered{Registered: executor}}
	if err := stream.Send(registered); err != nil {
		return nil, session.end(ctx, err)
	}
	if err := session.readopt(ctx); err != nil {
		return nil, session.end(ctx, err)
	}
	return session, nil
}

// readopt keeps the agents still leased to the executor. A turn running when the previous session broke is lost:
// it is closed and the agent goes back to idle, keeping its lease.
func (s *executorSession) readopt(ctx context.Context) error {
	agentModels, err := s.service.agentPostgresStore.ListLeasedAgents(ctx, s.executor)
	if err != nil {
		return status.FromError(err, "listing leased agents").Err()
	}
	for _, agentModel := range agentModels {
		agentRn := &agentpb.AgentRn{Organization: agentModel.OrganizationID, User: agentModel.UserID, Agent: agentModel.AgentID}
		if err := s.service.transact(ctx, func(ctx context.Context, t *txn) error {
			agent, err := t.getAgent(ctx, agentRn.String())
			if err != nil {
				return err
			}
			if agent.DeleteTime != nil || agent.LeasedExecutor != s.executor || agent.State != agentpb.Agent_STATE_RUNNING {
				return nil
			}
			return t.endTurn(ctx, agent, agentpb.Agent_TURN_OUTCOME_FAILED, executorLostStatus)
		}); err != nil {
			return err
		}
	}
	return nil
}

// run relays the session until its stream ends, returning why: io.EOF on a clean close.
func (s *executorSession) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The first reconcile sends the leases and pending inputs the executor holds.
	s.requestReconcile(ctx)
	sendDone := make(chan error, 1)
	go func() { sendDone <- s.sendLoop(ctx) }()
	receiveDone := make(chan error, 1)
	go func() { receiveDone <- s.receiveLoop(ctx) }()
	s.service.placePendingAgents(ctx, s.executorRn.Organization, s.executorRn.User)

	select {
	case err := <-receiveDone:
		cancel()
		<-sendDone
		return err
	case err := <-sendDone:
		// The receive loop ends with the stream.
		return err
	}
}

// end settles the executor once its stream ended: a clean close takes it offline and releases its agents, a broken
// stream only detaches it, keeping its leases until its heartbeat deadline for it to reconnect.
func (s *executorSession) end(ctx context.Context, err error) error {
	s.subscription.Close()
	ctx = context.WithoutCancel(ctx)
	now := time.Now().UTC()
	switch {
	case isSuperseded(err):
		return err

	case errors.Is(err, io.EOF):
		disconnected, err := s.service.agentPostgresStore.DisconnectExecutor(ctx, s.executorRn.Organization, s.executorRn.User, s.executorRn.Executor, s.id, now)
		if err != nil {
			return status.FromError(err, "disconnecting executor").Err()
		}
		if !disconnected {
			return nil
		}
		agentModels, err := s.service.agentPostgresStore.ListLeasedAgents(ctx, s.executor)
		if err != nil {
			return status.FromError(err, "listing leased agents").Err()
		}
		for _, agentModel := range agentModels {
			agentRn := &agentpb.AgentRn{Organization: agentModel.OrganizationID, User: agentModel.UserID, Agent: agentModel.AgentID}
			if err := s.service.releaseAgentFromExecutor(ctx, agentRn.String(), s.executor); err != nil {
				s.service.log.ErrorContext(ctx, "releasing agent", "agent", agentRn.String(), "executor", s.executor, "error", err)
			}
		}
		s.service.placePendingAgents(ctx, s.executorRn.Organization, s.executorRn.User)
		return nil

	default:
		if _, detachErr := s.service.agentPostgresStore.DetachExecutor(ctx, s.executorRn.Organization, s.executorRn.User, s.executorRn.Executor, s.id, now); detachErr != nil {
			s.service.log.ErrorContext(ctx, "detaching executor", "executor", s.executor, "error", detachErr)
		}
		return err
	}
}

// receiveLoop handles the executor's messages in order until the stream ends.
func (s *executorSession) receiveLoop(ctx context.Context) error {
	for {
		request, err := s.stream.Recv()
		if err != nil {
			return err
		}
		if err := protovalidate.Validate(request); err != nil {
			return status.Errorf(codes.InvalidArgument, "%v", err).Err()
		}
		switch content := request.Content.(type) {
		case *agentservicepb.ConnectExecutorRequest_Register_:
			return status.Errorf(codes.FailedPrecondition, "the executor is already registered").Err()
		case *agentservicepb.ConnectExecutorRequest_Heartbeat_:
			err = s.handleHeartbeat(ctx)
		case *agentservicepb.ConnectExecutorRequest_Report_:
			err = s.handleReport(ctx, content.Report)
		case *agentservicepb.ConnectExecutorRequest_Delta:
			s.handleDelta(ctx, content.Delta)
		case *agentservicepb.ConnectExecutorRequest_Release_:
			err = s.handleRelease(ctx, content.Release)
		}
		if err != nil {
			return err
		}
	}
}

func (s *executorSession) handleHeartbeat(ctx context.Context) error {
	heartbeated, err := s.service.agentPostgresStore.HeartbeatExecutor(
		ctx, s.executorRn.Organization, s.executorRn.User, s.executorRn.Executor, s.id, time.Now().UTC(),
	)
	if err != nil {
		return status.FromError(err, "heartbeating executor").Err()
	}
	if !heartbeated {
		return newSupersededError("superseded or expired")
	}
	s.requestReconcile(ctx)
	return nil
}

// handleDelta fans out a delta of an agent leased on this session; any other is dropped.
func (s *executorSession) handleDelta(ctx context.Context, delta *agentpb.Delta) {
	s.mutex.Lock()
	sentLease, ok := s.agentToSentLease[delta.Agent]
	s.mutex.Unlock()
	if ok {
		s.service.broker.publishDelta(ctx, sentLease.rootAgent, delta)
	}
}

// requestReconcile has the sender reconcile with Postgres, unless a reconcile is already queued.
func (s *executorSession) requestReconcile(ctx context.Context) {
	if s.reconcileRequested.Swap(true) {
		return
	}
	s.schedule(ctx, func(ctx context.Context) error {
		s.reconcileRequested.Store(false)
		return s.reconcile(ctx)
	})
}

// forget has the sender drop an agent the executor gave up, without revoking it.
func (s *executorSession) forget(ctx context.Context, agent string) {
	s.schedule(ctx, func(ctx context.Context) error {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		delete(s.agentToSentLease, agent)
		return nil
	})
}

func (s *executorSession) schedule(ctx context.Context, task func(context.Context) error) {
	select {
	case s.tasks <- task:
	case <-ctx.Done():
	}
}

// sendLoop owns stream.Send: it forwards the broker's commands and runs the session's tasks, one at a time.
func (s *executorSession) sendLoop(ctx context.Context) error {
	for {
		var err error
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response := <-s.subscription.C:
			err = s.forward(ctx, response)
		case task := <-s.tasks:
			err = task(ctx)
		}
		if err != nil {
			return err
		}
	}
}

// forward sends a command from the broker, deduplicated against what the session already sent.
func (s *executorSession) forward(ctx context.Context, response *agentservicepb.ConnectExecutorResponse) error {
	switch content := response.Content.(type) {
	case *agentservicepb.ConnectExecutorResponse_Lease:
		return s.sendLease(content.Lease)
	case *agentservicepb.ConnectExecutorResponse_Input:
		inputRn, err := agentpb.ParseInputRn(content.Input.Name)
		if err != nil {
			return status.Errorf(codes.Internal, "parsing input name: %v", err).Err()
		}
		// The input overtook its agent's lease: the lease is read back from Postgres, inputs included.
		if !s.hasSentLease(inputRn.AgentRn().String()) {
			return s.reconcile(ctx)
		}
		return s.sendInput(inputRn.AgentRn().String(), content.Input)
	case *agentservicepb.ConnectExecutorResponse_Revoke_:
		return s.sendRevoke(content.Revoke.Agent)
	case *agentservicepb.ConnectExecutorResponse_Cancel_:
		// An agent not leased on this session cannot be running on it.
		if !s.hasSentLease(content.Cancel.Agent) {
			return nil
		}
		return s.stream.Send(response)
	default:
		return s.stream.Send(response)
	}
}

// reconcile brings the executor in line with Postgres: the leases it misses, the pending inputs not sent, and
// revokes for agents no longer leased to it. Run by the sender, so every command forwarded before was committed
// before these reads, and every one forwarded after supersedes them.
func (s *executorSession) reconcile(ctx context.Context) error {
	agentModels, err := s.service.agentPostgresStore.ListLeasedAgents(ctx, s.executor)
	if err != nil {
		return status.FromError(err, "listing leased agents").Err()
	}
	inputModels, err := s.service.agentPostgresStore.ListPendingInputsOfExecutor(ctx, s.executor)
	if err != nil {
		return status.FromError(err, "listing pending inputs").Err()
	}

	leasedAgentSet := make(map[string]struct{}, len(agentModels))
	for _, agentModel := range agentModels {
		agent, err := agentModel.ToPb()
		if err != nil {
			return status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
		}
		leasedAgentSet[agent.Name] = struct{}{}
		if err := s.sendLease(agent); err != nil {
			return err
		}
	}
	for _, agent := range s.sentLeaseAgents() {
		if _, ok := leasedAgentSet[agent]; !ok {
			if err := s.sendRevoke(agent); err != nil {
				return err
			}
		}
	}

	agentToPendingInputSet := map[string]map[string]struct{}{}
	for _, inputModel := range inputModels {
		input, err := inputModel.ToPb()
		if err != nil {
			return status.Errorf(codes.Internal, "converting input from model to pb: %v", err).Err()
		}
		agent := (&agentpb.AgentRn{Organization: inputModel.OrganizationID, User: inputModel.UserID, Agent: inputModel.AgentID}).String()
		// Leased after the agents were read: its lease, then its inputs, are on their way.
		if !s.hasSentLease(agent) {
			continue
		}
		if err := s.sendInput(agent, input); err != nil {
			return err
		}
		if agentToPendingInputSet[agent] == nil {
			agentToPendingInputSet[agent] = map[string]struct{}{}
		}
		agentToPendingInputSet[agent][input.Name] = struct{}{}
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for agent, sentLease := range s.agentToSentLease {
		sentLease.inputSet = agentToPendingInputSet[agent]
		if sentLease.inputSet == nil {
			sentLease.inputSet = map[string]struct{}{}
		}
	}
	return nil
}

func (s *executorSession) sendLease(agent *agentpb.Agent) error {
	if s.hasSentLease(agent.Name) {
		return nil
	}
	lease := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Lease{Lease: agent}}
	if err := s.stream.Send(lease); err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.agentToSentLease[agent.Name] = &sentLease{rootAgent: agent.RootAgent, inputSet: map[string]struct{}{}}
	return nil
}

func (s *executorSession) sendInput(agent string, input *agentpb.Input) error {
	s.mutex.Lock()
	sentLease := s.agentToSentLease[agent]
	_, sent := sentLease.inputSet[input.Name]
	s.mutex.Unlock()
	if sent {
		return nil
	}
	response := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Input{Input: input}}
	if err := s.stream.Send(response); err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	sentLease.inputSet[input.Name] = struct{}{}
	return nil
}

func (s *executorSession) sendRevoke(agent string) error {
	if !s.hasSentLease(agent) {
		return nil
	}
	revoke := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Revoke_{
		Revoke: &agentservicepb.ConnectExecutorResponse_Revoke{Agent: agent},
	}}
	if err := s.stream.Send(revoke); err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.agentToSentLease, agent)
	return nil
}

func (s *executorSession) hasSentLease(agent string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, ok := s.agentToSentLease[agent]
	return ok
}

func (s *executorSession) sentLeaseAgents() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	agents := make([]string, 0, len(s.agentToSentLease))
	for agent := range s.agentToSentLease {
		agents = append(agents, agent)
	}
	return agents
}

// newSupersededError ends a session another one replaced, or that missed its heartbeat deadline.
func newSupersededError(message string) error {
	return status.Errorf(codes.Aborted, "%s", message).Err()
}

func isSuperseded(err error) bool {
	return status.HasCode(err, codes.Aborted)
}
