package agent_service

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/agent/model"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

var agentNamespace = aip.MustGetUUIDNamespace(&agentpb.Agent{})

// CreateAgent creates an idle agent, from a definition or ad-hoc, with the chat holding its transcript.
func (s *Service) CreateAgent(ctx context.Context, request *agentservicepb.CreateAgentRequest) (*agentpb.Agent, error) {
	return redacted(s.createAgent(ctx, request))
}

func (s *Service) createAgent(ctx context.Context, request *agentservicepb.CreateAgentRequest) (*agentpb.Agent, error) {
	userRn, err := agentpb.ParseUserRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	agentID := request.AgentId
	if agentID == "" {
		agentID = aip.NewSystemGeneratedBase32ResourceID()
	}
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	agentRn := userRn.AgentRn(agentID)
	// Clients set only these fields: every other one is the service's.
	agent := &agentpb.Agent{
		Name:        agentRn.String(),
		Labels:      request.Agent.Labels,
		Annotations: request.Agent.Annotations,
		Title:       request.Agent.Title,
		Definition:  request.Agent.Definition,
		Spec:        request.Agent.Spec,
		ParentAgent: request.Agent.ParentAgent,
		State:       agentpb.Agent_STATE_IDLE,
	}

	var parentAgent *agentpb.Agent
	if agent.ParentAgent != "" {
		if parentAgent, err = s.getLiveParentAgent(ctx, agentRn, agent.ParentAgent); err != nil {
			return nil, err
		}
	}
	if agent.Principal, err = principal(ctx, request, parentAgent); err != nil {
		return nil, err
	}
	if agent.Definition != "" {
		if agent.Spec, err = s.snapshotDefinitionSpec(ctx, agentRn, agent.Definition, parentAgent); err != nil {
			return nil, err
		}
	}
	if err := s.validateTools(agent.Spec); err != nil {
		return nil, err
	}
	if parentAgent != nil {
		// A sub-agent launches sub-agents one level less deep than its parent may.
		agent.Spec = proto.CloneOf(agent.Spec)
		agent.Spec.MaxSubAgentDepth = max(0, min(agent.Spec.GetMaxSubAgentDepth(), parentAgent.GetSpec().GetMaxSubAgentDepth()-1))
	}

	// The chat is created outside the transaction, which may be retried; its request id makes replays reuse it. Agent
	// ids match label values: lowercase letters, digits and hyphens.
	createChatRequest := &aiservicepb.CreateChatRequest{
		Parent:    (&aipb.OrganizationRn{Organization: userRn.Organization}).UserRn(userRn.User).String(),
		Chat:      &aipb.Chat{Title: agent.Title, Labels: map[string]string{agentpb.Labels.Agent.GetKey(): agentID}},
		RequestId: uuid.NewV5(agentNamespace, requestID).String(),
	}
	chat, err := s.aiServiceClient.CreateChat(ctx, createChatRequest)
	if err != nil {
		return nil, status.FromError(err, "creating chat").Err()
	}
	agent.Chat = chat.Name

	var createdAgent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		var err error
		createdAgent, err = t.createAgent(ctx, requestID, proto.CloneOf(agent))
		return err
	}); err != nil {
		return nil, err
	}
	return createdAgent, nil
}

// createAgent inserts an agent. A replayed request id returns the agent it created, whose id may differ.
func (t *txn) createAgent(ctx context.Context, requestID string, agent *agentpb.Agent) (*agentpb.Agent, error) {
	agent.RootAgent = agent.Name
	if agent.ParentAgent != "" {
		// Locked, so a parent being deleted never gains a child.
		parentAgent, err := t.getAgent(ctx, agent.ParentAgent)
		if err != nil {
			return nil, err
		}
		if parentAgent.DeleteTime != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "parent agent %s is deleted", agent.ParentAgent).Err()
		}
		agent.RootAgent = parentAgent.RootAgent
	}
	agent.CreateTime = timestamppb.New(t.now)
	agent.UpdateTime = agent.CreateTime
	var err error
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return nil, status.Errorf(codes.Internal, "computing etag: %v", err).Err()
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting agent from pb to model: %v", err).Err()
	}
	agentModel, inserted, err := t.InsertAgent(ctx, requestID, agentModel)
	if err != nil {
		if errors.Is(err, model.ErrAgentAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "agent %s already exists", agent.Name).Err()
		}
		return nil, fmt.Errorf("inserting agent: %w", err)
	}
	if agent, err = agentModel.ToPb(); err != nil {
		return nil, status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
	}
	if inserted {
		t.snapshots = append(t.snapshots, proto.CloneOf(agent))
	}
	return agent, nil
}

// getLiveParentAgent reads the parent of a new agent, which must be a live agent of the same user.
func (s *Service) getLiveParentAgent(ctx context.Context, agentRn *agentpb.AgentRn, name string) (*agentpb.Agent, error) {
	parentAgentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent_agent: %v", err).Err()
	}
	if parentAgentRn.Organization != agentRn.Organization || parentAgentRn.User != agentRn.User {
		return nil, status.Errorf(codes.InvalidArgument, "parent_agent %s belongs to another user", name).Err()
	}
	// Whole, as its principal passes on to the new agent.
	parentAgent, err := s.getAgent(ctx, name)
	if err != nil {
		return nil, status.FromError(err, "getting parent agent").Err()
	}
	if parentAgent.DeleteTime != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "parent agent %s is deleted", name).Err()
	}
	return parentAgent, nil
}

// snapshotDefinitionSpec returns the spec of a live definition of the agent's organization, which a parent agent must allow.
func (s *Service) snapshotDefinitionSpec(ctx context.Context, agentRn *agentpb.AgentRn, name string, parentAgent *agentpb.Agent) (*agentpb.AgentSpec, error) {
	definitionRn, err := agentpb.ParseDefinitionRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing definition: %v", err).Err()
	}
	if definitionRn.Organization != agentRn.Organization {
		return nil, status.Errorf(codes.InvalidArgument, "definition %s belongs to another organization", name).Err()
	}
	if parentAgent != nil && !slices.Contains(parentAgent.GetSpec().GetSubAgentDefinitions(), name) {
		return nil, status.Errorf(codes.FailedPrecondition, "parent agent %s may not launch sub-agents from definition %s", parentAgent.Name, name).Err()
	}
	getDefinitionRequest := &agentservicepb.GetDefinitionRequest{Name: name}
	definition, err := s.GetDefinition(ctx, getDefinitionRequest)
	if err != nil {
		return nil, status.FromError(err, "getting definition").Err()
	}
	if definition.DeleteTime != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "definition %s is deleted", name).Err()
	}
	return definition.Spec, nil
}

// UpdateAgent updates an agent's labels, annotations or title.
func (s *Service) UpdateAgent(ctx context.Context, request *agentservicepb.UpdateAgentRequest) (*agentpb.Agent, error) {
	agent, err := s.AgentServiceServer.UpdateAgent(ctx, request)
	if err != nil {
		return nil, err
	}
	s.publishSnapshot(agent)
	return redactAgent(agent), nil
}

// DeleteAgent soft-deletes an agent, cancelling its schedules and its running turn. With force, its live descendants
// go with it, in one transaction at one delete time, which UndeleteAgent restores as a unit. Its chat is kept.
func (s *Service) DeleteAgent(ctx context.Context, request *agentservicepb.DeleteAgentRequest) (*agentpb.Agent, error) {
	agent, err := s.getAgent(ctx, request.Name)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime != nil {
		if request.AllowMissing {
			return redactAgent(agent), nil
		}
		return nil, status.Errorf(codes.NotFound, "agent %s already deleted", request.Name).Err()
	}
	if request.Etag != "" && request.Etag != agent.Etag {
		return nil, status.Errorf(codes.Aborted, "etag changed").Err()
	}
	// Deepest first, the agent last.
	names := []string{request.Name}
	if request.Force {
		treeAgents, err := s.agentPostgresStore.ListTreeAgents(ctx, agent.RootAgent)
		if err != nil {
			return nil, status.FromError(err, "listing tree agents").Err()
		}
		isLive := func(treeAgent *model.Agent) bool { return treeAgent.DeleteTime == nil }
		names = append(descendantAgents(treeAgents, request.Name, isLive), request.Name)
	}
	// Before the delete commits, so a failure deletes nothing: a retry cancels them again.
	for _, name := range names {
		if err := s.cancelSchedules(ctx, name); err != nil {
			return nil, status.FromError(err, "cancelling the schedules of agent %s", name).Err()
		}
	}

	var deletedAgent *agentpb.Agent
	var cancelledTurns []*cancelledTurn
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		cancelledTurns = nil
		for _, name := range names {
			// A descendant deleted since is skipped.
			allowMissing, etag := true, ""
			if name == request.Name {
				allowMissing, etag = request.AllowMissing, request.Etag
			}
			var turn *cancelledTurn
			var err error
			if deletedAgent, turn, err = t.deleteAgent(ctx, name, allowMissing, etag, names); err != nil {
				return err
			}
			if turn != nil {
				cancelledTurns = append(cancelledTurns, turn)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for _, turn := range cancelledTurns {
		s.stopTurn(ctx, turn)
	}
	return redactAgent(deletedAgent), nil
}

// descendantAgents returns, deepest first, the descendants of an agent among its tree's agents that match, reached
// through matching agents only.
func descendantAgents(treeAgents []*model.Agent, name string, match func(*model.Agent) bool) []string {
	parentToChildren := map[string][]string{}
	for _, treeAgent := range treeAgents {
		if match(treeAgent) {
			treeAgentName := (&agentpb.AgentRn{Organization: treeAgent.OrganizationID, User: treeAgent.UserID, Agent: treeAgent.AgentID}).String()
			parentToChildren[treeAgent.ParentAgent] = append(parentToChildren[treeAgent.ParentAgent], treeAgentName)
		}
	}
	var names []string
	var visit func(parent string)
	visit = func(parent string) {
		for _, child := range parentToChildren[parent] {
			visit(child)
			names = append(names, child)
		}
	}
	visit(name)
	return names
}

// cancelledTurn is the turn of a deleted agent, which another live replica may still run.
type cancelledTurn struct {
	agent         string
	holderAddress string
}

// deleteAgent soft-deletes an agent, whose live children must all be deleted with it, ending its running turn
// CANCELLED and dropping its lease. It returns the agent, and its cancelled turn if any.
func (t *txn) deleteAgent(ctx context.Context, name string, allowMissing bool, etag string, deletedWith []string) (*agentpb.Agent, *cancelledTurn, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	agent, err := t.getAgent(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	if agent.DeleteTime != nil {
		if allowMissing {
			return agent, nil, nil
		}
		return nil, nil, status.Errorf(codes.NotFound, "agent %s already deleted", name).Err()
	}
	if etag != "" && etag != agent.Etag {
		return nil, nil, status.Errorf(codes.Aborted, "etag changed").Err()
	}
	// Read under the agent's lock, which CreateAgent takes on a parent: no child can appear before we commit.
	childAgents, err := t.service.agentPostgresStore.ListChildAgents(ctx, name)
	if err != nil {
		return nil, nil, fmt.Errorf("listing child agents: %w", err)
	}
	for _, childAgent := range childAgents {
		childAgentName := (&agentpb.AgentRn{Organization: childAgent.OrganizationID, User: childAgent.UserID, Agent: childAgent.AgentID}).String()
		if !slices.Contains(deletedWith, childAgentName) {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "agent %s has sub-agents: delete them first or set force", name).Err()
		}
	}
	var turn *cancelledTurn
	if agent.State != agentpb.Agent_STATE_IDLE {
		turn = &cancelledTurn{agent: name}
		lease, err := t.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return nil, nil, fmt.Errorf("getting lease: %w", err)
		}
		if lease != nil && !lease.Expired && lease.Replica != t.service.replica {
			turn.holderAddress = lease.Address
		}
		if err := t.cancelTurn(ctx, agent); err != nil {
			return nil, nil, err
		}
	}
	agent.DeleteTime = timestamppb.New(t.now)
	if err := t.saveAgent(ctx, agent); err != nil {
		return nil, nil, err
	}
	return agent, turn, nil
}

// stopTurn stops a deleted agent's cancelled turn wherever it runs. Its lease is gone, so it stops without ending
// again: this only stops it sooner than its replica's next heartbeat would.
func (s *Service) stopTurn(ctx context.Context, turn *cancelledTurn) {
	if _, ok := s.turns.cancel(turn.agent, errTurnCancelled); ok || turn.holderAddress == "" {
		return
	}
	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: turn.agent}
	peerClient, err := s.peerClient(ctx, turn.holderAddress)
	if err == nil {
		_, err = peerClient.CancelAgent(forwardedContext(ctx), cancelAgentRequest)
	}
	if err != nil {
		s.log.WarnContext(ctx, "cancelling a deleted agent's turn on the replica running it", "agent", turn.agent, "address", turn.holderAddress, "error", err)
	}
}

// cancelTurn ends the running or awaiting turn of an agent locked by getAgent CANCELLED and drops its lease, whoever
// holds it: a replica still running the turn stops at its next write. The calls the turn awaited are resolved as
// cancelled, results the next turn writes.
func (t *txn) cancelTurn(ctx context.Context, agent *agentpb.Agent) error {
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	if err := t.DeleteLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent); err != nil {
		return fmt.Errorf("deleting lease: %w", err)
	}
	agent.State = agentpb.Agent_STATE_IDLE
	if agent.LatestTurn == nil {
		agent.LatestTurn = &agentpb.Turn{StartTime: timestamppb.New(t.now)}
	}
	agent.LatestTurn.EndTime = timestamppb.New(t.now)
	agent.LatestTurn.Outcome = agentpb.Turn_OUTCOME_CANCELLED
	agent.LatestTurn.Error = nil
	resolveAwaitedToolCalls(agent, agentRn.UserRn().String(), errToolCallCancelled)
	return nil
}

// UndeleteAgent restores a deleted agent, whose parent must be live, with the descendants its forced delete removed:
// those sharing its delete time. Sub-agents deleted apart stay deleted, and cancelled schedules stay cancelled.
func (s *Service) UndeleteAgent(ctx context.Context, request *agentservicepb.UndeleteAgentRequest) (*agentpb.Agent, error) {
	agent, err := s.getAgent(ctx, request.Name)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime == nil {
		return nil, status.Errorf(codes.AlreadyExists, "agent %s is not deleted", request.Name).Err()
	}
	treeAgents, err := s.agentPostgresStore.ListTreeAgents(ctx, agent.RootAgent)
	if err != nil {
		return nil, status.FromError(err, "listing tree agents").Err()
	}
	deleteTime := agent.DeleteTime.AsTime()
	deletedWith := func(treeAgent *model.Agent) bool {
		return treeAgent.DeleteTime != nil && treeAgent.DeleteTime.Equal(deleteTime)
	}
	descendants := descendantAgents(treeAgents, request.Name, deletedWith)

	var undeletedAgent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		// Children before their parents, as every transaction locks them.
		for _, name := range descendants {
			descendant, err := t.getAgent(ctx, name)
			if err != nil {
				return err
			}
			if !proto.Equal(descendant.DeleteTime, agent.DeleteTime) {
				continue
			}
			descendant.DeleteTime = nil
			if err := t.saveAgent(ctx, descendant); err != nil {
				return err
			}
		}
		lockedAgent, err := t.getAgent(ctx, request.Name)
		if err != nil {
			return err
		}
		if lockedAgent.DeleteTime == nil {
			return status.Errorf(codes.AlreadyExists, "agent %s is not deleted", request.Name).Err()
		}
		if !proto.Equal(lockedAgent.DeleteTime, agent.DeleteTime) || request.Etag != "" && request.Etag != lockedAgent.Etag {
			return status.Errorf(codes.Aborted, "etag changed").Err()
		}
		if lockedAgent.ParentAgent != "" {
			parentAgent, err := t.getAgent(ctx, lockedAgent.ParentAgent)
			if err != nil {
				return err
			}
			if parentAgent.DeleteTime != nil {
				return status.Errorf(codes.FailedPrecondition, "parent agent %s is deleted: undelete it first", lockedAgent.ParentAgent).Err()
			}
		}
		lockedAgent.DeleteTime = nil
		undeletedAgent = lockedAgent
		return t.saveAgent(ctx, lockedAgent)
	}); err != nil {
		return nil, err
	}
	return redactAgent(undeletedAgent), nil
}
