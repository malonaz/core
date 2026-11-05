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
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: name}
	parentAgent, err := s.GetAgent(ctx, getAgentRequest)
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
	return agent, nil
}

// DeleteAgent soft-deletes an agent, cancelling its running turn. With force, its descendants go first, deepest
// first. Its chat is kept.
func (s *Service) DeleteAgent(ctx context.Context, request *agentservicepb.DeleteAgentRequest) (*agentpb.Agent, error) {
	if request.Force {
		// Checked before the cascade, so a stale etag or a missing agent never costs the descendants.
		getAgentRequest := &agentservicepb.GetAgentRequest{Name: request.Name}
		agent, err := s.GetAgent(ctx, getAgentRequest)
		if err != nil {
			return nil, err
		}
		if agent.DeleteTime == nil && request.Etag != "" && request.Etag != agent.Etag {
			return nil, status.Errorf(codes.Aborted, "etag changed").Err()
		}
		if agent.DeleteTime == nil {
			if err := s.deleteDescendantAgents(ctx, request.Name); err != nil {
				return nil, err
			}
		}
	}
	return s.deleteAgent(ctx, request.Name, request.AllowMissing, request.Etag)
}

// deleteDescendantAgents deletes every live descendant of an agent, deepest first, each in its own transaction.
func (s *Service) deleteDescendantAgents(ctx context.Context, name string) error {
	childAgents, err := s.agentPostgresStore.ListChildAgents(ctx, name)
	if err != nil {
		return status.FromError(err, "listing child agents").Err()
	}
	for _, childAgent := range childAgents {
		childAgentName := (&agentpb.AgentRn{Organization: childAgent.OrganizationID, User: childAgent.UserID, Agent: childAgent.AgentID}).String()
		if err := s.deleteDescendantAgents(ctx, childAgentName); err != nil {
			return err
		}
		if _, err := s.deleteAgent(ctx, childAgentName, true, ""); err != nil {
			return err
		}
	}
	return nil
}

// deleteAgent soft-deletes an agent, which must have no live children, ending its running turn CANCELLED and
// dropping its lease, then cancels the turn wherever it runs.
func (s *Service) deleteAgent(ctx context.Context, name string, allowMissing bool, etag string) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	var agent *agentpb.Agent
	var cancelTurn bool
	// The address of another live replica running the turn, if any.
	var holderAddress string
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		var err error
		if agent, err = t.getAgent(ctx, name); err != nil {
			return err
		}
		if agent.DeleteTime != nil {
			if allowMissing {
				return nil
			}
			return status.Errorf(codes.NotFound, "agent %s already deleted", name).Err()
		}
		if etag != "" && etag != agent.Etag {
			return status.Errorf(codes.Aborted, "etag changed").Err()
		}
		// Read under the agent's lock, which CreateAgent takes on a parent: no child can appear before we commit.
		childAgents, err := s.agentPostgresStore.ListChildAgents(ctx, name)
		if err != nil {
			return fmt.Errorf("listing child agents: %w", err)
		}
		if len(childAgents) > 0 {
			return status.Errorf(codes.FailedPrecondition, "agent %s has sub-agents: delete them first or set force", name).Err()
		}
		cancelTurn = agent.State != agentpb.Agent_STATE_IDLE
		if cancelTurn {
			lease, err := t.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
			if err != nil {
				return fmt.Errorf("getting lease: %w", err)
			}
			if lease != nil && !lease.Expired && lease.Replica != s.replica {
				holderAddress = lease.Address
			}
			if err := t.cancelTurn(ctx, agent); err != nil {
				return err
			}
		}
		agent.DeleteTime = timestamppb.New(t.now)
		return t.saveAgent(ctx, agent)
	}); err != nil {
		return nil, err
	}
	if !cancelTurn {
		return agent, nil
	}
	// The turn's lease is gone, so it stops without ending again: cancelling it only stops it sooner than its
	// replica's next heartbeat would.
	if _, ok := s.turns.cancel(name, errTurnCancelled); ok || holderAddress == "" {
		return agent, nil
	}
	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: name}
	peerClient, err := s.peerClient(ctx, holderAddress)
	if err == nil {
		_, err = peerClient.CancelAgent(forwardedContext(ctx), cancelAgentRequest)
	}
	if err != nil {
		s.log.WarnContext(ctx, "cancelling a deleted agent's turn on the replica running it", "agent", name, "address", holderAddress, "error", err)
	}
	return agent, nil
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

// UndeleteAgent restores a deleted agent, whose parent must be live.
func (s *Service) UndeleteAgent(ctx context.Context, request *agentservicepb.UndeleteAgentRequest) (*agentpb.Agent, error) {
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: request.Name}
	agent, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if agent.ParentAgent != "" {
		getParentAgentRequest := &agentservicepb.GetAgentRequest{Name: agent.ParentAgent}
		parentAgent, err := s.GetAgent(ctx, getParentAgentRequest)
		if err != nil {
			return nil, status.FromError(err, "getting parent agent").Err()
		}
		if parentAgent.DeleteTime != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "parent agent %s is deleted: undelete it first", agent.ParentAgent).Err()
		}
	}
	if agent, err = s.AgentServiceServer.UndeleteAgent(ctx, request); err != nil {
		return nil, err
	}
	s.publishSnapshot(agent)
	return agent, nil
}
