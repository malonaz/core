package agent_service

import (
	"context"
	"errors"
	"maps"
	"slices"

	"buf.build/go/protovalidate"
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

var (
	agentNamespace           = aip.MustGetUUIDNamespace(&agentpb.Agent{})
	updateAgentRequestParser = aip.MustNewUpdateRequestParser[*agentservicepb.UpdateAgentRequest, *agentpb.Agent]()
)

// CreateAgent creates an agent, from a definition or ad-hoc, with the chat holding its transcript, then places it.
func (s *Service) CreateAgent(ctx context.Context, request *agentservicepb.CreateAgentRequest) (*agentpb.Agent, error) {
	userRn, err := agentpb.ParseUserRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	if request.Agent.Definition != "" && request.Agent.Spec != nil {
		return nil, status.Errorf(codes.InvalidArgument, "exactly one of agent.definition and agent.spec must be set").Err()
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
		Executor:    request.Agent.Executor,
		State:       agentpb.Agent_STATE_PENDING,
	}
	if err := validateExecutorPin(agentRn, agent.Executor); err != nil {
		return nil, err
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

	// The chat is created outside the transaction, which may be retried; its request id makes replays reuse it.
	createChatRequest := &aiservicepb.CreateChatRequest{
		Parent:    (&aipb.OrganizationRn{Organization: userRn.Organization}).UserRn(userRn.User).String(),
		Chat:      &aipb.Chat{Title: agent.Title},
		RequestId: uuid.NewV5(agentNamespace, requestID).String(),
	}
	chat, err := s.aiServiceClient.CreateChat(ctx, createChatRequest)
	if err != nil {
		return nil, status.FromError(err, "creating chat").Err()
	}
	agent.Chat = chat.Name

	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		return t.createAgent(ctx, requestID, proto.CloneOf(agent))
	}); err != nil {
		return nil, err
	}
	if err := s.placeAgent(ctx, agent.Name); err != nil {
		s.log.WarnContext(ctx, "placing agent", "agent", agent.Name, "error", err)
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: agent.Name}
	return s.GetAgent(ctx, getAgentRequest)
}

// createAgent inserts an agent and records its creation on it and its parent. A replayed request id records nothing.
func (t *txn) createAgent(ctx context.Context, requestID string, agent *agentpb.Agent) error {
	agent.RootAgent = agent.Name
	var parentAgent *agentpb.Agent
	if agent.ParentAgent != "" {
		var err error
		if parentAgent, err = t.getAgent(ctx, agent.ParentAgent); err != nil {
			return err
		}
		if parentAgent.DeleteTime != nil {
			return status.Errorf(codes.FailedPrecondition, "parent agent %s is deleted", agent.ParentAgent).Err()
		}
		agent.RootAgent = parentAgent.RootAgent
	}
	agent.CreateTime = timestamppb.New(t.now)
	agent.UpdateTime = agent.CreateTime
	var err error
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return status.Errorf(codes.Internal, "computing etag: %v", err).Err()
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return status.Errorf(codes.Internal, "converting agent from pb to model: %v", err).Err()
	}
	agentModel, inserted, err := t.InsertAgent(ctx, requestID, agentModel)
	if err != nil {
		if errors.Is(err, model.ErrAgentAlreadyExists) {
			return status.Errorf(codes.AlreadyExists, "agent %s already exists", agent.Name).Err()
		}
		return status.FromError(err, "inserting agent").Err()
	}
	if !inserted {
		return nil
	}
	if agent, err = agentModel.ToPb(); err != nil {
		return status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
	}

	stateChanged := newStateChangedEvent(agentpb.Agent_STATE_PENDING)
	if err := t.saveAgent(ctx, agent, stateChanged); err != nil {
		return err
	}
	if parentAgent == nil {
		return nil
	}
	subAgentCreated := &agentpb.Event{Kind: &agentpb.Event_SubAgentCreated_{SubAgentCreated: &agentpb.Event_SubAgentCreated{
		Agent: agent.Name,
	}}}
	return t.saveAgent(ctx, parentAgent, subAgentCreated)
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

// validateExecutorPin rejects a pin on another user's executor: an executor runs its own user's agents only.
func validateExecutorPin(agentRn *agentpb.AgentRn, executor string) error {
	if executor == "" {
		return nil
	}
	executorRn, err := agentpb.ParseExecutorRn(executor)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing executor: %v", err).Err()
	}
	if executorRn.Organization != agentRn.Organization || executorRn.User != agentRn.User {
		return status.Errorf(codes.InvalidArgument, "executor %s belongs to another user", executor).Err()
	}
	return nil
}

// UpdateAgent updates an agent's labels, annotations, title or executor pin. A new pin places a pending agent but
// never moves a held lease: it applies from the agent's next placement.
func (s *Service) UpdateAgent(ctx context.Context, request *agentservicepb.UpdateAgentRequest) (*agentpb.Agent, error) {
	parsedRequest, err := updateAgentRequestParser.Parse(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing request: %v", err).Err()
	}
	agentRn, err := agentpb.ParseAgentRn(request.Agent.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}

	var patchedAgent *agentpb.Agent
	var executorChanged bool
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getAgent(ctx, request.Agent.Name)
		if err != nil {
			return err
		}
		if agent.DeleteTime != nil {
			return status.Errorf(codes.NotFound, "agent %s does not exist", agent.Name).Err()
		}
		if request.Agent.Etag != "" && request.Agent.Etag != agent.Etag {
			return status.Errorf(codes.Aborted, "etag changed").Err()
		}
		patchedAgent = proto.CloneOf(agent)
		parsedRequest.ApplyFieldMask(patchedAgent, request.Agent)
		if err := protovalidate.Validate(patchedAgent); err != nil {
			return status.Errorf(codes.InvalidArgument, "validating patched agent: %v", err).Err()
		}
		if err := validateExecutorPin(agentRn, patchedAgent.Executor); err != nil {
			return err
		}
		executorChanged = patchedAgent.Executor != agent.Executor
		var events []*agentpb.Event
		if !maps.Equal(patchedAgent.Labels, agent.Labels) {
			events = append(events, &agentpb.Event{Kind: &agentpb.Event_LabelsChanged_{LabelsChanged: &agentpb.Event_LabelsChanged{
				Labels: patchedAgent.Labels,
			}}})
		}
		return t.saveAgent(ctx, patchedAgent, events...)
	}); err != nil {
		return nil, err
	}

	if executorChanged && patchedAgent.State == agentpb.Agent_STATE_PENDING {
		if err := s.placeAgent(ctx, patchedAgent.Name); err != nil {
			s.log.WarnContext(ctx, "placing agent", "agent", patchedAgent.Name, "error", err)
		}
		getAgentRequest := &agentservicepb.GetAgentRequest{Name: patchedAgent.Name}
		return s.GetAgent(ctx, getAgentRequest)
	}
	return patchedAgent, nil
}

// DeleteAgent soft-deletes an agent, ending its turn and revoking its lease. With force, its descendants go first,
// deepest first. Its chat is kept.
func (s *Service) DeleteAgent(ctx context.Context, request *agentservicepb.DeleteAgentRequest) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
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

	var agent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err = t.deleteAgent(ctx, request.Name, request.AllowMissing, request.Etag)
		return err
	}); err != nil {
		return nil, err
	}
	// The deleted agent's lease freed capacity.
	s.placePendingAgents(ctx, agentRn.Organization, agentRn.User)
	return agent, nil
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
		if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
			_, err := t.deleteAgent(ctx, childAgentName, true, "")
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// deleteAgent soft-deletes an agent locked here, which must have no live children: its running turn is cancelled,
// its lease revoked and released.
func (t *txn) deleteAgent(ctx context.Context, name string, allowMissing bool, etag string) (*agentpb.Agent, error) {
	agent, err := t.getAgent(ctx, name)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime != nil {
		if allowMissing {
			return agent, nil
		}
		return nil, status.Errorf(codes.NotFound, "agent %s already deleted", name).Err()
	}
	if etag != "" && etag != agent.Etag {
		return nil, status.Errorf(codes.Aborted, "etag changed").Err()
	}
	// Read under the agent's lock, which CreateAgent takes on a parent: no child can appear before we commit.
	childAgents, err := t.service.agentPostgresStore.ListChildAgents(ctx, name)
	if err != nil {
		return nil, status.FromError(err, "listing child agents").Err()
	}
	if len(childAgents) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "agent %s has sub-agents: delete them first or set force", name).Err()
	}

	if agent.State == agentpb.Agent_STATE_RUNNING {
		if err := t.endTurn(ctx, agent, agentpb.Agent_TURN_OUTCOME_CANCELLED, nil); err != nil {
			return nil, err
		}
	}
	if agent.LeasedExecutor != "" {
		revoke := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Revoke_{
			Revoke: &agentservicepb.ConnectExecutorResponse_Revoke{Agent: agent.Name},
		}}
		if err := t.send(ctx, agent.LeasedExecutor, revoke); err != nil {
			return nil, err
		}
	}
	// Deleted first, so the release's agent count no longer includes it.
	agent.DeleteTime = timestamppb.New(t.now)
	if err := t.releaseAgent(ctx, agent); err != nil {
		return nil, err
	}
	return agent, nil
}

// UndeleteAgent restores a deleted agent, whose parent must be live, then places it.
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
	if err := s.placeAgent(ctx, agent.Name); err != nil {
		s.log.WarnContext(ctx, "placing agent", "agent", agent.Name, "error", err)
	}
	return s.GetAgent(ctx, getAgentRequest)
}

// CancelAgent asks the agent's executor to cancel its running turn. Nothing is recorded here: the executor
// reports the cancelled turn.
func (s *Service) CancelAgent(ctx context.Context, request *agentservicepb.CancelAgentRequest) (*agentpb.Agent, error) {
	var agent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		var err error
		if agent, err = t.getAgent(ctx, request.Name); err != nil {
			return err
		}
		if agent.DeleteTime != nil {
			return status.Errorf(codes.NotFound, "agent %s does not exist", agent.Name).Err()
		}
		if agent.LeasedExecutor == "" {
			return nil
		}
		cancel := &agentservicepb.ConnectExecutorResponse{Content: &agentservicepb.ConnectExecutorResponse_Cancel_{
			Cancel: &agentservicepb.ConnectExecutorResponse_Cancel{Agent: agent.Name},
		}}
		return t.send(ctx, agent.LeasedExecutor, cancel)
	}); err != nil {
		return nil, err
	}
	return agent, nil
}

func newStateChangedEvent(state agentpb.Agent_State) *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_StateChanged_{StateChanged: &agentpb.Event_StateChanged{State: state}}}
}
