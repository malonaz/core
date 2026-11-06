package agent_service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/authentication"
	"github.com/malonaz/core/go/grpc/status"
)

// principal returns the identity a new agent acts as: its parent's for a sub-agent, else the one its creator named,
// which the permission interceptor checked they may act as, else its creator's own. Unset for an anonymous creator.
func principal(ctx context.Context, request *agentservicepb.CreateAgentRequest, parentAgent *agentpb.Agent) (*authenticationpb.Identity, error) {
	if parentAgent != nil {
		if request.Agent.Principal != nil {
			return nil, status.Errorf(codes.InvalidArgument, "a sub-agent acts as its parent's principal: leave agent.principal unset").Err()
		}
		return parentAgent.Principal, nil
	}
	if request.Agent.Principal != nil {
		return request.Agent.Principal, nil
	}
	session, err := authentication.GetSession(ctx)
	if err != nil {
		if errors.Is(err, authentication.ErrSignedSessionNotFound) {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "getting session: %v", err).Err()
	}
	identity, _ := authentication.IdentityOf(session)
	return identity, nil
}

// delegate returns the context a tool call of an agent runs on: acting as its principal on its behalf, with a
// deadline at the delegated session's expiry, so a call never outlives its session. An agent without a principal
// calls anonymously.
func (s *Service) delegate(ctx context.Context, agent *agentpb.Agent) (context.Context, context.CancelFunc, error) {
	if agent.GetPrincipal() == nil {
		return withoutIdentity(ctx), func() {}, nil
	}
	if s.sessionManager == nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "agent %s has a principal but the service has no session manager to act as it", agent.GetName()).Err()
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.ToolSessionLifetime)
	ctx, err := s.sessionManager.Delegate(ctx, agent.GetPrincipal(), agent.GetName(), s.opts.ToolSessionLifetime)
	if err != nil {
		cancel()
		return nil, nil, status.Errorf(codes.Internal, "delegating to agent %s: %v", agent.GetName(), err).Err()
	}
	return ctx, cancel, nil
}

// redactAgent returns an agent as its readers see it: its principal reduced to who it is, a JWT identity's issuer
// and subject or a service account.
func redactAgent(agent *agentpb.Agent) *agentpb.Agent {
	jwtIdentity := agent.GetPrincipal().GetJwtIdentity()
	if jwtIdentity == nil {
		return agent
	}
	claims := &structpb.Struct{Fields: map[string]*structpb.Value{}}
	if subject, ok := jwtIdentity.GetClaims().GetFields()["sub"]; ok {
		claims.Fields["sub"] = subject
	}
	agent = proto.CloneOf(agent)
	agent.Principal = &authenticationpb.Identity{Identity: &authenticationpb.Identity_JwtIdentity{
		JwtIdentity: &authenticationpb.JwtIdentity{IssuerId: jwtIdentity.GetIssuerId(), Claims: claims},
	}}
	return agent
}

// redacted redacts the agent an RPC returns.
func redacted(agent *agentpb.Agent, err error) (*agentpb.Agent, error) {
	if err != nil {
		return nil, err
	}
	return redactAgent(agent), nil
}

// getAgent reads an agent unredacted, its principal whole: for the service's own use, never returned.
func (s *Service) getAgent(ctx context.Context, name string) (*agentpb.Agent, error) {
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: name}
	return s.AgentServiceServer.GetAgent(ctx, getAgentRequest)
}

// GetAgent gets an agent, its principal redacted.
func (s *Service) GetAgent(ctx context.Context, request *agentservicepb.GetAgentRequest) (*agentpb.Agent, error) {
	return redacted(s.AgentServiceServer.GetAgent(ctx, request))
}

// ListAgents lists agents, their principals redacted.
func (s *Service) ListAgents(ctx context.Context, request *agentservicepb.ListAgentsRequest) (*agentservicepb.ListAgentsResponse, error) {
	listAgentsResponse, err := s.AgentServiceServer.ListAgents(ctx, request)
	if err != nil {
		return nil, err
	}
	for i, agent := range listAgentsResponse.Agents {
		listAgentsResponse.Agents[i] = redactAgent(agent)
	}
	return listAgentsResponse, nil
}

// SearchAgents searches agents, their principals redacted.
func (s *Service) SearchAgents(ctx context.Context, request *agentservicepb.SearchAgentsRequest) (*agentservicepb.SearchAgentsResponse, error) {
	searchAgentsResponse, err := s.AgentServiceServer.SearchAgents(ctx, request)
	if err != nil {
		return nil, err
	}
	for i, agent := range searchAgentsResponse.Agents {
		searchAgentsResponse.Agents[i] = redactAgent(agent)
	}
	return searchAgentsResponse, nil
}
