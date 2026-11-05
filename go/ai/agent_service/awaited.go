package agent_service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

// errUserReplied denies the calls an agent awaits when its user replies with a message instead of settling them.
var errUserReplied = errors.New("the user replied instead of settling this call")

// ApproveToolCall approves a call the agent's turn awaits approval for. The turn resumes, running it, once its last
// awaited call is settled.
func (s *Service) ApproveToolCall(ctx context.Context, request *agentservicepb.ApproveToolCallRequest) (*agentpb.Agent, error) {
	return s.settleToolCall(ctx, request.Name, request.ToolCallId, func(awaitedToolCall *agentpb.AwaitedToolCall, resolver string) (*agentpb.AwaitedToolCall_Resolution, error) {
		if awaitedToolCall.GetReason() != agentpb.AwaitedToolCall_REASON_APPROVAL {
			return nil, status.Errorf(codes.FailedPrecondition, "tool call %s awaits an input, not an approval: resolve it with the user's answer", awaitedToolCall.GetToolCallId()).Err()
		}
		return &agentpb.AwaitedToolCall_Resolution{
			Resolver: resolver,
			Decision: &agentpb.AwaitedToolCall_Resolution_Approval_{Approval: &agentpb.AwaitedToolCall_Resolution_Approval{}},
		}, nil
	})
}

// ResolveToolCall supplies the result of a call the agent's turn awaits: an input component's answer, or an error
// result denying a call awaiting approval. The turn resumes, writing it, once its last awaited call is settled.
func (s *Service) ResolveToolCall(ctx context.Context, request *agentservicepb.ResolveToolCallRequest) (*agentpb.Agent, error) {
	return s.settleToolCall(ctx, request.Name, request.ToolCallId, func(awaitedToolCall *agentpb.AwaitedToolCall, resolver string) (*agentpb.AwaitedToolCall_Resolution, error) {
		toolResult := proto.CloneOf(request.Result)
		toolResult.ToolName = awaitedToolCall.GetToolName()
		toolResult.ToolCallId = awaitedToolCall.GetToolCallId()
		switch awaitedToolCall.GetReason() {
		case agentpb.AwaitedToolCall_REASON_INPUT:
			if err := s.genuiComponents.validateInputResponse(awaitedToolCall.GetToolName(), awaitedToolCall.GetToolCallId(), toolResult); err != nil {
				return nil, err
			}
		default:
			if toolResult.GetError() == nil {
				return nil, status.Errorf(codes.InvalidArgument, "only an error result denies a call awaiting approval: approve it with ApproveToolCall").Err()
			}
			agentpb.Annotations.DeniedBy.Set(toolResult, resolver)
		}
		return &agentpb.AwaitedToolCall_Resolution{
			Resolver: resolver,
			Decision: &agentpb.AwaitedToolCall_Resolution_Result{Result: toolResult},
		}, nil
	})
}

// settleToolCall records the resolution of a call an agent's turn awaits, by the user owning the agent, and resumes
// the turn once its last awaited call is settled. Settling a call again the same way is a no-op; any other way fails.
func (s *Service) settleToolCall(
	ctx context.Context, name, toolCallID string,
	resolve func(awaitedToolCall *agentpb.AwaitedToolCall, resolver string) (*agentpb.AwaitedToolCall_Resolution, error),
) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	var agent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		var err error
		if agent, err = t.getLiveAgent(ctx, name); err != nil {
			return err
		}
		var awaitedToolCall *agentpb.AwaitedToolCall
		for _, candidate := range agent.GetLatestTurn().GetAwaitedToolCalls() {
			if candidate.GetToolCallId() == toolCallID {
				awaitedToolCall = candidate
			}
		}
		if awaitedToolCall == nil {
			return status.Errorf(codes.NotFound, "agent %s awaits no tool call %s", name, toolCallID).Err()
		}
		resolution, err := resolve(awaitedToolCall, agentRn.UserRn().String())
		if err != nil {
			return err
		}
		if awaitedToolCall.Resolution != nil {
			if sameDecision(awaitedToolCall.Resolution, resolution) {
				return nil
			}
			return status.Errorf(codes.FailedPrecondition, "tool call %s was already settled otherwise", toolCallID).Err()
		}
		awaitedToolCall.Resolution = resolution
		return t.resumeIfSettled(ctx, agent)
	}); err != nil {
		return nil, err
	}
	return agent, nil
}

// sameDecision reports whether two resolutions decide the same.
func sameDecision(resolution, other *agentpb.AwaitedToolCall_Resolution) bool {
	if resolution.GetApproval() != nil || other.GetApproval() != nil {
		return resolution.GetApproval() != nil && other.GetApproval() != nil
	}
	return proto.Equal(resolution.GetResult(), other.GetResult())
}

// resumeIfSettled resumes the turn of an awaiting agent locked by getAgent once every call it awaits is settled.
func (t *txn) resumeIfSettled(ctx context.Context, agent *agentpb.Agent) error {
	if agent.State == agentpb.Agent_STATE_AWAITING {
		for _, awaitedToolCall := range agent.GetLatestTurn().GetAwaitedToolCalls() {
			if awaitedToolCall.GetResolution() == nil {
				return t.saveAgent(ctx, agent)
			}
		}
		return t.claimTurn(ctx, agent)
	}
	return t.saveAgent(ctx, agent)
}

// resolveAwaitedToolCalls settles every call an agent's turn still awaits with an error result from a resolver.
func resolveAwaitedToolCalls(agent *agentpb.Agent, resolver string, err error) {
	for _, awaitedToolCall := range agent.GetLatestTurn().GetAwaitedToolCalls() {
		if awaitedToolCall.GetResolution() != nil {
			continue
		}
		toolResult := ai.NewErrorToolResult(awaitedToolCall.GetToolName(), awaitedToolCall.GetToolCallId(), err)
		if awaitedToolCall.GetReason() == agentpb.AwaitedToolCall_REASON_APPROVAL {
			agentpb.Annotations.DeniedBy.Set(toolResult, resolver)
		}
		awaitedToolCall.Resolution = &agentpb.AwaitedToolCall_Resolution{
			Resolver: resolver,
			Decision: &agentpb.AwaitedToolCall_Resolution_Result{Result: toolResult},
		}
	}
}
