package agent_service

import (
	"context"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

var (
	// errUserReplied resolves the calls an agent awaits when its user replies with a message instead of settling them.
	errUserReplied = grpcstatus.Error(codes.Canceled, "the user replied instead of settling this call: it did not run")
	// errAwaitedToolCallCancelled resolves the calls an agent awaits when its turn is cancelled.
	errAwaitedToolCallCancelled = grpcstatus.Error(codes.Canceled, "the turn was cancelled before this call was settled: it did not run")
)

// ApproveToolCall approves a call the agent's turn awaits approval for. The turn resumes, running it, once its last
// awaited call is settled. With always, the agent stops asking before calls to the tool.
func (s *Service) ApproveToolCall(ctx context.Context, request *agentservicepb.ApproveToolCallRequest) (*agentpb.Agent, error) {
	return s.settleToolCall(ctx, request.Name, request.ToolCallId, func(agent *agentpb.Agent, awaitedToolCall *agentpb.AwaitedToolCall, approver requester) (bool, error) {
		if awaitedToolCall.GetReason() != agentpb.AwaitedToolCall_REASON_APPROVAL {
			return false, status.Errorf(codes.FailedPrecondition, "tool call %s awaits its result, not an approval: resolve it", awaitedToolCall.GetToolCallId()).Err()
		}
		resolution := awaitedToolCall.GetResolution()
		switch {
		case resolution == nil:
			awaitedToolCall.Resolution = newApproval(approver, request.Always)
		case resolution.GetApproval() == nil:
			return false, status.Errorf(codes.FailedPrecondition, "tool call %s was already settled otherwise", awaitedToolCall.GetToolCallId()).Err()
		case !request.Always || resolution.GetApproval().GetAlways():
			return false, nil
		default:
			resolution.GetApproval().Always = true
		}
		if !request.Always {
			return true, nil
		}
		return true, s.approveAlways(agent, awaitedToolCall.GetTool(), approver)
	})
}

// newApproval returns an approval by an approver.
func newApproval(approver requester, always bool) *agentpb.AwaitedToolCall_Resolution {
	return &agentpb.AwaitedToolCall_Resolution{
		Resolver: approver.principal,
		Actor:    approver.actor,
		Decision: &agentpb.AwaitedToolCall_Resolution_Approval_{Approval: &agentpb.AwaitedToolCall_Resolution_Approval{Always: always}},
	}
}

// approveAlways stops an agent asking before calls to a tool: its spec's grant of the tool needs no approval, and its
// other calls to the tool awaiting approval are approved.
func (s *Service) approveAlways(agent *agentpb.Agent, tool string, approver requester) error {
	if tool == "" {
		return status.Errorf(codes.FailedPrecondition, "the call names no tool to approve always").Err()
	}
	spec := proto.CloneOf(agent.GetSpec())
	index := grantIndex(spec.GetTools(), tool)
	if index < 0 {
		spec.Tools = append(spec.Tools, &agentpb.AgentSpec_ToolGrant{Grant: &agentpb.AgentSpec_ToolGrant_Tool{Tool: tool}})
		index = len(spec.Tools) - 1
	}
	spec.Tools[index].Approval = agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED
	if err := protovalidate.Validate(spec); err != nil {
		return status.Errorf(codes.FailedPrecondition, "granting %s without approval: %v", tool, err).Err()
	}
	agent.Spec = spec
	for _, awaitedToolCall := range agent.GetLatestTurn().GetAwaitedToolCalls() {
		if awaitedToolCall.GetTool() == tool && awaitedToolCall.GetReason() == agentpb.AwaitedToolCall_REASON_APPROVAL && awaitedToolCall.GetResolution() == nil {
			awaitedToolCall.Resolution = newApproval(approver, true)
		}
	}
	return nil
}

// grantIndex returns the index of the grant of a tool among grants, or -1.
func grantIndex(grants []*agentpb.AgentSpec_ToolGrant, tool string) int {
	for i, grant := range grants {
		if grant.GetTool() == tool {
			return i
		}
	}
	return -1
}

// ResolveToolCall supplies the outcome of a call the agent's turn awaits, whatever it awaits: an input component's
// answer, the result of a call the caller ran itself, or an error status refusing it. The turn resumes, writing it,
// once its last awaited call is settled.
func (s *Service) ResolveToolCall(ctx context.Context, request *agentservicepb.ResolveToolCallRequest) (*agentpb.Agent, error) {
	return s.settleToolCall(ctx, request.Name, request.ToolCallId, func(agent *agentpb.Agent, awaitedToolCall *agentpb.AwaitedToolCall, resolver requester) (bool, error) {
		toolResult := proto.CloneOf(request.Result)
		toolResult.ToolName = awaitedToolCall.GetToolName()
		toolResult.ToolCallId = awaitedToolCall.GetToolCallId()
		if err := s.genuiComponents.validateInputResponse(awaitedToolCall.GetToolName(), awaitedToolCall.GetToolCallId(), toolResult); err != nil {
			return false, err
		}
		resolver.stamp(toolResult, agentpb.Annotations.ResolvedBy)
		resolution := &agentpb.AwaitedToolCall_Resolution{
			Resolver: resolver.principal,
			Actor:    resolver.actor,
			Decision: &agentpb.AwaitedToolCall_Resolution_Result{Result: toolResult},
		}
		if awaitedToolCall.Resolution != nil {
			if proto.Equal(awaitedToolCall.Resolution.GetResult(), toolResult) {
				return false, nil
			}
			return false, status.Errorf(codes.FailedPrecondition, "tool call %s was already settled otherwise", awaitedToolCall.GetToolCallId()).Err()
		}
		awaitedToolCall.Resolution = resolution
		return true, nil
	})
}

// settleToolCall settles a call an agent's turn awaits, by the caller, and resumes the turn once its last awaited call
// is settled. settle reports whether it changed the agent.
func (s *Service) settleToolCall(
	ctx context.Context, name, toolCallID string,
	settle func(agent *agentpb.Agent, awaitedToolCall *agentpb.AwaitedToolCall, resolver requester) (bool, error),
) (*agentpb.Agent, error) {
	if _, err := agentpb.ParseAgentRn(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	resolver, err := requesterOf(ctx)
	if err != nil {
		return nil, err
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
		changed, err := settle(agent, awaitedToolCall, resolver)
		if err != nil || !changed {
			return err
		}
		return t.resumeIfSettled(ctx, agent)
	}); err != nil {
		return nil, err
	}
	return agent, nil
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

// resolveAwaitedToolCalls resolves every call an agent's turn still awaits with an error, by a resolver: the service's
// own resolve, when a user replies or cancels instead of settling them.
func resolveAwaitedToolCalls(agent *agentpb.Agent, resolver requester, err error) {
	for _, awaitedToolCall := range agent.GetLatestTurn().GetAwaitedToolCalls() {
		if awaitedToolCall.GetResolution() != nil {
			continue
		}
		toolResult := ai.NewErrorToolResult(awaitedToolCall.GetToolName(), awaitedToolCall.GetToolCallId(), err)
		resolver.stamp(toolResult, agentpb.Annotations.ResolvedBy)
		awaitedToolCall.Resolution = &agentpb.AwaitedToolCall_Resolution{
			Resolver: resolver.principal,
			Actor:    resolver.actor,
			Decision: &agentpb.AwaitedToolCall_Resolution_Result{Result: toolResult},
		}
	}
}
