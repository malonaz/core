package agent_executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// instructionsMessageID is the fixed ID of an agent's system message, so every
// lease can ensure it without duplicating it.
const instructionsMessageID = "instructions"

// inputsRequestIDNamespace keys an inputs message's request id on the inputs
// it delivers, so redelivered inputs are persisted once.
var inputsRequestIDNamespace = uuid.MustParse("31a36018-4937-45ed-843c-19bb447034bf")

// errToolCallCancelled answers the tool calls a cancelled turn left unfinished.
var errToolCallCancelled = errors.New("the turn was cancelled before this tool call completed")

// runTurn runs one turn of the agent and reports how it ended.
func (a *agent) runTurn(ctx context.Context, inputs []*agentpb.Input) {
	turnEnded := &agentpb.Event_TurnEnded{Outcome: agentpb.Agent_TURN_OUTCOME_SUCCEEDED}
	if err := a.turn(ctx, inputs); err != nil {
		switch {
		case a.ctx.Err() != nil:
			// The lease ended: the service closes the turn itself.
			return
		case ctx.Err() != nil:
			turnEnded.Outcome = agentpb.Agent_TURN_OUTCOME_CANCELLED
		default:
			a.executor.log.Warn("turn failed", "agent", a.agent.GetName(), "error", err)
			turnEnded.Outcome = agentpb.Agent_TURN_OUTCOME_FAILED
			turnEnded.Status = grpcstatus.Convert(err).Proto()
		}
	}
	event := &agentpb.Event{Kind: &agentpb.Event_TurnEnded_{TurnEnded: turnEnded}}
	if err := a.report(event); err != nil {
		a.executor.log.Warn("reporting turn end", "agent", a.agent.GetName(), "error", err)
	}
}

// turn generates until the model answers without calling tools. Generation and
// tools run on the turn's context; persistence and reports on the lease's, so
// whatever a cancelled turn already took or did still lands in the transcript.
func (a *agent) turn(ctx context.Context, inputs []*agentpb.Input) error {
	turnStarted := &agentpb.Event{Kind: &agentpb.Event_TurnStarted_{TurnStarted: &agentpb.Event_TurnStarted{}}}
	if err := a.report(turnStarted); err != nil {
		return err
	}
	if err := a.ensureInstructions(); err != nil {
		return err
	}
	if err := a.deliverInputs(inputs); err != nil {
		return err
	}
	toolDefinitions, toolNameToTool := a.tools()
	for {
		generatedMessage, err := a.generate(ctx, toolDefinitions)
		if err != nil {
			return err
		}
		var toolCalls []*aipb.ToolCall
		for _, block := range ai.FilterBlocks(generatedMessage.GetBlocks(), ai.BlockTypeToolCall) {
			toolCalls = append(toolCalls, block.GetToolCall())
		}
		if len(toolCalls) == 0 {
			return nil
		}
		if err := a.answerToolCalls(ctx, toolNameToTool, toolCalls); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if inputs := a.takeInputs(); len(inputs) > 0 {
			if err := a.deliverInputs(inputs); err != nil {
				return err
			}
		}
	}
}

// ensureInstructions persists the spec's instructions as the chat's system
// message, once per lease.
func (a *agent) ensureInstructions() error {
	instructions := a.agent.GetSpec().GetInstructions()
	if a.instructionsEnsured || instructions == "" {
		return nil
	}
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    a.agent.GetChat(),
		Message:   ai.NewSystemMessage(ai.NewTextBlock(instructions)),
		MessageId: instructionsMessageID,
	}
	if _, err := a.executor.aiServiceClient.CreateMessage(a.ctx, createMessageRequest); err != nil && !status.HasCode(err, codes.AlreadyExists) {
		return err
	}
	a.instructionsEnsured = true
	return nil
}

// deliverInputs persists drained inputs as one user message, naming the sender
// of any input its own user did not send, e.g. a sub-agent's report.
func (a *agent) deliverInputs(inputs []*agentpb.Input) error {
	agentRn, err := agentpb.ParseAgentRn(a.agent.GetName())
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	userName := agentRn.UserRn().String()
	inputNames := make([]string, 0, len(inputs))
	var blocks []*aipb.Block
	for _, input := range inputs {
		inputNames = append(inputNames, input.GetName())
		if sender, ok := agentpb.Annotations.Sender.Get(input.GetMessage()); ok && sender != userName {
			blocks = append(blocks, ai.NewTextBlock(fmt.Sprintf("[Message from %s]", sender)))
		}
		blocks = append(blocks, input.GetMessage().GetBlocks()...)
	}
	requestID := uuid.NewV5(inputsRequestIDNamespace, strings.Join(inputNames, ",")).String()
	return a.persistMessage(ai.NewUserMessage(blocks...), requestID, inputNames...)
}

// generate streams one assistant message, forwarding its blocks as deltas.
func (a *agent) generate(ctx context.Context, toolDefinitions []*aipb.Tool) (*aipb.Message, error) {
	// The stream is left once the generated message arrives: cancel it then.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	spec := a.agent.GetSpec()
	generateMessageRequest := &aiservicepb.GenerateMessageRequest{
		Parent: a.agent.GetChat(),
		Model:  spec.GetModel(),
		Tools:  toolDefinitions,
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			ReasoningEffort:        spec.GetReasoningEffort(),
			StreamPartialToolCalls: true,
		},
	}
	stream, err := a.executor.aiServiceClient.StreamGenerateMessage(ctx, generateMessageRequest)
	if err != nil {
		return nil, err
	}
	for {
		streamGenerateMessageResponse, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, status.Errorf(codes.Internal, "generation ended without a generated message").Err()
		}
		if err != nil {
			return nil, err
		}
		switch content := streamGenerateMessageResponse.GetContent().(type) {
		case *aiservicepb.StreamGenerateMessageResponse_Block:
			if err := a.delta(content.Block); err != nil {
				return nil, err
			}
		case *aiservicepb.StreamGenerateMessageResponse_GeneratedMessage:
			if err := a.reportMessageCreated(content.GeneratedMessage); err != nil {
				return nil, err
			}
			return content.GeneratedMessage, nil
		}
	}
}

// answerToolCalls executes a generation's tool calls and persists their
// results as one tool message, even when cancelled, so every call stays paired.
func (a *agent) answerToolCalls(ctx context.Context, toolNameToTool map[string]Tool, toolCalls []*aipb.ToolCall) error {
	toolResults := a.executeToolCalls(ctx, toolNameToTool, toolCalls)
	blocks := make([]*aipb.Block, 0, len(toolResults))
	for _, toolResult := range toolResults {
		blocks = append(blocks, ai.NewToolResultBlock(toolResult))
	}
	return a.persistMessage(ai.NewToolMessage(blocks...), "")
}

// executeToolCalls runs tool calls concurrently, as parallel calls are
// independent, and returns their results in call order. Cancelled, it stops
// waiting and answers the calls not completed as cancelled.
func (a *agent) executeToolCalls(ctx context.Context, toolNameToTool map[string]Tool, toolCalls []*aipb.ToolCall) []*aipb.ToolResult {
	type indexedToolResult struct {
		index      int
		toolResult *aipb.ToolResult
	}
	toolResults := make([]*aipb.ToolResult, len(toolCalls))
	// Buffered so calls finishing after a cancellation never block.
	indexedToolResults := make(chan indexedToolResult, len(toolCalls))
	running := 0
	for i, toolCall := range toolCalls {
		switch {
		case toolCall.GetResult() != nil:
			// Resolved server-side (discovery, unknown tool): fed back verbatim.
			toolResults[i] = toolCall.GetResult()
		case ctx.Err() == nil:
			running++
			go func() {
				indexedToolResults <- indexedToolResult{index: i, toolResult: a.executeToolCall(ctx, toolNameToTool, toolCall)}
			}()
		}
	}
wait:
	for ; running > 0; running-- {
		select {
		case result := <-indexedToolResults:
			toolResults[result.index] = result.toolResult
		case <-ctx.Done():
			break wait
		}
	}
	// Calls that completed as the turn was cancelled keep their results.
	for len(indexedToolResults) > 0 {
		result := <-indexedToolResults
		toolResults[result.index] = result.toolResult
	}
	for i, toolResult := range toolResults {
		if toolResult == nil {
			toolResults[i] = ai.NewErrorToolResult(toolCalls[i].GetName(), toolCalls[i].GetId(), errToolCallCancelled)
		}
	}
	return toolResults
}

// executeToolCall runs one tool call, turning any failure into an error result.
func (a *agent) executeToolCall(ctx context.Context, toolNameToTool map[string]Tool, toolCall *aipb.ToolCall) *aipb.ToolResult {
	tool, ok := toolNameToTool[toolCall.GetName()]
	if !ok {
		return ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), fmt.Errorf("unknown tool %q: call only the tools you were given", toolCall.GetName()))
	}
	toolResult, err := tool.Execute(ctx, a.agent, toolCall)
	if err != nil {
		return ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), err)
	}
	// Results pair with their calls by ID, whatever the tool stamped.
	toolResult.ToolName = toolCall.GetName()
	toolResult.ToolCallId = toolCall.GetId()
	return toolResult
}

// tools returns the definitions the agent generates with and the tools it may
// call: the built-in agent tools, then its spec's, in a stable order.
func (a *agent) tools() ([]*aipb.Tool, map[string]Tool) {
	toolNames := slices.Sorted(maps.Keys(a.executor.agentToolNameSet))
	for _, toolName := range a.agent.GetSpec().GetTools() {
		if _, ok := a.executor.agentToolNameSet[toolName]; !ok {
			toolNames = append(toolNames, toolName)
		}
	}
	toolDefinitions := make([]*aipb.Tool, 0, len(toolNames))
	toolNameToTool := make(map[string]Tool, len(toolNames))
	for _, toolName := range toolNames {
		// Leases asking for tools the executor lacks are released, never run.
		tool, ok := a.executor.toolNameToTool[toolName]
		if !ok {
			continue
		}
		toolDefinitions = append(toolDefinitions, tool.Definition())
		toolNameToTool[toolName] = tool
	}
	return toolDefinitions, toolNameToTool
}

// persistMessage persists a message to the agent's chat and reports it.
func (a *agent) persistMessage(message *aipb.Message, requestID string, inputNames ...string) error {
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    a.agent.GetChat(),
		Message:   message,
		RequestId: requestID,
	}
	createdMessage, err := a.executor.aiServiceClient.CreateMessage(a.ctx, createMessageRequest)
	if err != nil {
		return err
	}
	return a.reportMessageCreated(createdMessage, inputNames...)
}

// reportMessageCreated reports a message persisted to the agent's chat, with
// the inputs it delivered, if any.
func (a *agent) reportMessageCreated(message *aipb.Message, inputNames ...string) error {
	messageCreated := &agentpb.Event_MessageCreated{
		Message: message,
		Inputs:  inputNames,
	}
	event := &agentpb.Event{Kind: &agentpb.Event_MessageCreated_{MessageCreated: messageCreated}}
	return a.report(event)
}
