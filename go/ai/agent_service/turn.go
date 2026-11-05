package agent_service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/uuid"
)

// instructionsMessageID is the fixed id of an agent's system message, so every turn can ensure it without
// duplicating it.
const instructionsMessageID = "instructions"

var (
	// inputsMessageNamespace derives the id of the message delivering inputs from the oldest of them, so a
	// delivery retried after a crash is persisted once.
	inputsMessageNamespace = uuid.MustParse("31a36018-4937-45ed-843c-19bb447034bf")
	// toolResultNamespace derives a tool result's request id from its call.
	toolResultNamespace = uuid.MustParse("0d7c4b0e-5f2a-4c39-9b57-2a8f3e61c4d1")
	// reportNamespace derives the request id of a turn's report to the parent from the turn's final message.
	reportNamespace = uuid.MustParse("e3b54a8c-1f6d-4e2b-a7c0-94d2f8b1e365")

	errToolCallInterrupted = errors.New("interrupted by a server restart; the call may or may not have completed")
	errToolCallCancelled   = errors.New("cancelled: the turn was cancelled before this call completed; it may or may not have run")
)

// historyFilter excludes the messages the AiService leaves out of generations.
var historyFilter = fmt.Sprintf("-status:* AND -labels.%q:*", aipb.Labels.Superseded.GetKey())

// runTurns runs the agent's turns while it has inputs, until it goes idle or its run stops.
func (s *Service) runTurns(turn *turn) {
	for {
		turnEnd := s.runTurn(turn)
		if turnEnd == nil {
			return
		}
		next, err := s.endTurn(s.runCtx, turn.agent, turnEnd)
		if err != nil {
			if !errors.Is(err, errLeaseLost) {
				s.log.Warn("ending turn", "agent", turn.agent, "error", err)
			}
			return
		}
		if !next {
			return
		}
	}
}

// runTurn runs one turn of the agent and returns how it ended, or nil if it stopped without ending: its lease was
// lost or the replica is shutting down, and another replica takes it over.
func (s *Service) runTurn(turn *turn) *turnEnd {
	ctx := turn.ctx
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: turn.agent}
	agent, err := s.GetAgent(ctx, getAgentRequest)
	var finalMessage *aipb.Message
	if err == nil {
		runner := &turnRunner{service: s, agent: agent}
		finalMessage, err = runner.run(ctx)
	}
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, errLeaseLost), errors.Is(cause, errShuttingDown), errors.Is(err, errShuttingDown):
		return nil
	case errors.Is(cause, errTurnCancelled):
		return &turnEnd{outcome: agentpb.Agent_TURN_OUTCOME_CANCELLED, report: "My turn was cancelled."}
	case err != nil:
		s.log.Warn("turn failed", "agent", turn.agent, "error", err)
		return &turnEnd{
			outcome: agentpb.Agent_TURN_OUTCOME_FAILED,
			err:     err,
			report:  fmt.Sprintf("My turn failed: %s", grpcstatus.Convert(err).Message()),
		}
	}
	turnEnd := &turnEnd{outcome: agentpb.Agent_TURN_OUTCOME_SUCCEEDED}
	if finalMessage != nil {
		turnEnd.report = cmp.Or(messageText(finalMessage), "My turn ended without a reply.")
		turnEnd.reportRequestID = uuid.NewV5(reportNamespace, finalMessage.GetName()).String()
	}
	return turnEnd
}

// endTurn records how the agent's turn ended and queues its report on its parent, if any, in one transaction under
// the agent's lock. With pending inputs, a succeeded turn keeps the agent RUNNING and returns true: the next turn
// follows. Otherwise the agent goes IDLE and its lease is released.
func (s *Service) endTurn(ctx context.Context, name string, turnEnd *turnEnd) (bool, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return false, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	var next bool
	err = s.transact(ctx, func(ctx context.Context, t *txn) error {
		next = false
		agent, err := t.getAgent(ctx, name)
		if err != nil {
			return err
		}
		lease, err := t.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return fmt.Errorf("getting lease: %w", err)
		}
		if lease == nil || lease.Replica != s.replica {
			return errLeaseLost
		}
		if agent.DeleteTime != nil {
			if _, err := t.ReleaseLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, s.replica); err != nil {
				return fmt.Errorf("releasing lease: %w", err)
			}
			return nil
		}

		agent.LastTurnOutcome = turnEnd.outcome
		agent.Status = nil
		if turnEnd.err != nil {
			agent.Status = grpcstatus.Convert(turnEnd.err).Proto()
		}
		if turnEnd.outcome == agentpb.Agent_TURN_OUTCOME_SUCCEEDED {
			if next, err = t.HasPendingInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent); err != nil {
				return fmt.Errorf("checking pending inputs: %w", err)
			}
		}
		if next {
			if _, err := t.ClaimLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, s.replica, s.address, s.opts.LeaseDuration); err != nil {
				return fmt.Errorf("renewing lease: %w", err)
			}
		} else {
			agent.State = agentpb.Agent_STATE_IDLE
			if _, err := t.ReleaseLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, s.replica); err != nil {
				return fmt.Errorf("releasing lease: %w", err)
			}
		}
		if err := t.saveAgent(ctx, agent); err != nil {
			return err
		}
		return t.report(ctx, agent, turnEnd)
	})
	return next, err
}

// report queues a turn's report on the agent's parent, so a parent never waits forever on its sub-agents.
func (t *txn) report(ctx context.Context, agent *agentpb.Agent, turnEnd *turnEnd) error {
	if agent.ParentAgent == "" || turnEnd.report == "" {
		return nil
	}
	parentAgent, err := t.getAgent(ctx, agent.ParentAgent)
	if err != nil {
		return err
	}
	if parentAgent.DeleteTime != nil {
		return nil
	}
	header := fmt.Sprintf("[Report from sub-agent %s (%q), whose turn ended]", agent.Name, agent.Title)
	message := ai.NewUserMessage(ai.NewTextBlock(header), ai.NewTextBlock(turnEnd.report))
	agentpb.Annotations.Sender.Set(message, agent.Name)
	requestID := turnEnd.reportRequestID
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	_, err = t.queueInput(ctx, parentAgent, requestID, message)
	return err
}

// turnRunner runs one turn of an agent.
type turnRunner struct {
	service *Service
	agent   *agentpb.Agent
}

// run runs the turn until the model answers without calling tools, and returns that final message: nil when the
// chat holds nothing to answer.
func (r *turnRunner) run(ctx context.Context) (*aipb.Message, error) {
	if err := r.ensureInstructions(ctx); err != nil {
		return nil, err
	}
	last, err := r.recover(ctx)
	if err != nil {
		return nil, err
	}
	toolbox, err := r.service.newToolbox(ctx, r.agent)
	if err != nil {
		return nil, err
	}
	for {
		delivered, err := r.drainInputs(ctx)
		if err != nil {
			return nil, err
		}
		if delivered != nil {
			last = delivered
		}
		switch {
		case last == nil:
			return nil, nil
		case last.GetRole() == aipb.Role_ROLE_ASSISTANT && len(toolCalls(last)) == 0:
			return last, nil
		}
		generatedMessage, err := r.generate(ctx, toolbox)
		if err != nil {
			return nil, err
		}
		last = generatedMessage
		if calls := toolCalls(generatedMessage); len(calls) > 0 {
			if last, err = r.answerToolCalls(ctx, toolbox, calls); err != nil {
				return nil, err
			}
		}
	}
}

// ensureInstructions persists the spec's instructions as the chat's system message, once.
func (r *turnRunner) ensureInstructions(ctx context.Context) error {
	instructions := r.agent.GetSpec().GetInstructions()
	if instructions == "" {
		return nil
	}
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    r.agent.GetChat(),
		Message:   ai.NewSystemMessage(ai.NewTextBlock(instructions)),
		MessageId: instructionsMessageID,
	}
	message, err := r.service.aiServiceClient.CreateMessage(ctx, createMessageRequest)
	if err != nil {
		if status.HasCode(err, codes.AlreadyExists) {
			return nil
		}
		return status.FromError(err, "creating instructions message").Err()
	}
	r.publishMessage(message)
	return nil
}

// recover reads the end of the chat, where a turn interrupted on another replica left off, answers the tool calls
// it left without a result and returns the chat's last message.
func (r *turnRunner) recover(ctx context.Context) (*aipb.Message, error) {
	listMessagesRequest := &aiservicepb.ListMessagesRequest{
		Parent:  r.agent.GetChat(),
		Filter:  historyFilter,
		OrderBy: "create_time desc",
	}
	// The messages after the latest assistant message, newest first: only its tool calls may lack results.
	var tail []*aipb.Message
	var assistantMessage *aipb.Message
	for message, err := range aip.Iterator[*aipb.Message](ctx, listMessagesRequest, r.service.aiServiceClient.ListMessages) {
		if err != nil {
			return nil, status.FromError(err, "listing messages").Err()
		}
		switch message.GetRole() {
		case aipb.Role_ROLE_ASSISTANT:
			assistantMessage = message
		case aipb.Role_ROLE_SYSTEM:
			// The instructions answer nothing, wherever a first turn's takeover persisted them.
			continue
		default:
			tail = append(tail, message)
			continue
		}
		break
	}
	last := assistantMessage
	if len(tail) > 0 {
		last = tail[0]
	}
	if assistantMessage == nil {
		return last, nil
	}

	toolCallIDSet := map[string]struct{}{}
	for _, message := range tail {
		for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolResult) {
			toolCallIDSet[block.GetToolResult().GetToolCallId()] = struct{}{}
		}
	}
	for _, toolCall := range toolCalls(assistantMessage) {
		if _, ok := toolCallIDSet[toolCall.GetId()]; ok {
			continue
		}
		toolResult := toolCall.GetResult()
		if toolResult == nil {
			toolResult = ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), errToolCallInterrupted)
		}
		message, err := r.persistToolResult(ctx, toolResult)
		if err != nil {
			return nil, err
		}
		last = message
	}
	return last, nil
}

// drainInputs delivers the agent's pending inputs as one user message, listing them in its inputs annotation, then
// marks them consumed. It returns the message, or nil if there was nothing new to deliver.
func (r *turnRunner) drainInputs(ctx context.Context) (*aipb.Message, error) {
	agentRn, err := agentpb.ParseAgentRn(r.agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	for {
		inputModels, err := r.service.agentPostgresStore.ListPendingInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return nil, status.FromError(err, "listing pending inputs").Err()
		}
		if len(inputModels) == 0 {
			return nil, nil
		}
		inputNames := make([]string, 0, len(inputModels))
		var blocks []*aipb.Block
		for _, inputModel := range inputModels {
			input, err := inputModel.ToPb()
			if err != nil {
				return nil, status.Errorf(codes.Internal, "converting input from model to pb: %v", err).Err()
			}
			inputNames = append(inputNames, input.GetName())
			blocks = append(blocks, input.GetMessage().GetBlocks()...)
		}
		message := ai.NewUserMessage(blocks...)
		agentpb.Annotations.Inputs.Set(message, strings.Join(inputNames, ","))
		messageID := aip.NewDeterministicBase32ResourceID(inputsMessageNamespace, inputNames[0])
		createMessageRequest := &aiservicepb.CreateMessageRequest{
			Parent:    r.agent.GetChat(),
			Message:   message,
			MessageId: messageID,
		}
		createdMessage, err := r.service.aiServiceClient.CreateMessage(ctx, createMessageRequest)
		if err == nil {
			r.publishMessage(createdMessage)
			return createdMessage, r.consumeInputs(ctx, agentRn, inputNames)
		}
		if !status.HasCode(err, codes.AlreadyExists) {
			return nil, status.FromError(err, "creating inputs message").Err()
		}

		// Delivered before a crash left its inputs pending: consume what it delivered, then drain the rest.
		chatRn, err := aipb.ParseChatRn(r.agent.GetChat())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parsing chat name: %v", err).Err()
		}
		getMessageRequest := &aiservicepb.GetMessageRequest{Name: chatRn.MessageRn(messageID).String()}
		existingMessage, err := r.service.aiServiceClient.GetMessage(ctx, getMessageRequest)
		if err != nil {
			return nil, status.FromError(err, "getting inputs message").Err()
		}
		deliveredInputs, _ := agentpb.Annotations.Inputs.Get(existingMessage)
		if err := r.consumeInputs(ctx, agentRn, strings.Split(deliveredInputs, ",")); err != nil {
			return nil, err
		}
	}
}

func (r *turnRunner) consumeInputs(ctx context.Context, agentRn *agentpb.AgentRn, inputNames []string) error {
	inputIDs := make([]string, 0, len(inputNames))
	for _, inputName := range inputNames {
		inputRn, err := agentpb.ParseInputRn(inputName)
		if err != nil {
			return status.Errorf(codes.Internal, "parsing input name: %v", err).Err()
		}
		inputIDs = append(inputIDs, inputRn.Input)
	}
	if err := r.service.agentPostgresStore.ConsumeInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, inputIDs, r.service.now()); err != nil {
		return status.FromError(err, "consuming inputs").Err()
	}
	return nil
}

// generate streams one assistant message, which the AiService persists, publishing its deltas and announcing its
// tool calls. A stopping replica cuts its generations short.
func (r *turnRunner) generate(ctx context.Context, toolbox *toolbox) (*aipb.Message, error) {
	select {
	case <-r.service.turns.draining:
		return nil, errShuttingDown
	default:
	}
	// The stream is left once the generated message arrives: cancel it then.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopDraining := r.service.onDraining(cancel)
	defer stopDraining()

	spec := r.agent.GetSpec()
	generateMessageRequest := &aiservicepb.GenerateMessageRequest{
		Parent:   r.agent.GetChat(),
		Model:    spec.GetModel(),
		Tools:    toolbox.tools,
		ToolSets: toolbox.toolSets,
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			ReasoningEffort:        spec.GetReasoningEffort(),
			StreamPartialToolCalls: true,
		},
	}
	var streamed bool
	generatedMessage, err := r.streamGeneration(ctx, generateMessageRequest, &streamed)
	if err == nil {
		return generatedMessage, nil
	}
	if streamed {
		r.publish(&agentservicepb.StreamAgentResponse{
			Content: &agentservicepb.StreamAgentResponse_GenerationReset_{GenerationReset: &agentservicepb.StreamAgentResponse_GenerationReset{}},
		})
	}
	select {
	case <-r.service.turns.draining:
		return nil, errShuttingDown
	default:
		return nil, status.FromError(err, "generating message").Err()
	}
}

func (r *turnRunner) streamGeneration(ctx context.Context, request *aiservicepb.GenerateMessageRequest, streamed *bool) (*aipb.Message, error) {
	stream, err := r.service.aiServiceClient.StreamGenerateMessage(ctx, request)
	if err != nil {
		return nil, err
	}
	announcedToolCallIDSet := map[string]struct{}{}
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
			*streamed = true
			r.publish(&agentservicepb.StreamAgentResponse{
				Content: &agentservicepb.StreamAgentResponse_Delta{Delta: content.Block},
			})
			r.announceToolCall(content.Block, announcedToolCallIDSet)
		case *aiservicepb.StreamGenerateMessageResponse_GeneratedMessage:
			r.publishMessage(content.GeneratedMessage)
			return content.GeneratedMessage, nil
		}
	}
}

// announceToolCall announces a streamed tool call once its title is complete: once its arguments hold another
// field after the title, generated first, or once the call is whole.
func (r *turnRunner) announceToolCall(block *aipb.Block, announcedToolCallIDSet map[string]struct{}) {
	toolCall := block.GetToolCall()
	if toolCall == nil {
		toolCall = block.GetPartialToolCall()
		if toolCall == nil || len(toolCall.GetArguments().GetFields()) < 2 {
			return
		}
	}
	if _, ok := announcedToolCallIDSet[toolCall.GetId()]; ok {
		return
	}
	announcedToolCallIDSet[toolCall.GetId()] = struct{}{}
	title, _ := pbjson.GetTitle(toolCall.GetArguments().AsMap())
	r.publish(&agentservicepb.StreamAgentResponse{
		Content: &agentservicepb.StreamAgentResponse_ToolCall_{ToolCall: &agentservicepb.StreamAgentResponse_ToolCall{
			Id:    toolCall.GetId(),
			Name:  toolCall.GetName(),
			Title: title,
		}},
	})
}

// answerToolCalls runs a generation's tool calls concurrently, persisting each result as soon as its call returns,
// one write at a time, and returns the last result's message. Calls the AiService answered are fed back verbatim.
// A cancelled turn answers its unfinished calls as cancelled; a stopped one leaves them to its next replica.
func (r *turnRunner) answerToolCalls(ctx context.Context, toolbox *toolbox, calls []*aipb.ToolCall) (*aipb.Message, error) {
	type indexedToolResult struct {
		index      int
		toolResult *aipb.ToolResult
	}
	// Buffered so calls returning after the turn stopped never block.
	indexedToolResults := make(chan indexedToolResult, len(calls))
	for i, toolCall := range calls {
		go func() {
			toolResult := toolCall.GetResult()
			if toolResult == nil {
				toolResult = toolbox.call(ctx, toolCall)
				// A call failing as the turn is cancelled failed because of it.
				if toolResult.GetError() != nil && ctx.Err() != nil {
					toolResult = ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), errToolCallCancelled)
				}
			}
			indexedToolResults <- indexedToolResult{index: i, toolResult: toolResult}
		}()
	}

	answeredSet := map[int]struct{}{}
	var last *aipb.Message
	for len(answeredSet) < len(calls) {
		select {
		case result := <-indexedToolResults:
			// A turn stopped while the call ran must not answer it: its next replica does.
			if ctx.Err() != nil && !errors.Is(context.Cause(ctx), errTurnCancelled) {
				return nil, ctx.Err()
			}
			message, err := r.persistToolResult(context.WithoutCancel(ctx), result.toolResult)
			if err != nil {
				return nil, err
			}
			answeredSet[result.index] = struct{}{}
			last = message
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), errTurnCancelled) {
				return nil, ctx.Err()
			}
			// Calls that returned as the turn was cancelled keep their results.
			for len(indexedToolResults) > 0 {
				result := <-indexedToolResults
				if _, err := r.persistToolResult(context.WithoutCancel(ctx), result.toolResult); err != nil {
					return nil, err
				}
				answeredSet[result.index] = struct{}{}
			}
			for i, toolCall := range calls {
				if _, ok := answeredSet[i]; ok {
					continue
				}
				toolResult := ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), errToolCallCancelled)
				if _, err := r.persistToolResult(context.WithoutCancel(ctx), toolResult); err != nil {
					return nil, err
				}
			}
			return nil, ctx.Err()
		}
	}
	return last, nil
}

// persistToolResult persists a tool result as its own tool message, idempotently, and announces the call's end.
func (r *turnRunner) persistToolResult(ctx context.Context, toolResult *aipb.ToolResult) (*aipb.Message, error) {
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    r.agent.GetChat(),
		Message:   ai.NewToolMessage(ai.NewToolResultBlock(toolResult)),
		RequestId: uuid.NewV5(toolResultNamespace, r.agent.GetChat()+"/"+toolResult.GetToolCallId()).String(),
	}
	message, err := r.service.aiServiceClient.CreateMessage(ctx, createMessageRequest)
	if err != nil {
		return nil, status.FromError(err, "creating tool result message").Err()
	}
	r.publish(&agentservicepb.StreamAgentResponse{
		Content: &agentservicepb.StreamAgentResponse_ToolCallEnded_{ToolCallEnded: &agentservicepb.StreamAgentResponse_ToolCallEnded{
			Id:     toolResult.GetToolCallId(),
			Failed: toolResult.GetError() != nil,
		}},
	})
	r.publishMessage(message)
	return message, nil
}

func (r *turnRunner) publishMessage(message *aipb.Message) {
	r.publish(&agentservicepb.StreamAgentResponse{
		Content: &agentservicepb.StreamAgentResponse_Message{Message: message},
	})
}

// publish publishes a piece of the agent's activity.
func (r *turnRunner) publish(response *agentservicepb.StreamAgentResponse) {
	response.Agent = r.agent.GetName()
	r.service.hub.publish(response)
}

// onDraining calls fn once the replica starts shutting down, until the returned stop is called.
func (s *Service) onDraining(fn func()) func() {
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.turns.draining:
			fn()
		case <-stop:
		}
	}()
	return func() { close(stop) }
}

// toolCalls returns the tool calls of a message.
func toolCalls(message *aipb.Message) []*aipb.ToolCall {
	var calls []*aipb.ToolCall
	for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolCall) {
		calls = append(calls, block.GetToolCall())
	}
	return calls
}

// messageText returns the text of a message's text blocks.
func messageText(message *aipb.Message) string {
	var texts []string
	for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeText) {
		texts = append(texts, block.GetText())
	}
	return strings.Join(slices.DeleteFunc(texts, func(text string) bool { return text == "" }), "\n\n")
}
