package agent_service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
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
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/uuid"
)

// instructionsMessageID is the fixed id of an agent's system message, so every turn can ensure it without
// duplicating it.
const instructionsMessageID = "instructions"

var (
	// deliveryMessageNamespace derives the id of the message delivering queued messages from the oldest of them, so a
	// delivery retried after a crash is persisted once.
	deliveryMessageNamespace = uuid.MustParse("31a36018-4937-45ed-843c-19bb447034bf")
	// toolResultNamespace derives a tool result's request id from its call.
	toolResultNamespace = uuid.MustParse("0d7c4b0e-5f2a-4c39-9b57-2a8f3e61c4d1")
	// reportNamespace derives the request id of a turn's report to the parent from the turn's final message.
	reportNamespace = uuid.MustParse("e3b54a8c-1f6d-4e2b-a7c0-94d2f8b1e365")

	errToolCallInterrupted = errors.New("interrupted by a server restart; the call may or may not have completed")
	errToolCallCancelled   = errors.New("cancelled: the turn was cancelled before this call completed; it may or may not have run")
)

// pendingFilter lists the messages of an agent's chat queued for its next delivery.
var pendingFilter = fmt.Sprintf("labels.%q:*", agentpb.Labels.Pending.GetKey())

// runTurns runs the agent's turns while messages keep arriving, until it goes idle or its run stops.
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
	runner := &turnRunner{service: s, agent: agent}
	var finalMessage *aipb.Message
	if err == nil {
		finalMessage, err = runner.run(ctx)
	}
	turnEnd := &turnEnd{deliveredCount: runner.deliveredCount, delivered: runner.delivered}
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, errLeaseLost), errors.Is(cause, errShuttingDown), errors.Is(err, errShuttingDown):
		return nil
	case errors.Is(cause, errTurnCancelled):
		turnEnd.outcome = agentpb.Agent_TURN_OUTCOME_CANCELLED
		turnEnd.report = "My turn was cancelled."
	case err != nil:
		s.log.Warn("turn failed", "agent", turn.agent, "error", err)
		turnEnd.outcome = agentpb.Agent_TURN_OUTCOME_FAILED
		turnEnd.err = err
		turnEnd.report = fmt.Sprintf("My turn failed: %s", grpcstatus.Convert(err).Message())
	default:
		turnEnd.outcome = agentpb.Agent_TURN_OUTCOME_SUCCEEDED
		if finalMessage != nil {
			turnEnd.report = cmp.Or(messageText(finalMessage), "My turn ended without a reply.")
		}
	}
	turnEnd.reportRequestID = runner.reportRequestID(turnEnd.outcome)
	return turnEnd
}

// endTurn sends the turn's report to the agent's parent, if any, then records how the turn ended under the
// agent's lock. If messages were sent since the turn's latest delivery, a succeeded turn keeps the agent RUNNING and
// returns true: the next turn follows. Otherwise the agent goes IDLE and its lease is released. The report goes first:
// a turn ended without its report would leave the parent waiting, while a report sent again is deduped by its request
// id, derived from the turn's final message.
func (s *Service) endTurn(ctx context.Context, name string, turnEnd *turnEnd) (bool, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return false, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	if err := s.report(ctx, name, turnEnd); err != nil {
		return false, err
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
		mailbox, err := t.GetMailbox(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return fmt.Errorf("getting mailbox: %w", err)
		}
		deliveredCount := mailbox.DeliveredCount
		if turnEnd.delivered {
			deliveredCount = turnEnd.deliveredCount
			if err := t.SetDeliveredCount(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, deliveredCount); err != nil {
				return fmt.Errorf("setting delivered count: %w", err)
			}
		}
		next = turnEnd.outcome == agentpb.Agent_TURN_OUTCOME_SUCCEEDED && mailbox.SentCount > deliveredCount
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
		return t.saveAgent(ctx, agent)
	})
	return next, err
}

// report sends a turn's report to the agent's parent, so a parent never waits forever on its sub-agents.
func (s *Service) report(ctx context.Context, name string, turnEnd *turnEnd) error {
	if turnEnd.report == "" {
		return nil
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: name}
	agent, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return err
	}
	if agent.ParentAgent == "" {
		return nil
	}
	message := ai.NewUserMessage(ai.NewTextBlock(turnEnd.report))
	agentpb.Annotations.Sender.Set(message, agent.Name)
	// A deleted parent awaits no report.
	if _, err := s.messageAgent(ctx, agent.ParentAgent, message, turnEnd.reportRequestID); err != nil && !status.HasCode(err, codes.NotFound) {
		return status.FromError(err, "sending the turn's report").Err()
	}
	return nil
}

// turnRunner runs one turn of an agent.
type turnRunner struct {
	service *Service
	agent   *agentpb.Agent
	// The agent's sent message count read before the turn's latest delivery, once it delivered.
	deliveredCount int64
	delivered      bool
	// The chat's last message the turn saw or wrote.
	last *aipb.Message
}

// reportRequestID derives the request id of the turn's report from how it ended and the chat's last message it saw,
// which a replay of the turn after a crash sees again: a report is never sent twice. A turn that saw no message falls
// back on the agent's etag as the turn read it, which only the turn's end changes.
func (r *turnRunner) reportRequestID(outcome agentpb.Agent_TurnOutcome) string {
	key := r.agent.GetEtag()
	if r.last != nil {
		key = r.last.GetName()
	}
	return uuid.NewV5(reportNamespace, outcome.String()+"/"+key).String()
}

// run runs the turn until the model answers without calling tools, and returns that final message: nil when the
// chat holds nothing to answer.
func (r *turnRunner) run(ctx context.Context) (*aipb.Message, error) {
	if err := r.ensureInstructions(ctx); err != nil {
		return nil, err
	}
	if err := r.resume(ctx); err != nil {
		return nil, err
	}
	toolbox, err := r.service.newToolbox(ctx, r.agent)
	if err != nil {
		return nil, err
	}
	for {
		if err := r.deliverMessages(ctx); err != nil {
			return nil, err
		}
		switch {
		case r.last == nil:
			return nil, nil
		case r.last.GetRole() == aipb.Role_ROLE_ASSISTANT && len(toolCalls(r.last)) == 0:
			return r.last, nil
		}
		generatedMessage, err := r.generate(ctx, toolbox)
		if err != nil {
			return nil, err
		}
		r.last = generatedMessage
		if calls := toolCalls(generatedMessage); len(calls) > 0 {
			if err := r.answerToolCalls(ctx, toolbox, calls); err != nil {
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

// resume reads the end of the chat, where a turn interrupted on another replica left off, and answers the tool calls
// it left without a result.
func (r *turnRunner) resume(ctx context.Context) error {
	listMessagesRequest := &aiservicepb.ListMessagesRequest{
		Parent:  r.agent.GetChat(),
		Filter:  ai.HistoryFilter,
		OrderBy: "create_time desc",
	}
	// The messages after the latest assistant message, newest first: only its tool calls may lack results.
	var tail []*aipb.Message
	var assistantMessage *aipb.Message
	for message, err := range aip.Iterator[*aipb.Message](ctx, listMessagesRequest, r.service.aiServiceClient.ListMessages) {
		if err != nil {
			return status.FromError(err, "listing messages").Err()
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
	r.last = assistantMessage
	if len(tail) > 0 {
		r.last = tail[0]
	}
	if assistantMessage == nil {
		return nil
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
		if err := r.persistToolResult(ctx, toolResult); err != nil {
			return err
		}
	}
	return nil
}

// deliverMessages delivers the messages queued for the agent as one user message at the chat's end, listing them in
// its messages annotation, then un-pends them. The sent count is read first: a message sent after it is counted after
// it, and the turn's end sees it.
func (r *turnRunner) deliverMessages(ctx context.Context) error {
	agentRn, err := agentpb.ParseAgentRn(r.agent.GetName())
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	chatRn, err := aipb.ParseChatRn(r.agent.GetChat())
	if err != nil {
		return status.Errorf(codes.Internal, "parsing chat name: %v", err).Err()
	}
	for {
		sentCount, err := r.service.agentPostgresStore.GetSentCount(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return status.FromError(err, "getting sent count").Err()
		}
		listMessagesRequest := &aiservicepb.ListMessagesRequest{
			Parent:  r.agent.GetChat(),
			Filter:  pendingFilter,
			OrderBy: "create_time asc",
		}
		pendingMessages, err := aip.Paginate[*aipb.Message](ctx, listMessagesRequest, r.service.aiServiceClient.ListMessages)
		if err != nil {
			return status.FromError(err, "listing pending messages").Err()
		}
		if len(pendingMessages) == 0 {
			r.deliveredCount, r.delivered = sentCount, true
			return nil
		}

		messageNames := make([]string, 0, len(pendingMessages))
		var blocks []*aipb.Block
		senderToHeader := map[string]string{}
		for _, pendingMessage := range pendingMessages {
			messageNames = append(messageNames, pendingMessage.GetName())
			header, err := r.senderHeader(ctx, pendingMessage, senderToHeader)
			if err != nil {
				return err
			}
			if header != "" {
				blocks = append(blocks, ai.NewTextBlock(header))
			}
			blocks = append(blocks, pendingMessage.GetBlocks()...)
		}
		message := ai.NewUserMessage(blocks...)
		agentpb.Annotations.Messages.Set(message, strings.Join(messageNames, ","))
		messageID := aip.NewDeterministicBase32ResourceID(deliveryMessageNamespace, messageNames[0])
		createMessageRequest := &aiservicepb.CreateMessageRequest{
			Parent:    r.agent.GetChat(),
			Message:   message,
			MessageId: messageID,
		}
		deliveredMessage, err := r.service.aiServiceClient.CreateMessage(ctx, createMessageRequest)
		if err == nil {
			r.publishMessage(deliveredMessage)
			if err := r.unpend(ctx, pendingMessages); err != nil {
				return err
			}
			r.deliveredCount, r.delivered = sentCount, true
			r.last = deliveredMessage
			return nil
		}
		if !status.HasCode(err, codes.AlreadyExists) {
			return status.FromError(err, "creating delivery message").Err()
		}

		// Delivered before a crash left its messages pending: un-pend what it delivered, then deliver the rest.
		getMessageRequest := &aiservicepb.GetMessageRequest{Name: chatRn.MessageRn(messageID).String()}
		existingMessage, err := r.service.aiServiceClient.GetMessage(ctx, getMessageRequest)
		if err != nil {
			return status.FromError(err, "getting delivery message").Err()
		}
		deliveredMessageNames, _ := agentpb.Annotations.Messages.Get(existingMessage)
		deliveredMessageNameSet := map[string]struct{}{}
		for _, deliveredMessageName := range strings.Split(deliveredMessageNames, ",") {
			deliveredMessageNameSet[deliveredMessageName] = struct{}{}
		}
		var deliveredMessages []*aipb.Message
		for _, pendingMessage := range pendingMessages {
			if _, ok := deliveredMessageNameSet[pendingMessage.GetName()]; ok {
				deliveredMessages = append(deliveredMessages, pendingMessage)
			}
		}
		if err := r.unpend(ctx, deliveredMessages); err != nil {
			return err
		}
	}
}

// unpend removes the pending label from delivered messages, which stay excluded from the model's view.
func (r *turnRunner) unpend(ctx context.Context, messages []*aipb.Message) error {
	labelsMask := pbfieldmask.FromPaths("labels").MustValidate(&aipb.Message{}).Proto()
	for _, message := range messages {
		labels := maps.Clone(message.GetLabels())
		delete(labels, agentpb.Labels.Pending.GetKey())
		updateMessageRequest := &aiservicepb.UpdateMessageRequest{
			Message:    &aipb.Message{Name: message.GetName(), Labels: labels},
			UpdateMask: labelsMask,
		}
		updatedMessage, err := r.service.aiServiceClient.UpdateMessage(ctx, updateMessageRequest)
		if err != nil {
			return status.FromError(err, "un-pending message %s", message.GetName()).Err()
		}
		r.publishMessage(updatedMessage)
	}
	return nil
}

// senderHeader returns the header naming the agent that sent a message, empty for any other sender: the model sees
// only the blocks it is delivered, while the message keeps exactly what its sender said.
func (r *turnRunner) senderHeader(ctx context.Context, message *aipb.Message, senderToHeader map[string]string) (string, error) {
	sender, ok := agentpb.Annotations.Sender.Get(message)
	if !ok || !agentpb.MatchAgentRn(sender) {
		return "", nil
	}
	if header, ok := senderToHeader[sender]; ok {
		return header, nil
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: sender}
	senderAgent, err := r.service.GetAgent(ctx, getAgentRequest)
	var header string
	switch {
	case status.HasCode(err, codes.NotFound):
		header = fmt.Sprintf("[Message from agent %s]", sender)
	case err != nil:
		return "", status.FromError(err, "getting sender agent").Err()
	case senderAgent.GetParentAgent() == r.agent.GetName():
		header = fmt.Sprintf("[Report from sub-agent %s (%q)]", sender, senderAgent.GetTitle())
	default:
		header = fmt.Sprintf("[Message from agent %s (%q)]", sender, senderAgent.GetTitle())
	}
	senderToHeader[sender] = header
	return header, nil
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
	generatedMessage, streamed, err := r.streamGeneration(ctx, generateMessageRequest)
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

// streamGeneration streams a generation, reporting whether any of its deltas were published.
func (r *turnRunner) streamGeneration(ctx context.Context, request *aiservicepb.GenerateMessageRequest) (*aipb.Message, bool, error) {
	stream, err := r.service.aiServiceClient.StreamGenerateMessage(ctx, request)
	if err != nil {
		return nil, false, err
	}
	var streamed bool
	announcer := newToolCallAnnouncer()
	for {
		streamGenerateMessageResponse, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, streamed, status.Errorf(codes.Internal, "generation ended without a generated message").Err()
		}
		if err != nil {
			return nil, streamed, err
		}
		switch content := streamGenerateMessageResponse.GetContent().(type) {
		case *aiservicepb.StreamGenerateMessageResponse_Block:
			streamed = true
			r.publish(&agentservicepb.StreamAgentResponse{
				Content: &agentservicepb.StreamAgentResponse_Delta{Delta: content.Block},
			})
			if toolCall := announcer.complete(content.Block); toolCall != nil {
				r.announceToolCall(toolCall)
			}
		case *aiservicepb.StreamGenerateMessageResponse_GeneratedMessage:
			// Calls never streamed whole are announced with the message holding them.
			for _, toolCall := range toolCalls(content.GeneratedMessage) {
				if toolCall := announcer.complete(ai.NewToolCallBlock(toolCall)); toolCall != nil {
					r.announceToolCall(toolCall)
				}
			}
			r.publishMessage(content.GeneratedMessage)
			return content.GeneratedMessage, streamed, nil
		}
	}
}

// toolCallAnnouncer tells when a streamed tool call's title is complete, whatever order the model generates its
// arguments in: once the call is whole, or once another argument appeared after the title did.
type toolCallAnnouncer struct {
	announcedToolCallIDSet map[string]struct{}
	// The number of arguments a partial call held when its title first appeared.
	toolCallIDToTitleFieldCount map[string]int
}

func newToolCallAnnouncer() *toolCallAnnouncer {
	return &toolCallAnnouncer{announcedToolCallIDSet: map[string]struct{}{}, toolCallIDToTitleFieldCount: map[string]int{}}
}

// complete returns the block's tool call the first time its title is complete, nil otherwise.
func (a *toolCallAnnouncer) complete(block *aipb.Block) *aipb.ToolCall {
	toolCall := block.GetToolCall()
	if toolCall == nil {
		toolCall = block.GetPartialToolCall()
		if toolCall == nil || !a.partialTitleComplete(toolCall) {
			return nil
		}
	}
	if _, ok := a.announcedToolCallIDSet[toolCall.GetId()]; ok {
		return nil
	}
	a.announcedToolCallIDSet[toolCall.GetId()] = struct{}{}
	return toolCall
}

// partialTitleComplete reports whether a partial call's title is complete: a later argument streamed after it.
func (a *toolCallAnnouncer) partialTitleComplete(toolCall *aipb.ToolCall) bool {
	fieldCount := len(toolCall.GetArguments().GetFields())
	if _, ok := pbjson.GetTitle(toolCall.GetArguments().AsMap()); !ok {
		return false
	}
	titleFieldCount, ok := a.toolCallIDToTitleFieldCount[toolCall.GetId()]
	if !ok {
		a.toolCallIDToTitleFieldCount[toolCall.GetId()] = fieldCount
		return false
	}
	return fieldCount > titleFieldCount
}

// announceToolCall announces a tool call with its title.
func (r *turnRunner) announceToolCall(toolCall *aipb.ToolCall) {
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
// one write at a time. Calls the AiService answered are fed back verbatim. A cancelled turn answers its unfinished
// calls as cancelled; a stopped one leaves them to its next replica.
func (r *turnRunner) answerToolCalls(ctx context.Context, toolbox *toolbox, calls []*aipb.ToolCall) error {
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
	for len(answeredSet) < len(calls) {
		select {
		case result := <-indexedToolResults:
			// A turn stopped while the call ran must not answer it: its next replica does.
			if ctx.Err() != nil && !errors.Is(context.Cause(ctx), errTurnCancelled) {
				return ctx.Err()
			}
			if err := r.persistToolResult(context.WithoutCancel(ctx), result.toolResult); err != nil {
				return err
			}
			answeredSet[result.index] = struct{}{}
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), errTurnCancelled) {
				return ctx.Err()
			}
			// Calls that returned as the turn was cancelled keep their results.
			for len(indexedToolResults) > 0 {
				result := <-indexedToolResults
				if err := r.persistToolResult(context.WithoutCancel(ctx), result.toolResult); err != nil {
					return err
				}
				answeredSet[result.index] = struct{}{}
			}
			for i, toolCall := range calls {
				if _, ok := answeredSet[i]; ok {
					continue
				}
				toolResult := ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), errToolCallCancelled)
				if err := r.persistToolResult(context.WithoutCancel(ctx), toolResult); err != nil {
					return err
				}
			}
			return ctx.Err()
		}
	}
	return nil
}

// persistToolResult persists a tool result as its own tool message, idempotently, and announces the call's end.
func (r *turnRunner) persistToolResult(ctx context.Context, toolResult *aipb.ToolResult) error {
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    r.agent.GetChat(),
		Message:   ai.NewToolMessage(ai.NewToolResultBlock(toolResult)),
		RequestId: uuid.NewV5(toolResultNamespace, r.agent.GetChat()+"/"+toolResult.GetToolCallId()).String(),
	}
	message, err := r.service.aiServiceClient.CreateMessage(ctx, createMessageRequest)
	if err != nil {
		return status.FromError(err, "creating tool result message").Err()
	}
	r.publish(&agentservicepb.StreamAgentResponse{
		Content: &agentservicepb.StreamAgentResponse_ToolCallEnded_{ToolCallEnded: &agentservicepb.StreamAgentResponse_ToolCallEnded{
			Id:     toolResult.GetToolCallId(),
			Failed: toolResult.GetError() != nil,
		}},
	})
	r.publishMessage(message)
	r.last = message
	return nil
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
