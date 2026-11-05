package agent_service

import (
	"context"
	"fmt"
	"maps"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// queuedMessageNamespace derives the id of a queued message from its request id, so a replayed request finds the
// message it queued.
var queuedMessageNamespace = uuid.MustParse("4c7e2a91-58d3-4b6f-9e0a-1f3b6d8c2e57")

// MessageAgent queues a message in an agent's chat for its next turn, waking it if idle.
func (s *Service) MessageAgent(ctx context.Context, request *agentservicepb.MessageAgentRequest) (*aipb.Message, error) {
	if request.Message.GetRole() != aipb.Role_ROLE_USER {
		return nil, status.Errorf(codes.InvalidArgument, "message must be a USER message").Err()
	}
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	return s.messageAgent(ctx, request.Name, request.Message, requestID)
}

// messageAgent queues a message in an agent's chat, labelled pending and excluded from the model's view until a turn
// delivers it, then counts it and claims the agent's turn if idle. The message comes first: a count without its
// message would wake a turn with nothing to deliver, while a message without its count is counted on retry.
func (s *Service) messageAgent(ctx context.Context, name string, message *aipb.Message, requestID string) (*aipb.Message, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: name}
	agent, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime != nil {
		return nil, status.Errorf(codes.NotFound, "agent %s does not exist", name).Err()
	}
	queuedMessage, err := s.queueMessage(ctx, agent, message, requestID)
	if err != nil {
		return nil, err
	}

	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getLiveAgent(ctx, name)
		if err != nil {
			return err
		}
		if err := t.CountSentMessage(ctx, agentRn.Organization, agentRn.User, agentRn.Agent); err != nil {
			return fmt.Errorf("counting sent message: %w", err)
		}
		// A running turn delivers the message itself: its end checks the count under this same row lock.
		if agent.State == agentpb.Agent_STATE_IDLE {
			return t.claimTurn(ctx, agent)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	s.hub.publish(&agentservicepb.StreamAgentResponse{
		Agent:   name,
		Content: &agentservicepb.StreamAgentResponse_Message{Message: queuedMessage},
	})
	return queuedMessage, nil
}

// queueMessage creates a message in an agent's chat, pending and excluded, idempotently by request id.
func (s *Service) queueMessage(ctx context.Context, agent *agentpb.Agent, message *aipb.Message, requestID string) (*aipb.Message, error) {
	message = proto.CloneOf(message)
	labels := maps.Clone(message.GetLabels())
	if labels == nil {
		labels = map[string]string{}
	}
	labels[aipb.Labels.Excluded.GetKey()] = aip.LabelValueTrue
	labels[agentpb.Labels.Pending.GetKey()] = aip.LabelValueTrue
	message.Labels = labels
	messageID := aip.NewDeterministicBase32ResourceID(queuedMessageNamespace, requestID)
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    agent.GetChat(),
		Message:   message,
		MessageId: messageID,
		RequestId: requestID,
	}
	queuedMessage, err := s.aiServiceClient.CreateMessage(ctx, createMessageRequest)
	if err == nil {
		return queuedMessage, nil
	}
	if !status.HasCode(err, codes.AlreadyExists) {
		return nil, status.FromError(err, "queueing message").Err()
	}
	chatRn, err := aipb.ParseChatRn(agent.GetChat())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing chat name: %v", err).Err()
	}
	getMessageRequest := &aiservicepb.GetMessageRequest{Name: chatRn.MessageRn(messageID).String()}
	queuedMessage, err = s.aiServiceClient.GetMessage(ctx, getMessageRequest)
	if err != nil {
		return nil, status.FromError(err, "getting queued message").Err()
	}
	return queuedMessage, nil
}

// sendAgentMessage queues a text message from an agent on another, stamped with its sender.
func (s *Service) sendAgentMessage(ctx context.Context, sender *agentpb.Agent, recipient, text, requestID string) error {
	message := ai.NewUserMessage(ai.NewTextBlock(text))
	agentpb.Annotations.Sender.Set(message, sender.GetName())
	_, err := s.messageAgent(ctx, recipient, message, requestID)
	return err
}
