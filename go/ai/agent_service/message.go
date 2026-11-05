package agent_service

import (
	"context"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

const (
	// maxQueuedMessageBytes bounds a queued message: large data belongs behind a URL, not inline in the agent's row.
	maxQueuedMessageBytes = 256 << 10
	// maxQueuedMessages and maxQueueBytes bound an agent's queue, which its turns drain.
	maxQueuedMessages = 100
	maxQueueBytes     = 1 << 20
)

// queuedMessageNamespace derives the id of a queued message from its request id: a replayed request finds the
// message it queued, and its delivery writes the chat message of that id once.
var queuedMessageNamespace = uuid.MustParse("4c7e2a91-58d3-4b6f-9e0a-1f3b6d8c2e57")

// MessageAgent queues a message from the user owning an agent on the agent, waking it if idle.
func (s *Service) MessageAgent(ctx context.Context, request *agentservicepb.MessageAgentRequest) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	if request.Message.GetRole() != aipb.Role_ROLE_USER {
		return nil, status.Errorf(codes.InvalidArgument, "message must be a USER message").Err()
	}
	// The user owning the agent is the principal its authorization admits.
	user := agentRn.UserRn().String()
	if sender, ok := agentpb.Annotations.Sender.Get(request.Message); ok && sender != user {
		return nil, status.Errorf(codes.InvalidArgument, "the message's sender %s is not the user owning the agent, %s", sender, user).Err()
	}
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	return s.messageAgent(ctx, request.Name, request.Message, user, requestID)
}

// messageAgent queues a message from a sender on an agent, claiming its turn if idle. A replayed request id finds the
// message already queued, or already delivered, and queues nothing.
func (s *Service) messageAgent(ctx context.Context, name string, message *aipb.Message, sender, requestID string) (*agentpb.Agent, error) {
	message = proto.CloneOf(message)
	agentpb.Annotations.Sender.Set(message, sender)
	if size := proto.Size(message); size > maxQueuedMessageBytes {
		return nil, status.Errorf(codes.InvalidArgument, "the message is %d bytes, over the %d a message may hold: reference large data by URL instead of inlining it", size, maxQueuedMessageBytes).Err()
	}
	var agent *agentpb.Agent
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		var err error
		if agent, err = t.getLiveAgent(ctx, name); err != nil {
			return err
		}
		return t.queueMessage(ctx, agent, message, requestID)
	}); err != nil {
		return nil, err
	}
	return agent, nil
}

// queueMessage appends a message to the queue of an agent locked by getAgent, named after the chat message its
// delivery writes, and claims the agent's turn if idle. A message already queued is not queued again.
func (t *txn) queueMessage(ctx context.Context, agent *agentpb.Agent, message *aipb.Message, requestID string) error {
	chatRn, err := aipb.ParseChatRn(agent.Chat)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing chat name: %v", err).Err()
	}
	message.Name = chatRn.MessageRn(aip.NewDeterministicBase32ResourceID(queuedMessageNamespace, requestID)).String()
	if slices.ContainsFunc(agent.QueuedMessages, func(queuedMessage *aipb.Message) bool { return queuedMessage.Name == message.Name }) {
		return nil
	}
	if len(agent.QueuedMessages) >= maxQueuedMessages {
		return status.Errorf(codes.ResourceExhausted, "agent %s already queues %d messages: wait for its turn to deliver them", agent.Name, len(agent.QueuedMessages)).Err()
	}
	queueBytes := proto.Size(message)
	for _, queuedMessage := range agent.QueuedMessages {
		queueBytes += proto.Size(queuedMessage)
	}
	if queueBytes > maxQueueBytes {
		return status.Errorf(codes.ResourceExhausted, "agent %s queues messages over %d bytes: wait for its turn to deliver them", agent.Name, maxQueueBytes).Err()
	}
	message.CreateTime = timestamppb.New(t.now)
	agent.QueuedMessages = append(agent.QueuedMessages, message)
	if agent.State == agentpb.Agent_STATE_IDLE {
		return t.claimTurn(ctx, agent)
	}
	// A running turn delivers the message itself: its end checks the queue under this same row lock.
	return t.saveAgent(ctx, agent)
}

// sendAgentMessage queues a text message from an agent on another.
func (s *Service) sendAgentMessage(ctx context.Context, sender *agentpb.Agent, recipient, text, requestID string) error {
	_, err := s.messageAgent(ctx, recipient, ai.NewUserMessage(ai.NewTextBlock(text)), sender.GetName(), requestID)
	return err
}
