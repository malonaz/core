package ai_service

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/gengo/ai/model"
	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// mutateChatMaxAttempts bounds the optimistic-lock retries of mutateChat.
const mutateChatMaxAttempts = 3

var roleToMessageCountColumn = map[aipb.Role]string{
	aipb.Role_ROLE_SYSTEM:    "system_message_count",
	aipb.Role_ROLE_ASSISTANT: "assistant_message_count",
	aipb.Role_ROLE_USER:      "user_message_count",
	aipb.Role_ROLE_TOOL:      "tool_message_count",
}

// CreateMessage keeps the parent chat's per-role live message count in step.
func (s *Service) CreateMessage(ctx context.Context, request *pb.CreateMessageRequest) (*aipb.Message, error) {
	message, err := s.AiServiceServer.CreateMessage(ctx, request)
	if err != nil || request.GetValidateOnly() {
		return message, err
	}
	return message, s.incrementChatMessageCount(ctx, message, 1)
}

// DeleteMessage keeps the parent chat's per-role live message count in step.
func (s *Service) DeleteMessage(ctx context.Context, request *pb.DeleteMessageRequest) (*aipb.Message, error) {
	startTime := time.Now()
	message, err := s.AiServiceServer.DeleteMessage(ctx, request)
	if err != nil {
		return nil, err
	}
	// With allow_missing, an already-deleted message is returned untouched: only
	// a deletion stamped by this call moves the count.
	if message.GetDeleteTime().AsTime().Before(startTime) {
		return message, nil
	}
	return message, s.incrementChatMessageCount(ctx, message, -1)
}

// UndeleteMessage keeps the parent chat's per-role live message count in step.
func (s *Service) UndeleteMessage(ctx context.Context, request *pb.UndeleteMessageRequest) (*aipb.Message, error) {
	message, err := s.AiServiceServer.UndeleteMessage(ctx, request)
	if err != nil {
		return nil, err
	}
	return message, s.incrementChatMessageCount(ctx, message, 1)
}

func (s *Service) incrementChatMessageCount(ctx context.Context, message *aipb.Message, delta int32) error {
	column, ok := roleToMessageCountColumn[message.GetRole()]
	if !ok {
		return status.Errorf(codes.Internal, "no message count column for role %s", message.GetRole()).Err()
	}
	messageRn, err := aipb.ParseMessageRn(message.GetName())
	if err != nil {
		return status.Errorf(codes.Internal, "parsing message name: %v", err).Err()
	}
	if _, err := s.aiPostgresStore.IncrementChatMessageCount(ctx, messageRn.Organization, messageRn.User, messageRn.Chat, column, delta); err != nil {
		if errors.Is(err, model.ErrChatNotExist) {
			return status.Errorf(codes.NotFound, "chat does not exist").Err()
		}
		return status.FromError(err, "incrementing chat message count").Err()
	}
	return nil
}

// mutateChat applies mutate to a fresh read of the chat and persists the
// returned paths. Reading right before writing (instead of reusing a snapshot
// taken earlier) lets read-modify-write fields such as `price` survive
// concurrent chat writes; an Aborted etag race is retried from a new read.
func (s *Service) mutateChat(ctx context.Context, name string, mutate func(*aipb.Chat) []string) (*aipb.Chat, error) {
	for attempt := 1; ; attempt++ {
		getChatRequest := &pb.GetChatRequest{Name: name}
		chat, err := s.GetChat(ctx, getChatRequest)
		if err != nil {
			return nil, err
		}
		updateChatRequest := &pb.UpdateChatRequest{
			Chat:       chat,
			UpdateMask: pbfieldmask.FromPaths(mutate(chat)...).Proto(),
		}
		updatedChat, err := s.UpdateChat(ctx, updateChatRequest)
		if err == nil {
			return updatedChat, nil
		}
		if !status.HasCode(err, codes.Aborted) || attempt == mutateChatMaxAttempts {
			return nil, err
		}
	}
}
