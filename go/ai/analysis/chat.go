// Package analysis holds the analyses of chats.
package analysis

import (
	"context"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/analysis"
)

// Clients are what chat analyses reach.
type Clients struct {
	AI aiservicepb.AiServiceClient
}

// Chat adapts chats for analysis.
var Chat = analysis.Resource[*aipb.Chat, Clients]{
	Get: func(ctx context.Context, clients Clients, name string) (*aipb.Chat, error) {
		return clients.AI.GetChat(ctx, &aiservicepb.GetChatRequest{Name: name})
	},
	Update: func(ctx context.Context, clients Clients, chat *aipb.Chat, updateMask *fieldmaskpb.FieldMask) (*aipb.Chat, error) {
		return clients.AI.UpdateChat(ctx, &aiservicepb.UpdateChatRequest{Chat: chat, UpdateMask: updateMask})
	},
}
