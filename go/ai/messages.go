package ai

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewSystemMessage(m *aipb.SystemMessage) *aipb.Message {
	return &aipb.Message{
		CreateTime: timestamppb.Now(),
		Role:       aipb.Role_ROLE_SYSTEM,
		Message: &aipb.Message_System{
			System: m,
		},
	}
}

func NewUserMessage(m *aipb.UserMessage) *aipb.Message {
	return &aipb.Message{
		CreateTime: timestamppb.Now(),
		Role:       aipb.Role_ROLE_USER,
		Message: &aipb.Message_User{
			User: m,
		},
	}
}

func NewAssistantMessage(m *aipb.AssistantMessage) *aipb.Message {
	return &aipb.Message{
		CreateTime: timestamppb.Now(),
		Role:       aipb.Role_ROLE_ASSISTANT,
		Message: &aipb.Message_Assistant{
			Assistant: m,
		},
	}
}

func NewToolResultMessage(m *aipb.ToolResultMessage) *aipb.Message {
	return &aipb.Message{
		CreateTime: timestamppb.Now(),
		Role:       aipb.Role_ROLE_TOOL,
		Message: &aipb.Message_Tool{
			Tool: m,
		},
	}
}

func NewToolResult(content string) *aipb.ToolResult {
	return &aipb.ToolResult{
		Result: &aipb.ToolResult_Content{
			Content: content,
		},
	}
}

func NewStructuredToolResult(content *structpb.Value) *aipb.ToolResult {
	return &aipb.ToolResult{
		Result: &aipb.ToolResult_StructuredContent{
			StructuredContent: content,
		},
	}
}

func NewErrorToolResult(err string) *aipb.ToolResult {
	return &aipb.ToolResult{
		Result: &aipb.ToolResult_Error{
			Error: err,
		},
	}
}
