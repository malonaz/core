package ai

import (
	"fmt"

	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
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

func NewErrorToolResult(err error) *aipb.ToolResult {
	return &aipb.ToolResult{
		Result: &aipb.ToolResult_Error{
			Error: status.Convert(err).Proto(),
		},
	}
}

func ParseToolResult(toolResult *aipb.ToolResult) (string, error) {
	switch r := toolResult.GetResult().(type) {
	case *aipb.ToolResult_Content:
		return r.Content, nil
	case *aipb.ToolResult_StructuredContent:
		bytes, err := pbutil.JSONMarshal(r.StructuredContent)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	case *aipb.ToolResult_Error:
		bytes, err := pbutil.JSONMarshal(r.Error)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	default:
		return "", fmt.Errorf("unknown tool result type: %T", r)
	}
}
