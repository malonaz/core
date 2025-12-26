package ai

import (
	aiv1 "github.com/malonaz/core/genproto/ai/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewSystemMessage(content string) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_System{
			System: &aiv1.SystemMessage{
				Content: content,
			},
		},
	}
}

func NewUserMessage(content string) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_User{
			User: &aiv1.UserMessage{
				Content: content,
			},
		},
	}
}

func NewAssistantMessage(content string, toolCalls ...*aiv1.ToolCall) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_Assistant{
			Assistant: &aiv1.AssistantMessage{
				Content:   content,
				ToolCalls: toolCalls,
			},
		},
	}
}

func NewAssistantMessageWithStruct(structuredContent *structpb.Struct, toolCalls ...*aiv1.ToolCall) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_Assistant{
			Assistant: &aiv1.AssistantMessage{
				StructuredContent: structuredContent,
				ToolCalls:         toolCalls,
			},
		},
	}
}

func NewToolResultMessage(toolCallID, content string) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_Tool{
			Tool: &aiv1.ToolResultMessage{
				ToolCallId: toolCallID,
				Result: &aiv1.ToolResult{
					Result: &aiv1.ToolResult_Content{
						Content: content,
					},
				},
			},
		},
	}
}

func NewToolResultMessageWithStruct(toolCallID string, structuredContent *structpb.Struct) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_Tool{
			Tool: &aiv1.ToolResultMessage{
				ToolCallId: toolCallID,
				Result: &aiv1.ToolResult{
					Result: &aiv1.ToolResult_StructuredContent{
						StructuredContent: structuredContent,
					},
				},
			},
		},
	}
}

func NewToolResultMessageWithError(toolCallID, err string) *aiv1.Message {
	return &aiv1.Message{
		CreateTime: timestamppb.Now(),
		Message: &aiv1.Message_Tool{
			Tool: &aiv1.ToolResultMessage{
				ToolCallId: toolCallID,
				Result: &aiv1.ToolResult{
					Result: &aiv1.ToolResult_Error{
						Error: err,
					},
				},
			},
		},
	}
}
