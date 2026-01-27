package ai

import (
	"fmt"

	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type BlockType int

const (
	BlockTypeText BlockType = iota
	BlockTypeThought
	BlockTypeToolCall
	BlockTypeToolResult
	BlockTypeImage
)

func newMessage(role aipb.Role, blocks ...*aipb.Block) *aipb.Message {
	return &aipb.Message{
		CreateTime: timestamppb.Now(),
		Role:       role,
		Blocks:     blocks,
	}
}

func NewSystemMessage(blocks ...*aipb.Block) *aipb.Message {
	return newMessage(aipb.Role_ROLE_SYSTEM, blocks...)
}

func NewAssistantMessage(blocks ...*aipb.Block) *aipb.Message {
	return newMessage(aipb.Role_ROLE_ASSISTANT, blocks...)
}

func NewUserMessage(blocks ...*aipb.Block) *aipb.Message {
	return newMessage(aipb.Role_ROLE_USER, blocks...)
}

func NewToolMessage(blocks ...*aipb.Block) *aipb.Message {
	return newMessage(aipb.Role_ROLE_TOOL, blocks...)
}

func NewTextBlock(text string) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_Text{Text: text}}
}

func NewThoughtBlock(thought string) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_Thought{Thought: thought}}
}

func NewToolCallBlock(toolCall *aipb.ToolCall) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_ToolCall{ToolCall: toolCall}}
}

func NewToolResultBlock(toolResult *aipb.ToolResult) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_ToolResult{ToolResult: toolResult}}
}

func NewImageBlock(image *aipb.Image) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_Image{Image: image}}
}

func NewImageFromURL(url string) *aipb.Image {
	return &aipb.Image{Source: &aipb.Image_Url{Url: url}}
}

func NewImageFromData(data []byte, mediaType string) *aipb.Image {
	return &aipb.Image{
		Source:    &aipb.Image_Data{Data: data},
		MediaType: mediaType,
	}
}

func NewToolResult(toolName, toolCallID, content string) *aipb.ToolResult {
	return &aipb.ToolResult{
		ToolName:   toolName,
		ToolCallId: toolCallID,
		Result:     &aipb.ToolResult_Content{Content: content},
	}
}

func NewStructuredToolResult(toolName, toolCallID string, content *structpb.Value) *aipb.ToolResult {
	return &aipb.ToolResult{
		ToolName:   toolName,
		ToolCallId: toolCallID,
		Result:     &aipb.ToolResult_StructuredContent{StructuredContent: content},
	}
}

func NewErrorToolResult(toolName, toolCallID string, err error) *aipb.ToolResult {
	return &aipb.ToolResult{
		ToolName:   toolName,
		ToolCallId: toolCallID,
		Result:     &aipb.ToolResult_Error{Error: status.Convert(err).Proto()},
	}
}

func ParseToolResult(toolResult *aipb.ToolResult) (string, error) {
	switch r := toolResult.GetResult().(type) {
	case *aipb.ToolResult_Content:
		return r.Content, nil
	case *aipb.ToolResult_StructuredContent:
		bytes, err := pbutil.JSONMarshal(r.StructuredContent)
		if err != nil {
			return "", fmt.Errorf("marshaling tool result structured content: %v", err)
		}
		return string(bytes), nil
	case *aipb.ToolResult_Error:
		bytes, err := pbutil.JSONMarshal(r.Error)
		if err != nil {
			return "", fmt.Errorf("marshaling tool result error content: %v", err)
		}
		return string(bytes), nil
	default:
		return "", fmt.Errorf("unknown tool result type: %T", r)
	}
}

func GetBlocks(message *aipb.Message, blockTypes ...BlockType) []*aipb.Block {
	typeSet := make(map[BlockType]struct{}, len(blockTypes))
	for _, bt := range blockTypes {
		typeSet[bt] = struct{}{}
	}

	var blocks []*aipb.Block
	for _, block := range message.GetBlocks() {
		var bt BlockType
		switch block.Content.(type) {
		case *aipb.Block_Text:
			bt = BlockTypeText
		case *aipb.Block_Thought:
			bt = BlockTypeThought
		case *aipb.Block_ToolCall:
			bt = BlockTypeToolCall
		case *aipb.Block_ToolResult:
			bt = BlockTypeToolResult
		case *aipb.Block_Image:
			bt = BlockTypeImage
		}
		if _, ok := typeSet[bt]; ok {
			blocks = append(blocks, block)
		}
	}
	return blocks
}
