package analysis

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/analysis"
)

// FetchMessages lists a chat's live messages, oldest first.
func FetchMessages(ctx context.Context, clients Clients, chat *aipb.Chat) ([]*aipb.Message, error) {
	request := &aiservicepb.ListMessagesRequest{
		Parent:  chat.GetName(),
		Filter:  fmt.Sprintf("-status:* AND -labels.%q:*", aipb.Labels.Superseded.GetKey()),
		OrderBy: "create_time asc",
	}
	messages, err := aip.Paginate[*aipb.Message](ctx, request, clients.AI.ListMessages)
	if err != nil {
		return nil, fmt.Errorf("listing messages: %w", err)
	}
	return messages, nil
}

// TranscriptInput is the transcript view's input.
func TranscriptInput(ctx context.Context, clients Clients, chat *aipb.Chat) ([]*aipb.Block, error) {
	messages, err := FetchMessages(ctx, clients, chat)
	if err != nil {
		return nil, err
	}
	return Transcript(messages)
}

var roleToSpeaker = map[aipb.Role]string{
	aipb.Role_ROLE_SYSTEM:    "SYSTEM",
	aipb.Role_ROLE_USER:      "USER",
	aipb.Role_ROLE_ASSISTANT: "ASSISTANT",
	aipb.Role_ROLE_TOOL:      "TOOL",
}

// Transcript renders the conversation as the assistant saw it, thoughts left out. Pure.
func Transcript(messages []*aipb.Message) ([]*aipb.Block, error) {
	var builder strings.Builder
	builder.WriteString("# Transcript\n\n")
	for _, message := range messages {
		var lines []string
		for _, block := range message.GetBlocks() {
			line, err := renderBlock(block)
			if err != nil {
				return nil, err
			}
			if line != "" {
				lines = append(lines, line)
			}
		}
		if len(lines) == 0 {
			continue
		}
		fmt.Fprintf(&builder, "%s:\n%s\n\n", roleToSpeaker[message.GetRole()], strings.Join(lines, "\n"))
	}
	return []*aipb.Block{analysis.TextBlock(strings.TrimSpace(builder.String()))}, nil
}

func renderBlock(block *aipb.Block) (string, error) {
	switch content := block.GetContent().(type) {
	case *aipb.Block_Text:
		return content.Text, nil
	case *aipb.Block_ToolCall:
		arguments, err := renderJSON(content.ToolCall.GetArguments())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[calls %s %s]", content.ToolCall.GetName(), arguments), nil
	case *aipb.Block_ToolResult:
		result, err := renderToolResult(content.ToolResult)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[%s returned %s]", content.ToolResult.GetToolName(), result), nil
	case *aipb.Block_Image:
		return "[image]", nil
	case *aipb.Block_Document:
		return "[document]", nil
	default:
		return "", nil
	}
}

func renderToolResult(toolResult *aipb.ToolResult) (string, error) {
	switch result := toolResult.GetResult().(type) {
	case *aipb.ToolResult_Content:
		return result.Content, nil
	case *aipb.ToolResult_StructuredContent:
		return renderJSON(result.StructuredContent)
	case *aipb.ToolResult_Error:
		return "error: " + result.Error.GetMessage(), nil
	default:
		return "", nil
	}
}

// renderJSON is compact, as protojson's whitespace varies by binary.
func renderJSON(message proto.Message) (string, error) {
	text, err := analysis.StableJSON(message)
	if err != nil {
		return "", err
	}
	return strings.Join(strings.Fields(text), " "), nil
}
