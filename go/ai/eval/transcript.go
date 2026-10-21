package eval

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
)

var roleToSpeaker = map[aipb.Role]string{
	aipb.Role_ROLE_SYSTEM:    "SYSTEM",
	aipb.Role_ROLE_USER:      "USER",
	aipb.Role_ROLE_ASSISTANT: "ASSISTANT",
	aipb.Role_ROLE_TOOL:      "TOOL",
}

// renderTranscript renders a conversation for the judge. The system prompt and tool traffic
// are kept, since criteria are often about following instructions; thoughts are not.
func renderTranscript(messages []*aipb.Message) (string, error) {
	var builder strings.Builder
	for _, message := range messages {
		var lines []string
		for _, block := range message.GetBlocks() {
			line, err := renderBlock(block)
			if err != nil {
				return "", err
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
	return strings.TrimSpace(builder.String()), nil
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

func renderJSON(message proto.Message) (string, error) {
	bytes, err := pbutil.JSONMarshal(message)
	if err != nil {
		return "", fmt.Errorf("rendering json: %w", err)
	}
	return string(bytes), nil
}
