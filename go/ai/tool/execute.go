package tool

import (
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

// ExecuteToolName is the proxy tool handed to models whose provider constrains
// tool call names to the declared tool list (see TttModelConfig.strict_tool_names).
const ExecuteToolName = "execute_tool"

func CreateExecuteTool() *aipb.Tool {
	return &aipb.Tool{
		Name: ExecuteToolName,
		Description: "Execute a tool discovered through a discovery tool. " +
			"Discovered tools are not listed directly: call them by passing their name and arguments here, " +
			"following the schema returned by the discovery tool.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"name": {
					Type:        "string",
					Description: "Name of the discovered tool to execute",
				},
				"arguments": {
					Type:        "object",
					Description: "Arguments for the tool, matching its discovered schema",
				},
			},
			Required: []string{"name", "arguments"},
		},
		Annotations: map[string]string{
			aipb.Annotations.ToolType.Key: ToolTypeExecute,
		},
	}
}

// UnwrapExecuteToolCall rewrites an execute_tool call in place into a call to
// the tool it names. Returns false while a partial call has not yet produced
// the target name, in which case there is nothing to forward.
func UnwrapExecuteToolCall(toolCall *aipb.ToolCall) bool {
	fields := toolCall.GetArguments().GetFields()
	name := fields["name"].GetStringValue()
	if name == "" {
		return false
	}
	arguments := fields["arguments"].GetStructValue()
	if arguments == nil {
		arguments = &structpb.Struct{}
	}
	toolCall.Name = name
	toolCall.Arguments = arguments
	return true
}
