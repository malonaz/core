package tool

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

// ExecuteToolName is the name of the generic execute tool.
const ExecuteToolName = "Execute"

// CreateExecuteTool returns the generic Execute tool. It is made available
// whenever at least one tool remains discoverable, so that discovered tools
// can be invoked without mutating the provider-visible tool list (which would
// break prompt caching).
func CreateExecuteTool() *aipb.Tool {
	return &aipb.Tool{
		Name:        ExecuteToolName,
		Description: "Invoke a tool previously discovered via a discovery tool. Discovered tools are ONLY callable through this tool; tools already in your tool list are ONLY callable directly — never copy this wrapper's shape onto a direct call.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"tool_name": {
					Type:        "string",
					Description: "Name of the discovered tool to execute",
				},
				"arguments": {
					Type:        "object",
					Description: "The discovered tool's request as a JSON object (never a JSON-encoded string), conforming exactly to its discovered schema",
				},
			},
			Required: []string{"tool_name", "arguments"},
		},
		Annotations: map[string]string{
			AnnotationKeyToolType: AnnotationValueToolTypeExecute,
		},
	}
}

// ParseExecuteToolCall parses an Execute tool call into its target tool name
// and arguments.
func ParseExecuteToolCall(toolCall *aipb.ToolCall) (*aipb.ToolCallExecute, error) {
	arguments := toolCall.GetArguments().AsMap()
	toolName, _ := arguments["tool_name"].(string)
	if toolName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "execute tool call missing tool_name").Err()
	}
	innerArgumentsMap, ok := arguments["arguments"].(map[string]any)
	if !ok {
		// Models sometimes emit the arguments as a JSON-encoded string.
		innerArgumentsMap, _ = decodeJSONObject(arguments["arguments"])
	}
	innerArguments, err := structpb.NewStruct(innerArgumentsMap)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing execute tool call arguments: %v", err).Err()
	}
	return &aipb.ToolCallExecute{
		ToolName:  toolName,
		Arguments: innerArguments,
	}, nil
}

// UnwrapExecuteToolCall resolves an Execute tool call into a synthetic tool
// call targeting the discovered tool, inheriting that tool's annotations.
func UnwrapExecuteToolCall(toolCall *aipb.ToolCall, toolNameToTool map[string]*aipb.Tool) (*aipb.ToolCall, error) {
	toolCallExecute, err := ParseExecuteToolCall(toolCall)
	if err != nil {
		return nil, err
	}
	tool, ok := toolNameToTool[toolCallExecute.GetToolName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "execute targets unknown or undiscovered tool %q", toolCallExecute.GetToolName()).Err()
	}
	if value, _ := aip.GetAnnotation(tool, AnnotationKeyDiscoverableTool); value != aip.LabelValueTrue {
		return nil, status.Errorf(codes.InvalidArgument, "tool %q is a native tool: call it directly, not through the %s tool", tool.GetName(), ExecuteToolName).Err()
	}
	annotations := make(map[string]string, len(tool.GetAnnotations()))
	for key, value := range tool.GetAnnotations() {
		annotations[key] = value
	}
	return &aipb.ToolCall{
		Id:          toolCall.GetId(),
		Name:        tool.GetName(),
		Arguments:   toolCallExecute.GetArguments(),
		ExtraFields: toolCall.GetExtraFields(),
		Annotations: annotations,
		Partial:     toolCall.GetPartial(),
	}, nil
}
