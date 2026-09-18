package agent_service

import (
	"fmt"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

const (
	toolCreateTask   = "create_task"
	toolCreateMemory = "create_memory"
)

// createTaskTool is the built-in delegation tool. The call does not resolve
// inline: it defers the turn until the spawned task terminates, at which point
// its report is written back as this call's result.
func createTaskTool() *aipb.Tool {
	return &aipb.Tool{
		Name: toolCreateTask,
		Description: "Delegate a self-contained unit of work to a subordinate task that runs " +
			"with its own fresh context. Provide a fully self-contained goal: the task sees " +
			"nothing but it. Your turn pauses until every task spawned in it completes; each " +
			"task's final report is returned as this tool call's result. Spawn several in one " +
			"turn to run them in parallel.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"title": {Type: "string", Description: "Short human-readable title for the task."},
				"goal":  {Type: "string", Description: "Self-contained brief of the work. Include all necessary context: the task cannot see this conversation."},
			},
			Required: []string{"title", "goal"},
		},
	}
}

// createMemoryTool is the built-in long-term memory tool.
func createMemoryTool() *aipb.Tool {
	return &aipb.Tool{
		Name: toolCreateMemory,
		Description: "Persist one piece of long-term memory. Memories survive across conversations: " +
			"record durable facts, preferences, decisions and lessons — not transient chit-chat. " +
			"Title and description are your future retrieval surface: make them specific.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"title":       {Type: "string", Description: "Short specific title."},
				"description": {Type: "string", Description: "One-or-two-line summary."},
				"content":     {Type: "string", Description: "The full content of the memory."},
			},
			Required: []string{"title", "content"},
		},
	}
}

// stringArgument extracts a string argument from a tool call.
func stringArgument(call *aipb.ToolCall, key string) (string, error) {
	field, ok := call.GetArguments().GetFields()[key]
	if !ok {
		return "", fmt.Errorf("missing argument %q", key)
	}
	return field.GetStringValue(), nil
}
