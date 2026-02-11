package ai

const (
	// Tool call arguments could not be unmarshaled from JSON.
	ErrorInfoReasonToolCallArgumentUnmarshal = "TOOL_CALL_ARGUMENT_UNMARSHAL"
	// Tool call references a tool that was not provided in the request.
	ErrorInfoReasonToolCallUnknownTool = "TOOL_CALL_UNKNOWN_TOOL"
)
