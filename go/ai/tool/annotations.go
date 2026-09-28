package tool

// Values of the aipb.Annotations.ToolType annotation.
const (
	// A discovery tool: reveals other tools of its tool set to the model.
	ToolTypeDiscovery = "discovery"
	// A proxy tool: executes a discovered tool by name, for providers with strict tool names.
	ToolTypeExecute = "execute"
	// Generates a standalone protobuf message.
	ToolTypeGenerateMessage = "generate-message"
	// Generates a gRPC request to be dispatched.
	ToolTypeGenerateRPCRequest = "generate-rpc-request"
)
