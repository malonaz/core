package tool

// Values of the aipb.Annotations.ToolType annotation.
const (
	// A discovery tool: reveals other tools of its tool set to the model.
	ToolTypeDiscovery = "discovery"
	// Generates a standalone protobuf message.
	ToolTypeGenerateMessage = "generate-message"
	// Generates a gRPC request to be dispatched.
	ToolTypeGenerateRPCRequest = "generate-rpc-request"
)
