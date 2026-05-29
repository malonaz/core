package tool

const (
	// Namespace prefix for all AI engine annotations.
	AnnotationKeyPrefix = "ai-engine.malonaz.com/"

	// --- gRPC descriptor annotations ---

	// The fully qualified gRPC service name that backs this tool.
	AnnotationKeyGRPCService = AnnotationKeyPrefix + "grpc-service"
	// The fully qualified gRPC method name that backs this tool.
	AnnotationKeyGRPCMethod = AnnotationKeyPrefix + "grpc-method"
	// The fully qualified protobuf message name used as the tool's input/output schema.
	AnnotationKeyProtoMessage = AnnotationKeyPrefix + "proto-message"

	// --- Tool metadata annotations ---

	// The type of tool: discovery, generate-message, or generate-rpc-request.
	AnnotationKeyToolType = AnnotationKeyPrefix + "tool-type"
	// When "true", indicates the tool has no side effects (safe to call speculatively).
	AnnotationKeyNoSideEffect = AnnotationKeyPrefix + "no-side-effect"
	// A comma-separated field mask applied to the generated message to restrict which fields are populated.
	AnnotationKeyGenerationFieldMask = AnnotationKeyPrefix + "generation-field-mask"

	// --- Tool set / discovery annotations ---

	// When present, indicates the tool was registered via a discovery tool set and must be discovered before use.
	AnnotationKeyDiscoverableTool = AnnotationKeyPrefix + "discoverable-tool"
	// The name of the tool set this tool belongs to, used to resolve discovery tool calls.
	AnnotationKeyToolSetName = AnnotationKeyPrefix + "tool-set-name"
	// When "true", indicates this tool is pre-discovered and available without requiring a discovery tool call.
	AnnotationKeyPreDiscoveredTool = AnnotationKeyPrefix + "pre-discovered-tool"
	// The comma-separated list of tool names returned by a discovery tool call.
	AnnotationKeyDiscoveredTools = AnnotationKeyPrefix + "discovered-tools"

	// --- Tool type values ---

	// Tool type indicating a discovery tool that reveals other tools to the model.
	AnnotationValueToolTypeDiscovery = "discovery"
	// Tool type indicating the tool generates a standalone protobuf message.
	AnnotationValueToolTypeGenerateMessage = "generate-message"
	// Tool type indicating the tool generates a gRPC request to be dispatched.
	AnnotationValueToolTypeGenerateRPCRequest = "generate-rpc-request"
)
