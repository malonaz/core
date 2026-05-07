package tool

const (
	// Annotation keys.
	AnnotationKeyPrefix              = "ai-engine.malonaz.com/"
	AnnotationKeyGRPCService         = AnnotationKeyPrefix + "grpc-service"
	AnnotationKeyGRPCMethod          = AnnotationKeyPrefix + "grpc-method"
	AnnotationKeyProtoMessage        = AnnotationKeyPrefix + "proto-message"
	AnnotationKeyToolType            = AnnotationKeyPrefix + "tool-type"
	AnnotationKeyNoSideEffect        = AnnotationKeyPrefix + "no-side-effect"
	AnnotationKeyDiscoverableTool    = AnnotationKeyPrefix + "discoverable-tool"
	AnnotationKeyGenerationFieldMask = AnnotationKeyPrefix + "generation-field-mask"
	AnnotationKeyToolSetName         = AnnotationKeyPrefix + "tool-set-name"

	// Annotation values.
	AnnotationValueToolTypeDiscovery          = "discovery"
	AnnotationValueToolTypeGenerateMessage    = "generate-message"
	AnnotationValueToolTypeGenerateRPCRequest = "generate-rpc-request"
)
