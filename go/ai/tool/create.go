package tool

import (
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// Method identifies a gRPC method to build a tool from.
type Method struct {
	fullName string
}

// NewMethod accepts a generated gRPC full method name, e.g.
// userservicepb.UserService_CreateContact_FullMethodName.
//
// gRPC spells these "/package.Service/Method", while protobuf descriptor
// lookups use the dotted "package.Service.Method" form.
func NewMethod(grpcFullMethodName string) Method {
	dottedName := strings.ReplaceAll(strings.TrimPrefix(grpcFullMethodName, "/"), "/", ".")
	return Method{fullName: dottedName}
}

// FullName returns the dotted "package.Service.Method" form. This is what
// ToolCallRpc.method_full_name carries, and what the middleware matches on
// (it resolves gRPC's slashed name to a descriptor before comparing), so the
// same value serves both tool dispatch and hook registration.
func (m Method) FullName() string {
	return m.fullName
}

// Option configures the schema derived from a descriptor.
type Option func(*pb.SchemaConfiguration)

// WithFieldMask restricts the tool schema to the masked fields.
func WithFieldMask(fieldMask *pbfieldmask.FieldMask) Option {
	return func(schemaConfiguration *pb.SchemaConfiguration) {
		schemaConfiguration.FieldMask = fieldMask.Proto()
	}
}

// WithMaxDepth bounds how deep the schema recurses. Left unset, the server
// applies its own default rather than us forking that policy client-side.
func WithMaxDepth(maxDepth int32) Option {
	return func(schemaConfiguration *pb.SchemaConfiguration) {
		schemaConfiguration.WithMaxDepth = maxDepth
	}
}

// WithTitle injects a required title field, documented with the given string,
// that the model fills first. Useful when surfacing tool calls for approval.
func WithTitle(title string) Option {
	return func(schemaConfiguration *pb.SchemaConfiguration) {
		schemaConfiguration.WithTitle = title
	}
}

// WithResponseReadMask lets the model request only the response fields it
// needs. Method tools only.
func WithResponseReadMask() Option {
	return func(schemaConfiguration *pb.SchemaConfiguration) {
		schemaConfiguration.WithResponseReadMask = true
	}
}

// WithResponseSchema includes the response schema in the tool description,
// bounded by maxDepth. Verbose, so use with care. Method tools only.
func WithResponseSchema(maxDepth int32) Option {
	return func(schemaConfiguration *pb.SchemaConfiguration) {
		schemaConfiguration.WithResponseSchemaMaxDepth = maxDepth
	}
}

// NewSchemaConfiguration builds a schema configuration from options, for the
// APIs that take one directly (e.g. CreateServiceToolSet).
func NewSchemaConfiguration(options ...Option) *pb.SchemaConfiguration {
	schemaConfiguration := &pb.SchemaConfiguration{}
	for _, option := range options {
		option(schemaConfiguration)
	}
	return schemaConfiguration
}

// MessageToolRequest builds a CreateToolRequest whose schema mirrors the given
// message, so the resulting tool call parses straight back into it.
func MessageToolRequest(message proto.Message, options ...Option) *pb.CreateToolRequest {
	return &pb.CreateToolRequest{
		DescriptorReference: &pb.DescriptorReference{
			FullName: &pb.DescriptorReference_Message{
				Message: string(message.ProtoReflect().Descriptor().FullName()),
			},
		},
		SchemaConfiguration: NewSchemaConfiguration(options...),
	}
}

// MethodToolRequest builds a CreateToolRequest whose schema mirrors the given
// method's request message.
func MethodToolRequest(method Method, options ...Option) *pb.CreateToolRequest {
	return &pb.CreateToolRequest{
		DescriptorReference: &pb.DescriptorReference{
			FullName: &pb.DescriptorReference_Method{Method: method.fullName},
		},
		SchemaConfiguration: NewSchemaConfiguration(options...),
	}
}
