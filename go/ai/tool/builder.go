package tool

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const defaultMaxDepth = 5

// Builder builds tools from the descriptors of a schema: a method's tool generates its request, a message's tool
// generates the message.
type Builder struct {
	schema        *pbreflection.Schema
	schemaBuilder *pbjson.SchemaBuilder
}

// NewBuilder returns a builder over a schema.
func NewBuilder(schema *pbreflection.Schema) *Builder {
	return &Builder{schema: schema, schemaBuilder: pbjson.NewSchemaBuilder(schema)}
}

// SchemaBuilder returns the JSON schema builder over the builder's schema, which parses the tools' calls back.
func (b *Builder) SchemaBuilder() *pbjson.SchemaBuilder {
	return b.schemaBuilder
}

// CreateTool builds the tool of a method or a message.
func (b *Builder) CreateTool(request *pb.CreateToolRequest) (*aipb.Tool, error) {
	var descriptorFullName protoreflect.FullName
	var messageDescriptor protoreflect.MessageDescriptor
	var toolName, toolDescription string
	var annotations = map[string]string{}
	var schemaOptions []pbjson.SchemaOption

	var maxDepth = defaultMaxDepth
	if request.GetSchemaConfiguration().GetWithMaxDepth() > 0 {
		maxDepth = int(request.GetSchemaConfiguration().GetWithMaxDepth())
	}
	schemaOptions = append(schemaOptions, pbjson.WithMaxDepth(maxDepth))

	switch target := request.GetDescriptorReference().GetFullName().(type) {
	case *pb.DescriptorReference_Method:
		descriptorFullName = protoreflect.FullName(target.Method)
		descriptor, err := b.schema.FindDescriptorByName(descriptorFullName)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "finding method descriptor (%s): %v", target.Method, err).Err()
		}
		methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "%s is not a method", target.Method).Err()
		}
		messageDescriptor = methodDescriptor.Input()
		toolName = string(methodDescriptor.Parent().Name()) + "_" + string(methodDescriptor.Name())
		toolDescription = b.schema.GetComment(methodDescriptor.FullName(), pbreflection.CommentStyleMultiline)
		annotations[aipb.Annotations.ToolType.Key] = ToolTypeGenerateRPCRequest
		annotations[aipb.Annotations.GrpcService.Key] = string(methodDescriptor.Parent().FullName())
		annotations[aipb.Annotations.GrpcMethod.Key] = string(methodDescriptor.FullName())
		if methodDescriptor.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS {
			annotations[aipb.Annotations.NoSideEffect.Key] = "true"
		}
		if request.GetSchemaConfiguration().GetWithResponseReadMask() {
			schemaOptions = append(schemaOptions, pbjson.WithResponseReadMask())
		}
		if responseSchemaMaxDepth := request.GetSchemaConfiguration().GetWithResponseSchemaMaxDepth(); responseSchemaMaxDepth > 0 {
			schemaOptions = append(schemaOptions, pbjson.WithResponseSchemaMaxDepth(int(responseSchemaMaxDepth)))
		}

	case *pb.DescriptorReference_Message:
		descriptorFullName = protoreflect.FullName(target.Message)
		descriptor, err := b.schema.FindDescriptorByName(descriptorFullName)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "finding message descriptor (%s): %v", target.Message, err).Err()
		}
		var ok bool
		messageDescriptor, ok = descriptor.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "%s is not a message", target.Message).Err()
		}
		toolName = fmt.Sprintf("Generate_%s", messageDescriptor.Name())
		toolDescription = fmt.Sprintf("Generate a %s message ", messageDescriptor.Name())
		annotations[aipb.Annotations.ToolType.Key] = ToolTypeGenerateMessage

	default:
		return nil, status.Errorf(codes.InvalidArgument, "descriptor reference required").Err()
	}

	annotations[aipb.Annotations.ProtoMessage.Key] = string(messageDescriptor.FullName())

	// Title applies to both message and method tools.
	if titleDescription := request.GetSchemaConfiguration().GetWithTitle(); titleDescription != "" {
		schemaOptions = append(schemaOptions, pbjson.WithTitle(titleDescription))
	}

	if len(request.GetSchemaConfiguration().GetFieldMask().GetPaths()) > 0 {
		fieldMask := pbfieldmask.New(request.GetSchemaConfiguration().GetFieldMask())
		message := dynamicpb.NewMessage(messageDescriptor)
		if err := fieldMask.Validate(message); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "validating field_mask: %v", err).Err()
		}
		annotations[aipb.Annotations.GenerationFieldMask.Key] = fieldMask.String()
		schemaOptions = append(schemaOptions, pbjson.WithFieldMaskPaths(fieldMask.GetPaths()...))
	}
	jsonSchema, err := b.schemaBuilder.BuildSchema(descriptorFullName, schemaOptions...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building schema: %v", err).Err()
	}

	return &aipb.Tool{
		Name:        toolName,
		Description: toolDescription,
		JsonSchema:  jsonSchema,
		Annotations: annotations,
	}, nil
}

// CreateServiceToolSet builds the tool set of a service: one tool per method, discovered through the set's
// discovery tool unless pre-discovered.
func (b *Builder) CreateServiceToolSet(request *pb.CreateServiceToolSetRequest) (*aipb.ToolSet, error) {
	descriptor, err := b.schema.FindDescriptorByName(protoreflect.FullName(request.ServiceFullName))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "finding service descriptor (%s): %v", request.ServiceFullName, err).Err()
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a service", request.ServiceFullName).Err()
	}

	var serviceMethodNames []string
	serviceMethodNameSet := map[string]struct{}{}
	methods := serviceDescriptor.Methods()
	for i := 0; i < methods.Len(); i++ {
		serviceMethodName := string(methods.Get(i).Name())
		serviceMethodNames = append(serviceMethodNames, serviceMethodName)
		serviceMethodNameSet[serviceMethodName] = struct{}{}
	}

	// No methods specified: every method.
	methodNames := request.GetMethodNames()
	if len(methodNames) == 0 {
		methodNames = serviceMethodNames
	}
	for methodName := range request.GetMethodNameToSchemaConfiguration() {
		if _, ok := serviceMethodNameSet[methodName]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown method name %q in method_name_to_schema_configuration", methodName).Err()
		}
	}
	discoveredMethodNameSet := make(map[string]struct{}, len(request.GetDiscoveredMethodNames()))
	for _, methodName := range request.GetDiscoveredMethodNames() {
		if _, ok := serviceMethodNameSet[methodName]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown method name %q in discovered_method_names", methodName).Err()
		}
		discoveredMethodNameSet[methodName] = struct{}{}
	}

	toolSetName := string(serviceDescriptor.FullName())
	var tools []*aipb.Tool
	var discoverableTools []*aipb.Tool
	for _, methodName := range methodNames {
		schemaConfiguration := request.GetMethodNameToSchemaConfiguration()[methodName]
		if schemaConfiguration == nil {
			schemaConfiguration = request.GetSchemaConfiguration()
		}
		createToolRequest := &pb.CreateToolRequest{
			DescriptorReference: &pb.DescriptorReference{
				FullName: &pb.DescriptorReference_Method{Method: request.ServiceFullName + "." + methodName},
			},
			SchemaConfiguration: schemaConfiguration,
		}
		tool, err := b.CreateTool(createToolRequest)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating tool for method %s.%s: %v", request.ServiceFullName, methodName, err).Err()
		}
		tools = append(tools, tool)
		aipb.Annotations.ToolSetName.Set(tool, toolSetName)
		if _, ok := discoveredMethodNameSet[methodName]; ok {
			aipb.Annotations.DiscoverableTool.Set(tool, aip.LabelValueFalse)
			aipb.Annotations.PreDiscoveredTool.Set(tool, aip.LabelValueTrue)
		} else {
			aipb.Annotations.DiscoverableTool.Set(tool, aip.LabelValueTrue)
			discoverableTools = append(discoverableTools, tool)
		}
	}

	createDiscoveryToolRequest := &CreateDiscoveryToolRequest{
		Name:        string(serviceDescriptor.Name()) + "_Discover",
		Description: b.schema.GetComment(serviceDescriptor.FullName(), pbreflection.CommentStyleMultiline),
		Tools:       discoverableTools,
	}
	discoveryTool := CreateDiscoveryTool(createDiscoveryToolRequest)
	aipb.Annotations.ToolSetName.Set(discoveryTool, toolSetName)

	return &aipb.ToolSet{
		Name:          toolSetName,
		DiscoveryTool: discoveryTool,
		Tools:         tools,
	}, nil
}
