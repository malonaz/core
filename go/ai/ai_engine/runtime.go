package ai_engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var defaultMaxDepth = 5

type Opts struct {
	DefaultModel             string   `long:"default-model" env:"DEFAULT_MODEL" description:"The default model to use" required:"true"`
	FileDescriptorSetConfigs []string `long:"file-descriptor-set" env:"FILE_DESCRIPTOR_SET" description:"Use a local file descriptor set instead of a grpc reflection client. each item can be passed as 'filepath:fqn_service_name1,fqn_service_name2', e.g. 'path/to/fds.bin:user.user_service.v1.UserService,chat.chat_service.v1.ChatService'"`
}

type runtime struct {
	reflectionServerOptions *reflection.ServerOptions
}

func newRuntime(opts *Opts) (*runtime, error) {
	var (
		aggregateFileDescriptorSet *descriptorpb.FileDescriptorSet
		serviceNames               []string
	)

	if len(opts.FileDescriptorSetConfigs) > 0 {
		aggregateFileDescriptorSet = &descriptorpb.FileDescriptorSet{}
	}
	fileDescriptorNameSet := map[string]struct{}{}
	for _, fileDescriptorSetConfig := range opts.FileDescriptorSetConfigs {
		parts := strings.SplitN(fileDescriptorSetConfig, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid file descriptor set config %q: expected format 'filepath:service1,service2'", fileDescriptorSetConfig)
		}
		fileDescriptorSetFilepath := parts[0]
		serviceNames = append(serviceNames, strings.Split(parts[1], ",")...)
		bytes, err := os.ReadFile(fileDescriptorSetFilepath)
		if err != nil {
			return nil, fmt.Errorf("reading file descriptor set file %q: %w", fileDescriptorSetFilepath, err)
		}
		fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
		if err := pbutil.Unmarshal(bytes, fileDescriptorSet); err != nil {
			return nil, fmt.Errorf("parsing file descriptor set file %q: %w", fileDescriptorSetFilepath, err)
		}
		for _, fileDescriptorProto := range fileDescriptorSet.GetFile() {
			if _, ok := fileDescriptorNameSet[fileDescriptorProto.GetName()]; ok {
				continue
			}
			fileDescriptorNameSet[fileDescriptorProto.GetName()] = struct{}{}
			aggregateFileDescriptorSet.File = append(aggregateFileDescriptorSet.File, fileDescriptorProto)
		}
	}

	var reflectionServerOptions *reflection.ServerOptions
	if aggregateFileDescriptorSet != nil {
		files, err := protodesc.NewFiles(aggregateFileDescriptorSet)
		if err != nil {
			return nil, fmt.Errorf("building file descriptor registry: %w", err)
		}
		types, err := pbreflection.NewTypesFromFiles(files)
		if err != nil {
			return nil, fmt.Errorf("new types from files: %w", err)
		}
		serviceInfoProvider, err := pbreflection.NewServiceInfoProvider(files, serviceNames)
		if err != nil {
			return nil, fmt.Errorf("instantiaing new service info provider: %w", err)
		}
		reflectionServerOptions = &reflection.ServerOptions{
			Services:           serviceInfoProvider,
			ExtensionResolver:  types,
			DescriptorResolver: files,
		}
	}

	return &runtime{
		reflectionServerOptions: reflectionServerOptions,
	}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	if s.reflectionServerOptions != nil && s.serverReflectionClient != nil {
		return nil, fmt.Errorf("cannot use both grpc reflection client and file descriptor set")
	}
	if s.reflectionServerOptions == nil && s.serverReflectionClient == nil {
		return nil, fmt.Errorf("must provide either a grpc reflection client or a file descriptor set")
	}
	if s.reflectionServerOptions != nil {
		s.serverReflectionClient = pbreflection.NewServerReflectionClientInProc(*s.reflectionServerOptions)
	}
	return func() {}, nil
}

func (s *Service) getSchema(ctx context.Context) (*pbreflection.Schema, error) {
	return pbreflection.ResolveSchema(ctx, s.serverReflectionClient, pbreflection.WithMemCache("schema", time.Hour))
}

func (s *Service) CreateTool(ctx context.Context, request *pb.CreateToolRequest) (*aipb.Tool, error) {
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)

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
		descriptor, err := schema.FindDescriptorByName(descriptorFullName)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "finding method descriptor (%s): %v", target.Method, err).Err()
		}
		methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "%s is not a method", target.Method).Err()
		}
		messageDescriptor = methodDescriptor.Input()
		toolName = string(methodDescriptor.Parent().Name()) + "_" + string(methodDescriptor.Name())
		toolDescription = schema.GetComment(methodDescriptor.FullName(), pbreflection.CommentStyleMultiline)
		annotations[aitool.AnnotationKeyToolType] = aitool.AnnotationValueToolTypeGenerateRPCRequest
		annotations[aitool.AnnotationKeyGRPCService] = string(methodDescriptor.Parent().FullName())
		annotations[aitool.AnnotationKeyGRPCMethod] = string(methodDescriptor.FullName())
		if methodDescriptor.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS {
			annotations[aitool.AnnotationKeyNoSideEffect] = "true"
		}
		if request.GetSchemaConfiguration().GetWithResponseReadMask() {
			schemaOptions = append(schemaOptions, pbjson.WithResponseReadMask())
		}
		if responseSchemaMaxDepth := request.GetSchemaConfiguration().GetWithResponseSchemaMaxDepth(); responseSchemaMaxDepth > 0 {
			schemaOptions = append(schemaOptions, pbjson.WithResponseSchemaMaxDepth(int(responseSchemaMaxDepth)))
		}

	case *pb.DescriptorReference_Message:
		descriptorFullName = protoreflect.FullName(target.Message)
		descriptor, err := schema.FindDescriptorByName(descriptorFullName)
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
		annotations[aitool.AnnotationKeyToolType] = aitool.AnnotationValueToolTypeGenerateMessage

	default:
		return nil, status.Errorf(codes.InvalidArgument, "descriptor reference required").Err()
	}

	annotations[aitool.AnnotationKeyProtoMessage] = string(messageDescriptor.FullName())

	if len(request.GetSchemaConfiguration().GetFieldMask().GetPaths()) > 0 {
		fieldMask := pbfieldmask.New(request.GetSchemaConfiguration().GetFieldMask())
		message := dynamicpb.NewMessage(messageDescriptor)
		if err := fieldMask.Validate(message); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "validating field_mask: %v", err).Err()
		}
		annotations[aitool.AnnotationKeyGenerationFieldMask] = fieldMask.String()
		schemaOptions = append(schemaOptions, pbjson.WithFieldMaskPaths(fieldMask.GetPaths()...))
	}
	jsonSchema, err := schemaBuilder.BuildSchema(descriptorFullName, schemaOptions...)
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

func (s *Service) ParseToolCall(ctx context.Context, request *pb.ParseToolCallRequest) (*pb.ParseToolCallResponse, error) {
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)
	return aitool.ParseToolCall(schemaBuilder, request.GetToolCall(), request.GetToolSets())
}

func (s *Service) GenerateMessage(ctx context.Context, request *pb.GenerateMessageRequest) (*structpb.Struct, error) {
	createToolRequest := &pb.CreateToolRequest{
		DescriptorReference: request.DescriptorReference,
		SchemaConfiguration: request.SchemaConfiguration,
	}
	tool, err := s.CreateTool(ctx, createToolRequest)
	if err != nil {
		return nil, err
	}
	tool.Annotations[aitool.AnnotationKeyToolType] = aitool.AnnotationValueToolTypeGenerateMessage

	model := request.GetModel()
	if model == "" {
		model = s.opts.DefaultModel
	}
	textToTextRequest := &aiservicepb.TextToTextRequest{
		Model: model,
		Messages: []*aipb.Message{
			ai.NewSystemMessage(ai.NewTextBlock(
				fmt.Sprintf("Use the `%s` tool to generate a JSON payload based on the data given to you by the user", tool.GetName()),
			)),
			ai.NewUserMessage(ai.NewTextBlock(request.GetPrompt())),
		},
		Configuration: &aiservicepb.TextToTextConfiguration{
			ToolChoice: &aipb.ToolChoice{
				Choice: &aipb.ToolChoice_ToolName{
					ToolName: tool.Name,
				},
			},
		},
		Tools: []*aipb.Tool{tool},
	}
	textToTextResponse, err := s.aiServiceClient.TextToText(ctx, textToTextRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "text to text: %v", err).Err()
	}

	var toolCalls []*aipb.ToolCall
	for _, block := range textToTextResponse.GetMessage().GetBlocks() {
		if toolCall := block.GetToolCall(); toolCall != nil {
			toolCalls = append(toolCalls, toolCall)
		}
	}
	if len(toolCalls) != 1 {
		return nil, status.Errorf(codes.Internal, "expected 1 tool call, got %d", len(toolCalls)).Err()
	}

	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)
	parseToolCallResponse, err := aitool.ParseToolCall(schemaBuilder, toolCalls[0], nil)
	if err != nil {
		return nil, err
	}

	switch result := parseToolCallResponse.GetResult().(type) {
	case *pb.ParseToolCallResponse_Rpc:
		return result.Rpc.GetRequest(), nil
	case *pb.ParseToolCallResponse_Message:
		return result.Message, nil
	default:
		return nil, status.Errorf(codes.Internal, "unexpected result type: %T", result).Err()
	}
}

func (s *Service) CreateDiscoveryTool(ctx context.Context, request *pb.CreateDiscoveryToolRequest) (*aipb.Tool, error) {
	return aitool.CreateDiscoveryTool(&aitool.CreateDiscoveryToolRequest{
		Name:        request.Name,
		Description: request.Description,
		Tools:       request.Tools,
	}), nil
}

func (s *Service) CreateServiceToolSet(ctx context.Context, request *pb.CreateServiceToolSetRequest) (*aipb.ToolSet, error) {
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}

	descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(request.ServiceFullName))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "finding service descriptor (%s): %v", request.ServiceFullName, err).Err()
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a service", request.ServiceFullName).Err()
	}

	if len(request.MethodNames) == 0 {
		methods := serviceDescriptor.Methods()
		for i := 0; i < methods.Len(); i++ {
			request.MethodNames = append(request.MethodNames, string(methods.Get(i).Name()))
		}
	}

	toolSetName := string(serviceDescriptor.FullName())
	var tools []*aipb.Tool
	toolNameToDiscoverTimestamp := map[string]int64{}
	if len(request.MethodNameToSchemaConfiguration) == 0 {
		request.MethodNameToSchemaConfiguration = map[string]*pb.SchemaConfiguration{}
	}
	for _, methodName := range request.MethodNames {
		schemaConfiguration := request.MethodNameToSchemaConfiguration[methodName]
		if schemaConfiguration == nil {
			schemaConfiguration = request.SchemaConfiguration
		}
		createToolRequest := &pb.CreateToolRequest{
			DescriptorReference: &pb.DescriptorReference{
				FullName: &pb.DescriptorReference_Method{Method: request.ServiceFullName + "." + methodName},
			},
			SchemaConfiguration: schemaConfiguration,
		}
		tool, err := s.CreateTool(ctx, createToolRequest)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating tool for method %s.%s: %v", request.ServiceFullName, methodName, err).Err()
		}
		tools = append(tools, tool)
		aip.SetAnnotation(tool, aitool.AnnotationKeyDiscoverableTool, "true")
		aip.SetAnnotation(tool, aitool.AnnotationKeyToolSetName, toolSetName)
		toolNameToDiscoverTimestamp[tool.Name] = 0
	}

	serviceComment := schema.GetComment(serviceDescriptor.FullName(), pbreflection.CommentStyleMultiline)
	createDiscoveryToolRequest := &aitool.CreateDiscoveryToolRequest{
		Name:        string(serviceDescriptor.Name()) + "_Discover",
		Description: serviceComment,
		Tools:       tools,
	}
	discoveryTool := aitool.CreateDiscoveryTool(createDiscoveryToolRequest)
	aip.SetAnnotation(discoveryTool, aitool.AnnotationKeyToolSetName, toolSetName)

	return &aipb.ToolSet{
		Name:                        toolSetName,
		DiscoveryTool:               discoveryTool,
		Tools:                       tools,
		ToolNameToDiscoverTimestamp: toolNameToDiscoverTimestamp,
	}, nil
}
