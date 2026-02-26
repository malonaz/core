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
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var (
	// Annotation keys.
	annotationKeyPrefix              = "ai-engine.malonaz.com/"
	annotationKeyGRPCService         = annotationKeyPrefix + "grpc-service"
	annotationKeyGRPCMethod          = annotationKeyPrefix + "grpc-method"
	annotationKeyProtoMessage        = annotationKeyPrefix + "proto-message"
	annotationKeyToolType            = annotationKeyPrefix + "tool-type"
	annotationKeyNoSideEffect        = annotationKeyPrefix + "no-side-effect"
	annotationKeyDiscoverableTool    = annotationKeyPrefix + "discoverable-tool"
	annotationKeyGenerationFieldMask = annotationKeyPrefix + "generation-field-mask"

	// Annotation values.
	annotationValueToolTypeDiscovery          = "discovery"
	annotationValueToolTypeGenerateMessage    = "generate-message"
	annotationValueToolTypeGenerateRPCRequest = "generate-rpc-request"

	defaultMaxDepth = 5
)

type Opts struct {
	DefaultModel             string   `long:"default-model" env:"DEFAULT_MODEL" description:"The default model to use" required:"true"`
	FileDescriptorSetConfigs []string `long:"file-descriptor-set" env:"FILE_DESCRIPTOR_SET" description:"Use a local file descriptor set instead of a grpc reflection client. each item can be passed as 'filepath:fqn_service_name1,fqn_service_name2', e.g. 'path/to/fds.bin:user.user_service.v1.UserService,chat.chat_service.v1.ChatService'"`
}

type runtime struct {
	reflectionServerOptions *reflection.ServerOptions
}

func newRuntime(opts *Opts) (*runtime, error) {
	// Construct the aggregate file descriptor set.
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

	// Build the reflection server options.
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

	// Get the message descriptor.
	var descriptorFullName protoreflect.FullName
	var messageDescriptor protoreflect.MessageDescriptor
	var toolName, toolDescription string
	var annotations = map[string]string{}
	var schemaOptions []pbjson.SchemaOption

	// Set max depth.
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
			return nil, grpc.Errorf(codes.InvalidArgument, "finding method descriptor (%s): %v", target.Method, err).Err()
		}
		methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
		if !ok {
			return nil, grpc.Errorf(codes.InvalidArgument, "%s is not a method", target.Method).Err()
		}
		messageDescriptor = methodDescriptor.Input()
		toolName = string(methodDescriptor.Name())
		toolDescription = schema.GetComment(methodDescriptor.FullName(), pbreflection.CommentStyleMultiline)
		annotations[annotationKeyToolType] = annotationValueToolTypeGenerateRPCRequest
		annotations[annotationKeyGRPCService] = string(methodDescriptor.Parent().FullName())
		annotations[annotationKeyGRPCMethod] = string(methodDescriptor.FullName())
		if methodDescriptor.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS {
			annotations[annotationKeyNoSideEffect] = "true"
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
			return nil, grpc.Errorf(codes.InvalidArgument, "finding message descriptor (%s): %v", target.Message, err).Err()
		}
		var ok bool
		messageDescriptor, ok = descriptor.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, grpc.Errorf(codes.InvalidArgument, "%s is not a message", target.Message).Err()
		}
		toolName = fmt.Sprintf("Generate_%s", messageDescriptor.Name())
		toolDescription = fmt.Sprintf("Generate a %s message ", messageDescriptor.Name())
		annotations[annotationKeyToolType] = annotationValueToolTypeGenerateMessage

	default:
		return nil, grpc.Errorf(codes.InvalidArgument, "descriptor reference required").Err()
	}

	// Set the proto message annotation.
	annotations[annotationKeyProtoMessage] = string(messageDescriptor.FullName())

	if len(request.GetSchemaConfiguration().GetFieldMask().GetPaths()) > 0 {
		fieldMask := pbfieldmask.New(request.GetSchemaConfiguration().GetFieldMask())
		message := dynamicpb.NewMessage(messageDescriptor)
		if err := fieldMask.Validate(message); err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, "validating field_mask: %v", err).Err()
		}
		annotations[annotationKeyGenerationFieldMask] = fieldMask.String()
		schemaOptions = append(schemaOptions, pbjson.WithFieldMaskPaths(fieldMask.GetPaths()...))
	}
	jsonSchema, err := schemaBuilder.BuildSchema(descriptorFullName, schemaOptions...)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "building schema: %v", err).Err()
	}

	return &aipb.Tool{
		Name:        toolName,
		Description: toolDescription,
		JsonSchema:  jsonSchema,
		Annotations: annotations,
	}, nil
}

func (s *Service) ParseToolCall(ctx context.Context, request *pb.ParseToolCallRequest) (*pb.ParseToolCallResponse, error) {
	toolCall := request.GetToolCall()
	annotations := toolCall.GetAnnotations()
	if len(annotations) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}

	switch toolType := annotations[annotationKeyToolType]; toolType {
	case annotationValueToolTypeDiscovery: // CASE 1: DISCOVERY TOOL CALL.
		// Parse the request.
		args := toolCall.GetArguments().AsMap()
		toolNamesRaw, _ := args["tools"].([]any)
		var toolNames []string
		for _, name := range toolNamesRaw {
			if s, ok := name.(string); ok {
				toolNames = append(toolNames, s)
			}
		}

		// Find the tool set.
		var targetToolSet *pb.ToolSet
		for _, toolSet := range request.GetToolSets() {
			if toolSet.GetDiscoveryTool().GetName() == toolCall.GetName() {
				targetToolSet = toolSet
				break
			}
		}
		if targetToolSet == nil {
			return nil, grpc.Errorf(codes.NotFound, "tool %q not found", toolCall.GetName()).Err()
		}

		// Validate the tool names.
		for _, toolName := range toolNames {
			discoverTimestamp, ok := targetToolSet.ToolNameToDiscoverTimestamp[toolName]
			if !ok {
				return nil, grpc.Errorf(codes.NotFound, "tool %q not found in tool set", toolName).Err()
			}
			if discoverTimestamp > 0 {
				return nil, grpc.Errorf(codes.AlreadyExists, "tool %q already discovered", toolName).
					WithDetails(&pb.ParseToolCallRecoverableError{
						ToolResult: ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), fmt.Errorf("tool already discovered")),
					}).Err()
			}
		}

		return &pb.ParseToolCallResponse{
			Result: &pb.ParseToolCallResponse_DiscoverToolsRequest{
				DiscoverToolsRequest: &pb.DiscoverToolsRequest{
					ToolSetName: targetToolSet.GetName(),
					ToolNames:   toolNames,
				},
			},
		}, nil

	case annotationValueToolTypeGenerateRPCRequest: // CASE 2: GRPC METHOD TOOL CALL.
		methodFullName, ok := annotations[annotationKeyGRPCMethod]
		if !ok {
			return nil, grpc.Errorf(codes.InvalidArgument, "tool of type %q missing annotation %q", toolType, annotationKeyGRPCMethod).Err()
		}
		serviceFullName, ok := annotations[annotationKeyGRPCService]
		if !ok {
			return nil, grpc.Errorf(codes.InvalidArgument, "tool of type %q missing annotation %q", toolType, annotationKeyGRPCService).Err()
		}

		// If it's a discoverable tool, verify it has been discovered.
		if _, ok := annotations[annotationKeyDiscoverableTool]; ok {
			var found bool
			for _, toolSet := range request.GetToolSets() {
				if discoverTimestamp, ok := toolSet.GetToolNameToDiscoverTimestamp()[toolCall.GetName()]; ok {
					if discoverTimestamp == 0 {
						return nil, grpc.Errorf(codes.FailedPrecondition, "tool %q has not been discovered", toolCall.GetName()).Err()
					}
					found = true
					break
				}
			}
			if !found {
				return nil, grpc.Errorf(codes.NotFound, "tool %q not found", toolCall.GetName()).Err()
			}
		}

		// Parse the request message.
		request, err := s.parseToolCallMessage(ctx, toolCall)
		if err != nil {
			return nil, err
		}
		// Parse the field mask (if it exists).
		fieldMask, _ := pbjson.GetResponseFieldMask(toolCall.GetArguments().AsMap())

		return &pb.ParseToolCallResponse{
			Result: &pb.ParseToolCallResponse_RpcRequest{
				RpcRequest: &pb.RpcRequest{
					ServiceFullName: serviceFullName,
					MethodFullName:  methodFullName,
					Request:         request,
					ReadMask:        fieldMask,
				},
			},
		}, nil

	case annotationValueToolTypeGenerateMessage:
		// CASE 3: GENERATE MESSAGE TOOL CALL.
		message, err := s.parseToolCallMessage(ctx, toolCall)
		if err != nil {
			return nil, err
		}
		return &pb.ParseToolCallResponse{
			Result: &pb.ParseToolCallResponse_Message{Message: message},
		}, nil

	default:
		return nil, grpc.Errorf(codes.InvalidArgument, "unknown tool type %s", toolType).Err()
	}
}

func (s *Service) GenerateMessage(ctx context.Context, request *pb.GenerateMessageRequest) (*structpb.Struct, error) {
	// Step 1: Create the tool
	createToolRequest := &pb.CreateToolRequest{
		DescriptorReference: request.DescriptorReference,
		SchemaConfiguration: request.SchemaConfiguration,
	}
	tool, err := s.CreateTool(ctx, createToolRequest)
	if err != nil {
		return nil, err
	}
	tool.Annotations[annotationKeyToolType] = annotationValueToolTypeGenerateMessage

	model := request.GetModel()
	if model == "" {
		model = s.opts.DefaultModel
	}
	// Submit text to text request.
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
		return nil, grpc.Errorf(codes.Internal, "text to text: %v", err).Err()
	}

	// Parse tool call.
	var toolCalls []*aipb.ToolCall
	for _, block := range textToTextResponse.GetMessage().GetBlocks() {
		if toolCall := block.GetToolCall(); toolCall != nil {
			toolCalls = append(toolCalls, toolCall)
		}
	}
	if len(toolCalls) != 1 {
		return nil, grpc.Errorf(codes.Internal, "expected 1 tool call, got %d", len(toolCalls)).Err()
	}
	parseToolCallRequest := &pb.ParseToolCallRequest{
		ToolCall: toolCalls[0],
	}
	parseToolCallResponse, err := s.ParseToolCall(ctx, parseToolCallRequest)
	if err != nil {
		return nil, err
	}

	switch result := parseToolCallResponse.GetResult().(type) {
	case *pb.ParseToolCallResponse_RpcRequest:
		return result.RpcRequest.GetRequest(), nil
	case *pb.ParseToolCallResponse_Message:
		return result.Message, nil
	default:
		return nil, grpc.Errorf(codes.Internal, "unexpected result type: %T", result).Err()
	}
}

func (s *Service) CreateDiscoveryTool(ctx context.Context, request *pb.CreateDiscoveryToolRequest) (*aipb.Tool, error) {
	// Gather the tool names.
	toolNames := make([]string, 0, len(request.Tools))
	for _, tool := range request.Tools {
		toolNames = append(toolNames, tool.Name)
	}

	// build the description.
	var desc strings.Builder
	if request.Description != "" {
		desc.WriteString(request.Description)
		desc.WriteString("\n\n")
	}
	desc.WriteString("Discover the following tools:")
	for _, tool := range request.Tools {
		desc.WriteString("\n- " + tool.Name)
		if tool.Description != "" {
			firstLine := strings.SplitN(tool.Description, "\n", 2)[0]
			desc.WriteString(": " + firstLine)
		}
	}

	// Build the tool.
	return &aipb.Tool{
		Name:        request.Name,
		Description: desc.String(),
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"tools": {
					Type:        "array",
					Description: "Tool names to discover",
					Items:       &jsonpb.Schema{Type: "string", Enum: toolNames},
				},
			},
			Required: []string{"tools"},
		},
		Annotations: map[string]string{
			annotationKeyToolType:     annotationValueToolTypeDiscovery,
			annotationKeyNoSideEffect: "true",
		},
	}, nil
}

func (s *Service) CreateServiceToolSet(ctx context.Context, request *pb.CreateServiceToolSetRequest) (*pb.ToolSet, error) {
	// Get the schema.
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}

	// Find the service descriptor.
	descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(request.ServiceFullName))
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "finding service descriptor (%s): %v", request.ServiceFullName, err).Err()
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, grpc.Errorf(codes.InvalidArgument, "%s is not a service", request.ServiceFullName).Err()
	}

	// Fill method names if not provided.
	if len(request.MethodNames) == 0 {
		methods := serviceDescriptor.Methods()
		for i := 0; i < methods.Len(); i++ {
			request.MethodNames = append(request.MethodNames, string(methods.Get(i).Name()))
		}
	}

	// Create the method tools.
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
			return nil, grpc.Errorf(codes.Internal, "creating tool for method %s.%s: %v", request.ServiceFullName, methodName, err).Err()
		}
		tools = append(tools, tool)
		tool.Annotations[annotationKeyDiscoverableTool] = "true"
		toolNameToDiscoverTimestamp[tool.Name] = 0
	}

	// Build the discover tool.
	serviceComment := schema.GetComment(serviceDescriptor.FullName(), pbreflection.CommentStyleMultiline)
	createDiscoveryToolRequest := &pb.CreateDiscoveryToolRequest{
		Name:        string(serviceDescriptor.Name()) + "_Discover",
		Description: serviceComment,
		Tools:       tools,
	}
	discoveryTool, err := s.CreateDiscoveryTool(ctx, createDiscoveryToolRequest)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "creating discovery tool: %v", err).Err()
	}

	return &pb.ToolSet{
		Name:                        string(serviceDescriptor.FullName()),
		DiscoveryTool:               discoveryTool,
		Tools:                       tools,
		ToolNameToDiscoverTimestamp: toolNameToDiscoverTimestamp,
	}, nil
}

func (s *Service) parseToolCallMessage(ctx context.Context, toolCall *aipb.ToolCall) (*structpb.Struct, error) {
	annotations := toolCall.GetAnnotations()
	if len(annotations) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}

	messageFullName, ok := annotations[annotationKeyProtoMessage]
	if !ok {
		return nil, grpc.Errorf(codes.InvalidArgument, "missing %s annotation", annotationKeyProtoMessage).Err()
	}

	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)
	dynamicMessage, err := schemaBuilder.BuildMessage(protoreflect.FullName(messageFullName), toolCall.GetArguments().AsMap())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "building message: %v", err).Err()
	}

	// Apply the generation field mask. (LLMs sometimes generate additional fields).
	if generationFieldMask, ok := annotations[annotationKeyGenerationFieldMask]; ok {
		fieldMask := pbfieldmask.FromString(generationFieldMask)
		if err := fieldMask.Validate(dynamicMessage); err != nil {
			return nil, grpc.Errorf(codes.Internal, "validating generation field mask: %v", err).Err()
		}
		fieldMask.Apply(dynamicMessage)
	}

	message := &structpb.Struct{}
	if err := pbutil.UnmarshalFromDynamic(message, dynamicMessage); err != nil {
		return nil, grpc.Errorf(codes.Internal, "marshaling to struct: %v", err).Err()
	}

	return message, nil
}
