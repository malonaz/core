package ai_engine

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var (
	annotationKeyPrefix          = "ai-engine.malonaz.com/"
	annotationKeyMessageFullName = annotationKeyPrefix + "message-full-name"
)

type Opts struct {
	DefaultModel string `long:"default-model" env:"DEFAULT_MODEL" description:"The default model to use" required:"true"`
}

type runtime struct{}

func newRuntime(opts *Opts) (*runtime, error) {
	return &runtime{}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) { return func() {}, nil }

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
	var messageDescriptor protoreflect.MessageDescriptor
	var toolName, toolDescription string
	var standardMethodType pbreflection.StandardMethodType

	switch target := request.GetDescriptorReference().GetFullName().(type) {
	case *pb.DescriptorReference_Method:
		descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(target.Method))
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
		standardMethodType = schema.GetStandardMethodType(methodDescriptor.FullName())

	case *pb.DescriptorReference_Message:
		descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(target.Message))
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
		standardMethodType = pbreflection.StandardMethodTypeUnspecified
	default:
		return nil, grpc.Errorf(codes.InvalidArgument, "descriptor reference required").Err()
	}

	withFieldMask := pbjson.WithFieldMaskPaths(request.GetFieldMask().GetPaths()...)
	jsonSchema, err := schemaBuilder.BuildSchema(messageDescriptor.FullName(), standardMethodType, withFieldMask)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "building schema: %v", err).Err()
	}

	return &aipb.Tool{
		Name:        toolName,
		Description: toolDescription,
		JsonSchema:  jsonSchema,
		Annotations: map[string]string{
			annotationKeyMessageFullName: string(messageDescriptor.FullName()),
		},
	}, nil
}

func (s *Service) ParseToolCall(ctx context.Context, request *pb.ParseToolCallRequest) (*structpb.Struct, error) {
	// Grab the message full name from the annotations.
	annotations := request.GetToolCall().GetAnnotations()
	if len(annotations) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}
	messageFullName, ok := annotations[annotationKeyMessageFullName]
	if !ok {
		return nil, grpc.Errorf(codes.InvalidArgument, "missing %s annotation", annotationKeyMessageFullName).Err()
	}

	// Resolve the message descriptor.
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)
	dynamicMessage, err := schemaBuilder.BuildMessage(protoreflect.FullName(messageFullName), request.GetToolCall().GetArguments().AsMap())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "building message: %v", err).Err()
	}

	result := &structpb.Struct{}
	if err := pbutil.UnmarshalFromDynamic(result, dynamicMessage); err != nil {
		return nil, grpc.Errorf(codes.Internal, "marshaling to struct: %v", err).Err()
	}
	return result, nil
}

func (s *Service) GenerateMessage(ctx context.Context, request *pb.GenerateMessageRequest) (*structpb.Struct, error) {
	// Step 1: Create the tool
	createToolRequest := &pb.CreateToolRequest{
		DescriptorReference: request.DescriptorReference,
		FieldMask:           request.FieldMask,
	}
	tool, err := s.CreateTool(ctx, createToolRequest)
	if err != nil {
		return nil, err
	}

	model := request.GetModel()
	if model == "" {
		model = s.opts.DefaultModel
	}
	// Submit text to text request.
	textToTextRequest := &aiservicepb.TextToTextRequest{
		Model: model,
		Messages: []*aipb.Message{
			ai.NewSystemMessage(&aipb.SystemMessage{Content: fmt.Sprintf("Use the `%s` tool to generate a JSON payload based on the data given to you by the user", tool.GetName())}),
			ai.NewUserMessage(&aipb.UserMessage{Content: request.GetPrompt()}),
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
	toolCalls := textToTextResponse.GetMessage().GetAssistant().GetToolCalls()
	if len(toolCalls) != 1 {
		return nil, grpc.Errorf(codes.Internal, "expected 1 tool call, got %d", len(toolCalls)).Err()
	}
	parseToolCallRequest := &pb.ParseToolCallRequest{
		ToolCall: toolCalls[0],
	}
	return s.ParseToolCall(ctx, parseToolCallRequest)
}
