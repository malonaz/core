package tool

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
)

func GetToolSetName(toolCall *aipb.ToolCall) (string, bool) {
	annotations := toolCall.GetAnnotations()
	if len(annotations) == 0 {
		return "", false
	}
	value, ok := annotations[AnnotationKeyToolSetName]
	return value, ok
}

func ParseToolCall(schema *pbjson.SchemaBuilder, toolCall *aipb.ToolCall, toolSets []*aipb.ToolSet) (*pb.ParseToolCallResponse, error) {
	annotations := toolCall.GetAnnotations()
	if len(annotations) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}

	switch toolType := annotations[AnnotationKeyToolType]; toolType {
	case AnnotationValueToolTypeDiscovery:
		return parseDiscoveryToolCall(toolCall, toolSets)

	case AnnotationValueToolTypeGenerateRPCRequest:
		return parseRPCToolCall(schema, toolCall, toolSets)

	case AnnotationValueToolTypeGenerateMessage:
		message, err := ParseToolCallMessage(schema, toolCall)
		if err != nil {
			return nil, err
		}
		return &pb.ParseToolCallResponse{
			Result: &pb.ParseToolCallResponse_Message{Message: message},
		}, nil

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown tool type %s", toolType).Err()
	}
}

func parseDiscoveryToolCall(toolCall *aipb.ToolCall, toolSets []*aipb.ToolSet) (*pb.ParseToolCallResponse, error) {
	args := toolCall.GetArguments().AsMap()
	toolNamesRaw, _ := args["tools"].([]any)
	var toolNames []string
	for _, name := range toolNamesRaw {
		if s, ok := name.(string); ok {
			toolNames = append(toolNames, s)
		}
	}

	var targetToolSet *aipb.ToolSet
	for _, toolSet := range toolSets {
		if toolSet.GetDiscoveryTool().GetName() == toolCall.GetName() {
			targetToolSet = toolSet
			break
		}
	}
	if targetToolSet == nil {
		return nil, status.Errorf(codes.NotFound, "tool %q not found", toolCall.GetName()).Err()
	}

	for _, toolName := range toolNames {
		discoverTimestamp, ok := targetToolSet.ToolNameToDiscoverTimestamp[toolName]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tool %q not found in tool set", toolName).Err()
		}
		if discoverTimestamp > 0 {
			return nil, status.Errorf(codes.AlreadyExists, "tool %q already discovered", toolName).
				WithDetails(&pb.ParseToolCallRecoverableError{
					ToolResult: ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), fmt.Errorf("tool already discovered")),
				}).Err()
		}
	}

	return &pb.ParseToolCallResponse{
		Result: &pb.ParseToolCallResponse_Discovery{
			Discovery: &aipb.ToolCallDiscovery{
				ToolSetName: targetToolSet.GetName(),
				ToolNames:   toolNames,
			},
		},
	}, nil
}

func parseRPCToolCall(schema *pbjson.SchemaBuilder, toolCall *aipb.ToolCall, toolSets []*aipb.ToolSet) (*pb.ParseToolCallResponse, error) {
	annotations := toolCall.GetAnnotations()
	toolType := annotations[AnnotationKeyToolType]

	methodFullName, ok := annotations[AnnotationKeyGRPCMethod]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "tool of type %q missing annotation %q", toolType, AnnotationKeyGRPCMethod).Err()
	}
	serviceFullName, ok := annotations[AnnotationKeyGRPCService]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "tool of type %q missing annotation %q", toolType, AnnotationKeyGRPCService).Err()
	}

	if _, ok := annotations[AnnotationKeyDiscoverableTool]; ok {
		var found bool
		for _, toolSet := range toolSets {
			if discoverTimestamp, ok := toolSet.GetToolNameToDiscoverTimestamp()[toolCall.GetName()]; ok {
				if discoverTimestamp == 0 {
					return nil, status.Errorf(codes.FailedPrecondition, "tool %q has not been discovered", toolCall.GetName()).Err()
				}
				found = true
				break
			}
		}
		if !found {
			return nil, status.Errorf(codes.NotFound, "tool %q not found", toolCall.GetName()).Err()
		}
	}

	request, err := ParseToolCallMessage(schema, toolCall)
	if err != nil {
		return nil, err
	}
	readMask, _ := pbjson.GetResponseReadMask(toolCall.GetArguments().AsMap())

	return &pb.ParseToolCallResponse{
		Result: &pb.ParseToolCallResponse_Rpc{
			Rpc: &aipb.ToolCallRpc{
				ServiceFullName: serviceFullName,
				MethodFullName:  methodFullName,
				Request:         request,
				ReadMask:        readMask,
			},
		},
	}, nil
}

func ParseToolCallMessage(schemaBuilder *pbjson.SchemaBuilder, toolCall *aipb.ToolCall) (*structpb.Struct, error) {
	annotations := toolCall.GetAnnotations()
	if len(annotations) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}

	messageFullName, ok := annotations[AnnotationKeyProtoMessage]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing %s annotation", AnnotationKeyProtoMessage).Err()
	}

	dynamicMessage, err := schemaBuilder.BuildMessage(protoreflect.FullName(messageFullName), toolCall.GetArguments().AsMap())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building message: %v", err).Err()
	}

	if generationFieldMask, ok := annotations[AnnotationKeyGenerationFieldMask]; ok {
		fieldMask := pbfieldmask.FromString(generationFieldMask)
		if err := fieldMask.Validate(dynamicMessage); err != nil {
			return nil, status.Errorf(codes.Internal, "validating generation field mask: %v", err).Err()
		}
		fieldMask.Apply(dynamicMessage)
	}

	message := &structpb.Struct{}
	if err := pbutil.UnmarshalFromDynamic(message, dynamicMessage); err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling to struct: %v", err).Err()
	}

	return message, nil
}
