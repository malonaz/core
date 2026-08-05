package tool

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
)

func ParseDiscoveryToolCall(toolCall *aipb.ToolCall) (*aipb.ToolCallDiscovery, error) {
	args := toolCall.GetArguments().AsMap()
	toolNamesRaw, _ := args["tools"].([]any)
	var toolNames []string
	for _, name := range toolNamesRaw {
		if s, ok := name.(string); ok {
			toolNames = append(toolNames, s)
		}
	}

	toolSetName, ok := toolCall.GetAnnotations()[AnnotationKeyToolSetName]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing %s annotation", AnnotationKeyToolSetName).Err()
	}

	return &aipb.ToolCallDiscovery{
		ToolSetName: toolSetName,
		ToolNames:   toolNames,
	}, nil
}

func ParseToolCall(schema *pbjson.SchemaBuilder, toolCall *aipb.ToolCall, toolSets []*aipb.ToolSet) (*pb.ParseToolCallResponse, error) {
	toolType, ok := aip.GetAnnotation(toolCall, AnnotationKeyToolType)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing annotations on tool call").Err()
	}
	switch toolType {
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
			for _, tool := range toolSet.GetTools() {
				if toolCall.GetName() == tool.GetName() {
					found = true
					break
				}
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

	arguments := toolCall.GetArguments().AsMap()

	var fieldMask *pbfieldmask.FieldMask
	if generationFieldMask, ok := annotations[AnnotationKeyGenerationFieldMask]; ok {
		fieldMask = pbfieldmask.FromString(generationFieldMask)
		// Prune before building: fields outside the mask were never in the schema
		// shown to the model, and hallucinated values may not even type-check
		// against the proto (e.g. an update_mask emitted as an object), which
		// would fail the build outright.
		arguments = pruneArguments(arguments, "", fieldMask)
	}

	dynamicMessage, err := schemaBuilder.BuildMessage(protoreflect.FullName(messageFullName), arguments)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building message: %v", err).Err()
	}

	if fieldMask != nil {
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

// pruneArguments returns a copy of arguments restricted to the field mask.
// Keys on or below a mask path are kept whole; keys that are ancestors of a
// mask path are recursed into to strip disallowed siblings; everything else
// (i.e. fields the schema never exposed to the model) is dropped.
func pruneArguments(arguments map[string]any, prefix string, fieldMask *pbfieldmask.FieldMask) map[string]any {
	prunedArguments := make(map[string]any, len(arguments))
	for key, value := range arguments {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if maskAllowsSubtree(path, fieldMask) {
			prunedArguments[key] = value
			continue
		}
		if !fieldMask.Contains(path) {
			continue
		}
		// The path is a strict ancestor of a mask path: recurse to keep only
		// the allowed descendants.
		switch typedValue := value.(type) {
		case map[string]any:
			prunedArguments[key] = pruneArguments(typedValue, path, fieldMask)
		case []any:
			prunedItems := make([]any, 0, len(typedValue))
			for _, item := range typedValue {
				if itemMap, ok := item.(map[string]any); ok {
					prunedItems = append(prunedItems, pruneArguments(itemMap, path, fieldMask))
					continue
				}
				prunedItems = append(prunedItems, item)
			}
			prunedArguments[key] = prunedItems
		default:
			// A scalar where the mask expects a subtree cannot match the schema; drop it.
		}
	}
	return prunedArguments
}

// maskAllowsSubtree reports whether the path is exactly on, or nested under,
// a mask path — in which case its entire subtree is retained.
func maskAllowsSubtree(path string, fieldMask *pbfieldmask.FieldMask) bool {
	for _, maskPath := range fieldMask.GetPaths() {
		if maskPath == pbfieldmask.WildcardPath || maskPath == path || strings.HasPrefix(path, maskPath+".") {
			return true
		}
	}
	return false
}
