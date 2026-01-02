package pbai

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbjson"
)

func (m *ToolManager) executeDiscovery(toolCall *aipb.ToolCall) (proto.Message, error) {
	serviceFQN, ok := toolCall.Annotations[annotationKeyGRPCService]
	if !ok {
		return nil, fmt.Errorf("discovery tool missing service annotation")
	}

	args := toolCall.Arguments.AsMap()
	methodsRaw, ok := args["methods"]
	if !ok {
		return nil, fmt.Errorf("discovery tool missing methods argument")
	}

	methodsArr, ok := methodsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("methods must be an array")
	}

	var methods []string
	for _, v := range methodsArr {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("method name must be a string")
		}
		methods = append(methods, s)
	}

	if err := m.enableMethods(serviceFQN, methods); err != nil {
		return nil, err
	}

	return structpb.NewStringValue("ok"), nil
}

func (m *ToolManager) executeMethod(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	methodFQN := toolCall.Annotations[annotationKeyGRPCMethod]
	if methodFQN == "" {
		return nil, fmt.Errorf("tool call %s missing method annotation", toolCall.Name)
	}

	desc, err := m.schema.FindDescriptorByName(protoreflect.FullName(methodFQN))
	if err != nil {
		return nil, fmt.Errorf("method not found: %s", methodFQN)
	}
	method, ok := desc.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a method: %s", methodFQN)
	}

	req, err := pbjson.BuildMessage(method.Input(), toolCall.Arguments.AsMap())
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	return m.invoker.Invoke(ctx, method, req)
}
