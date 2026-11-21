package agent_service

import (
	"context"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	genuipb "github.com/malonaz/core/genproto/ai/genui/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

// genuiComponentFile declares every genui component, through the Component envelope.
const genuiComponentFile = "malonaz/ai/genui/v1/component.proto"

// genuiComponent is a genui component an agent may render, by calling its tool.
type genuiComponent struct {
	tool *aipb.Tool
	// The component's message full name.
	name protoreflect.FullName
	// Whether the component collects the user's answer: its call awaits the user.
	input bool
}

// genuiComponents are the genui components agents may render, served by this binary: they need no tool target.
type genuiComponents struct {
	nameToComponent     map[string]*genuiComponent
	toolNameToComponent map[string]*genuiComponent
}

func newGenuiComponents(ctx context.Context) (*genuiComponents, error) {
	builder, err := newInProcessBuilder(ctx, genuiComponentFile)
	if err != nil {
		return nil, err
	}
	genuiComponents := &genuiComponents{nameToComponent: map[string]*genuiComponent{}, toolNameToComponent: map[string]*genuiComponent{}}
	fields := (&genuipb.Component{}).ProtoReflect().Descriptor().Oneofs().ByName("component").Fields()
	for i := 0; i < fields.Len(); i++ {
		message := fields.Get(i).Message()
		input := proto.GetExtension(message.Options(), genuipb.E_Input).(bool)
		createToolRequest := &aienginepb.CreateToolRequest{
			DescriptorReference: &aienginepb.DescriptorReference{
				FullName: &aienginepb.DescriptorReference_Message{Message: string(message.FullName())},
			},
			SchemaConfiguration: aitool.NewSchemaConfiguration(aitool.WithTitle(toolCallTitleDescription)),
		}
		tool, err := builder.CreateTool(createToolRequest)
		if err != nil {
			return nil, fmt.Errorf("creating the tool of genui component %s: %w", message.FullName(), err)
		}
		tool.Description = fmt.Sprintf("Show the user a %s, rendered as you generate it.", message.Name())
		if input {
			tool.Description = fmt.Sprintf("Ask the user with a %s. END YOUR TURN after calling it: the user's answer arrives as this call's result.", message.Name())
		}
		genuiComponent := &genuiComponent{tool: tool, name: message.FullName(), input: input}
		genuiComponents.nameToComponent[string(message.FullName())] = genuiComponent
		genuiComponents.toolNameToComponent[tool.GetName()] = genuiComponent
	}
	return genuiComponents, nil
}

// validateInputResponse checks that a result answering an input component's call carries the component's
// InputResponse, and fills its content with the response's canonical JSON if empty: the model reads the content.
func (c *genuiComponents) validateInputResponse(toolName, toolCallID string, toolResult *aipb.ToolResult) error {
	component, ok := c.toolNameToComponent[toolName]
	if !ok || !component.input {
		return status.Errorf(codes.Internal, "tool %s is no input component", toolName).Err()
	}
	inputResponse, ok, err := genuipb.Annotations.InputResponse.Get(toolResult)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "reading the input response: %v", err).Err()
	}
	if !ok {
		return status.Errorf(codes.InvalidArgument, "the result answering input component %s must carry the %s annotation", component.name, genuipb.Annotations.InputResponse.Key).Err()
	}
	if err := protovalidate.Validate(inputResponse); err != nil {
		return status.Errorf(codes.InvalidArgument, "validating the input response: %v", err).Err()
	}
	if inputResponse.GetComponentId() != toolCallID {
		return status.Errorf(codes.InvalidArgument, "the input response answers component %q, not tool call %q", inputResponse.GetComponentId(), toolCallID).Err()
	}
	// Each component's answer is its message name suffixed with "Response".
	response := inputResponse.ProtoReflect().WhichOneof(inputResponse.ProtoReflect().Descriptor().Oneofs().ByName("response"))
	if response.Message().FullName() != component.name+"Response" {
		return status.Errorf(codes.InvalidArgument, "a %s does not answer a %s", response.Message().Name(), component.name.Name()).Err()
	}
	if toolResult.GetError() != nil {
		return status.Errorf(codes.InvalidArgument, "an input component is answered by a result, not an error").Err()
	}
	if toolResult.GetResult() == nil {
		content, err := pbutil.JSONMarshal(inputResponse)
		if err != nil {
			return status.Errorf(codes.Internal, "marshaling the input response: %v", err).Err()
		}
		toolResult.Result = &aipb.ToolResult_Content{Content: string(content)}
	}
	return nil
}
