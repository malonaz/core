package agent_service

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbjson"
)

// toolCallTitleDescription documents the title every tool call carries, shown to the users following the agent.
const toolCallTitleDescription = "A short user-facing title for this call, shown in place of its arguments: " +
	"what it does and why, at most 8 words, sentence case, no trailing period (e.g. 'Find where pendingReview is used')."

// toolbox holds the tools of one turn of an agent: the built-in tools, and its spec's gRPC methods and genui components.
type toolbox struct {
	service  *Service
	agent    *agentpb.Agent
	tools    []*aipb.Tool
	toolSets []*aipb.ToolSet
	// toolNameToMethodTool and toolNameToGenuiComponent route the calls to the spec's tools, whatever the calls'
	// annotations say.
	toolNameToMethodTool     map[string]*methodTool
	toolNameToGenuiComponent map[string]*genuiComponent
}

// methodTool is a tool calling a gRPC method.
type methodTool struct {
	endpoint      string
	method        protoreflect.MethodDescriptor
	schemaBuilder *pbjson.SchemaBuilder
	// Whether a call awaits the user's approval.
	approvalRequired bool
}

// approvalRequired reports whether a call to a method awaits the user's approval: by its tool's setting, else unless
// the method has no side effects.
func approvalRequired(approval agentpb.AgentSpec_Tool_Approval, method protoreflect.MethodDescriptor) bool {
	switch approval {
	case agentpb.AgentSpec_Tool_APPROVAL_REQUIRED:
		return true
	case agentpb.AgentSpec_Tool_APPROVAL_NOT_REQUIRED:
		return false
	default:
		return method.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel() != descriptorpb.MethodOptions_NO_SIDE_EFFECTS
	}
}

// await reports whether a call awaits a user, and why: the approval of a gated method, or the answer to an input
// component.
func (b *toolbox) await(toolCall *aipb.ToolCall) (agentpb.AwaitedToolCall_Reason, bool) {
	if genuiComponent, ok := b.toolNameToGenuiComponent[toolCall.GetName()]; ok && genuiComponent.input {
		return agentpb.AwaitedToolCall_REASON_INPUT, true
	}
	if methodTool, ok := b.toolNameToMethodTool[toolCall.GetName()]; ok && methodTool.approvalRequired {
		return agentpb.AwaitedToolCall_REASON_APPROVAL, true
	}
	return agentpb.AwaitedToolCall_REASON_UNSPECIFIED, false
}

// newToolbox resolves the tools of the agent's turn: each spec tool naming a method becomes a tool, each naming a
// service a tool set its methods are discovered from, each naming a genui component that component's tool. Methods'
// schemas are reflected from the endpoints serving them.
func (s *Service) newToolbox(ctx context.Context, agent *agentpb.Agent) (*toolbox, error) {
	toolbox := &toolbox{
		service:                  s,
		agent:                    agent,
		tools:                    slices.Clone(s.builtinTools.tools),
		toolNameToMethodTool:     map[string]*methodTool{},
		toolNameToGenuiComponent: map[string]*genuiComponent{},
	}
	if definitions := agent.GetSpec().GetSubAgentDefinitions(); len(definitions) > 0 {
		for i, tool := range toolbox.tools {
			if tool.GetName() == createAgentsToolName {
				tool = proto.CloneOf(tool)
				tool.Description += "\n\nThe definitions you may launch: " + strings.Join(definitions, ", ") + "."
				toolbox.tools[i] = tool
			}
		}
	}
	toolNameSet := map[string]struct{}{}
	addToolName := func(name string) error {
		if _, ok := toolNameSet[name]; ok {
			return status.Errorf(codes.FailedPrecondition, "two tools of agent %s are named %s", agent.GetName(), name).Err()
		}
		toolNameSet[name] = struct{}{}
		return nil
	}
	for _, tool := range toolbox.tools {
		if err := addToolName(tool.GetName()); err != nil {
			return nil, err
		}
	}

	schemaConfiguration := aitool.NewSchemaConfiguration(aitool.WithTitle(toolCallTitleDescription))
	for _, specTool := range agent.GetSpec().GetTools() {
		name := specTool.GetName()
		if genuiComponent, ok := s.genuiComponents.nameToComponent[name]; ok {
			if err := addToolName(genuiComponent.tool.GetName()); err != nil {
				return nil, err
			}
			toolbox.tools = append(toolbox.tools, genuiComponent.tool)
			toolbox.toolNameToGenuiComponent[genuiComponent.tool.GetName()] = genuiComponent
			continue
		}
		endpoint, err := s.toolRouter.endpoint(name)
		if err != nil {
			return nil, err
		}
		schema, err := s.toolRouter.schema(ctx, endpoint)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "resolving tool %s: %v", name, err).Err()
		}
		builder := aitool.NewBuilder(schema)
		descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "tool %s is served by no service at %s", name, endpoint).Err()
		}

		var tools []*aipb.Tool
		switch descriptor := descriptor.(type) {
		case protoreflect.MethodDescriptor:
			if descriptor.IsStreamingClient() || descriptor.IsStreamingServer() {
				return nil, status.Errorf(codes.FailedPrecondition, "tool %s is a streaming method: only unary methods are tools", name).Err()
			}
			createToolRequest := &aienginepb.CreateToolRequest{
				DescriptorReference: &aienginepb.DescriptorReference{FullName: &aienginepb.DescriptorReference_Method{Method: name}},
				SchemaConfiguration: schemaConfiguration,
			}
			tool, err := builder.CreateTool(createToolRequest)
			if err != nil {
				return nil, status.FromError(err, "creating tool "+name).Err()
			}
			toolbox.tools = append(toolbox.tools, tool)
			tools = append(tools, tool)

		case protoreflect.ServiceDescriptor:
			createServiceToolSetRequest := &aienginepb.CreateServiceToolSetRequest{
				ServiceFullName:     name,
				MethodNames:         unaryMethodNames(descriptor),
				SchemaConfiguration: schemaConfiguration,
			}
			toolSet, err := builder.CreateServiceToolSet(createServiceToolSetRequest)
			if err != nil {
				return nil, status.FromError(err, "creating tool set "+name).Err()
			}
			if err := addToolName(toolSet.GetDiscoveryTool().GetName()); err != nil {
				return nil, err
			}
			toolbox.toolSets = append(toolbox.toolSets, toolSet)
			tools = toolSet.GetTools()

		default:
			return nil, status.Errorf(codes.FailedPrecondition, "tool %s is neither a method nor a service", name).Err()
		}

		for _, tool := range tools {
			if err := addToolName(tool.GetName()); err != nil {
				return nil, err
			}
			methodName, _ := aipb.Annotations.GrpcMethod.Get(tool)
			methodDescriptor, err := schema.FindDescriptorByName(protoreflect.FullName(methodName))
			if err != nil {
				return nil, status.Errorf(codes.Internal, "finding method %s: %v", methodName, err).Err()
			}
			method := methodDescriptor.(protoreflect.MethodDescriptor)
			toolbox.toolNameToMethodTool[tool.GetName()] = &methodTool{
				endpoint:         endpoint,
				method:           method,
				schemaBuilder:    builder.SchemaBuilder(),
				approvalRequired: approvalRequired(specTool.GetApproval(), method),
			}
		}
	}
	return toolbox, nil
}

// unaryMethodNames returns the names of a service's unary methods: streaming methods are no tools.
func unaryMethodNames(serviceDescriptor protoreflect.ServiceDescriptor) []string {
	var methodNames []string
	methods := serviceDescriptor.Methods()
	for i := 0; i < methods.Len(); i++ {
		if method := methods.Get(i); !method.IsStreamingClient() && !method.IsStreamingServer() {
			methodNames = append(methodNames, string(method.Name()))
		}
	}
	return methodNames
}

// call executes a tool call the AiService did not answer and returns its result, or an error result if it failed,
// stamped with the call's name and id.
func (b *toolbox) call(ctx context.Context, toolCall *aipb.ToolCall) *aipb.ToolResult {
	toolResult, err := b.execute(ctx, toolCall)
	if err != nil {
		return ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), err)
	}
	toolResult.ToolName = toolCall.GetName()
	toolResult.ToolCallId = toolCall.GetId()
	return toolResult
}

func (b *toolbox) execute(ctx context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	if builtinTool, ok := b.service.builtinTools.nameToBuiltinTool[toolCall.GetName()]; ok {
		return builtinTool.execute(ctx, b.service, b.agent, toolCall)
	}
	if _, ok := b.toolNameToGenuiComponent[toolCall.GetName()]; ok {
		// A display component is rendered by the clients following the agent: acknowledged as such.
		return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), "ok"), nil
	}
	methodTool, ok := b.toolNameToMethodTool[toolCall.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown tool %q: call only the tools you were given", toolCall.GetName()).Err()
	}
	request, err := buildToolCallMessage(methodTool.schemaBuilder, methodTool.method.Input(), toolCall)
	if err != nil {
		return nil, err
	}
	response, err := b.service.toolRouter.invoke(ctx, methodTool.endpoint, methodTool.method, request)
	if err != nil {
		return nil, err
	}
	responseBytes, err := pbutil.JSONMarshal(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling response: %v", err).Err()
	}
	content := &structpb.Value{}
	if err := pbutil.JSONUnmarshal(responseBytes, content); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling response: %v", err).Err()
	}
	return ai.NewStructuredToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// buildToolCallMessage builds the message a tool call's arguments describe, its title stripped.
func buildToolCallMessage(schemaBuilder *pbjson.SchemaBuilder, messageDescriptor protoreflect.MessageDescriptor, toolCall *aipb.ToolCall) (protoreflect.ProtoMessage, error) {
	arguments, err := structpb.NewStruct(pbjson.StripTitle(toolCall.GetArguments().AsMap()))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reading arguments: %v", err).Err()
	}
	// Routed by the tool's own descriptor, never by the call's annotations.
	routedToolCall := &aipb.ToolCall{
		Id:          toolCall.GetId(),
		Name:        toolCall.GetName(),
		Arguments:   arguments,
		Annotations: map[string]string{aipb.Annotations.ProtoMessage.Key: string(messageDescriptor.FullName())},
	}
	message, err := aitool.BuildToolCallMessage(schemaBuilder, routedToolCall)
	if err != nil {
		return nil, err
	}
	return message, nil
}
