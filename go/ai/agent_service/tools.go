package agent_service

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/uuid"
)

// toolCallTitleDescription documents the title every tool call carries, shown to the users following the agent.
const toolCallTitleDescription = "A short user-facing title for this call, shown in place of its arguments: " +
	"what it does and why, at most 8 words, sentence case, no trailing period (e.g. 'Find where pendingReview is used')."

// toolRequestIDNamespace derives the request id of an RPC tool call from the call's id.
var toolRequestIDNamespace = uuid.MustParse("6d1f2b84-93a7-4c5e-b0d8-71e4a9c3f652")

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
	ownerToken               ownerToken
	// toolSetToReachable holds the names of each granted service's methods its owner could call, asked once a turn.
	toolSetToReachable map[string][]string
}

// methodTool is a tool calling a gRPC method.
type methodTool struct {
	// The tool's resource name in the catalogue.
	resourceName  string
	method        protoreflect.MethodDescriptor
	schemaBuilder *pbjson.SchemaBuilder
	// Whether a call awaits the user's approval.
	approvalRequired bool
}

// approvalRequired reports whether a call to a method awaits the user's approval: by its grant's setting, else unless
// the method has no side effects.
func approvalRequired(approval agentpb.AgentSpec_ToolGrant_Approval, method protoreflect.MethodDescriptor) bool {
	switch approval {
	case agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED:
		return true
	case agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED:
		return false
	default:
		return !readOnly(method)
	}
}

// readOnly reports whether a method declares it has no side effects.
func readOnly(method protoreflect.MethodDescriptor) bool {
	return method.Options().(*descriptorpb.MethodOptions).GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS
}

// await reports whether a call awaits a user, why, and the resource name of the tool called: the approval of a gated
// method, or the answer to an input component.
func (b *toolbox) await(toolCall *aipb.ToolCall) (agentpb.AwaitedToolCall_Reason, string, bool) {
	if genuiComponent, ok := b.toolNameToGenuiComponent[toolCall.GetName()]; ok && genuiComponent.input {
		return agentpb.AwaitedToolCall_REASON_RESULT, genuiComponent.resourceName, true
	}
	if methodTool, ok := b.toolNameToMethodTool[toolCall.GetName()]; ok && methodTool.approvalRequired {
		return agentpb.AwaitedToolCall_REASON_APPROVAL, methodTool.resourceName, true
	}
	return agentpb.AwaitedToolCall_REASON_UNSPECIFIED, "", false
}

// toolGrants indexes a spec's grants by what they grant.
type toolGrants struct {
	toolSetToGrant map[string]*agentpb.AgentSpec_ToolGrant
	toolToGrant    map[string]*agentpb.AgentSpec_ToolGrant
}

func newToolGrants(grants []*agentpb.AgentSpec_ToolGrant) *toolGrants {
	toolGrants := &toolGrants{toolSetToGrant: map[string]*agentpb.AgentSpec_ToolGrant{}, toolToGrant: map[string]*agentpb.AgentSpec_ToolGrant{}}
	for _, grant := range grants {
		if grant.GetToolSet() != "" {
			toolGrants.toolSetToGrant[grant.GetToolSet()] = grant
		} else {
			toolGrants.toolToGrant[grant.GetTool()] = grant
		}
	}
	return toolGrants
}

// approval returns the approval governing a tool: its own grant's, else its set's.
func (g *toolGrants) approval(toolRn *agentpb.ToolRn) agentpb.AgentSpec_ToolGrant_Approval {
	if grant, ok := g.toolToGrant[toolRn.String()]; ok {
		return grant.GetApproval()
	}
	return g.toolSetToGrant[toolSetOf(toolRn)].GetApproval()
}

// toolSetOf returns the resource name of a tool's set.
func toolSetOf(toolRn *agentpb.ToolRn) string {
	return (&agentpb.ToolSetRn{ToolSet: toolRn.ToolSet}).String()
}

// validateTools checks that every grant of a spec names a configured tool set, a genui component that exists, and no
// approval for a genui component, whose calls never await one. Methods are checked when a turn reads its targets.
func (s *Service) validateTools(spec *agentpb.AgentSpec) error {
	for _, grant := range spec.GetTools() {
		var toolSet, tool string
		if grant.GetToolSet() != "" {
			toolSetRn, err := agentpb.ParseToolSetRn(grant.GetToolSet())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "parsing tool set %q: %v", grant.GetToolSet(), err).Err()
			}
			toolSet = toolSetRn.ToolSet
		} else {
			toolRn, err := agentpb.ParseToolRn(grant.GetTool())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "parsing tool %q: %v", grant.GetTool(), err).Err()
			}
			toolSet, tool = toolRn.ToolSet, toolRn.Tool
		}
		if toolSet != genuiToolSet {
			if _, ok := s.toolRouter.serviceToEndpoint[toolSet]; !ok {
				return status.Errorf(codes.InvalidArgument, "no tool target configured for tool set %s: add --tool-target for it", toolSet).Err()
			}
			continue
		}
		if _, ok := s.genuiComponents.idToComponent[tool]; tool != "" && !ok {
			return status.Errorf(codes.InvalidArgument, "%s is no genui component", grant.GetTool()).Err()
		}
		if grant.GetApproval() != agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED {
			return status.Errorf(codes.InvalidArgument, "genui components never await approval: %s takes none", grant.GetToolSet()+grant.GetTool()).Err()
		}
	}
	return nil
}

// newToolbox resolves the tools of the agent's turn from its grants: a granted service becomes a tool set its methods
// are discovered from, the granted genui set each component's tool, a tool granted alone that tool. A tool granted
// inside a granted set only refines its approval. Methods' schemas are reflected from the endpoints serving them, and
// only the methods the owner could call are offered.
func (s *Service) newToolbox(ctx context.Context, agent *agentpb.Agent) (*toolbox, error) {
	toolbox := &toolbox{
		service:                  s,
		agent:                    agent,
		tools:                    slices.Clone(s.builtinTools.tools),
		toolNameToMethodTool:     map[string]*methodTool{},
		toolNameToGenuiComponent: map[string]*genuiComponent{},
		toolSetToReachable:       map[string][]string{},
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
	addGenuiComponent := func(genuiComponent *genuiComponent) error {
		if err := addToolName(genuiComponent.tool.GetName()); err != nil {
			return err
		}
		toolbox.tools = append(toolbox.tools, genuiComponent.tool)
		toolbox.toolNameToGenuiComponent[genuiComponent.tool.GetName()] = genuiComponent
		return nil
	}

	grants := newToolGrants(agent.GetSpec().GetTools())
	schemaConfiguration := aitool.NewSchemaConfiguration(aitool.WithTitle(toolCallTitleDescription))
	for _, grant := range agent.GetSpec().GetTools() {
		toolSet, toolID := grant.GetToolSet(), ""
		if toolSet == "" {
			toolRn, err := agentpb.ParseToolRn(grant.GetTool())
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "parsing tool %q: %v", grant.GetTool(), err).Err()
			}
			if toolSet = toolSetOf(toolRn); grants.toolSetToGrant[toolSet] != nil {
				continue
			}
			toolID = toolRn.Tool
		}
		toolSetRn, err := agentpb.ParseToolSetRn(toolSet)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "parsing tool set %q: %v", toolSet, err).Err()
		}

		if toolSetRn.ToolSet == genuiToolSet {
			for _, genuiComponent := range s.genuiComponents.components {
				if toolID != "" && genuiComponent.resourceName != grant.GetTool() {
					continue
				}
				if err := addGenuiComponent(genuiComponent); err != nil {
					return nil, err
				}
			}
			continue
		}

		// Schemas are the same for every owner: read as the service.
		service, schema, err := s.toolRouter.service(s.asService(ctx), toolSetRn.ToolSet)
		if err != nil {
			return nil, err
		}
		builder := aitool.NewBuilder(schema)
		var tools []*aipb.Tool
		if toolID != "" {
			method, ok := unaryMethod(service, toolID)
			if !ok {
				return nil, status.Errorf(codes.FailedPrecondition, "tool %s is no unary method of %s", grant.GetTool(), service.FullName()).Err()
			}
			// The model is offered only what its owner could call.
			if !slices.Contains(toolbox.reachable(ctx, service), toolID) {
				continue
			}
			createToolRequest := &aienginepb.CreateToolRequest{
				DescriptorReference: &aienginepb.DescriptorReference{FullName: &aienginepb.DescriptorReference_Method{Method: string(method.FullName())}},
				SchemaConfiguration: methodSchemaConfiguration(method),
			}
			tool, err := builder.CreateTool(createToolRequest)
			if err != nil {
				return nil, status.FromError(err, "creating tool "+grant.GetTool()).Err()
			}
			toolbox.tools = append(toolbox.tools, tool)
			tools = append(tools, tool)
		} else {
			methodNames := toolbox.reachable(ctx, service)
			if len(methodNames) == 0 {
				continue
			}
			methodNameToSchemaConfiguration := map[string]*aienginepb.SchemaConfiguration{}
			for i := 0; i < service.Methods().Len(); i++ {
				method := service.Methods().Get(i)
				if requestIDField(method) != nil {
					methodNameToSchemaConfiguration[string(method.Name())] = methodSchemaConfiguration(method)
				}
			}
			createServiceToolSetRequest := &aienginepb.CreateServiceToolSetRequest{
				ServiceFullName:                 string(service.FullName()),
				MethodNames:                     methodNames,
				SchemaConfiguration:             schemaConfiguration,
				MethodNameToSchemaConfiguration: methodNameToSchemaConfiguration,
			}
			toolSet, err := builder.CreateServiceToolSet(createServiceToolSetRequest)
			if err != nil {
				return nil, status.FromError(err, "creating tool set "+grant.GetToolSet()).Err()
			}
			if err := addToolName(toolSet.GetDiscoveryTool().GetName()); err != nil {
				return nil, err
			}
			toolbox.toolSets = append(toolbox.toolSets, toolSet)
			tools = toolSet.GetTools()
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
			toolRn := toolSetRn.ToolRn(string(method.Name()))
			toolbox.toolNameToMethodTool[tool.GetName()] = &methodTool{
				resourceName:     toolRn.String(),
				method:           method,
				schemaBuilder:    builder.SchemaBuilder(),
				approvalRequired: approvalRequired(grants.approval(toolRn), method),
			}
		}
	}
	return toolbox, nil
}

// reachable returns the names of a service's unary methods the owner could call, asked of its target once a turn.
func (b *toolbox) reachable(ctx context.Context, service protoreflect.ServiceDescriptor) []string {
	id := string(service.FullName())
	if names, ok := b.toolSetToReachable[id]; ok {
		return names
	}
	names := unaryMethodNames(service)
	// Without the owner's token the target cannot be asked, nor called: its calls fail with that error.
	if ownerCtx, err := b.asOwner(ctx); err == nil {
		names = b.service.reachableMethodNames(ownerCtx, service)
	}
	b.toolSetToReachable[id] = names
	return names
}

// reachableMethodNames returns the names of a routed service's unary methods the session ctx carries could call, as
// its target answers. A target that cannot say (it serves no AuthorizationService, or fails) keeps them all: its own
// rules still decide every call.
func (s *Service) reachableMethodNames(ctx context.Context, service protoreflect.ServiceDescriptor) []string {
	names := unaryMethodNames(service)
	reachable, err := s.toolRouter.testMethods(ctx, service, names)
	if err != nil {
		if !status.HasCode(err, codes.Unimplemented) {
			s.log.Warn("testing the methods of a tool set", "tool_set", service.FullName(), "error", err)
		}
		return names
	}
	return reachable
}

// requestIDField returns the request_id field of a method's request, if any: the service seeds it from the call's
// id, so a call run again after a crash is deduplicated, and the model never generates it.
func requestIDField(method protoreflect.MethodDescriptor) protoreflect.FieldDescriptor {
	return messageRequestIDField(method.Input())
}

// messageRequestIDField returns a request message's string request_id field, if any.
func messageRequestIDField(message protoreflect.MessageDescriptor) protoreflect.FieldDescriptor {
	field := message.Fields().ByName("request_id")
	if field == nil || field.Kind() != protoreflect.StringKind || field.Cardinality() == protoreflect.Repeated {
		return nil
	}
	return field
}

// methodSchemaConfiguration returns the schema configuration of a method's tool: titled, without a request id.
func methodSchemaConfiguration(method protoreflect.MethodDescriptor) *aienginepb.SchemaConfiguration {
	options := []aitool.Option{aitool.WithTitle(toolCallTitleDescription)}
	if requestIDField(method) != nil {
		var paths []string
		fields := method.Input().Fields()
		for i := 0; i < fields.Len(); i++ {
			if field := fields.Get(i); field.Name() != "request_id" {
				paths = append(paths, string(field.Name()))
			}
		}
		options = append(options, aitool.WithFieldMask(pbfieldmask.FromPaths(paths...)))
	}
	return aitool.NewSchemaConfiguration(options...)
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
func (b *toolbox) call(ctx context.Context, toolCall *aipb.ToolCall) (toolResult *aipb.ToolResult) {
	// A tool's panic fails its call, not the replica.
	defer func() {
		if recovered := recover(); recovered != nil {
			err := status.Errorf(codes.Internal, "tool call panicked: %v", recovered).Err()
			toolResult = ai.NewErrorToolResult(toolCall.GetName(), toolCall.GetId(), err)
		}
	}()
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
	if field := requestIDField(methodTool.method); field != nil {
		requestID := uuid.NewV5(toolRequestIDNamespace, b.agent.GetName()+"/"+toolCall.GetId()).String()
		request.ProtoReflect().Set(field, protoreflect.ValueOfString(requestID))
	}
	// The builder sets generated well-known types on the dynamic request: validation walks one descriptor set.
	if request, err = uniformMessage(methodTool.method.Input(), request); err != nil {
		return nil, err
	}
	ownerCtx, err := b.asOwner(ctx)
	if err != nil {
		return nil, err
	}
	response, err := b.service.toolRouter.invoke(ownerCtx, methodTool.method, request)
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

// uniformMessage re-reads a message as a dynamic message of one descriptor, its nested messages included.
func uniformMessage(messageDescriptor protoreflect.MessageDescriptor, message proto.Message) (protoreflect.ProtoMessage, error) {
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling request: %v", err).Err()
	}
	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)
	if err := proto.Unmarshal(messageBytes, dynamicMessage); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling request: %v", err).Err()
	}
	return dynamicMessage, nil
}

// buildToolCallMessage builds the message a tool call's arguments describe, its title stripped, and any request id
// the model made up.
func buildToolCallMessage(schemaBuilder *pbjson.SchemaBuilder, messageDescriptor protoreflect.MessageDescriptor, toolCall *aipb.ToolCall) (protoreflect.ProtoMessage, error) {
	argumentMap := pbjson.StripTitle(toolCall.GetArguments().AsMap())
	if messageRequestIDField(messageDescriptor) != nil {
		delete(argumentMap, "request_id")
	}
	arguments, err := structpb.NewStruct(argumentMap)
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
