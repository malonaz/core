package agent_service

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/uuid"
)

// toolRequestIDNamespace derives the request id of an RPC tool call from the call's id.
var toolRequestIDNamespace = uuid.MustParse("6d1f2b84-93a7-4c5e-b0d8-71e4a9c3f652")

// toolbox holds the tools of one turn of an agent: the built-in tools, and its spec's stored tools and genui
// components.
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
}

// methodTool is a stored tool calling a gRPC method.
type methodTool struct {
	// The stored tool's resource name.
	resourceName string
	method       *aipb.ToolMethod
	// Whether a call awaits the user's approval.
	approvalRequired bool
}

// approvalRequired reports whether a call to a method awaits the user's approval: by its grant's setting, else unless
// the method has no side effects.
func approvalRequired(approval agentpb.AgentSpec_ToolGrant_Approval, method *aipb.ToolMethod) bool {
	switch approval {
	case agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED:
		return true
	case agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED:
		return false
	default:
		return !method.GetReadOnly()
	}
}

// await reports whether a call awaits a user, why, and the resource name of the tool called: the approval of a gated
// method, or the answer to an input component.
func (b *toolbox) await(toolCall *aipb.ToolUse) (agentpb.AwaitedToolCall_Reason, string, bool) {
	if genuiComponent, ok := b.toolNameToGenuiComponent[toolCall.GetName()]; ok && genuiComponent.input {
		return agentpb.AwaitedToolCall_REASON_RESULT, genuiComponent.resourceName, true
	}
	if methodTool, ok := b.toolNameToMethodTool[toolCall.GetName()]; ok && methodTool.approvalRequired {
		return agentpb.AwaitedToolCall_REASON_APPROVAL, methodTool.resourceName, true
	}
	return agentpb.AwaitedToolCall_REASON_UNSPECIFIED, "", false
}

// toolSetKey identifies a tool set: its tools' parent and their tool-set label.
type toolSetKey struct{ parent, id string }

// genuiToolSetKey is the genui components' set, among the platform tools.
var genuiToolSetKey = toolSetKey{id: genuiToolSet}

func selectorKey(selector *agentpb.ToolSetSelector) toolSetKey {
	return toolSetKey{parent: selector.GetParent(), id: selector.GetId()}
}

// toolSetKeyOf returns the set of a stored tool.
func toolSetKeyOf(tool *aipb.Tool) toolSetKey {
	parent := ""
	if toolRn, err := aipb.ParseToolRn(tool.GetName()); err == nil {
		parent = toolRn.Parent()
	}
	return toolSetKey{parent: parent, id: tool.GetLabels()[aipb.Labels.ToolSet.GetKey()]}
}

// toolGrants indexes a spec's grants by what they grant.
type toolGrants struct {
	toolSetToGrant map[toolSetKey]*agentpb.AgentSpec_ToolGrant
	toolToGrant    map[string]*agentpb.AgentSpec_ToolGrant
}

func newToolGrants(grants []*agentpb.AgentSpec_ToolGrant) *toolGrants {
	toolGrants := &toolGrants{toolSetToGrant: map[toolSetKey]*agentpb.AgentSpec_ToolGrant{}, toolToGrant: map[string]*agentpb.AgentSpec_ToolGrant{}}
	for _, grant := range grants {
		if grant.GetToolSet() != nil {
			toolGrants.toolSetToGrant[selectorKey(grant.GetToolSet())] = grant
		} else {
			toolGrants.toolToGrant[grant.GetTool()] = grant
		}
	}
	return toolGrants
}

// approval returns the approval governing a tool of a set: its own grant's, else its set's.
func (g *toolGrants) approval(tool string, toolSet toolSetKey) agentpb.AgentSpec_ToolGrant_Approval {
	if grant, ok := g.toolToGrant[tool]; ok {
		return grant.GetApproval()
	}
	return g.toolSetToGrant[toolSet].GetApproval()
}

// grantParent returns the parent of what a grant grants: empty for platform tools, else an organization.
func grantParent(grant *agentpb.AgentSpec_ToolGrant) string {
	if grant.GetToolSet() != nil {
		return grant.GetToolSet().GetParent()
	}
	toolRn, err := aipb.ParseToolRn(grant.GetTool())
	if err != nil {
		return ""
	}
	return toolRn.Parent()
}

// agentOrganization returns the name of an agent's organization.
func agentOrganization(agent string) string {
	agentRn, err := agentpb.ParseAgentRn(agent)
	if err != nil {
		return ""
	}
	return agentRn.OrganizationRn().String()
}

// validateTools checks that every grant of a spec of an organization names platform tools or the organization's, and
// no approval for genui components, whose calls never await one. Whether the tools exist is read at each turn.
func (s *Service) validateTools(organization string, spec *agentpb.AgentSpec) error {
	for _, grant := range spec.GetTools() {
		if parent := grantParent(grant); parent != "" && parent != organization {
			return status.Errorf(codes.InvalidArgument, "%s: an agent of %s may be granted only platform tools or its organization's", grantName(grant), organization).Err()
		}
		_, genui := s.genuiComponents.resourceNameToComponent[grant.GetTool()]
		if genui = genui || (grant.GetToolSet() != nil && selectorKey(grant.GetToolSet()) == genuiToolSetKey); genui && grant.GetApproval() != agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED {
			return status.Errorf(codes.InvalidArgument, "genui components never await approval: %s takes none", grantName(grant)).Err()
		}
	}
	return nil
}

// String names a tool set for the model and in errors.
func (k toolSetKey) String() string {
	if k.parent == "" {
		return k.id
	}
	return k.id + " of " + k.parent
}

// grantName returns what a grant grants.
func grantName(grant *agentpb.AgentSpec_ToolGrant) string {
	if grant.GetToolSet() != nil {
		return "tool set " + selectorKey(grant.GetToolSet()).String()
	}
	return grant.GetTool()
}

// grantedTool is a stored tool a spec grants, with the set it is granted through, if any.
type grantedTool struct {
	tool    *aipb.Tool
	method  *aipb.ToolMethod
	toolSet toolSetKey
	inSet   bool
}

// newToolbox resolves the tools of the agent's turn from its grants: a granted set becomes a tool set its tools are
// discovered from, the genui set each component's tool, a tool granted alone that tool. A tool granted inside a
// granted set only refines its approval. Stored tools are read as the service, and only the methods the owner could
// call are offered.
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

	grants := newToolGrants(agent.GetSpec().GetTools())
	for _, genuiComponent := range s.genuiComponents.components {
		if grants.toolSetToGrant[genuiToolSetKey] == nil && grants.toolToGrant[genuiComponent.resourceName] == nil {
			continue
		}
		if err := addToolName(genuiComponent.tool.GetName()); err != nil {
			return nil, err
		}
		toolbox.tools = append(toolbox.tools, genuiComponent.tool)
		toolbox.toolNameToGenuiComponent[genuiComponent.tool.GetName()] = genuiComponent
	}

	grantedTools, err := s.readGrantedTools(ctx, agent.GetSpec().GetTools())
	if err != nil {
		return nil, err
	}
	// The model is offered only what its owner could call.
	reachable := toolbox.reachable(ctx, grantedTools)
	toolSetToTools := map[toolSetKey][]*aipb.Tool{}
	for _, grantedTool := range grantedTools {
		if !reachable[grantedTool.method.GetMethod()] {
			continue
		}
		// The model is given a stored tool under its title, and calls it by it.
		tool := proto.CloneOf(grantedTool.tool)
		tool.Name = tool.GetTitle()
		if err := addToolName(tool.GetName()); err != nil {
			return nil, err
		}
		toolbox.toolNameToMethodTool[tool.GetName()] = &methodTool{
			resourceName:     grantedTool.tool.GetName(),
			method:           grantedTool.method,
			approvalRequired: approvalRequired(grants.approval(grantedTool.tool.GetName(), grantedTool.toolSet), grantedTool.method),
		}
		if grantedTool.inSet {
			toolSetToTools[grantedTool.toolSet] = append(toolSetToTools[grantedTool.toolSet], tool)
		} else {
			toolbox.tools = append(toolbox.tools, tool)
		}
	}
	for _, grant := range agent.GetSpec().GetTools() {
		tools := toolSetToTools[selectorKey(grant.GetToolSet())]
		if grant.GetToolSet() == nil || len(tools) == 0 {
			continue
		}
		// A mirrored set is one service's: its discovery tool is named after it, and lists the set's tools.
		method, _, _ := aipb.Annotations.Method.Get(tools[0])
		serviceName := method.GetService()[strings.LastIndex(method.GetService(), ".")+1:]
		toolSet := aitool.NewToolSet(selectorKey(grant.GetToolSet()).String(), serviceName+"_Discover", "", tools)
		if err := addToolName(toolSet.GetDiscoveryTool().GetName()); err != nil {
			return nil, err
		}
		toolbox.toolSets = append(toolbox.toolSets, toolSet)
	}
	return toolbox, nil
}

// readGrantedTools reads, as the service, the stored tools a spec grants that call a method of a routed service, in
// grant order: every tool of a granted set, then each tool granted alone outside them. A tool granted alone that no
// longer exists is left out.
func (s *Service) readGrantedTools(ctx context.Context, grants []*agentpb.AgentSpec_ToolGrant) ([]*grantedTool, error) {
	ctx = s.asService(ctx)
	var tools []*aipb.Tool
	toolNameSet := map[string]struct{}{}
	for _, grant := range grants {
		toolSet := grant.GetToolSet()
		if toolSet == nil || selectorKey(toolSet) == genuiToolSetKey {
			continue
		}
		listToolsRequest := &aiservicepb.ListToolsRequest{
			Parent:   toolSet.GetParent(),
			Filter:   fmt.Sprintf("labels.%q = %q", aipb.Labels.ToolSet.GetKey(), toolSet.GetId()),
			PageSize: 1000,
		}
		for tool, err := range aip.Iterator[*aipb.Tool](ctx, listToolsRequest, s.aiServiceClient.ListTools) {
			if err != nil {
				return nil, status.FromError(err, "listing the tools of "+grantName(grant)).Err()
			}
			tools = append(tools, tool)
			toolNameSet[tool.GetName()] = struct{}{}
		}
	}
	for _, grant := range grants {
		if _, ok := toolNameSet[grant.GetTool()]; grant.GetTool() == "" || ok || s.genuiComponents.resourceNameToComponent[grant.GetTool()] != nil {
			continue
		}
		tool, err := s.aiServiceClient.GetTool(ctx, &aiservicepb.GetToolRequest{Name: grant.GetTool()})
		if status.HasCode(err, codes.NotFound) {
			continue
		}
		if err != nil {
			return nil, status.FromError(err, "getting tool "+grant.GetTool()).Err()
		}
		tools = append(tools, tool)
	}

	grantedToolSets := newToolGrants(grants).toolSetToGrant
	var grantedTools []*grantedTool
	for _, tool := range tools {
		// Only a method of a routed service can be called: other tools are left out.
		method, ok, err := aipb.Annotations.Method.Get(tool)
		if err != nil || !ok || s.toolRouter.serviceToEndpoint[method.GetService()] == "" {
			continue
		}
		toolSet := toolSetKeyOf(tool)
		_, inSet := grantedToolSets[toolSet]
		grantedTools = append(grantedTools, &grantedTool{tool: tool, method: method, toolSet: toolSet, inSet: inSet})
	}
	return grantedTools, nil
}

// reachable returns the methods of tools the owner could call, as each service's target answers, asked once a turn.
func (b *toolbox) reachable(ctx context.Context, grantedTools []*grantedTool) map[string]bool {
	serviceToMethods := map[string][]string{}
	reachable := map[string]bool{}
	for _, grantedTool := range grantedTools {
		service, method := grantedTool.method.GetService(), grantedTool.method.GetMethod()
		if !reachable[method] {
			serviceToMethods[service] = append(serviceToMethods[service], method)
			reachable[method] = true
		}
	}
	// Without the owner's token the targets cannot be asked, nor called: the calls fail with that error.
	ownerCtx, err := b.asOwner(ctx)
	if err != nil {
		return reachable
	}
	for service, methods := range serviceToMethods {
		reachableMethods, err := b.service.toolRouter.testMethods(ownerCtx, service, methods)
		if err != nil {
			// A target that cannot say keeps them all: its own rules still decide every call.
			if !status.HasCode(err, codes.Unimplemented) {
				b.service.log.Warn("testing the methods of a tool set", "service", service, "error", err)
			}
			continue
		}
		for _, method := range methods {
			reachable[method] = slices.Contains(reachableMethods, method)
		}
	}
	return reachable
}

// call executes a tool call the AiService did not answer and returns its result, or an error result if it failed,
// stamped with the call's name and id.
func (b *toolbox) call(ctx context.Context, toolCall *aipb.ToolUse) (toolResult *aipb.ToolResult) {
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

func (b *toolbox) execute(ctx context.Context, toolCall *aipb.ToolUse) (*aipb.ToolResult, error) {
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
	ownerCtx, err := b.asOwner(ctx)
	if err != nil {
		return nil, err
	}
	requestID := uuid.NewV5(toolRequestIDNamespace, b.agent.GetName()+"/"+toolCall.GetId()).String()
	response, err := b.service.toolRouter.invoke(b.service.asService(ctx), ownerCtx, methodTool.method, toolCall.GetArguments(), requestID)
	if err != nil {
		return nil, err
	}
	return ai.NewStructuredToolResult(toolCall.GetName(), toolCall.GetId(), response), nil
}

// buildToolCallMessage builds the message a tool call's arguments describe, its title stripped, and any request id
// the model made up.
func buildToolCallMessage(schemaBuilder *pbjson.SchemaBuilder, messageDescriptor protoreflect.MessageDescriptor, toolCall *aipb.ToolUse) (protoreflect.ProtoMessage, error) {
	argumentMap := pbjson.StripTitle(toolCall.GetArguments().AsMap())
	if messageDescriptor.Fields().ByName("request_id") != nil {
		delete(argumentMap, "request_id")
	}
	arguments, err := structpb.NewStruct(argumentMap)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reading arguments: %v", err).Err()
	}
	// Routed by the tool's own descriptor, never by the call's annotations.
	routedToolCall := &aipb.ToolUse{
		Id:          toolCall.GetId(),
		Name:        toolCall.GetName(),
		Arguments:   arguments,
		Annotations: map[string]string{aipb.Annotations.ProtoMessage.Key: string(messageDescriptor.FullName())},
	}
	return aitool.BuildToolCallMessage(schemaBuilder, routedToolCall)
}
