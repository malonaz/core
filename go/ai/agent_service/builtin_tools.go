package agent_service

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"buf.build/go/protovalidate"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbreflection"
	"github.com/malonaz/core/go/uuid"
)

const (
	createAgentsToolName = "create_agents"
	sendInputToolName    = "send_input"
	listAgentsToolName   = "list_agents"
	updateLabelsToolName = "update_labels"

	// builtinToolFile declares the built-in tools' arguments.
	builtinToolFile = "malonaz/ai/agent/agent_service/v1/builtin_tool.proto"

	// endTurnHint closes the results of the tools that message agents: replies are inputs that wake the caller,
	// never something to wait for.
	endTurnHint = "End your turn now: each reply arrives later as a new message that wakes you. Never poll for it."
)

// builtinToolNamespace derives the request ids of what built-in tools create from their call's id, so a call run
// again after a crash creates nothing twice.
var builtinToolNamespace = uuid.MustParse("9b1f6c2e-7d84-4a53-b0e6-3c5a8f21d947")

// builtinTools are the tools every agent holds, implemented natively.
type builtinTools struct {
	tools             []*aipb.Tool
	nameToBuiltinTool map[string]*builtinTool
}

// builtinTool is a built-in tool: its arguments are a message of builtinToolFile.
type builtinTool struct {
	tool    *aipb.Tool
	execute func(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error)
}

func newBuiltinTools(ctx context.Context) (*builtinTools, error) {
	// The arguments' schemas come from this binary's own descriptors, through the same machinery as RPC tools.
	serviceInfoProvider, err := pbreflection.NewServiceInfoProvider(protoregistry.GlobalFiles, nil)
	if err != nil {
		return nil, err
	}
	reflectionServerOptions := reflection.ServerOptions{
		Services:           serviceInfoProvider,
		DescriptorResolver: protoregistry.GlobalFiles,
		ExtensionResolver:  protoregistry.GlobalTypes,
	}
	reflectionClient := pbreflection.NewServerReflectionClientInProc(reflectionServerOptions)
	schema, err := pbreflection.ResolveSchema(ctx, reflectionClient, pbreflection.WithFiles(builtinToolFile))
	if err != nil {
		return nil, fmt.Errorf("resolving the built-in tools' schema: %w", err)
	}
	builder := aitool.NewBuilder(schema)

	createAgentsTool, err := newBuiltinTool(builder, createAgentsToolName, createAgentsToolDescription, createAgents)
	if err != nil {
		return nil, err
	}
	sendInputTool, err := newBuiltinTool(builder, sendInputToolName, sendInputToolDescription, sendInput)
	if err != nil {
		return nil, err
	}
	listAgentsTool, err := newBuiltinTool(builder, listAgentsToolName, listAgentsToolDescription, listAgents)
	if err != nil {
		return nil, err
	}
	updateLabelsTool, err := newBuiltinTool(builder, updateLabelsToolName, updateLabelsToolDescription, updateLabels)
	if err != nil {
		return nil, err
	}
	builtinTools := &builtinTools{nameToBuiltinTool: map[string]*builtinTool{}}
	for _, builtinTool := range []*builtinTool{createAgentsTool, sendInputTool, listAgentsTool, updateLabelsTool} {
		builtinTools.tools = append(builtinTools.tools, builtinTool.tool)
		builtinTools.nameToBuiltinTool[builtinTool.tool.GetName()] = builtinTool
	}
	return builtinTools, nil
}

// newBuiltinTool builds a built-in tool whose arguments are a T, parsed and validated before run.
func newBuiltinTool[T proto.Message](
	builder *aitool.Builder,
	name, description string,
	run func(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments T) (*aipb.ToolResult, error),
) (*builtinTool, error) {
	var arguments T
	tool, err := builder.CreateTool(aitool.MessageToolRequest(arguments, aitool.WithTitle(toolCallTitleDescription)))
	if err != nil {
		return nil, fmt.Errorf("creating built-in tool %s: %w", name, err)
	}
	tool.Name = name
	tool.Description = description
	messageDescriptor := arguments.ProtoReflect().Descriptor()
	execute := func(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
		dynamicArguments, err := buildToolCallMessage(builder.SchemaBuilder(), messageDescriptor, toolCall)
		if err != nil {
			return nil, err
		}
		// The schema's descriptors are reflected copies: convert to the generated type through the wire format.
		argumentsBytes, err := proto.Marshal(dynamicArguments)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling arguments: %v", err).Err()
		}
		typedArguments := arguments.ProtoReflect().New().Interface().(T)
		if err := proto.Unmarshal(argumentsBytes, typedArguments); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshaling arguments: %v", err).Err()
		}
		if err := protovalidate.Validate(typedArguments); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid arguments: %v", err).Err()
		}
		return run(ctx, s, agent, toolCall, typedArguments)
	}
	return &builtinTool{tool: tool, execute: execute}, nil
}

// builtinRequestID derives the request id of something a built-in tool call creates.
func builtinRequestID(agent *agentpb.Agent, toolCall *aipb.ToolCall, parts ...string) string {
	return uuid.NewV5(builtinToolNamespace, strings.Join(append([]string{agent.GetName(), toolCall.GetId()}, parts...), "/")).String()
}

// sendAgentInput queues a text message from an agent on another, opening with who sent it: the recipient's model
// sees only the blocks of the inputs it is delivered.
func (s *Service) sendAgentInput(ctx context.Context, sender *agentpb.Agent, recipient, text, requestID string) error {
	header := fmt.Sprintf("[Message from agent %s (%q)]", sender.GetName(), sender.GetTitle())
	message := ai.NewUserMessage(ai.NewTextBlock(header), ai.NewTextBlock(text))
	agentpb.Annotations.Sender.Set(message, sender.GetName())
	createInputRequest := &agentservicepb.CreateInputRequest{
		Parent:    recipient,
		Input:     &agentpb.Input{Message: message},
		RequestId: requestID,
	}
	_, err := s.CreateInput(ctx, createInputRequest)
	return err
}

// structuredToolResult renders a message as a tool call's structured result.
func structuredToolResult(toolCall *aipb.ToolCall, message proto.Message) (*aipb.ToolResult, error) {
	content, err := pbutil.MarshalToStruct(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling result: %v", err).Err()
	}
	return ai.NewStructuredToolResult(toolCall.GetName(), toolCall.GetId(), structpb.NewStructValue(content)), nil
}

// ============================== create_agents ==============================

const createAgentsToolDescription = "Launch one or more sub-agents, each working on its own task concurrently with you. " +
	"Each sub-agent runs either a `definition` you may launch, which sets its instructions, model, reasoning effort and tools, " +
	"or ad-hoc `instructions`, with a `model`, `reasoning_effort` and `tools` (a subset of yours) that default to yours. " +
	"It receives its `task` as its first message and shares none of your context: include everything it needs, and what to report back. " +
	"Launching never blocks: this returns the sub-agents' names at once. " +
	"After launching, END YOUR TURN: each time a sub-agent ends a turn, its report arrives as a new message that wakes you, naming the sub-agent. " +
	"Never poll for reports with list_agents. Use send_input to follow up with a sub-agent."

func createAgents(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments *agentservicepb.CreateAgentsToolArguments) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	// Validate every launch first, so an invalid one launches none.
	subAgents := make([]*agentpb.Agent, 0, len(arguments.GetAgents()))
	for i, subAgentArguments := range arguments.GetAgents() {
		subAgent, err := newSubAgent(agent, subAgentArguments)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "agent %d: %v", i+1, err).Err()
		}
		subAgents = append(subAgents, subAgent)
	}

	lines := make([]string, len(subAgents))
	var eg errgroup.Group
	for i, subAgent := range subAgents {
		eg.Go(func() error {
			createAgentRequest := &agentservicepb.CreateAgentRequest{
				Parent:    agentRn.UserRn().String(),
				Agent:     subAgent,
				RequestId: builtinRequestID(agent, toolCall, strconv.Itoa(i), "agent"),
			}
			// Failures are reported per sub-agent, never aborting the others.
			if err := protovalidate.Validate(createAgentRequest); err != nil {
				lines[i] = fmt.Sprintf("- %q: launch failed: %v", subAgent.GetTitle(), err)
				return nil
			}
			createdAgent, err := s.CreateAgent(ctx, createAgentRequest)
			if err != nil {
				lines[i] = fmt.Sprintf("- %q: launch failed: %v", subAgent.GetTitle(), err)
				return nil
			}
			task := arguments.GetAgents()[i].GetTask()
			if err := s.sendAgentInput(ctx, agent, createdAgent.GetName(), task, builtinRequestID(agent, toolCall, strconv.Itoa(i), "task")); err != nil {
				lines[i] = fmt.Sprintf("- %s (%q): created, but sending its task failed, so it is idle; retry with send_input: %v", createdAgent.GetName(), subAgent.GetTitle(), err)
				return nil
			}
			lines[i] = fmt.Sprintf("- %s (%q): launched", createdAgent.GetName(), subAgent.GetTitle())
			return nil
		})
	}
	_ = eg.Wait()
	content := strings.Join(lines, "\n") + "\n" + endTurnHint
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// newSubAgent validates a launch against what the caller may delegate: only its own definitions and tools. Ad-hoc
// settings default to the caller's.
func newSubAgent(agent *agentpb.Agent, arguments *agentservicepb.CreateAgentsToolArguments_SubAgent) (*agentpb.Agent, error) {
	spec := agent.GetSpec()
	subAgent := &agentpb.Agent{
		Title:       arguments.GetTitle(),
		ParentAgent: agent.GetName(),
	}

	if arguments.GetDefinition() != "" {
		if arguments.GetInstructions() != "" || arguments.GetModel() != "" || arguments.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED || len(arguments.GetTools()) > 0 {
			return nil, fmt.Errorf("a definition sets the instructions, model, reasoning_effort and tools: omit them")
		}
		if !slices.Contains(spec.GetSubAgentDefinitions(), arguments.GetDefinition()) {
			if len(spec.GetSubAgentDefinitions()) == 0 {
				return nil, fmt.Errorf("you may not launch definition %q: launch an ad-hoc sub-agent instead", arguments.GetDefinition())
			}
			return nil, fmt.Errorf("you may not launch definition %q: launch one of %s, or an ad-hoc sub-agent",
				arguments.GetDefinition(), strings.Join(spec.GetSubAgentDefinitions(), ", "))
		}
		subAgent.Definition = arguments.GetDefinition()
		return subAgent, nil
	}

	if strings.TrimSpace(arguments.GetInstructions()) == "" {
		return nil, fmt.Errorf("instructions are required without a definition")
	}
	tools := spec.GetTools()
	if len(arguments.GetTools()) > 0 {
		var unavailableTools []string
		for _, tool := range arguments.GetTools() {
			if !slices.Contains(spec.GetTools(), tool) {
				unavailableTools = append(unavailableTools, tool)
			}
		}
		if len(unavailableTools) > 0 {
			return nil, fmt.Errorf("tools you do not hold: %s (yours: %s)", strings.Join(unavailableTools, ", "), strings.Join(spec.GetTools(), ", "))
		}
		tools = arguments.GetTools()
	}
	subAgent.Spec = &agentpb.AgentSpec{
		Instructions:    arguments.GetInstructions(),
		Model:           cmp.Or(arguments.GetModel(), spec.GetModel()),
		ReasoningEffort: cmp.Or(arguments.GetReasoningEffort(), spec.GetReasoningEffort()),
		Tools:           slices.Clone(tools),
	}
	return subAgent, nil
}

// ================================ send_input ================================

const sendInputToolDescription = "Send a message to another agent in your tree: typically one of your sub-agents, to follow up or redirect it, or your parent agent. " +
	"Sending never blocks and returns no reply: the recipient reads the message on its next turn. " +
	"After sending, END YOUR TURN: a sub-agent's next turn report arrives later as a new message that wakes you. Never poll for it."

func sendInput(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments *agentservicepb.SendInputToolArguments) (*aipb.ToolResult, error) {
	if _, err := agentpb.ParseAgentRn(arguments.GetAgent()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid agent %q: %v", arguments.GetAgent(), err).Err()
	}
	if arguments.GetAgent() == agent.GetName() {
		return nil, status.Errorf(codes.InvalidArgument, "you cannot message yourself").Err()
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: arguments.GetAgent()}
	recipient, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if recipient.GetRootAgent() != agent.GetRootAgent() {
		return nil, status.Errorf(codes.PermissionDenied, "agent %q is not in your agent tree", arguments.GetAgent()).Err()
	}
	if err := s.sendAgentInput(ctx, agent, recipient.GetName(), arguments.GetMessage(), builtinRequestID(agent, toolCall)); err != nil {
		return nil, err
	}
	content := fmt.Sprintf("Sent to %s. %s", recipient.GetName(), endTurnHint)
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// ================================ list_agents ===============================

const listAgentsToolDescription = "List your sub-agents: their name, title, state, how their latest turn ended and labels. " +
	"For an overview only: sub-agent reports arrive by themselves as new messages, so never call this to wait for them."

var subAgentSummaryMask = pbfieldmask.FromPaths("name", "title", "state", "last_turn_outcome", "labels").MustValidate(&agentpb.Agent{})

func listAgents(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, _ *agentservicepb.ListAgentsToolArguments) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	listAgentsRequest := &agentservicepb.ListAgentsRequest{
		Parent: agentRn.UserRn().String(),
		Filter: fmt.Sprintf("parent_agent = %q", agent.GetName()),
		// Launch order, stable while paginating.
		OrderBy: "create_time asc",
	}
	subAgents, err := aip.Paginate[*agentpb.Agent](ctx, listAgentsRequest, s.ListAgents)
	if err != nil {
		return nil, err
	}
	for _, subAgent := range subAgents {
		subAgentSummaryMask.Apply(subAgent)
	}
	listAgentsResponse := &agentservicepb.ListAgentsResponse{Agents: subAgents}
	return structuredToolResult(toolCall, listAgentsResponse)
}

// =============================== update_labels ==============================

const updateLabelsToolDescription = "Set and remove labels on yourself, e.g. to publish your progress or status to the agents and users watching you. " +
	"`set` maps keys to values (lowercase letters, digits, '_' and '-', at most 63 characters); `remove` lists keys to drop. " +
	"Returns your labels after the change."

// updateLabelsMaxAttempts bounds the retries of a label update racing other updates of the agent.
const updateLabelsMaxAttempts = 5

func updateLabels(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments *agentservicepb.UpdateLabelsToolArguments) (*aipb.ToolResult, error) {
	if len(arguments.GetSet()) == 0 && len(arguments.GetRemove()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "nothing to set or remove").Err()
	}
	for attempt := 1; ; attempt++ {
		// Merged into the current labels, guarded by their etag.
		getAgentRequest := &agentservicepb.GetAgentRequest{Name: agent.GetName()}
		currentAgent, err := s.GetAgent(ctx, getAgentRequest)
		if err != nil {
			return nil, err
		}
		labels := maps.Clone(currentAgent.GetLabels())
		if labels == nil {
			labels = map[string]string{}
		}
		for _, key := range arguments.GetRemove() {
			delete(labels, key)
		}
		maps.Copy(labels, arguments.GetSet())
		updateAgentRequest := &agentservicepb.UpdateAgentRequest{
			Agent:      &agentpb.Agent{Name: agent.GetName(), Labels: labels, Etag: currentAgent.GetEtag()},
			UpdateMask: pbfieldmask.FromPaths("labels").MustValidate(&agentpb.Agent{}).Proto(),
		}
		updatedAgent, err := s.UpdateAgent(ctx, updateAgentRequest)
		if status.HasCode(err, codes.Aborted) && attempt < updateLabelsMaxAttempts {
			continue
		}
		if err != nil {
			return nil, err
		}
		updatedLabels := &agentpb.Agent{Labels: updatedAgent.GetLabels()}
		return structuredToolResult(toolCall, updatedLabels)
	}
}
