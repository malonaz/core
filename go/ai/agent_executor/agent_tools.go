package agent_executor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/uuid"
)

const (
	createAgentsToolName = "create_agents"
	sendInputToolName    = "send_input"
	listAgentsToolName   = "list_agents"
	updateLabelsToolName = "update_labels"
)

// endTurnHint closes the results of the tools that message agents: replies are
// inputs that wake the caller, never something to wait for.
const endTurnHint = "End your turn now: each reply arrives later as a new message that wakes you. Never poll for it."

// reasoningEffortNames are the efforts a sub-agent may run at, as the model names them.
var reasoningEffortNames = []string{"low", "medium", "high", "xhigh", "max"}

// newAgentTools returns the built-in agent tools, given to every agent: they
// launch, message, list and label agents through the AgentService.
func newAgentTools(agentServiceClient pb.AgentServiceClient) []Tool {
	return []Tool{
		&createAgentsTool{agentServiceClient: agentServiceClient},
		&sendInputTool{agentServiceClient: agentServiceClient},
		&listAgentsTool{agentServiceClient: agentServiceClient},
		&updateLabelsTool{agentServiceClient: agentServiceClient},
	}
}

// createInput queues a text message on an agent, stamped with its sender.
func createInput(ctx context.Context, agentServiceClient pb.AgentServiceClient, sender, recipient, text string) error {
	message := ai.NewUserMessage(ai.NewTextBlock(text))
	agentpb.Annotations.Sender.Set(message, sender)
	createInputRequest := &pb.CreateInputRequest{
		Parent:    recipient,
		Input:     &agentpb.Input{Message: message},
		RequestId: uuid.MustNewV7().String(),
	}
	_, err := agentServiceClient.CreateInput(ctx, createInputRequest)
	return err
}

// structuredToolResult renders a message as a tool call's structured result.
func structuredToolResult(toolCall *aipb.ToolCall, message proto.Message) (*aipb.ToolResult, error) {
	content, err := pbutil.MarshalToStruct(message)
	if err != nil {
		return nil, fmt.Errorf("marshaling result: %w", err)
	}
	return ai.NewStructuredToolResult(toolCall.GetName(), toolCall.GetId(), structpb.NewStructValue(content)), nil
}

// ============================== create_agents ==============================

type createAgentsTool struct {
	agentServiceClient pb.AgentServiceClient
}

type createAgentsArguments struct {
	Agents []*subAgentArguments `json:"agents"`
}

type subAgentArguments struct {
	Title           string   `json:"title"`
	Task            string   `json:"task"`
	Definition      string   `json:"definition"`
	Instructions    string   `json:"instructions"`
	Model           string   `json:"model"`
	ReasoningEffort string   `json:"reasoning_effort"`
	Tools           []string `json:"tools"`
}

func (t *createAgentsTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: createAgentsToolName,
		Description: "Launch one or more sub-agents, each working on its own task concurrently with you. " +
			"A sub-agent runs either a definition you may launch or ad-hoc instructions, and receives its task as its first message. " +
			"Launching never blocks: this returns the sub-agents' names at once. " +
			"After launching, END YOUR TURN: each time a sub-agent ends a turn, its report arrives as a new message that wakes you, naming the sub-agent. " +
			"Never poll for reports with list_agents. Use send_input to follow up with a sub-agent.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"agents": {
					Type:        "array",
					Description: "The sub-agents to launch.",
					MinItems:    1,
					Items: &jsonpb.Schema{
						Type: "object",
						Properties: map[string]*jsonpb.Schema{
							"title": {
								Type:        "string",
								Description: "A short human-readable title for the sub-agent.",
							},
							"task": {
								Type:        "string",
								Description: "The sub-agent's task. It shares none of your context: include everything it needs, and what to report back.",
							},
							"definition": {
								Type:        "string",
								Description: "The definition to run, one of those you may launch. Format: organizations/{organization}/definitions/{definition}. Omit for an ad-hoc sub-agent.",
							},
							"instructions": {
								Type:        "string",
								Description: "An ad-hoc sub-agent's system prompt. Required without a definition.",
							},
							"model": {
								Type:        "string",
								Description: "An ad-hoc sub-agent's model. Format: providers/{provider}/models/{model}. Defaults to yours.",
							},
							"reasoning_effort": {
								Type:        "string",
								Description: "An ad-hoc sub-agent's reasoning effort. Defaults to yours.",
								Enum:        reasoningEffortNames,
							},
							"tools": {
								Type:        "array",
								Description: "An ad-hoc sub-agent's tools, a subset of yours. Defaults to all of yours. Agent tools are always given.",
								Items:       &jsonpb.Schema{Type: "string"},
							},
						},
						Required: []string{"title", "task"},
					},
				},
			},
			Required: []string{"agents"},
		},
	}
}

func (t *createAgentsTool) Execute(ctx context.Context, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &createAgentsArguments{}
	if err := DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if len(arguments.Agents) == 0 {
		return nil, errors.New("no agents to launch")
	}
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, fmt.Errorf("parsing agent name: %w", err)
	}
	// Validate every launch first, so an invalid one launches none.
	subAgents := make([]*agentpb.Agent, 0, len(arguments.Agents))
	for i, subAgentArguments := range arguments.Agents {
		subAgent, err := newSubAgent(agent, subAgentArguments)
		if err != nil {
			return nil, fmt.Errorf("agent %d: %w", i+1, err)
		}
		subAgents = append(subAgents, subAgent)
	}

	lines := make([]string, len(subAgents))
	var eg errgroup.Group
	for i, subAgent := range subAgents {
		eg.Go(func() error {
			launchedAgent, err := t.launch(ctx, agentRn, subAgent, arguments.Agents[i].Task)
			switch {
			case launchedAgent == nil:
				lines[i] = fmt.Sprintf("- %q: launch failed: %v", subAgent.GetTitle(), err)
			case err != nil:
				lines[i] = fmt.Sprintf("- %s (%q): created, but sending its task failed, so it is idle; retry with send_input: %v", launchedAgent.GetName(), subAgent.GetTitle(), err)
			default:
				lines[i] = fmt.Sprintf("- %s (%q): launched", launchedAgent.GetName(), subAgent.GetTitle())
			}
			// Failures are reported per agent, never aborting the others.
			return nil
		})
	}
	_ = eg.Wait()
	content := strings.Join(lines, "\n") + "\n" + endTurnHint
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// launch creates a sub-agent under the caller's user and sends it its task.
// It returns the created agent even when sending the task failed.
func (t *createAgentsTool) launch(ctx context.Context, agentRn *agentpb.AgentRn, subAgent *agentpb.Agent, task string) (*agentpb.Agent, error) {
	createAgentRequest := &pb.CreateAgentRequest{
		Parent:    agentRn.UserRn().String(),
		Agent:     subAgent,
		RequestId: uuid.MustNewV7().String(),
	}
	createdAgent, err := t.agentServiceClient.CreateAgent(ctx, createAgentRequest)
	if err != nil {
		return nil, err
	}
	return createdAgent, createInput(ctx, t.agentServiceClient, agentRn.String(), createdAgent.GetName(), task)
}

// newSubAgent validates a launch against what the caller may delegate: only its
// own definitions and tools. Ad-hoc settings default to the caller's.
func newSubAgent(agent *agentpb.Agent, arguments *subAgentArguments) (*agentpb.Agent, error) {
	if strings.TrimSpace(arguments.Title) == "" {
		return nil, errors.New("title is required")
	}
	if strings.TrimSpace(arguments.Task) == "" {
		return nil, errors.New("task is required")
	}
	spec := agent.GetSpec()
	subAgent := &agentpb.Agent{
		Title:       arguments.Title,
		ParentAgent: agent.GetName(),
	}

	if arguments.Definition != "" {
		if arguments.Instructions != "" || arguments.Model != "" || arguments.ReasoningEffort != "" || arguments.Tools != nil {
			return nil, errors.New("a definition sets the instructions, model, reasoning_effort and tools: omit them")
		}
		if !slices.Contains(spec.GetSubAgentDefinitions(), arguments.Definition) {
			if len(spec.GetSubAgentDefinitions()) == 0 {
				return nil, fmt.Errorf("you may not launch definition %q: launch an ad-hoc sub-agent instead", arguments.Definition)
			}
			return nil, fmt.Errorf("you may not launch definition %q: launch one of %s, or an ad-hoc sub-agent",
				arguments.Definition, strings.Join(spec.GetSubAgentDefinitions(), ", "))
		}
		subAgent.Definition = arguments.Definition
		return subAgent, nil
	}

	if strings.TrimSpace(arguments.Instructions) == "" {
		return nil, errors.New("instructions are required without a definition")
	}
	reasoningEffort := spec.GetReasoningEffort()
	if arguments.ReasoningEffort != "" {
		if !slices.Contains(reasoningEffortNames, arguments.ReasoningEffort) {
			return nil, fmt.Errorf("unknown reasoning_effort %q: want one of %s", arguments.ReasoningEffort, strings.Join(reasoningEffortNames, ", "))
		}
		reasoningEffort = aipb.ReasoningEffort(aipb.ReasoningEffort_value["REASONING_EFFORT_"+strings.ToUpper(arguments.ReasoningEffort)])
	}
	tools := spec.GetTools()
	if arguments.Tools != nil {
		var unavailableTools []string
		for _, tool := range arguments.Tools {
			if !slices.Contains(spec.GetTools(), tool) {
				unavailableTools = append(unavailableTools, tool)
			}
		}
		if len(unavailableTools) > 0 {
			return nil, fmt.Errorf("tools you do not hold: %s (yours: %s)", strings.Join(unavailableTools, ", "), strings.Join(spec.GetTools(), ", "))
		}
		tools = arguments.Tools
	}
	subAgent.Spec = &agentpb.AgentSpec{
		Instructions:     arguments.Instructions,
		Model:            cmp.Or(arguments.Model, spec.GetModel()),
		ReasoningEffort:  reasoningEffort,
		Tools:            slices.Clone(tools),
		ExecutorSelector: maps.Clone(spec.GetExecutorSelector()),
	}
	return subAgent, nil
}

// ================================ send_input ================================

type sendInputTool struct {
	agentServiceClient pb.AgentServiceClient
}

type sendInputArguments struct {
	Agent   string `json:"agent"`
	Message string `json:"message"`
}

func (t *sendInputTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: sendInputToolName,
		Description: "Send a message to another agent in your tree: typically one of your sub-agents, to follow up or redirect it, or your parent agent. " +
			"Sending never blocks and returns no reply: the recipient reads the message on its next turn. " +
			"After sending, END YOUR TURN: a sub-agent's next turn report arrives later as a new message that wakes you. Never poll for it.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"agent": {
					Type:        "string",
					Description: "The recipient. Format: organizations/{organization}/users/{user}/agents/{agent}",
				},
				"message": {
					Type:        "string",
					Description: "The message to send.",
				},
			},
			Required: []string{"agent", "message"},
		},
	}
}

func (t *sendInputTool) Execute(ctx context.Context, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &sendInputArguments{}
	if err := DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if strings.TrimSpace(arguments.Message) == "" {
		return nil, errors.New("message is required")
	}
	if _, err := agentpb.ParseAgentRn(arguments.Agent); err != nil {
		return nil, fmt.Errorf("invalid agent %q: %w", arguments.Agent, err)
	}
	if arguments.Agent == agent.GetName() {
		return nil, errors.New("you cannot message yourself")
	}
	getAgentRequest := &pb.GetAgentRequest{Name: arguments.Agent}
	recipient, err := t.agentServiceClient.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if recipient.GetRootAgent() != agent.GetRootAgent() {
		return nil, fmt.Errorf("agent %q is not in your agent tree", arguments.Agent)
	}
	if err := createInput(ctx, t.agentServiceClient, agent.GetName(), recipient.GetName(), arguments.Message); err != nil {
		return nil, err
	}
	content := fmt.Sprintf("Sent to %s. %s", recipient.GetName(), endTurnHint)
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// ================================ list_agents ===============================

type listAgentsTool struct {
	agentServiceClient pb.AgentServiceClient
}

func (t *listAgentsTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: listAgentsToolName,
		Description: "List your sub-agents: their name, title, state, how their latest turn ended and labels. " +
			"For an overview only: sub-agent reports arrive by themselves as new messages, so never call this to wait for them.",
		JsonSchema: &jsonpb.Schema{Type: "object"},
	}
}

func (t *listAgentsTool) Execute(ctx context.Context, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, fmt.Errorf("parsing agent name: %w", err)
	}
	listAgentsRequest := &pb.ListAgentsRequest{
		Parent: agentRn.UserRn().String(),
		Filter: fmt.Sprintf("parent_agent = %q", agent.GetName()),
		// Launch order, stable while paginating.
		OrderBy: "create_time asc",
	}
	subAgents, err := aip.Paginate[*agentpb.Agent](ctx, listAgentsRequest, t.agentServiceClient.ListAgents)
	if err != nil {
		return nil, err
	}
	summaryMask := pbfieldmask.FromPaths("name", "title", "state", "last_turn_outcome", "labels").MustValidate(&agentpb.Agent{})
	for _, subAgent := range subAgents {
		summaryMask.Apply(subAgent)
	}
	listAgentsResponse := &pb.ListAgentsResponse{Agents: subAgents}
	return structuredToolResult(toolCall, listAgentsResponse)
}

// =============================== update_labels ==============================

type updateLabelsTool struct {
	agentServiceClient pb.AgentServiceClient
}

type updateLabelsArguments struct {
	Set    map[string]string `json:"set"`
	Remove []string          `json:"remove"`
}

func (t *updateLabelsTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name:        updateLabelsToolName,
		Description: "Set and remove labels on yourself, e.g. to publish your progress or status to the agents and users watching you. Returns your labels after the change.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"set": {
					Type:                 "object",
					Description:          "Labels to set, by key. Values are lowercase letters, digits, '_' and '-', at most 63 characters.",
					AdditionalProperties: &jsonpb.Schema{Type: "string"},
				},
				"remove": {
					Type:        "array",
					Description: "Keys of labels to remove.",
					Items:       &jsonpb.Schema{Type: "string"},
				},
			},
		},
	}
}

func (t *updateLabelsTool) Execute(ctx context.Context, agent *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &updateLabelsArguments{}
	if err := DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if len(arguments.Set) == 0 && len(arguments.Remove) == 0 {
		return nil, errors.New("nothing to set or remove")
	}
	// The labels may have changed since the lease: merge into the current ones.
	getAgentRequest := &pb.GetAgentRequest{Name: agent.GetName()}
	currentAgent, err := t.agentServiceClient.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{}
	maps.Copy(labels, currentAgent.GetLabels())
	for _, key := range arguments.Remove {
		delete(labels, key)
	}
	maps.Copy(labels, arguments.Set)
	updateAgentRequest := &pb.UpdateAgentRequest{
		Agent: &agentpb.Agent{
			Name:   agent.GetName(),
			Labels: labels,
		},
		UpdateMask: pbfieldmask.FromPaths("labels").MustValidate(&agentpb.Agent{}).Proto(),
	}
	updatedAgent, err := t.agentServiceClient.UpdateAgent(ctx, updateAgentRequest)
	if err != nil {
		return nil, err
	}
	pbfieldmask.FromPaths("labels").Apply(updatedAgent)
	return structuredToolResult(toolCall, updatedAgent)
}
