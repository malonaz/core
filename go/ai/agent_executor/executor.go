package agent_executor

import (
	"fmt"
	"log/slog"
	"time"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

// Opts configures an executor.
type Opts struct {
	Organization      string            `long:"organization" env:"ORGANIZATION" description:"The organization the executor serves." required:"true"`
	ExecutorID        string            `long:"executor-id" env:"EXECUTOR_ID" description:"The executor's ID, stable across restarts." required:"true"`
	DisplayName       string            `long:"display-name" env:"DISPLAY_NAME" description:"A human-readable name for the executor."`
	Labels            map[string]string `long:"label" env:"LABELS" env-delim:"," description:"A key:value label matched against agents' executor selectors. Repeatable."`
	MaxAgents         int32             `long:"max-agents" env:"MAX_AGENTS" description:"The most agents the executor runs at once." default:"8"`
	HeartbeatInterval time.Duration     `long:"heartbeat-interval" env:"HEARTBEAT_INTERVAL" description:"How often the executor heartbeats." default:"10s"`
}

// Executor runs agents' turns for an AgentService: it dials in over
// ConnectExecutor, holds the agents it is leased and runs each on its own actor.
type Executor struct {
	opts               *Opts
	log                *slog.Logger
	agentServiceClient pb.AgentServiceClient
	aiServiceClient    aiservicepb.AiServiceClient
	// toolNameToTool holds the built-in agent tools and the executor's own.
	toolNameToTool map[string]Tool
	// agentToolNameSet names the built-in agent tools, given to every agent.
	agentToolNameSet map[string]struct{}
}

// New returns an executor providing the given tools, plus the built-in agent tools.
func New(opts *Opts, agentServiceClient pb.AgentServiceClient, aiServiceClient aiservicepb.AiServiceClient, tools ...Tool) (*Executor, error) {
	e := &Executor{
		opts:               opts,
		log:                slog.Default(),
		agentServiceClient: agentServiceClient,
		aiServiceClient:    aiServiceClient,
		toolNameToTool:     map[string]Tool{},
		agentToolNameSet:   map[string]struct{}{},
	}
	agentTools := newAgentTools(agentServiceClient)
	for _, tool := range agentTools {
		e.agentToolNameSet[tool.Definition().GetName()] = struct{}{}
	}
	for _, tool := range append(agentTools, tools...) {
		name := tool.Definition().GetName()
		if _, ok := e.toolNameToTool[name]; ok {
			return nil, fmt.Errorf("duplicate tool %q", name)
		}
		e.toolNameToTool[name] = tool
	}
	return e, nil
}

// WithLogger sets the executor's logger.
func (e *Executor) WithLogger(logger *slog.Logger) *Executor {
	e.log = logger
	return e
}
