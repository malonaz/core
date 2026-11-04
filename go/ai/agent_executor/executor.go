package agent_executor

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

// Opts configures an executor.
type Opts struct {
	User              string            `long:"user" env:"USER" description:"The user the executor runs agents for. Format: organizations/{organization}/users/{user}" required:"true"`
	ExecutorID        string            `long:"executor-id" env:"ID" description:"The executor's ID, stable across restarts." required:"true"`
	DisplayName       string            `long:"display-name" env:"DISPLAY_NAME" description:"A human-readable name for the executor."`
	Labels            map[string]string `long:"label" env:"LABELS" env-delim:"," description:"A key:value label matched against agents' executor selectors. Repeatable."`
	MaxAgents         int32             `long:"max-agents" env:"MAX_AGENTS" description:"The most agents the executor runs at once." default:"8"`
	HeartbeatInterval time.Duration     `long:"heartbeat-interval" env:"HEARTBEAT_INTERVAL" description:"How often the executor heartbeats." default:"10s"`
	// Reconnects back off exponentially, with jitter, between these bounds.
	ReconnectInitialInterval time.Duration `long:"reconnect-initial-interval" env:"RECONNECT_INITIAL_INTERVAL" description:"The first delay before reconnecting." default:"1s"`
	ReconnectMaxInterval     time.Duration `long:"reconnect-max-interval" env:"RECONNECT_MAX_INTERVAL" description:"The longest delay before reconnecting." default:"1m"`
	ShutdownTimeout          time.Duration `long:"shutdown-timeout" env:"SHUTDOWN_TIMEOUT" description:"How long a shutdown waits for cancelled turns to report their end." default:"10s"`
}

// Executor runs agents' turns for an AgentService: it dials in over
// ConnectExecutor, holds the agents it is leased and runs each on its own actor.
type Executor struct {
	opts               *Opts
	userRn             *agentpb.UserRn
	log                *slog.Logger
	agentServiceClient pb.AgentServiceClient
	aiServiceClient    aiservicepb.AiServiceClient
	// toolNameToTool holds the built-in agent tools and the executor's own.
	toolNameToTool map[string]Tool
	// agentToolNameSet names the built-in agent tools, given to every agent.
	agentToolNameSet map[string]struct{}

	mutex sync.Mutex
	// agentNameToAgent holds every actor not yet done, revoked or not, so a
	// re-leased agent's new actor waits out its predecessor.
	agentNameToAgent map[string]*agent
}

// New returns an executor providing the given tools, plus the built-in agent tools.
func New(opts *Opts, agentServiceClient pb.AgentServiceClient, aiServiceClient aiservicepb.AiServiceClient, tools ...Tool) (*Executor, error) {
	userRn, err := agentpb.ParseUserRn(opts.User)
	if err != nil {
		return nil, fmt.Errorf("invalid user %q: %w", opts.User, err)
	}
	e := &Executor{
		opts:               opts,
		userRn:             userRn,
		log:                slog.Default(),
		agentServiceClient: agentServiceClient,
		aiServiceClient:    aiServiceClient,
		toolNameToTool:     map[string]Tool{},
		agentToolNameSet:   map[string]struct{}{},
		agentNameToAgent:   map[string]*agent{},
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

// missingToolNames returns the tools an agent needs that the executor lacks.
func (e *Executor) missingToolNames(agent *agentpb.Agent) []string {
	var missingToolNames []string
	for _, toolName := range agent.GetSpec().GetTools() {
		if _, ok := e.toolNameToTool[toolName]; !ok {
			missingToolNames = append(missingToolNames, toolName)
		}
	}
	return missingToolNames
}

// adopt records a new actor, returning a channel closed once the previous actor
// of its agent, if any, is done.
func (e *Executor) adopt(actor *agent) <-chan struct{} {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	predecessor, ok := e.agentNameToAgent[actor.agent.GetName()]
	e.agentNameToAgent[actor.agent.GetName()] = actor
	if !ok {
		return nil
	}
	return predecessor.done
}

// forget drops a done actor, unless a successor already replaced it.
func (e *Executor) forget(actor *agent) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.agentNameToAgent[actor.agent.GetName()] == actor {
		delete(e.agentNameToAgent, actor.agent.GetName())
	}
}
