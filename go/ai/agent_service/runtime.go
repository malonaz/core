package agent_service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/uuid"
)

type Opts struct {
	AdvertisedAddress  string            `long:"advertised-address" env:"ADVERTISED_ADDRESS" required:"true" description:"Where the other replicas reach this replica's gRPC server, to cancel and stream the turns it runs: http://host:port (plaintext), https://host:port or host:port (TLS), or unix:path"`
	LeaseDuration      time.Duration     `long:"lease-duration" env:"LEASE_DURATION" default:"15s" description:"How long a turn's lease outlives its replica's last heartbeat before another replica takes the turn over"`
	HeartbeatInterval  time.Duration     `long:"heartbeat-interval" env:"HEARTBEAT_INTERVAL" default:"5s" description:"How often a replica renews the leases of the turns it runs"`
	ClaimInterval      time.Duration     `long:"claim-interval" env:"CLAIM_INTERVAL" default:"2s" description:"How often a replica claims the turns needing one: running agents without a live lease"`
	MaxConcurrentTurns int               `long:"max-concurrent-turns" env:"MAX_CONCURRENT_TURNS" default:"100" description:"How many turns a replica runs at once"`
	ShutdownGrace      time.Duration     `long:"shutdown-grace" env:"SHUTDOWN_GRACE" default:"10s" description:"How long a stopping replica lets in-flight tool calls finish before cancelling its turns and releasing their leases"`
	StreamPollInterval time.Duration     `long:"stream-poll-interval" env:"STREAM_POLL_INTERVAL" default:"1s" description:"How often a StreamAgent call re-reads its agents for changes made elsewhere: snapshots, turns moving between replicas, new descendants"`
	ToolTargets        map[string]string `long:"tool-target" env:"TOOL_TARGETS" env-delim:"," key-value-delimiter:"=" description:"Where a tool service is served, as pkg.Service=endpoint (endpoint as for advertised-address). Repeatable. Every service an agent's tools name needs one: specs naming another are refused"`
}

type runtime struct {
	// replica identifies this process in the leases it holds.
	replica string
	// address is this replica's advertised address.
	address     string
	connections *connections
	toolRouter  *toolRouter
	// builtinTools are the tools every agent holds.
	builtinTools *builtinTools
	hub          *hub
	turns        *turns
	// runCtx spans the replica's life, with the service's identity: turns run on it.
	runCtx context.Context
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.AdvertisedAddress == "" {
		return nil, fmt.Errorf("advertised address is required")
	}
	if _, err := grpc.ParseClientOpts(opts.AdvertisedAddress); err != nil {
		return nil, fmt.Errorf("parsing advertised address: %w", err)
	}
	connections := newConnections()
	toolRouter, err := newToolRouter(opts.ToolTargets, connections)
	if err != nil {
		return nil, fmt.Errorf("instantiating tool router: %w", err)
	}
	builtinTools, err := newBuiltinTools(context.Background())
	if err != nil {
		return nil, fmt.Errorf("instantiating built-in tools: %w", err)
	}
	return &runtime{
		replica:      uuid.MustNewV7().String(),
		builtinTools: builtinTools,
		address:      opts.AdvertisedAddress,
		connections:  connections,
		toolRouter:   toolRouter,
		hub:          newHub(),
		turns:        newTurns(),
	}, nil
}

// asService runs a call with the service's own identity.
func (s *Service) asService(ctx context.Context) context.Context {
	if s.withServiceAccount == nil {
		return ctx
	}
	return s.withServiceAccount(ctx)
}

// now returns the current time at Postgres precision.
func (s *Service) now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// connections holds one gRPC connection per endpoint: tool targets and peer replicas.
type connections struct {
	mutex                sync.Mutex
	endpointToConnection map[string]*grpc.Connection
}

func newConnections() *connections {
	return &connections{endpointToConnection: map[string]*grpc.Connection{}}
}

// get returns the connection to an endpoint, dialing it lazily on first use.
func (c *connections) get(ctx context.Context, endpoint string) (*grpc.Connection, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if connection, ok := c.endpointToConnection[endpoint]; ok {
		return connection, nil
	}
	clientOpts, err := grpc.ParseClientOpts(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint %q: %w", endpoint, err)
	}
	connection, err := grpc.NewConnection(clientOpts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("instantiating connection to %q: %w", endpoint, err)
	}
	if err := connection.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connecting to %q: %w", endpoint, err)
	}
	c.endpointToConnection[endpoint] = connection
	return connection, nil
}

func (c *connections) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for endpoint, connection := range c.endpointToConnection {
		connection.Close()
		delete(c.endpointToConnection, endpoint)
	}
}
