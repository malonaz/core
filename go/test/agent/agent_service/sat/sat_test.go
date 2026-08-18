package sat

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	agentservicepb "github.com/malonaz/core/genproto/agent/agent_service/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/uuid"
)

var (
	ctx                = context.Background()
	agentServiceClient agentservicepb.AgentServiceClient
	aiServiceClient    aiservicepb.AiServiceClient
	satEnvironment     *sat.SAT
)

const (
	host = "localhost"

	aiServicePort    = 9290
	agentServicePort = 9291

	postgresHost = "localhost"
	postgresPort = 5433

	// The scriptable mock model registered by the mock provider.
	mockModel = "providers/mock/models/mock-1"
)

// Both services share one physical database in the SAT: ai tables live in
// public, agent tables in the agent schema.
var environmentVariables = map[string]string{
	"POSTGRES_HOST":     postgresHost,
	"POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"POSTGRES_DATABASE": "postgres",
	"POSTGRES_USER":     "postgres",
	"POSTGRES_PASSWORD": "postgres",

	"SAT_POSTGRES_HOST":     postgresHost,
	"SAT_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"SAT_POSTGRES_DATABASE": "sat",
	"SAT_POSTGRES_USER":     "sat",
	"SAT_POSTGRES_PASSWORD": "sat",

	"AI_POSTGRES_HOST":     postgresHost,
	"AI_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"AI_POSTGRES_DATABASE": "sat",
	"AI_POSTGRES_USER":     "sat",
	"AI_POSTGRES_PASSWORD": "sat",

	"AGENT_POSTGRES_HOST":     postgresHost,
	"AGENT_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"AGENT_POSTGRES_DATABASE": "sat",
	"AGENT_POSTGRES_USER":     "sat",
	"AGENT_POSTGRES_PASSWORD": "sat",
}

func newAgentParent() string {
	return "organizations/" + uuid.MustNewV7().String() + "/users/" + uuid.MustNewV7().String()
}

// waitFor polls until fn returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestMain(m *testing.M) {
	cleanup, err := run(context.Background())
	if err != nil {
		panic(err)
	}
	defer cleanup()
	os.Exit(m.Run())
}

func run(ctx context.Context) (func(), error) {
	var cleanupFns []func()
	cleanup := func() {
		for _, fn := range cleanupFns {
			fn()
		}
	}

	config := &sat.Config{
		SUTS: []sat.SUT{
			{
				Name: "ai-service",
				Path: "cmd/ai-service/ai-service",
				Port: aiServicePort,
				Args: []string{
					"--ai-service-external-grpc.host", host,
					"--ai-service-external-grpc.port", strconv.Itoa(aiServicePort),
					"--ai-service-external-grpc.disable-tls",
					"--ai-service.mock-provider",
				},
			},
			{
				Name: "agent-service",
				Path: "cmd/agent-service/agent-service",
				Port: agentServicePort,
				Args: []string{
					"--agent-service-external-grpc.host", host,
					"--agent-service-external-grpc.port", strconv.Itoa(agentServicePort),
					"--agent-service-external-grpc.disable-tls",
					"--ai-service-grpc.host", host,
					"--ai-service-grpc.port", strconv.Itoa(aiServicePort),
					"--ai-service-grpc.disable-tls",
					"--agent-service.poll-interval", "300ms",
					"--agent-service.lease-timeout", "30s",
				},
			},
		},
		PostgresServerConfig: sat.PostgresServerConfig{
			Host:     postgresHost,
			Port:     postgresPort,
			User:     "postgres",
			Password: "postgres",
		},
		Initializer: sat.SUT{
			Name: "database-initializer",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{"--mode", "init", "--dir", "go/ai/migrations", "--target-namespace", "sat"},
		},
		Migrator: sat.SUT{
			Name: "database-migrator",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{"--mode", "migrate", "--dir", "go/ai/migrations", "--target-namespace", "sat"},
		},
		EnvironmentVariables: environmentVariables,
	}
	satEnvironment = sat.New(config)
	if err := satEnvironment.Start(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, satEnvironment.Cleanup)

	// The SAT harness runs a single migrator; run the agent one by hand.
	agentMigrator, err := binary.New("cmd/postgres-migrator/postgres-migrator",
		"--mode", "migrate", "--dir", "go/agent/migrations", "--target-namespace", "sat")
	if err != nil {
		return cleanup, err
	}
	if err := agentMigrator.Run(); err != nil {
		return cleanup, err
	}

	connect := func(port int) (*grpc.Connection, error) {
		connection, err := grpc.NewConnection(&grpc.Opts{Host: host, Port: port, DisableTLS: true}, nil, &prometheus.Opts{})
		if err != nil {
			return nil, err
		}
		if err := connection.Connect(ctx); err != nil {
			return nil, err
		}
		cleanupFns = append(cleanupFns, func() { connection.Close() })
		return connection, nil
	}
	aiConnection, err := connect(aiServicePort)
	if err != nil {
		return cleanup, err
	}
	aiServiceClient = aiservicepb.NewAiServiceClient(aiConnection.Get())
	agentConnection, err := connect(agentServicePort)
	if err != nil {
		return cleanup, err
	}
	agentServiceClient = agentservicepb.NewAgentServiceClient(agentConnection.Get())
	return cleanup, nil
}
