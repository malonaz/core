package agent_service

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/malonaz/core/gengo/ai/agent/store"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
)

// agentMigrationsDirectory holds the agent database's migrations, provided to the test as data.
const agentMigrationsDirectory = "go/ai/agent/migrations"

const testModel = "providers/p/models/m"

// testPostgresClient is connected to the test Postgres, migrated, shared by every test: isolate tests by
// organization. Claim loops span every organization: tests starting replicas never run in parallel.
var testPostgresClient *postgres.Client

func TestMain(m *testing.M) {
	testserver.RunWithPostgres(context.Background(), m, slog.Default(), &testPostgresClient, nil, "", os.ReadFile, agentMigrationsDirectory)
}

// newTestOpts returns opts with timings short enough for tests to watch leases expire.
func newTestOpts(address string) *Opts {
	return &Opts{
		AdvertisedAddress:  address,
		LeaseDuration:      time.Second,
		HeartbeatInterval:  200 * time.Millisecond,
		ClaimInterval:      100 * time.Millisecond,
		MaxConcurrentTurns: 100,
		ShutdownGrace:      time.Second,
		StreamPollInterval: 100 * time.Millisecond,
	}
}

func newTestStore() *store.Store {
	return store.New(testPostgresClient)
}

// testReplica is one replica of the service, served on its own unix socket, its advertised address.
type testReplica struct {
	*Service
	aiService *fakeAiService
	// stop shuts the replica down, as on SIGTERM, then stops its server. Idempotent.
	stop func()
}

// newTestReplica starts a replica over the test Postgres and the given AiService, stopped when the test ends.
// configure, if set, adjusts its opts.
func newTestReplica(t *testing.T, aiService *fakeAiService, configure func(*Opts)) *testReplica {
	t.Helper()
	// Unix socket paths are capped at 108 bytes: test temp dirs may be too deep.
	socketDirectory, err := os.MkdirTemp("", "agent")
	require.NoError(t, err)
	socketPath := filepath.Join(socketDirectory, "grpc.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	opts := newTestOpts("unix:" + socketPath)
	if configure != nil {
		configure(opts)
	}
	service, err := New(opts, newTestStore(), aiService)
	require.NoError(t, err)
	server := grpc.NewServer()
	agentservicepb.RegisterAgentServiceServer(server, service)
	reflection.Register(server)
	go server.Serve(listener)

	stopService, err := service.Start(context.Background(), nil)
	require.NoError(t, err)
	stopped := false
	replica := &testReplica{Service: service, aiService: aiService}
	replica.stop = func() {
		if stopped {
			return
		}
		stopped = true
		stopService()
		server.Stop()
		os.RemoveAll(socketDirectory)
	}
	t.Cleanup(replica.stop)
	return replica
}

// newTestService starts one replica over a fresh fake AiService.
func newTestService(t *testing.T) *testReplica {
	t.Helper()
	return newTestReplica(t, newFakeAiService(), nil)
}

// newTestUser returns a fresh user name, isolating a test's rows in the shared database.
func newTestUser(t *testing.T) string {
	t.Helper()
	userRn := &agentpb.UserRn{Organization: aip.NewSystemGeneratedBase32ResourceID(), User: "user"}
	return userRn.String()
}

// createTestAgent creates an ad-hoc root agent with the given tools under a fresh user.
func createTestAgent(t *testing.T, service *testReplica, tools ...string) *agentpb.Agent {
	t.Helper()
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: newTestUser(t),
		Agent: &agentpb.Agent{
			Title: "test agent",
			Spec:  &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel, Tools: tools},
		},
	}
	agent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	return agent
}

// sendText queues a text input on an agent.
func sendText(t *testing.T, service *testReplica, agent, text string) *agentpb.Input {
	t.Helper()
	createInputRequest := &agentservicepb.CreateInputRequest{
		Parent: agent,
		Input:  &agentpb.Input{Message: ai.NewUserMessage(ai.NewTextBlock(text))},
	}
	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)
	return input
}

// getAgent reads an agent.
func getAgent(t *testing.T, service *testReplica, name string) *agentpb.Agent {
	t.Helper()
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: name}
	agent, err := service.GetAgent(context.Background(), getAgentRequest)
	require.NoError(t, err)
	return agent
}

// waitForIdle waits until the agent's latest turn ended with the given outcome and the agent is idle.
func waitForIdle(t *testing.T, service *testReplica, name string, outcome agentpb.Agent_TurnOutcome) *agentpb.Agent {
	t.Helper()
	var agent *agentpb.Agent
	require.Eventually(t, func() bool {
		agent = getAgent(t, service, name)
		return agent.State == agentpb.Agent_STATE_IDLE && agent.LastTurnOutcome == outcome
	}, 10*time.Second, 10*time.Millisecond, "agent %s never went idle with outcome %s", name, outcome)
	return agent
}

// roles returns the roles of messages, in order.
func roles(messages []*aipb.Message) []aipb.Role {
	roles := make([]aipb.Role, 0, len(messages))
	for _, message := range messages {
		roles = append(roles, message.GetRole())
	}
	return roles
}
