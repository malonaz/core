package sat

import (
	"context"
	"os"
	"strconv"
	"testing"

	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
)

var (
	ctx            = context.Background()
	aiEngineClient aienginepb.AiEngineClient
	satEnvironment *sat.SAT
)

const (
	aiEngineName = "ai-engine"
	aiEnginePath = "cmd/ai-engine/ai-engine"
	aiEngineHost = "localhost"
	aiEnginePort = 9090
)

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

	// SAT Config. The engine's reflection client points at the engine's own
	// gRPC server: the engine introspects (and builds tools for) itself.
	config := &sat.Config{
		SUTS: []sat.SUT{
			{
				Name: aiEngineName,
				Path: aiEnginePath,
				Port: aiEnginePort,
				Args: []string{
					"--ai-engine-external-grpc.host", aiEngineHost,
					"--ai-engine-external-grpc.port", strconv.Itoa(aiEnginePort),
					"--ai-engine-external-grpc.disable-tls",
					"--ai-engine-external-grpc.enable-reflection",
					"--server-reflection-grpc.host", aiEngineHost,
					"--server-reflection-grpc.port", strconv.Itoa(aiEnginePort),
					"--server-reflection-grpc.disable-tls",
					"--ai-engine.default-model", "providers/mock/models/mock-1",
				},
			},
		},
	}
	satEnvironment = sat.New(config)
	if err := satEnvironment.Start(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, satEnvironment.Cleanup)

	grpcOpts := &grpc.Opts{
		Host:       aiEngineHost,
		Port:       aiEnginePort,
		DisableTLS: true,
	}
	connection, err := grpc.NewConnection(grpcOpts, nil, &prometheus.Opts{})
	if err != nil {
		return cleanup, err
	}
	if err := connection.Connect(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, func() { connection.Close() })
	aiEngineClient = aienginepb.NewAiEngineClient(connection.Get())
	return cleanup, nil
}
