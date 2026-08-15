package sat

import (
	"context"
	"os"
	"strconv"
	"testing"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/uuid"
)

var (
	ctx             = context.Background()
	aiServiceClient aiservicepb.AiServiceClient
	satEnvironment  *sat.SAT
)

const (
	aiServiceName = "ai-service"
	aiServicePath = "cmd/ai-service/ai-service"
	aiServiceHost = "localhost"
	aiServicePort = 9090

	postgresHost = "localhost"
	postgresPort = 5432

	// The scriptable mock model registered by the mock provider.
	mockModel = "providers/mock/models/mock-1"
)

var environmentVariables = map[string]string{
	"POSTGRES_HOST":     postgresHost,
	"POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"POSTGRES_DATABASE": "postgres",
	"POSTGRES_USER":     "postgres",
	"POSTGRES_PASSWORD": "postgres",

	"AI_POSTGRES_HOST":     postgresHost,
	"AI_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"AI_POSTGRES_DATABASE": "ai",
	"AI_POSTGRES_USER":     "ai",
	"AI_POSTGRES_PASSWORD": "ai",
}

func newChatParent() string {
	return "organizations/" + uuid.MustNewV7().String() +
		"/users/" + uuid.MustNewV7().String() +
		"/chats/" + uuid.MustNewV7().String()
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

	// SAT Config.
	config := &sat.Config{
		SUTS: []sat.SUT{
			{
				Name: aiServiceName,
				Path: aiServicePath,
				Port: aiServicePort,
				Args: []string{
					"--ai-service-external-grpc.host", aiServiceHost,
					"--ai-service-external-grpc.port", strconv.Itoa(aiServicePort),
					"--ai-service-external-grpc.disable-tls",
					"--ai-service.mock-provider",
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
			Args: []string{
				"--mode", "init",
				"--dir", "go/ai/migrations",
				"--target-namespace", "ai",
			},
		},
		Migrator: sat.SUT{
			Name: "database-migrator",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{
				"--mode", "migrate",
				"--dir", "go/ai/migrations",
				"--target-namespace", "ai",
			},
		},
		EnvironmentVariables: environmentVariables,
	}
	satEnvironment = sat.New(config)
	if err := satEnvironment.Start(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, satEnvironment.Cleanup)

	grpcOpts := &grpc.Opts{
		Host:       aiServiceHost,
		Port:       aiServicePort,
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
	aiServiceClient = aiservicepb.NewAiServiceClient(connection.Get())
	return cleanup, nil
}
