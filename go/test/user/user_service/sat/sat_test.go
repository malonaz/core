package sat

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/uuid"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
)

var (
	ctx               = context.Background()
	userServiceClient userservicepb.UserServiceClient
	satEnvironment    *sat.SAT
)

func getOrganizationParent() string {
	return "organizations/" + uuid.MustNewV7().String()
}

const (
	userServiceName = "user-service"
	userServicePath = "cmd/user-service/user-service"
	userServiceHost = "localhost"
	userServicePort = 9090

	postgresHost = "localhost"
	postgresPort = 5432
)

var environmentVariables = map[string]string{
	"POSTGRES_HOST":     postgresHost,
	"POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"POSTGRES_DATABASE": "postgres",
	"POSTGRES_USER":     "postgres",
	"POSTGRES_PASSWORD": "postgres",

	"USER_POSTGRES_HOST":     postgresHost,
	"USER_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"USER_POSTGRES_DATABASE": "user",
	"USER_POSTGRES_USER":     "user",
	"USER_POSTGRES_PASSWORD": "user",
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
				Name: userServiceName,
				Path: userServicePath,
				Port: userServicePort,
				Args: []string{
					"--user-service-external-grpc.host", userServiceHost,
					"--user-service-external-grpc.port", strconv.Itoa(userServicePort),
					"--user-service-external-grpc.disable-tls",
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
				"--dir", "go/test/user/migrations",
				"--target-namespace", "user",
			},
		},
		Migrator: sat.SUT{
			Name: "database-migrator",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{
				"--mode", "migrate",
				"--dir", "go/test/user/migrations",
				"--target-namespace", "user",
			},
		},
		EnvironmentVariables: environmentVariables,
		Nats:                 true,
	}
	satEnvironment = sat.New(config)
	if err := satEnvironment.Start(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, satEnvironment.Cleanup)

	grpcOpts := &grpc.Opts{
		Host:       userServiceHost,
		Port:       userServicePort,
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
	userServiceClient = userservicepb.NewUserServiceClient(connection.Get())
	return cleanup, nil
}
