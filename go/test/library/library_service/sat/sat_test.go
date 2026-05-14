package sat

import (
	"context"
	"os"
	"strconv"
	"testing"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/uuid"
)

var (
	ctx                  = context.Background()
	libraryServiceClient libraryservicepb.LibraryServiceClient
	satEnvironment       *sat.SAT
)

func getOrganizationParent() string {
	return "organizations/" + uuid.MustNewV7().String()
}

const (
	libraryServiceName = "library-service"
	libraryServicePath = "cmd/library-service/library-service"
	libraryServiceHost = "localhost"
	libraryServicePort = 9090

	postgresHost = "localhost"
	postgresPort = 5432
)

var environmentVariables = map[string]string{
	"POSTGRES_HOST":     postgresHost,
	"POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"POSTGRES_DATABASE": "postgres",
	"POSTGRES_USER":     "postgres",
	"POSTGRES_PASSWORD": "postgres",

	"LIBRARY_POSTGRES_HOST":     postgresHost,
	"LIBRARY_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"LIBRARY_POSTGRES_DATABASE": "library",
	"LIBRARY_POSTGRES_USER":     "library",
	"LIBRARY_POSTGRES_PASSWORD": "library",
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
				Name: libraryServiceName,
				Path: libraryServicePath,
				Port: libraryServicePort,
				Args: []string{
					"--library-service-external-grpc.host", libraryServiceHost,
					"--library-service-external-grpc.port", strconv.Itoa(libraryServicePort),
					"--library-service-external-grpc.disable-tls",
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
				"--dir", "go/test/library/migrations",
				"--target-namespace", "library",
			},
		},
		Migrator: sat.SUT{
			Name: "database-migrator",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{
				"--mode", "migrate",
				"--dir", "go/test/library/migrations",
				"--target-namespace", "library",
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
		Host:       libraryServiceHost,
		Port:       libraryServicePort,
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
	libraryServiceClient = libraryservicepb.NewLibraryServiceClient(connection.Get())
	return cleanup, nil
}
