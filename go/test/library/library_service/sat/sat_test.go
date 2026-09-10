package sat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/uuid"
)

var (
	ctx                    = context.Background()
	libraryServiceClient   libraryservicepb.LibraryServiceClient
	bookmarkServiceClient  libraryservicepb.BookmarkServiceClient
	operationsClient       longrunningpb.OperationsClient
	schedulerServiceClient schedulerservicepb.SchedulerServiceClient
	satEnvironment         *sat.SAT
)

func getOrganizationParent() string {
	return "organizations/" + uuid.MustNewV7().String()
}

const (
	libraryServiceName = "library-service"
	libraryServicePath = "cmd/library-service/library-service"
	libraryServiceHost = "localhost"
	libraryServicePort = 9090

	// The scheduler runs the library's long-running operations; its health and
	// metrics ports must not collide with the library's defaults.
	schedulerServiceName           = "scheduler-service"
	schedulerServicePath           = "cmd/scheduler-service/scheduler-service"
	schedulerServicePort           = 9091
	schedulerServiceHealthPort     = 4041
	schedulerServicePrometheusPort = 13435
	schedulerPollInterval          = 100 * time.Millisecond
	// Short enough for a sat to observe the cap on an absurd WaitOperation timeout.
	schedulerWaitJobMaxTimeout = 2 * time.Second

	// The methods the dispatcher discovers on the library service: ImportBooks,
	// and GetShelf standing in for another service's jobs.
	libraryServiceFullName = "malonaz.test.library.library_service.v1.LibraryService"

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

	"SCHEDULER_POSTGRES_HOST":     postgresHost,
	"SCHEDULER_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"SCHEDULER_POSTGRES_DATABASE": "scheduler",
	"SCHEDULER_POSTGRES_USER":     "scheduler",
	"SCHEDULER_POSTGRES_PASSWORD": "scheduler",
}

// migrationJob returns the postgres-migrator invocation setting up a database.
func migrationJob(mode, directory, namespace string) sat.SUT {
	return sat.SUT{
		Name: namespace + "-database-" + mode,
		Path: "cmd/postgres-migrator/postgres-migrator",
		Args: []string{"--mode", mode, "--dir", directory, "--target-namespace", namespace},
	}
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
					"--library-service-external-grpc.port", strconv.Itoa(libraryServicePort),
					"--library-service-external-grpc.disable-tls",
					// The dispatcher discovers the library's scheduler-run methods over reflection.
					"--library-service-external-grpc.enable-reflection",
					"--scheduler-service-grpc.host", libraryServiceHost,
					"--scheduler-service-grpc.port", strconv.Itoa(schedulerServicePort),
					"--scheduler-service-grpc.disable-tls",
				},
			},
			{
				Name: schedulerServiceName,
				Path: schedulerServicePath,
				Port: schedulerServicePort,
				Args: []string{
					"--scheduler-service-external-grpc.port", strconv.Itoa(schedulerServicePort),
					"--scheduler-service-external-grpc.disable-tls",
					"--health.port", strconv.Itoa(schedulerServiceHealthPort),
					"--prometheus.port", strconv.Itoa(schedulerServicePrometheusPort),
					"--scheduler-dispatcher.poll-interval", schedulerPollInterval.String(),
					"--scheduler-dispatcher.endpoint", fmt.Sprintf("http://%s:%d", libraryServiceHost, libraryServicePort),
					"--scheduler-service.wait-job-max-timeout", schedulerWaitJobMaxTimeout.String(),
				},
			},
		},
		PostgresServerConfig: sat.PostgresServerConfig{
			Host:     postgresHost,
			Port:     postgresPort,
			User:     "postgres",
			Password: "postgres",
		},
		Databases: []sat.Database{
			{
				Initializer: migrationJob("init", "go/test/library/migrations", "library"),
				Migrator:    migrationJob("migrate", "go/test/library/migrations", "library"),
			},
			{
				Initializer: migrationJob("init", "go/scheduler/migrations", "scheduler"),
				Migrator:    migrationJob("migrate", "go/scheduler/migrations", "scheduler"),
			},
		},
		EnvironmentVariables: environmentVariables,
		Nats:                 true,
		// Buffered by satEnvironment.NatsStore, which nats_store_test.go asserts on.
		NatsSubjects: []string{librarypb.GetShelfStream().Get().Subject(">").Name()},
	}
	satEnvironment = sat.New(config)
	if err := satEnvironment.Start(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, satEnvironment.Cleanup)

	grpcOpts := &grpc.ClientOpts{
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
	bookmarkServiceClient = libraryservicepb.NewBookmarkServiceClient(connection.Get())
	operationsClient = longrunningpb.NewOperationsClient(connection.Get())

	schedulerGrpcOpts := &grpc.ClientOpts{
		Host:       libraryServiceHost,
		Port:       schedulerServicePort,
		DisableTLS: true,
	}
	schedulerConnection, err := grpc.NewConnection(schedulerGrpcOpts, nil, &prometheus.Opts{})
	if err != nil {
		return cleanup, err
	}
	if err := schedulerConnection.Connect(ctx); err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, func() { schedulerConnection.Close() })
	schedulerServiceClient = schedulerservicepb.NewSchedulerServiceClient(schedulerConnection.Get())
	return cleanup, nil
}

// queueOf returns the queue the dispatcher created for a library method.
func queueOf(t *testing.T, method string) *schedulerpb.Queue {
	t.Helper()
	listQueuesRequest := &schedulerservicepb.ListQueuesRequest{Filter: fmt.Sprintf("service = %q AND method = %q", libraryServiceFullName, method)}
	listQueuesResponse, err := schedulerServiceClient.ListQueues(ctx, listQueuesRequest)
	require.NoError(t, err)
	require.Len(t, listQueuesResponse.GetQueues(), 1, "queue of %s", method)
	return listQueuesResponse.GetQueues()[0]
}
