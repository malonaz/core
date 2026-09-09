package sat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/scheduler"
)

var (
	ctx                    = context.Background()
	schedulerServiceClient schedulerservicepb.SchedulerServiceClient
	satEnvironment         *sat.SAT
	testProcessor          *processor
)

const (
	schedulerServiceName = "scheduler-service"
	schedulerServicePath = "cmd/scheduler-service/scheduler-service"
	schedulerServiceHost = "localhost"
	schedulerServicePort = 9090
	processorPort        = 9091

	postgresHost = "localhost"
	postgresPort = 5432

	// Timings the scheduler runs with; tests derive their expectations from them.
	pollInterval    = 100 * time.Millisecond
	leaseDuration   = 1500 * time.Millisecond
	maxParallelJobs = 16

	// Per job type policy; see configuration.jsonnet.
	flakyBackoffInitial = 300 * time.Millisecond
	flakyMaxAttempts    = 3
	deadlineTimeout     = 1 * time.Second
	deadlineMaxAttempts = 2
	sleepTimeout        = 10 * time.Second

	// Generous ceiling for polling assertions.
	waitTimeout = 30 * time.Second
)

var environmentVariables = map[string]string{
	"POSTGRES_HOST":     postgresHost,
	"POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"POSTGRES_DATABASE": "postgres",
	"POSTGRES_USER":     "postgres",
	"POSTGRES_PASSWORD": "postgres",

	"SCHEDULER_POSTGRES_HOST":     postgresHost,
	"SCHEDULER_POSTGRES_PORT":     strconv.Itoa(postgresPort),
	"SCHEDULER_POSTGRES_DATABASE": "scheduler",
	"SCHEDULER_POSTGRES_USER":     "scheduler",
	"SCHEDULER_POSTGRES_PASSWORD": "scheduler",
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

	// The processor lives in the test process so tests can script and inspect it.
	testProcessor = newProcessor()
	stopProcessor, err := testProcessor.serve(fmt.Sprintf("%s:%d", schedulerServiceHost, processorPort))
	if err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, stopProcessor)

	config := &sat.Config{
		SUTS: []sat.SUT{
			{
				Name: schedulerServiceName,
				Path: schedulerServicePath,
				Port: schedulerServicePort,
				Args: []string{
					"--scheduler-service-external-grpc.host", schedulerServiceHost,
					"--scheduler-service-external-grpc.port", strconv.Itoa(schedulerServicePort),
					"--scheduler-service-external-grpc.disable-tls",
					"--scheduler-service.configuration", "go/test/scheduler/scheduler_service/sat/configuration.jsonnet",
					"--scheduler-service.ignore-job", jobType(&processorpb.IgnoredRequest{}),
					"--scheduler-service.max-parallel-jobs", strconv.Itoa(maxParallelJobs),
					"--scheduler-service.poll-interval", pollInterval.String(),
					"--scheduler-service.lease-duration", leaseDuration.String(),
					"--scheduler-service.sweep-interval", "500ms",
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
				"--dir", "go/scheduler/migrations",
				"--target-namespace", "scheduler",
			},
		},
		Migrator: sat.SUT{
			Name: "database-migrator",
			Path: "cmd/postgres-migrator/postgres-migrator",
			Args: []string{
				"--mode", "migrate",
				"--dir", "go/scheduler/migrations",
				"--target-namespace", "scheduler",
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
		Host:       schedulerServiceHost,
		Port:       schedulerServicePort,
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
	schedulerServiceClient = schedulerservicepb.NewSchedulerServiceClient(connection.Get())
	testProcessor.schedulerServiceClient = schedulerServiceClient
	return cleanup, nil
}

// jobType returns the type URL the scheduler routes the message's jobs by.
func jobType(message proto.Message) string {
	payload, err := anypb.New(message)
	if err != nil {
		panic(err)
	}
	return payload.GetTypeUrl()
}

func createJob(t *testing.T, message proto.Message, options ...scheduler.CreateJobOption) *schedulerpb.Job {
	t.Helper()
	createJobRequest, err := scheduler.NewCreateJobRequest(message, options...)
	require.NoError(t, err)
	job, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	return job
}

func getJob(t *testing.T, name string) *schedulerpb.Job {
	t.Helper()
	getJobRequest := &schedulerservicepb.GetJobRequest{Name: name}
	job, err := schedulerServiceClient.GetJob(ctx, getJobRequest)
	require.NoError(t, err)
	return job
}

// waitForJob polls the job until predicate holds and returns it.
func waitForJob(t *testing.T, name string, predicate func(*schedulerpb.Job) bool) *schedulerpb.Job {
	t.Helper()
	var job *schedulerpb.Job
	require.Eventually(t, func() bool {
		job = getJob(t, name)
		return predicate(job)
	}, waitTimeout, 20*time.Millisecond, "job %s never matched; last: %v", name, job)
	return job
}

func waitForState(t *testing.T, name string, state schedulerpb.JobState) *schedulerpb.Job {
	t.Helper()
	return waitForJob(t, name, func(job *schedulerpb.Job) bool { return job.GetState() == state })
}

func waitForTerminal(t *testing.T, name string) *schedulerpb.Job {
	t.Helper()
	return waitForJob(t, name, func(job *schedulerpb.Job) bool {
		switch job.GetState() {
		case schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED:
			return true
		}
		return false
	})
}

// unpackAny unmarshals an Any into M.
func unpackAny[M proto.Message](t *testing.T, payload *anypb.Any) M {
	t.Helper()
	var message M
	message = message.ProtoReflect().New().Interface().(M)
	require.NoError(t, payload.UnmarshalTo(message))
	return message
}

func mustAny(t *testing.T, message proto.Message) *anypb.Any {
	t.Helper()
	payload, err := anypb.New(message)
	require.NoError(t, err)
	return payload
}

func grpcCode(err error) codes.Code {
	return grpcstatus.Code(err)
}
