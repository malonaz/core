package sat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	// The fixtures every replica converges onto at boot.
	bootstrapPath        = "go/test/scheduler/scheduler_service/sat/bootstrap.jsonnet"
	schedulerServiceHost = "localhost"
	schedulerServicePort = 9090
	// A second replica claiming from the same database.
	schedulerReplicaPort = 9092
	processorPort        = 9091
	// A port nothing listens on, for targets pointed at a dead endpoint.
	deadPort = 9093
	// A gRPC server without reflection.
	barePort = 9094

	postgresHost = "localhost"
	postgresPort = 5432

	// Timings the scheduler runs with; tests derive their expectations from them.
	pollInterval    = 100 * time.Millisecond
	leaseDuration   = 1500 * time.Millisecond
	maxParallelJobs = 16
	replicaCount    = 2

	// The in-process processor every sat queue routes to.
	targetName      = "targets/test-processor"
	bootstrapTarget = "targets/bootstrap-target"
	processorURL    = "http://localhost:9091"
	deadURL         = "http://localhost:9093"
	bareURL         = "http://localhost:9094"
	testHeader      = "x-test-header"
	processorPath   = "/malonaz.test.scheduler.processor.v1.Processor/"

	// The sat queues; see bootstrap.jsonnet for their policies.
	echoQueue            = "queues/echo"
	flakyQueue           = "queues/flaky"
	sleepQueue           = "queues/sleep"
	deadlineQueue        = "queues/deadline"
	progressQueue        = "queues/progress"
	limitedQueue         = "queues/limited"
	operateQueue         = "queues/operate"
	bootstrapPausedQueue = "queues/bootstrap-paused"

	// Policies as declared in bootstrap.jsonnet.
	flakyBackoffInitial = 300 * time.Millisecond
	flakyMaxAttempts    = 3
	deadlineTimeout     = 1 * time.Second
	deadlineMaxAttempts = 2
	sleepTimeout        = 10 * time.Second
	limitedConcurrency  = 2
	operateMaxAttempts  = 3
	// Short enough for a sat to observe the cap on an absurd WaitJob timeout.
	waitJobMaxTimeout = 2 * time.Second

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

// schedulerSUT describes one scheduler replica converging onto the bootstrap
// files. Every replica needs its own health and metrics ports or the second
// dies at boot.
func schedulerSUT(name string, port, healthPort, prometheusPort int, bootstrapPaths ...string) sat.SUT {
	args := make([]string, 0, 2*len(bootstrapPaths))
	for _, path := range bootstrapPaths {
		args = append(args, "--scheduler-service.bootstrap", path)
	}
	return sat.SUT{
		Name: name,
		Path: schedulerServicePath,
		Port: port,
		Args: append(args,
			"--scheduler-service-external-grpc.port", strconv.Itoa(port),
			"--scheduler-service-external-grpc.disable-tls",
			"--health.port", strconv.Itoa(healthPort),
			"--prometheus.port", strconv.Itoa(prometheusPort),
			"--scheduler-service.max-parallel-jobs", strconv.Itoa(maxParallelJobs),
			"--scheduler-service.poll-interval", pollInterval.String(),
			"--scheduler-service.lease-duration", leaseDuration.String(),
			"--scheduler-service.sweep-interval", "500ms",
			"--scheduler-service.wait-job-max-timeout", waitJobMaxTimeout.String(),
			"--scheduler-service.worker-id", name,
		),
	}
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
	stopBare, err := serveBare(fmt.Sprintf("%s:%d", schedulerServiceHost, barePort))
	if err != nil {
		return cleanup, err
	}
	cleanupFns = append(cleanupFns, stopBare)

	config := &sat.Config{
		SUTS: []sat.SUT{
			schedulerSUT(schedulerServiceName, schedulerServicePort, 4040, 13434, bootstrapPath),
			schedulerSUT(schedulerServiceName+"-replica", schedulerReplicaPort, 4041, 13435, bootstrapPath),
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

	grpcOpts := &grpc.ClientOpts{
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

// handler routes a processor method to the shared target.
func handler(method string) *schedulerpb.Handler {
	return &schedulerpb.Handler{Method: processorPath + method, Target: targetName}
}

// resourceID returns the last segment of a resource name.
func resourceID(name string) string {
	return name[strings.LastIndex(name, "/")+1:]
}

// typeURL returns the type URL of the message, as carried by payloads and
// handler request/response types.
func typeURL(message proto.Message) string {
	payload, err := anypb.New(message)
	if err != nil {
		panic(err)
	}
	return payload.GetTypeUrl()
}

// queueFor returns the shared queue routing the message's type.
func queueFor(message proto.Message) string {
	switch message.(type) {
	case *processorpb.EchoRequest:
		return echoQueue
	case *processorpb.FlakyRequest:
		return flakyQueue
	case *processorpb.SleepRequest:
		return sleepQueue
	case *processorpb.DeadlineRequest:
		return deadlineQueue
	case *processorpb.ProgressRequest:
		return progressQueue
	case *processorpb.OperateRequest:
		return operateQueue
	}
	panic(fmt.Sprintf("no shared queue for %T", message))
}

// createJob creates a system job (no parent) in the message type's shared queue.
func createJob(t *testing.T, message proto.Message, options ...scheduler.CreateJobOption) *schedulerpb.Job {
	t.Helper()
	return createJobUnder(t, "", message, options...)
}

func createJobUnder(t *testing.T, parent string, message proto.Message, options ...scheduler.CreateJobOption) *schedulerpb.Job {
	t.Helper()
	return createJobIn(t, parent, queueFor(message), message, options...)
}

func createJobIn(t *testing.T, parent, queue string, message proto.Message, options ...scheduler.CreateJobOption) *schedulerpb.Job {
	t.Helper()
	createJobRequest, err := scheduler.NewCreateJobRequest(parent, queue, message, options...)
	require.NoError(t, err)
	job, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	return job
}

func cancelJob(t *testing.T, name string) {
	t.Helper()
	cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: name}
	_, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
	require.NoError(t, err)
}

func getJob(t *testing.T, name string) *schedulerpb.Job {
	t.Helper()
	getJobRequest := &schedulerservicepb.GetJobRequest{Name: name}
	job, err := schedulerServiceClient.GetJob(ctx, getJobRequest)
	require.NoError(t, err)
	return job
}

func getQueue(t *testing.T, name string) *schedulerpb.Queue {
	t.Helper()
	getQueueRequest := &schedulerservicepb.GetQueueRequest{Name: name}
	queue, err := schedulerServiceClient.GetQueue(ctx, getQueueRequest)
	require.NoError(t, err)
	return queue
}

// createQueue creates a queue private to a test, routing the given processor
// methods to the shared target, and deletes it once the test ends.
func createQueue(t *testing.T, policy *schedulerpb.QueuePolicy, methods ...string) *schedulerpb.Queue {
	t.Helper()
	handlers := make([]*schedulerpb.Handler, len(methods))
	for i, method := range methods {
		handlers[i] = handler(method)
	}
	createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: &schedulerpb.Queue{Policy: policy, Handlers: handlers}}
	queue, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err)
	t.Cleanup(func() { deleteQueueOnceIdle(t, queue.GetName()) })
	return queue
}

// deleteQueueOnceIdle cancels the queue's live jobs, then deletes it.
func deleteQueueOnceIdle(t *testing.T, name string) {
	t.Helper()
	listJobsRequest := &schedulerservicepb.ListJobsRequest{Filter: fmt.Sprintf(`queue = "%s" AND (state = JOB_STATE_PENDING OR state = JOB_STATE_RUNNING)`, name)}
	listJobsResponse, err := schedulerServiceClient.ListJobs(ctx, listJobsRequest)
	require.NoError(t, err)
	for _, job := range listJobsResponse.GetJobs() {
		cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: job.GetName()}
		_, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
		require.True(t, err == nil || grpcCode(err) == codes.FailedPrecondition, "cancelling %s: %v", job.GetName(), err)
	}
	require.Eventually(t, func() bool {
		deleteQueueRequest := &schedulerservicepb.DeleteQueueRequest{Name: name, AllowMissing: true}
		_, err := schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
		return err == nil
	}, waitTimeout, 50*time.Millisecond)
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
	return waitForJob(t, name, func(job *schedulerpb.Job) bool { return isTerminal(job.GetState()) })
}

func isTerminal(state schedulerpb.JobState) bool {
	switch state {
	case schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED:
		return true
	}
	return false
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
