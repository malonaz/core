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

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
	"github.com/malonaz/core/go/sat"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
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
	tickInterval    = 100 * time.Millisecond
	leaseDuration   = 1500 * time.Millisecond
	maxParallelJobs = 16
	replicaCount    = 2

	// The in-process processor the dispatcher discovers and delivers to.
	processorURL     = "http://localhost:9091"
	deadURL          = "http://localhost:9093"
	bareURL          = "http://localhost:9094"
	processorService = "malonaz.test.scheduler.processor.v1.Processor"
	processorPath    = "/" + processorService + "/"

	// Policies as declared on the processor's methods (processor.proto).
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

// The queues the dispatcher created for the processor's methods, resolved once
// the scheduler is up.
var (
	echoQueue, flakyQueue, sleepQueue, deadlineQueue, progressQueue, limitedQueue, serialQueue, operateQueue string
	pausableQueue, tunableQueue, redialQueue                                                                 string
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

// schedulerSUT describes one scheduler replica dispatching to the endpoints.
// Every replica needs its own health and metrics ports or the second dies at boot.
func schedulerSUT(name string, port, healthPort, prometheusPort int, endpoints ...string) sat.SUT {
	args := make([]string, 0, 2*len(endpoints))
	for _, endpoint := range endpoints {
		args = append(args, "--scheduler-dispatcher.endpoint", endpoint)
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
			"--scheduler-service.lease-duration", leaseDuration.String(),
			"--scheduler-service.sweep-interval", "500ms",
			"--scheduler-service.tick-interval", tickInterval.String(),
			"--scheduler-service.wait-job-max-timeout", waitJobMaxTimeout.String(),
			"--scheduler-dispatcher.max-parallel-jobs", strconv.Itoa(maxParallelJobs),
			"--scheduler-dispatcher.poll-interval", pollInterval.String(),
			"--scheduler-dispatcher.lease-duration", leaseDuration.String(),
			"--scheduler-dispatcher.worker-id", name,
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
			schedulerSUT(schedulerServiceName, schedulerServicePort, 4040, 13434, processorURL),
			schedulerSUT(schedulerServiceName+"-replica", schedulerReplicaPort, 4041, 13435, processorURL),
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
	if err := resolveQueues(ctx); err != nil {
		return cleanup, err
	}
	return cleanup, nil
}

// resolveQueues looks up the queue the dispatcher created for each processor method.
func resolveQueues(ctx context.Context) error {
	for method, queue := range map[string]*string{
		"Echo": &echoQueue, "Flaky": &flakyQueue, "Sleep": &sleepQueue, "Deadline": &deadlineQueue,
		"Progress": &progressQueue, "Limited": &limitedQueue, "Serial": &serialQueue, "Operate": &operateQueue,
		"Pausable": &pausableQueue, "Tunable": &tunableQueue, "Redial": &redialQueue,
	} {
		name, err := queueNameOf(ctx, processorService, method)
		if err != nil {
			return err
		}
		*queue = name
	}
	return nil
}

// queueNameOf returns the name of the queue serving the method.
func queueNameOf(ctx context.Context, service, method string) (string, error) {
	listQueuesRequest := &schedulerservicepb.ListQueuesRequest{Filter: fmt.Sprintf("service = %q AND method = %q", service, method)}
	listQueuesResponse, err := schedulerServiceClient.ListQueues(ctx, listQueuesRequest)
	if err != nil {
		return "", fmt.Errorf("listing queues: %w", err)
	}
	if len(listQueuesResponse.GetQueues()) != 1 {
		return "", fmt.Errorf("expected one queue of %s.%s, got %d", service, method, len(listQueuesResponse.GetQueues()))
	}
	return listQueuesResponse.GetQueues()[0].GetName(), nil
}

// requestType is the type URL of a processor method's request.
func requestType(method string) string {
	return "type.googleapis.com/malonaz.test.scheduler.processor.v1." + method + "Request"
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
	case *processorpb.LimitedRequest:
		return limitedQueue
	case *processorpb.SerialRequest:
		return serialQueue
	case *processorpb.PausableRequest:
		return pausableQueue
	case *processorpb.TunableRequest:
		return tunableQueue
	case *processorpb.RedialRequest:
		return redialQueue
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
	job, err := scheduler.CreateJob(ctx, schedulerServiceClient, parent, message, options...)
	require.NoError(t, err)
	return job
}

// newCreateJobRequest builds the request scheduler.CreateJob would send, for
// tests that tamper with it before sending.
func newCreateJobRequest(t *testing.T, parent string, message proto.Message, options ...scheduler.CreateJobOption) *schedulerservicepb.CreateJobRequest {
	t.Helper()
	payload, err := anypb.New(message)
	require.NoError(t, err)
	request := &schedulerservicepb.CreateJobRequest{
		Parent:    parent,
		Job:       &schedulerpb.Job{Payload: payload},
		RequestId: uuid.MustNewV7().String(),
	}
	for _, option := range options {
		option(request)
	}
	return request
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

// declareQueue creates a queue as the dispatcher would, for a fake method on
// the given endpoint, and deletes it once the test ends.
func declareQueue(t *testing.T, endpoint string, policy *policypb.QueuePolicy) *schedulerpb.Queue {
	t.Helper()
	method := "Method" + strings.ReplaceAll(uuid.MustNewV7().String(), "-", "")
	queue, err := schedulerServiceClient.CreateQueue(ctx, &schedulerservicepb.CreateQueueRequest{Queue: fakeQueue(method, endpoint, policy)})
	require.NoError(t, err)
	t.Cleanup(func() { deleteQueueOnceIdle(t, queue.GetName()) })
	return queue
}

// fakeQueue declares a queue for a method of a service that exists nowhere.
func fakeQueue(method, endpoint string, policy *policypb.QueuePolicy) *schedulerpb.Queue {
	return &schedulerpb.Queue{
		Service:      "test.fake.v1.Fake",
		Method:       method,
		Endpoint:     endpoint,
		RequestType:  "type.googleapis.com/test.fake.v1." + method + "Request",
		ResponseType: "type.googleapis.com/test.fake.v1." + method + "Response",
		Policy:       policy,
	}
}

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
