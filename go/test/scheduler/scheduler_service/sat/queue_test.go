package sat

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

func newPolicy() *policypb.QueuePolicy {
	return &policypb.QueuePolicy{AttemptTimeout: durationpb.New(5 * time.Second), MaxAttempts: 1}
}

func TestQueue_Discovered(t *testing.T) {
	t.Parallel()
	// The dispatcher declared one queue per annotated processor method, with
	// the method's types and policy; Unrouted has none.
	queue := getQueue(t, echoQueue)
	require.Equal(t, processorService, queue.GetService())
	require.Equal(t, "Echo", queue.GetMethod())
	require.Equal(t, processorURL, queue.GetEndpoint())
	require.Equal(t, typeURL(&processorpb.EchoRequest{}), queue.GetRequestType())
	require.Equal(t, typeURL(&processorpb.EchoResponse{}), queue.GetResponseType())
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, queue.GetState())
	require.Equal(t, 5*time.Second, queue.GetPolicy().GetAttemptTimeout().AsDuration())
	require.Equal(t, int32(1), queue.GetPolicy().GetMaxAttempts())

	flaky := getQueue(t, flakyQueue)
	require.Equal(t, flakyBackoffInitial, flaky.GetPolicy().GetRetryBackoff().GetInitial().AsDuration())
	require.Equal(t, int32(flakyMaxAttempts), flaky.GetPolicy().GetMaxAttempts())

	listQueuesRequest := &schedulerservicepb.ListQueuesRequest{Filter: fmt.Sprintf("service = %q AND method = %q", processorService, "Unrouted")}
	listQueuesResponse, err := schedulerServiceClient.ListQueues(ctx, listQueuesRequest)
	require.NoError(t, err)
	require.Empty(t, listQueuesResponse.GetQueues())
}

func TestQueue_CRUD(t *testing.T) {
	t.Parallel()
	created := declareQueue(t, deadURL, newPolicy())
	require.Regexp(t, `^queues/[a-z0-9]+$`, created.GetName())
	require.NotEmpty(t, created.GetEtag())
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, created.GetState())
	require.Equal(t, created.GetCreateTime().AsTime(), created.GetUpdateTime().AsTime())
	require.Equal(t, deadURL, created.GetEndpoint())

	// Defaults are applied server-side and echoed back.
	policy := created.GetPolicy()
	require.Equal(t, 5*time.Second, policy.GetAttemptTimeout().AsDuration())
	require.Equal(t, int32(1), policy.GetMaxAttempts())
	require.Equal(t, 10*time.Second, policy.GetRetryBackoff().GetInitial().AsDuration())
	require.Equal(t, 10*time.Minute, policy.GetRetryBackoff().GetMax().AsDuration())
	require.Equal(t, float64(2), policy.GetRetryBackoff().GetMultiplier())
	require.ElementsMatch(t, []codepb.Code{
		codepb.Code_UNAVAILABLE, codepb.Code_INTERNAL, codepb.Code_UNKNOWN,
		codepb.Code_DEADLINE_EXCEEDED, codepb.Code_RESOURCE_EXHAUSTED, codepb.Code_ABORTED,
	}, policy.GetRetryableCodes())
	grpcrequire.Equal(t, &schedulerpb.QueueStats{}, created.GetStats())

	grpcrequire.Equal(t, created, getQueue(t, created.GetName()))

	batchGetQueuesRequest := &schedulerservicepb.BatchGetQueuesRequest{Names: []string{created.GetName(), echoQueue}}
	batchGetQueuesResponse, err := schedulerServiceClient.BatchGetQueues(ctx, batchGetQueuesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetQueuesResponse.GetQueues(), 2)
	grpcrequire.Equal(t, created, batchGetQueuesResponse.GetQueues()[0])

	listQueuesRequest := &schedulerservicepb.ListQueuesRequest{Filter: fmt.Sprintf(`create_time >= "%s" AND state = QUEUE_STATE_RUNNING`, created.GetCreateTime().AsTime().Format(time.RFC3339Nano))}
	queues, err := aip.Paginate[*schedulerpb.Queue](ctx, listQueuesRequest, schedulerServiceClient.ListQueues)
	require.NoError(t, err)
	index := slices.IndexFunc(queues, func(queue *schedulerpb.Queue) bool { return queue.GetName() == created.GetName() })
	require.GreaterOrEqual(t, index, 0)
	grpcrequire.Equal(t, created, queues[index])

	// A sub-path update rewrites just that field of the policy.
	updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: created.GetName(), Policy: &policypb.QueuePolicy{MaxAttempts: 4}, Etag: created.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
	}
	updated, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Equal(t, int32(4), updated.GetPolicy().GetMaxAttempts())
	require.NotEqual(t, created.GetEtag(), updated.GetEtag())
	grpcrequire.Equal(t, created, updated, protocmp.IgnoreFields(&schedulerpb.Queue{}, "etag", "update_time"), protocmp.IgnoreFields(&policypb.QueuePolicy{}, "max_attempts"))

	// The stale etag is refused.
	_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)

	// The endpoint moves.
	updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: created.GetName(), Endpoint: bareURL},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"endpoint"}},
	}
	updated, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Equal(t, bareURL, updated.GetEndpoint())

	// What identifies the queue, and output-only fields, are not updatable.
	for _, path := range []string{"service", "method", "request_type", "state"} {
		updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: created.GetName(), Service: "x.v1.X", Method: "X", RequestType: "type.googleapis.com/x.v1.XRequest", State: schedulerpb.QueueState_QUEUE_STATE_PAUSED},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}},
		}
		_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	}

	deleteQueueRequest := &schedulerservicepb.DeleteQueueRequest{Name: created.GetName(), Etag: created.GetEtag()}
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	deleteQueueRequest.Etag = ""
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.GetQueue(ctx, &schedulerservicepb.GetQueueRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestQueue_Validation(t *testing.T) {
	t.Parallel()
	create := func(queue *schedulerpb.Queue) error {
		createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: queue}
		_, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
		return err
	}
	method := "Method" + uuid.MustNewV7().String()[:8]
	t.Run("missing policy", func(t *testing.T) {
		queue := fakeQueue(method, deadURL, nil)
		grpcrequire.Error(t, codes.InvalidArgument, create(queue))
	})
	t.Run("zero attempt timeout", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(fakeQueue(method, deadURL, &policypb.QueuePolicy{MaxAttempts: 1})))
	})
	t.Run("malformed identity", func(t *testing.T) {
		for _, mutate := range []func(*schedulerpb.Queue){
			func(queue *schedulerpb.Queue) { queue.Service = "" },
			func(queue *schedulerpb.Queue) { queue.Service = "no-dots" },
			func(queue *schedulerpb.Queue) { queue.Method = "" },
			func(queue *schedulerpb.Queue) { queue.Method = "/slash" },
			func(queue *schedulerpb.Queue) { queue.Endpoint = "" },
			func(queue *schedulerpb.Queue) { queue.RequestType = "EchoRequest" },
			func(queue *schedulerpb.Queue) { queue.ResponseType = "" },
		} {
			queue := fakeQueue(method, deadURL, newPolicy())
			mutate(queue)
			grpcrequire.Error(t, codes.InvalidArgument, create(queue))
		}
	})
	t.Run("bad retryable code", func(t *testing.T) {
		policy := newPolicy()
		policy.RetryableCodes = []codepb.Code{codepb.Code_OK}
		grpcrequire.Error(t, codes.InvalidArgument, create(fakeQueue(method, deadURL, policy)))
	})
	t.Run("duplicate method or request type", func(t *testing.T) {
		existing := declareQueue(t, deadURL, newPolicy())
		duplicate := fakeQueue(existing.GetMethod(), deadURL, newPolicy())
		duplicate.RequestType += "Other"
		grpcrequire.Error(t, codes.AlreadyExists, create(duplicate))
		duplicate = fakeQueue(existing.GetMethod()+"Other", deadURL, newPolicy())
		duplicate.RequestType = existing.GetRequestType()
		grpcrequire.Error(t, codes.AlreadyExists, create(duplicate))
	})
	t.Run("invalid update", func(t *testing.T) {
		queue := declareQueue(t, deadURL, newPolicy())
		updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: queue.GetName(), Policy: &policypb.QueuePolicy{}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
		}
		_, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		updateQueueRequest.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"policy.no_such_field"}}
		_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestQueue_Stats(t *testing.T) {
	// Shares the pausable queue with TestQueue_PauseResume, so not parallel.
	key := uuid.MustNewV7().String()
	running := createJob(t, &processorpb.PausableRequest{Key: key, Duration: durationpb.New(sleepTimeout)})
	waitForState(t, running.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
	later := createJob(t, &processorpb.PausableRequest{Key: key}, scheduler.WithScheduleTime(farFuture))
	sooner := createJob(t, &processorpb.PausableRequest{Key: key}, scheduler.WithScheduleTime(farFuture.Add(-time.Hour)))

	stats := getQueue(t, pausableQueue).GetStats()
	require.Equal(t, int32(2), stats.GetPendingCount())
	require.Equal(t, int32(1), stats.GetRunningCount())
	require.True(t, sooner.GetScheduleTime().AsTime().Equal(stats.GetOldestPendingScheduleTime().AsTime()))

	cancelJob(t, sooner.GetName())
	stats = getQueue(t, pausableQueue).GetStats()
	require.Equal(t, int32(1), stats.GetPendingCount())
	require.True(t, later.GetScheduleTime().AsTime().Equal(stats.GetOldestPendingScheduleTime().AsTime()))

	cancelJob(t, later.GetName())
	cancelJob(t, running.GetName())
	grpcrequire.Equal(t, &schedulerpb.QueueStats{}, getQueue(t, pausableQueue).GetStats())
}

func TestQueue_DeleteWithJobs(t *testing.T) {
	t.Parallel()
	queue := declareQueue(t, deadURL, newPolicy())
	// No Go type exists for the fake request: the payload is built by hand.
	createJobRequest := &schedulerservicepb.CreateJobRequest{Job: &schedulerpb.Job{
		Payload:      &anypb.Any{TypeUrl: queue.GetRequestType()},
		ScheduleTime: timestamppb.New(farFuture),
	}}
	pending, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	require.Equal(t, queue.GetName(), pending.GetQueue())

	deleteQueueRequest := &schedulerservicepb.DeleteQueueRequest{Name: queue.GetName()}
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)

	// Terminal jobs do not hold the queue.
	cancelJob(t, pending.GetName())
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	grpcrequire.Error(t, codes.NotFound, err)
	deleteQueueRequest.AllowMissing = true
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	require.NoError(t, err)
	// The job outlives its queue and still reads.
	require.Equal(t, schedulerpb.JobState_JOB_STATE_CANCELLED, getJob(t, pending.GetName()).GetState())
}

func TestQueue_PauseResume(t *testing.T) {
	// Shares the pausable queue with TestQueue_Stats, so not parallel.
	queue := getQueue(t, pausableQueue)
	key := uuid.MustNewV7().String()
	running := createJob(t, &processorpb.PausableRequest{Key: key, Duration: durationpb.New(2 * time.Second)})
	waitForState(t, running.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)

	pauseQueueRequest := &schedulerservicepb.PauseQueueRequest{Name: queue.GetName(), Etag: "stale"}
	_, err := schedulerServiceClient.PauseQueue(ctx, pauseQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	pauseQueueRequest.Etag = queue.GetEtag()
	paused, err := schedulerServiceClient.PauseQueue(ctx, pauseQueueRequest)
	require.NoError(t, err)
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_PAUSED, paused.GetState())
	require.NotEqual(t, queue.GetEtag(), paused.GetEtag())
	require.True(t, paused.GetUpdateTime().AsTime().After(queue.GetUpdateTime().AsTime()))

	// Pausing again is a no-op that returns the queue unchanged.
	pauseQueueRequest.Etag = ""
	pausedAgain, err := schedulerServiceClient.PauseQueue(ctx, pauseQueueRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, paused, pausedAgain, protocmp.IgnoreFields(&schedulerpb.Queue{}, "stats"))

	// New jobs are accepted and stay PENDING; the running one finishes.
	keys := make([]string, 3)
	jobs := make([]*schedulerpb.Job, 3)
	for i := range jobs {
		keys[i] = fmt.Sprintf("%s-%d", key, i)
		jobs[i] = createJob(t, &processorpb.PausableRequest{Key: keys[i]})
	}
	finished := waitForTerminal(t, running.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, finished.GetState())
	time.Sleep(5 * pollInterval)
	for i, job := range jobs {
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, getJob(t, job.GetName()).GetState())
		require.Empty(t, testProcessor.calls(keys[i]))
	}
	require.Equal(t, int32(3), getQueue(t, queue.GetName()).GetStats().GetPendingCount())

	resumeQueueRequest := &schedulerservicepb.ResumeQueueRequest{Name: queue.GetName(), Etag: "stale"}
	_, err = schedulerServiceClient.ResumeQueue(ctx, resumeQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	resumeQueueRequest.Etag = ""
	resumed, err := schedulerServiceClient.ResumeQueue(ctx, resumeQueueRequest)
	require.NoError(t, err)
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, resumed.GetState())
	resumedAgain, err := schedulerServiceClient.ResumeQueue(ctx, resumeQueueRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, resumed, resumedAgain, protocmp.IgnoreFields(&schedulerpb.Queue{}, "stats"))
	for i, job := range jobs {
		waitForState(t, job.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		require.Len(t, testProcessor.calls(keys[i]), 1)
	}

	_, err = schedulerServiceClient.PauseQueue(ctx, &schedulerservicepb.PauseQueueRequest{Name: "queues/does-not-exist"})
	grpcrequire.Error(t, codes.NotFound, err)
}
