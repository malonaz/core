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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

func newPolicy() *schedulerpb.QueuePolicy {
	return &schedulerpb.QueuePolicy{AttemptTimeout: durationpb.New(5 * time.Second), MaxAttempts: 1}
}

func TestQueue_CRUD(t *testing.T) {
	t.Parallel()
	created := createQueue(t, newPolicy(), "Echo", "Sleep")
	require.Regexp(t, `^queues/[a-z0-9]+$`, created.GetName())
	require.NotEmpty(t, created.GetEtag())
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, created.GetState())
	require.Equal(t, created.GetCreateTime().AsTime(), created.GetUpdateTime().AsTime())

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

	// Handler types come from the target, over reflection.
	require.Len(t, created.GetHandlers(), 2)
	require.Equal(t, processorPath+"Echo", created.GetHandlers()[0].GetMethod())
	require.Equal(t, targetName, created.GetHandlers()[0].GetTarget())
	require.Equal(t, typeURL(&processorpb.EchoRequest{}), created.GetHandlers()[0].GetRequestType())
	require.Equal(t, typeURL(&processorpb.EchoResponse{}), created.GetHandlers()[0].GetResponseType())
	require.Equal(t, typeURL(&processorpb.SleepRequest{}), created.GetHandlers()[1].GetRequestType())
	require.Equal(t, typeURL(&processorpb.SleepResponse{}), created.GetHandlers()[1].GetResponseType())
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
		Queue:      &schedulerpb.Queue{Name: created.GetName(), Policy: &schedulerpb.QueuePolicy{MaxAttempts: 4}, Etag: created.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
	}
	updated, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Equal(t, int32(4), updated.GetPolicy().GetMaxAttempts())
	require.NotEqual(t, created.GetEtag(), updated.GetEtag())
	grpcrequire.Equal(t, created, updated, protocmp.IgnoreFields(&schedulerpb.Queue{}, "etag", "update_time"), protocmp.IgnoreFields(&schedulerpb.QueuePolicy{}, "max_attempts"))

	// The stale etag is refused.
	_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)

	// Handlers are replaced wholesale and re-resolved.
	updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: created.GetName(), Handlers: []*schedulerpb.Handler{handler("Flaky")}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"handlers"}},
	}
	updated, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Len(t, updated.GetHandlers(), 1)
	require.Equal(t, typeURL(&processorpb.FlakyRequest{}), updated.GetHandlers()[0].GetRequestType())

	// Output-only fields are not updatable.
	updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: created.GetName(), State: schedulerpb.QueueState_QUEUE_STATE_PAUSED},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	}
	_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)

	deleteQueueRequest := &schedulerservicepb.DeleteQueueRequest{Name: created.GetName(), Etag: created.GetEtag()}
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	deleteQueueRequest.Etag = ""
	_, err = schedulerServiceClient.DeleteQueue(ctx, deleteQueueRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.GetQueue(ctx, &schedulerservicepb.GetQueueRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.NotFound, err)

	// Handlers sharing a target share one schema fetch, cached for later mutations.
	header := uuid.MustNewV7().String()
	target := createTarget(t, processorURL, map[string]string{testHeader: header})
	createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: &schedulerpb.Queue{
		Policy: newPolicy(),
		Handlers: []*schedulerpb.Handler{
			{Method: processorPath + "Echo", Target: target.GetName()},
			{Method: processorPath + "Sleep", Target: target.GetName()},
		},
	}}
	private, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err)
	t.Cleanup(func() { deleteQueueOnceIdle(t, private.GetName()) })
	require.Equal(t, typeURL(&processorpb.SleepRequest{}), private.GetHandlers()[1].GetRequestType())
	require.Equal(t, 1, testProcessor.reflectionStreams(header))
	updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: private.GetName(), Handlers: []*schedulerpb.Handler{{Method: processorPath + "Flaky", Target: target.GetName()}}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"handlers"}},
	}
	_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Equal(t, 1, testProcessor.reflectionStreams(header))
}

func TestQueue_Validation(t *testing.T) {
	t.Parallel()
	create := func(queue *schedulerpb.Queue) error {
		createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: queue}
		_, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
		return err
	}
	t.Run("missing policy", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Handlers: []*schedulerpb.Handler{handler("Echo")}}))
	})
	t.Run("zero attempt timeout", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: &schedulerpb.QueuePolicy{MaxAttempts: 1}, Handlers: []*schedulerpb.Handler{handler("Echo")}}))
	})
	t.Run("no handlers", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy()}))
	})
	t.Run("unknown method", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{handler("Nope")}}))
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{{Method: "malformed", Target: targetName}}}))
	})
	t.Run("unreachable target", func(t *testing.T) {
		target := createTarget(t, deadURL, nil)
		grpcrequire.Error(t, codes.FailedPrecondition, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: target.GetName()}}}))
	})
	t.Run("target without reflection", func(t *testing.T) {
		target := createTarget(t, bareURL, nil)
		grpcrequire.Error(t, codes.FailedPrecondition, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: target.GetName()}}}))
	})
	t.Run("unknown target", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: "targets/does-not-exist"}}}))
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: "nonsense"}}}))
	})
	t.Run("duplicate request type", func(t *testing.T) {
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{handler("Echo"), handler("Echo")}}))
	})
	t.Run("bad retryable code", func(t *testing.T) {
		policy := newPolicy()
		policy.RetryableCodes = []codepb.Code{codepb.Code_OK}
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Queue{Policy: policy, Handlers: []*schedulerpb.Handler{handler("Echo")}}))
	})
	t.Run("invalid update", func(t *testing.T) {
		queue := createQueue(t, newPolicy(), "Echo")
		updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: queue.GetName(), Handlers: []*schedulerpb.Handler{handler("Nope")}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"handlers"}},
		}
		_, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		updateQueueRequest = &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: queue.GetName(), Policy: &schedulerpb.QueuePolicy{}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
		}
		_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		updateQueueRequest.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"policy.no_such_field"}}
		_, err = schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestQueue_Stats(t *testing.T) {
	t.Parallel()
	queue := createQueue(t, newPolicy(), "Echo", "Sleep")
	key := uuid.MustNewV7().String()
	running := createJobIn(t, "", queue.GetName(), &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleepTimeout)})
	waitForState(t, running.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
	later := createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: key}, scheduler.WithScheduleTime(farFuture))
	sooner := createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: key}, scheduler.WithScheduleTime(farFuture.Add(-time.Hour)))

	stats := getQueue(t, queue.GetName()).GetStats()
	require.Equal(t, int32(2), stats.GetPendingCount())
	require.Equal(t, int32(1), stats.GetRunningCount())
	require.True(t, sooner.GetScheduleTime().AsTime().Equal(stats.GetOldestPendingScheduleTime().AsTime()))

	cancelJob(t, sooner.GetName())
	stats = getQueue(t, queue.GetName()).GetStats()
	require.Equal(t, int32(1), stats.GetPendingCount())
	require.True(t, later.GetScheduleTime().AsTime().Equal(stats.GetOldestPendingScheduleTime().AsTime()))

	cancelJob(t, later.GetName())
	cancelJob(t, running.GetName())
	grpcrequire.Equal(t, &schedulerpb.QueueStats{}, getQueue(t, queue.GetName()).GetStats())
}

func TestQueue_DeleteWithJobs(t *testing.T) {
	t.Parallel()
	createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: &schedulerpb.Queue{Policy: newPolicy(), Handlers: []*schedulerpb.Handler{handler("Echo")}}}
	queue, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err)
	pending := createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: "x"}, scheduler.WithScheduleTime(farFuture))

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
	t.Parallel()
	queue := createQueue(t, newPolicy(), "Echo", "Sleep")
	key := uuid.MustNewV7().String()
	running := createJobIn(t, "", queue.GetName(), &processorpb.SleepRequest{Key: key, Duration: durationpb.New(2 * time.Second)})
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
	values := make([]string, 3)
	jobs := make([]*schedulerpb.Job, 3)
	for i := range jobs {
		values[i] = fmt.Sprintf("%s-%d", key, i)
		jobs[i] = createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: values[i]})
	}
	finished := waitForTerminal(t, running.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, finished.GetState())
	time.Sleep(5 * pollInterval)
	for i, job := range jobs {
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, getJob(t, job.GetName()).GetState())
		require.Empty(t, testProcessor.calls(values[i]))
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
		require.Len(t, testProcessor.calls(values[i]), 1)
	}

	_, err = schedulerServiceClient.PauseQueue(ctx, &schedulerservicepb.PauseQueueRequest{Name: "queues/does-not-exist"})
	grpcrequire.Error(t, codes.NotFound, err)
}
