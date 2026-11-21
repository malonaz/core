package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/uuid"
)

// queueRetention is the retention the declared queues of these tests set.
const queueRetention = 90 * 24 * time.Hour

// requireRetention checks a terminal job is purged the retention after it completed.
func requireRetention(t *testing.T, job *schedulerpb.Job, retention time.Duration) {
	t.Helper()
	require.NotNil(t, job.GetCompleteTime())
	require.Equal(t, job.GetCompleteTime().AsTime().Add(retention), job.GetPurgeTime().AsTime())
}

// createQueueJob creates the job in a declared queue, whose request has no Go type.
func createQueueJob(t *testing.T, queue *schedulerpb.Queue, job *schedulerpb.Job) *schedulerpb.Job {
	t.Helper()
	job.Payload = &anypb.Any{TypeUrl: queue.GetRequestType()}
	createJobRequest := &schedulerservicepb.CreateJobRequest{Job: job}
	created, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	return created
}

// retentionQueue is a declared queue and the retention its jobs must be kept for.
type retentionQueue struct {
	queue     *schedulerpb.Queue
	retention time.Duration
}

// retentionQueues declares a dead-endpoint queue with its own retention and one without.
func retentionQueues(t *testing.T) map[string]retentionQueue {
	t.Helper()
	policy := newPolicy()
	policy.Retention = durationpb.New(queueRetention)
	return map[string]retentionQueue{
		"queue retention":   {queue: declareQueue(t, deadURL, policy), retention: queueRetention},
		"default retention": {queue: declareQueue(t, deadURL, newPolicy()), retention: defaultRetention},
	}
}

func TestRetention_DeclaredOnMethod(t *testing.T) {
	t.Parallel()
	// Flaky declares its retention; Echo declares none and keeps none, so the flag applies.
	require.Equal(t, flakyRetention, getQueue(t, flakyQueue).GetPolicy().GetRetention().AsDuration())
	require.Nil(t, getQueue(t, echoQueue).GetPolicy().GetRetention())

	t.Run("succeeded", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.FlakyRequest{Key: uuid.MustNewV7().String()})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		requireRetention(t, job, flakyRetention)
	})
	t.Run("failed", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.FlakyRequest{Key: uuid.MustNewV7().String(), Failures: 1, Code: int32(codes.InvalidArgument)})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
		requireRetention(t, job, flakyRetention)
	})
	t.Run("default", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.EchoRequest{Value: uuid.MustNewV7().String()})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		requireRetention(t, job, defaultRetention)
	})
}

func TestRetention_Cancel(t *testing.T) {
	t.Parallel()
	for name, test := range retentionQueues(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			created := createQueueJob(t, test.queue, &schedulerpb.Job{ScheduleTime: timestamppb.New(farFuture)})
			cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: created.GetName()}
			cancelled, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
			require.NoError(t, err)
			requireRetention(t, cancelled, test.retention)

			first := createQueueJob(t, test.queue, &schedulerpb.Job{ScheduleTime: timestamppb.New(farFuture)})
			second := createQueueJob(t, test.queue, &schedulerpb.Job{ScheduleTime: timestamppb.New(farFuture)})
			batchCancelJobsRequest := &schedulerservicepb.BatchCancelJobsRequest{Names: []string{first.GetName(), second.GetName()}}
			batchCancelJobsResponse, err := schedulerServiceClient.BatchCancelJobs(ctx, batchCancelJobsRequest)
			require.NoError(t, err)
			for _, job := range batchCancelJobsResponse.GetJobs() {
				requireRetention(t, job, test.retention)
			}
		})
	}
}

func TestRetention_Expiry(t *testing.T) {
	t.Parallel()
	for name, test := range retentionQueues(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Already past: never claimable, so the reaper fails it.
			created := createQueueJob(t, test.queue, &schedulerpb.Job{ExpireTime: timestamppb.New(time.Now().Add(-time.Second))})
			job := waitForTerminal(t, created.GetName())
			require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
			require.Equal(t, int32(codes.DeadlineExceeded), job.GetError().GetCode())
			requireRetention(t, job, test.retention)
		})
	}
}

func TestRetention_PolicyChangeAppliesToLaterJobs(t *testing.T) {
	t.Parallel()
	queue := declareQueue(t, deadURL, newPolicy())
	cancelNew := func() *schedulerpb.Job {
		created := createQueueJob(t, queue, &schedulerpb.Job{ScheduleTime: timestamppb.New(farFuture)})
		cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: created.GetName()}
		cancelled, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
		require.NoError(t, err)
		return cancelled
	}
	updateRetention := func(retention *durationpb.Duration) *schedulerpb.Queue {
		updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: queue.GetName(), Policy: &policypb.QueuePolicy{Retention: retention}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.retention"}},
		}
		updated, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		require.NoError(t, err)
		return updated
	}
	before := cancelNew()
	requireRetention(t, before, defaultRetention)

	updated := updateRetention(durationpb.New(queueRetention))
	require.Equal(t, queueRetention, updated.GetPolicy().GetRetention().AsDuration())
	require.Equal(t, queue.GetPolicy().GetAttemptTimeout().AsDuration(), updated.GetPolicy().GetAttemptTimeout().AsDuration(), "the rest of the policy is kept")
	requireRetention(t, cancelNew(), queueRetention)
	// A job already stamped keeps its purge time.
	require.Equal(t, before.GetPurgeTime().AsTime(), getJob(t, before.GetName()).GetPurgeTime().AsTime())

	// Clearing it returns the queue to the default, which is not filled in.
	require.Nil(t, updateRetention(nil).GetPolicy().GetRetention())
	requireRetention(t, cancelNew(), defaultRetention)
}
