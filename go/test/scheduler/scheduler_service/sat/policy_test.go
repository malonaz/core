package sat

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// countRunning returns how many of the labelled jobs are RUNNING right now.
func countRunning(t *testing.T, run string) int {
	t.Helper()
	listJobsRequest := &schedulerservicepb.ListJobsRequest{Filter: fmt.Sprintf(`labels.run = "%s" AND state = JOB_STATE_RUNNING`, run)}
	jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, schedulerServiceClient.ListJobs)
	require.NoError(t, err)
	return len(jobs)
}

func TestProcess_MaxConcurrency(t *testing.T) {
	t.Parallel()
	// Six 4s sleeps on a queue capped at 2: across both replicas, never more
	// than 2 run at once, and every one completes.
	run := uuid.MustNewV7().String()
	labels := scheduler.WithLabels(map[string]string{"run": run})
	const count = 6
	names := make([]string, count)
	for i := range names {
		names[i] = createJob(t, &processorpb.LimitedRequest{Key: fmt.Sprintf("%s-%d", run, i), Duration: durationpb.New(4 * time.Second)}, labels).GetName()
	}

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		require.LessOrEqual(t, countRunning(t, run), limitedConcurrency)
		stats := getQueue(t, limitedQueue).GetStats()
		if stats.GetPendingCount()+stats.GetRunningCount() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, name := range names {
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, waitForTerminal(t, name).GetState())
	}
}

func TestProcess_UnlimitedConcurrency(t *testing.T) {
	t.Parallel()
	// The same sleeps on an uncapped queue run side by side.
	run := uuid.MustNewV7().String()
	labels := scheduler.WithLabels(map[string]string{"run": run})
	const count = 6
	names := make([]string, count)
	for i := range names {
		names[i] = createJob(t, &processorpb.SleepRequest{Key: fmt.Sprintf("%s-%d", run, i), Duration: durationpb.New(2 * time.Second)}, labels).GetName()
	}
	require.Eventually(t, func() bool { return countRunning(t, run) > limitedConcurrency }, waitTimeout, 20*time.Millisecond)
	for _, name := range names {
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, waitForTerminal(t, name).GetState())
	}
}

func TestProcess_NonRetryableCodeFailsAtOnce(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: 1, Code: int32(codes.InvalidArgument)})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
	require.Equal(t, int32(1), job.GetAttemptCount(), "attempts left are not spent on a non-retryable error")
	require.Equal(t, int32(codes.InvalidArgument), job.GetError().GetCode())
	require.Len(t, job.GetMetadata().GetAttempts(), 1)
	time.Sleep(4 * flakyBackoffInitial)
	require.Len(t, testProcessor.calls(key), 1)
}

func TestProcess_RetryInfoOverridesBackoff(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	retryDelay := 1500 * time.Millisecond
	created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: 1, Code: int32(codes.Unavailable), RetryDelay: durationpb.New(retryDelay)})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(2), job.GetAttemptCount())
	calls := testProcessor.calls(key)
	require.Len(t, calls, 2)
	// The handler's delay applies instead of the queue's 300ms backoff.
	require.GreaterOrEqual(t, calls[1].time.Sub(calls[0].time), retryDelay)
	attempts := job.GetMetadata().GetAttempts()
	require.Len(t, attempts, 2)
	require.Equal(t, retryDelay, attempts[0].GetRetryDelay().AsDuration())
	require.Nil(t, attempts[1].GetRetryDelay())
}

func TestUpdateQueue_PolicyAppliesToNextJob(t *testing.T) {
	t.Parallel()
	// The tunable queue is this test's alone: its policy is declared with one attempt.
	queue := getQueue(t, tunableQueue)

	key := uuid.MustNewV7().String()
	first := createJob(t, &processorpb.TunableRequest{Key: key, Failures: 1, Code: int32(codes.Unavailable)})
	job := waitForTerminal(t, first.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
	require.Equal(t, int32(1), job.GetAttemptCount())

	// Raising max_attempts takes effect without a restart.
	updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: queue.GetName(), Policy: &policypb.QueuePolicy{MaxAttempts: 2}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
	}
	updated, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.Equal(t, int32(2), updated.GetPolicy().GetMaxAttempts())
	require.Equal(t, 200*time.Millisecond, updated.GetPolicy().GetRetryBackoff().GetInitial().AsDuration(), "the rest of the policy is kept")

	key = uuid.MustNewV7().String()
	second := createJob(t, &processorpb.TunableRequest{Key: key, Failures: 1, Code: int32(codes.Unavailable)})
	job = waitForTerminal(t, second.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(2), job.GetAttemptCount())
}
