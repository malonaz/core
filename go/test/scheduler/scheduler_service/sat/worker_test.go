package sat

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

func TestProcess_Succeeds(t *testing.T) {
	t.Parallel()
	value := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.EchoRequest{Value: value})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(1), job.GetAttemptCount())
	require.Nil(t, job.GetError())
	require.Nil(t, job.GetLockTime())
	require.NotNil(t, job.GetStartTime())
	require.NotNil(t, job.GetCompleteTime())
	require.False(t, job.GetCompleteTime().AsTime().Before(job.GetStartTime().AsTime()))
	require.NotNil(t, job.GetExpireTime(), "retention stamps terminal jobs")
	require.NotEqual(t, created.GetEtag(), job.GetEtag())

	// The response is stored under the configured type.
	require.Equal(t, jobType(&processorpb.EchoResponse{}), job.GetResponse().GetTypeUrl())
	require.Equal(t, value, unpackAny[*processorpb.EchoResponse](t, job.GetResponse()).GetValue())

	// The processor saw exactly one call, carrying the job name and the configured headers.
	calls := testProcessor.calls(value)
	require.Len(t, calls, 1)
	require.Equal(t, job.GetName(), calls[0].job)
	require.Equal(t, []string{"hello"}, calls[0].headers.Get("x-test-header"))
}

func TestProcess_ScheduleTime(t *testing.T) {
	t.Parallel()

	t.Run("future job is not claimed early", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		delay := 2 * time.Second
		scheduleTime := time.Now().Add(delay).Truncate(time.Microsecond)
		created := createJob(t, &processorpb.EchoRequest{Value: value}, scheduler.WithScheduleTime(scheduleTime))

		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.False(t, job.GetStartTime().AsTime().Before(scheduleTime), "started %v before schedule %v", job.GetStartTime().AsTime(), scheduleTime)
		require.True(t, scheduleTime.Equal(job.GetScheduleTime().AsTime()), "schedule_time is preserved")
	})

	t.Run("due jobs run in schedule order", func(t *testing.T) {
		t.Parallel()
		// Both are created while a short backlog blocks claiming, so the order they are claimed in is the schedule order.
		run := uuid.MustNewV7().String()
		base := time.Now().Add(1500 * time.Millisecond)
		late := createJob(t, &processorpb.EchoRequest{Value: run + "-late"}, scheduler.WithScheduleTime(base.Add(200*time.Millisecond)))
		early := createJob(t, &processorpb.EchoRequest{Value: run + "-early"}, scheduler.WithScheduleTime(base))

		waitForTerminal(t, late.GetName())
		waitForTerminal(t, early.GetName())
		require.True(t, testProcessor.calls(run + "-early")[0].time.Before(testProcessor.calls(run + "-late")[0].time))
	})
}

func TestProcess_RetriesWithBackoff(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: 2, Code: int32(codes.Unavailable)})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(3), job.GetAttemptCount())
	require.Nil(t, job.GetError(), "success clears the last attempt's error")
	require.Equal(t, int32(3), unpackAny[*processorpb.FlakyResponse](t, job.GetResponse()).GetCalls())

	// Waits between attempts follow the configured schedule: 300ms, then 600ms.
	calls := testProcessor.calls(key)
	require.Len(t, calls, 3)
	firstGap := calls[1].time.Sub(calls[0].time)
	secondGap := calls[2].time.Sub(calls[1].time)
	require.GreaterOrEqual(t, firstGap, flakyBackoffInitial)
	require.GreaterOrEqual(t, secondGap, 2*flakyBackoffInitial)
	// Neither wait overshoots by more than the poll interval plus slack for a busy suite.
	require.Less(t, firstGap, flakyBackoffInitial+pollInterval+time.Second)
	require.Less(t, secondGap, 2*flakyBackoffInitial+pollInterval+time.Second)
}

func TestProcess_RetryKeepsLastErrorWhilePending(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: 1, Code: int32(codes.Internal)})

	// Between the first failure and the retry the job is PENDING with the error and next schedule visible.
	job := waitForJob(t, created.GetName(), func(job *schedulerpb.Job) bool {
		return job.GetAttemptCount() == 1 && job.GetState() == schedulerpb.JobState_JOB_STATE_PENDING
	})
	require.Equal(t, int32(codes.Internal), job.GetError().GetCode())
	require.Contains(t, job.GetError().GetMessage(), "flaky failure 1/1")
	require.NotNil(t, job.GetScheduleTime())
	require.Nil(t, job.GetLockTime())

	job = waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
}

func TestProcess_ExhaustsAttempts(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: 100, Code: int32(codes.Internal)})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
	require.Equal(t, int32(flakyMaxAttempts), job.GetAttemptCount())
	require.Equal(t, int32(codes.Internal), job.GetError().GetCode())
	require.Contains(t, job.GetError().GetMessage(), fmt.Sprintf("flaky failure %d/100", flakyMaxAttempts))
	require.Nil(t, job.GetResponse())
	require.NotNil(t, job.GetCompleteTime())
	require.Len(t, testProcessor.calls(key), flakyMaxAttempts)

	// No further attempt is made.
	time.Sleep(4 * flakyBackoffInitial)
	require.Len(t, testProcessor.calls(key), flakyMaxAttempts)
}

func TestProcess_Timeout(t *testing.T) {
	t.Parallel()
	key := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.DeadlineRequest{Key: key, Duration: durationpb.New(deadlineTimeout + time.Second)})

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
	require.Equal(t, int32(deadlineMaxAttempts), job.GetAttemptCount())
	require.Equal(t, int32(codes.DeadlineExceeded), job.GetError().GetCode())

	// Every attempt was cut at the deadline, and retried.
	calls := testProcessor.calls(key)
	require.Len(t, calls, deadlineMaxAttempts)
	for _, call := range calls {
		require.True(t, call.cancelled)
	}
	require.GreaterOrEqual(t, calls[1].time.Sub(calls[0].time), deadlineTimeout)
}

func TestProcess_LongJobUnderTimeoutIsNotKilled(t *testing.T) {
	t.Parallel()
	// Runs for several lease durations: only lease renewal keeps it alive.
	key := uuid.MustNewV7().String()
	duration := 3 * leaseDuration
	created := createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(duration)})

	running := waitForState(t, created.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
	require.NotNil(t, running.GetLockTime())
	// The lease is renewed while the call is in flight.
	renewed := waitForJob(t, created.GetName(), func(job *schedulerpb.Job) bool {
		return job.GetState() == schedulerpb.JobState_JOB_STATE_RUNNING && job.GetLockTime().AsTime().After(running.GetLockTime().AsTime())
	})
	require.NotEqual(t, running.GetEtag(), renewed.GetEtag())

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(1), job.GetAttemptCount())
	require.GreaterOrEqual(t, job.GetCompleteTime().AsTime().Sub(job.GetStartTime().AsTime()), duration)
	calls := testProcessor.calls(key)
	require.Len(t, calls, 1)
	require.False(t, calls[0].cancelled)
}

func TestProcess_LapsedLeaseIsReaped(t *testing.T) {
	t.Parallel()
	// A job stuck RUNNING with an expired lease is what a crashed worker leaves behind.
	value := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.EchoRequest{Value: value}, scheduler.WithScheduleTime(farFuture))

	postgresClient, err := satEnvironment.GetPostgresClient(ctx, "scheduler")
	require.NoError(t, err)
	_, err = postgresClient.Exec(ctx,
		"UPDATE job SET state = $2, schedule_time = NULL, start_time = $3, lock_time = $3, attempt_count = 1 WHERE job_id = $1",
		created.GetName()[len("jobs/"):], int16(schedulerpb.JobState_JOB_STATE_RUNNING), time.Now().UTC().Add(-time.Hour))
	require.NoError(t, err)

	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, int32(2), job.GetAttemptCount(), "the reaped attempt counts")
	require.Len(t, testProcessor.calls(value), 1)
}

func TestCancelJob(t *testing.T) {
	t.Parallel()

	t.Run("pending", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.EchoRequest{Value: value}, scheduler.WithScheduleTime(farFuture))
		cancelled, err := schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_CANCELLED, cancelled.GetState())
		require.Equal(t, int32(codes.Canceled), cancelled.GetError().GetCode())
		require.NotNil(t, cancelled.GetCompleteTime())
		require.NotNil(t, cancelled.GetExpireTime())
		grpcrequire.Equal(t, cancelled, getJob(t, created.GetName()))

		// Terminal: cancelling again is refused.
		_, err = schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: created.GetName()})
		grpcrequire.Error(t, codes.FailedPrecondition, err)
		require.Empty(t, testProcessor.calls(value))
	})

	t.Run("running", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleepTimeout)})
		waitForState(t, created.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)

		cancelled, err := schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_CANCELLED, cancelled.GetState())

		// The in-flight call is cut short, and the outcome sticks: no retry, no overwrite.
		require.Eventually(t, func() bool {
			calls := testProcessor.calls(key)
			return len(calls) == 1 && calls[0].cancelled
		}, waitTimeout, 20*time.Millisecond)
		time.Sleep(2 * leaseDuration)
		job := getJob(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_CANCELLED, job.GetState())
		require.Equal(t, int32(1), job.GetAttemptCount())
		require.Len(t, testProcessor.calls(key), 1)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		_, err := schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: "jobs/does-not-exist"})
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestRetryJob(t *testing.T) {
	t.Parallel()

	t.Run("failed job runs again", func(t *testing.T) {
		t.Parallel()
		// Fails through the first run (3 attempts) and the first two attempts of the retry.
		key := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.FlakyRequest{Key: key, Failures: flakyMaxAttempts + 2, Code: int32(codes.Internal)})
		failed := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, failed.GetState())

		retried, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, retried.GetState())
		require.Zero(t, retried.GetAttemptCount())
		require.Nil(t, retried.GetError())
		require.Nil(t, retried.GetCompleteTime())
		require.Nil(t, retried.GetExpireTime())
		require.Nil(t, retried.GetStartTime())
		require.Equal(t, schedulerpb.Labels.Retried.True, retried.GetLabels()[schedulerpb.Labels.Retried.GetKey()])

		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.Equal(t, int32(flakyMaxAttempts), job.GetAttemptCount())
		require.Len(t, testProcessor.calls(key), 2*flakyMaxAttempts)
	})

	t.Run("cancelled job runs again", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.EchoRequest{Value: value}, scheduler.WithScheduleTime(farFuture))
		_, err := schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: created.GetName()})
		require.NoError(t, err)
		retried, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Nil(t, retried.GetScheduleTime(), "retry runs immediately")
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	})

	t.Run("non-terminal job is refused", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.EchoRequest{Value: "retry-pending"}, scheduler.WithScheduleTime(farFuture))
		_, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: created.GetName()})
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		key := uuid.MustNewV7().String()
		running := createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleepTimeout)})
		waitForState(t, running.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
		_, err = schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: running.GetName()})
		grpcrequire.Error(t, codes.FailedPrecondition, err)
		_, err = schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: running.GetName()})
		require.NoError(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		_, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: "jobs/does-not-exist"})
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestReportJobProgress(t *testing.T) {
	t.Parallel()

	t.Run("running job", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.ProgressRequest{Steps: 3})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.Equal(t, int32(3), unpackAny[*processorpb.Step](t, job.GetProgress()).GetIndex())
	})

	t.Run("job that is not running", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.EchoRequest{Value: "progress"}, scheduler.WithScheduleTime(farFuture))
		reportJobProgressRequest := &schedulerservicepb.ReportJobProgressRequest{Name: created.GetName(), Progress: mustAny(t, &processorpb.Step{Index: 1})}
		_, err := schedulerServiceClient.ReportJobProgress(ctx, reportJobProgressRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		reportJobProgressRequest.Name = "jobs/does-not-exist"
		_, err = schedulerServiceClient.ReportJobProgress(ctx, reportJobProgressRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestProcess_Concurrency(t *testing.T) {
	t.Parallel()
	const count = 200
	run := uuid.MustNewV7().String()

	var wg sync.WaitGroup
	names := make([]string, count)
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			names[i] = createJob(t, &processorpb.EchoRequest{Value: fmt.Sprintf("%s-%d", run, i)}).GetName()
		}()
	}
	wg.Wait()

	for i, name := range names {
		job := waitForTerminal(t, name)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState(), "job %s: %v", name, job.GetError())
		require.Equal(t, int32(1), job.GetAttemptCount())
		require.Len(t, testProcessor.calls(fmt.Sprintf("%s-%d", run, i)), 1, "processed exactly once")
	}
}

func TestProcess_IgnoredJobTypeStaysPending(t *testing.T) {
	t.Parallel()
	created := createJob(t, &processorpb.IgnoredRequest{})
	time.Sleep(10 * pollInterval)
	job := getJob(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, job.GetState())
	require.Zero(t, job.GetAttemptCount())
}

func TestProcess_UnknownJobTypeFails(t *testing.T) {
	t.Parallel()
	created := createJob(t, wrapperspb.String("no processor for me"))
	job := waitForTerminal(t, created.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
	require.Equal(t, int32(1), job.GetAttemptCount())
	require.Equal(t, int32(codes.FailedPrecondition), job.GetError().GetCode())
	require.Contains(t, job.GetError().GetMessage(), "no configuration for job type")
}

func TestRetention_SweepsExpiredJobs(t *testing.T) {
	t.Parallel()
	value := uuid.MustNewV7().String()
	created := createJob(t, &processorpb.EchoRequest{Value: value})
	waitForState(t, created.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)

	postgresClient, err := satEnvironment.GetPostgresClient(ctx, "scheduler")
	require.NoError(t, err)
	_, err = postgresClient.Exec(ctx, "UPDATE job SET expire_time = $2 WHERE job_id = $1",
		created.GetName()[len("jobs/"):], time.Now().UTC().Add(-time.Minute))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := schedulerServiceClient.GetJob(ctx, &schedulerservicepb.GetJobRequest{Name: created.GetName()})
		return err != nil && codes.NotFound == grpcCode(err)
	}, waitTimeout, 50*time.Millisecond)
}
