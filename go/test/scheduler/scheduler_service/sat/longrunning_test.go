package sat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// waitJob calls WaitJob with the given timeout, under a client deadline wide
// enough for the server cap to be what ends the wait.
func waitJob(t *testing.T, name string, timeout time.Duration) *schedulerpb.Job {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	waitJobRequest := &schedulerservicepb.WaitJobRequest{Name: name, Timeout: durationpb.New(timeout)}
	job, err := schedulerServiceClient.WaitJob(callCtx, waitJobRequest)
	require.NoError(t, err)
	return job
}

func TestWaitJob(t *testing.T) {
	t.Parallel()

	t.Run("returns on completion", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.SleepRequest{Key: uuid.MustNewV7().String(), Duration: durationpb.New(500 * time.Millisecond)})
		job := waitJob(t, created.GetName(), 10*time.Second)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	})

	t.Run("returns the live job on timeout", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.SleepRequest{Key: uuid.MustNewV7().String(), Duration: durationpb.New(sleepTimeout)})
		t.Cleanup(func() { cancelJob(t, created.GetName()) })
		start := time.Now()
		job := waitJob(t, created.GetName(), 300*time.Millisecond)
		require.Less(t, time.Since(start), waitJobMaxTimeout)
		require.False(t, isTerminal(job.GetState()), "job is %s", job.GetState())
	})

	t.Run("caps the timeout", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.SleepRequest{Key: uuid.MustNewV7().String(), Duration: durationpb.New(sleepTimeout)})
		t.Cleanup(func() { cancelJob(t, created.GetName()) })
		start := time.Now()
		job := waitJob(t, created.GetName(), time.Hour)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, waitJobMaxTimeout)
		require.Less(t, elapsed, 2*waitJobMaxTimeout)
		require.False(t, isTerminal(job.GetState()), "job is %s", job.GetState())
	})

	t.Run("unset timeout means the cap", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.SleepRequest{Key: uuid.MustNewV7().String(), Duration: durationpb.New(sleepTimeout)})
		t.Cleanup(func() { cancelJob(t, created.GetName()) })
		start := time.Now()
		waitJobRequest := &schedulerservicepb.WaitJobRequest{Name: created.GetName()}
		_, err := schedulerServiceClient.WaitJob(ctx, waitJobRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, time.Since(start), waitJobMaxTimeout)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		waitJobRequest := &schedulerservicepb.WaitJobRequest{Name: "jobs/does-not-exist"}
		_, err := schedulerServiceClient.WaitJob(ctx, waitJobRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestWorker_UnwrapsOperations(t *testing.T) {
	t.Parallel()

	t.Run("done with response", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.OperateRequest{Value: value})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.Equal(t, value, unpackAny[*processorpb.EchoResponse](t, job.GetResponse()).GetValue())
	})

	t.Run("done with error", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.OperateRequest{Value: value, Code: int32(codes.Internal)})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
		require.Equal(t, int32(codes.Internal), job.GetError().GetCode())
		// The operation's error is retried like a returned error.
		require.Equal(t, int32(operateMaxAttempts), job.GetAttemptCount())
		require.Len(t, testProcessor.calls(value), operateMaxAttempts)
	})

	t.Run("unfinished fails without retry", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		created := createJob(t, &processorpb.OperateRequest{Value: value, Unfinished: true})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
		require.Equal(t, int32(codes.FailedPrecondition), job.GetError().GetCode())
		require.Contains(t, job.GetError().GetMessage(), "unfinished operation")
		require.Equal(t, int32(1), job.GetAttemptCount(), "FAILED_PRECONDITION is retryable on this queue, but not for an unfinished operation")
		require.Len(t, testProcessor.calls(value), 1)
	})
}
