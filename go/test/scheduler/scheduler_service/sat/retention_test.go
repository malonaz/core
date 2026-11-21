package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// declaredRetention is the retention a processor method declares.
type declaredRetention struct {
	duration time.Duration // 0 when none is declared.
	forever  bool
}

// requirePurgeTime checks the terminal job is purged as its queue declares,
// else the scheduler's default retention after it completed.
func requirePurgeTime(t *testing.T, job *schedulerpb.Job, declared declaredRetention) {
	t.Helper()
	require.True(t, isTerminal(job.GetState()))
	switch {
	case declared.forever:
		require.Nil(t, job.GetPurgeTime())
	case declared.duration > 0:
		require.Equal(t, job.GetCompleteTime().AsTime().Add(declared.duration), job.GetPurgeTime().AsTime())
	default:
		require.Equal(t, job.GetCompleteTime().AsTime().Add(defaultRetention), job.GetPurgeTime().AsTime())
	}
}

func TestRetention(t *testing.T) {
	t.Parallel()
	// Each queue's retention is declared on its method's annotation, or not at all.
	for name, test := range map[string]struct {
		queue    string
		declared declaredRetention
		// A request that succeeds.
		request func() proto.Message
	}{
		"duration": {
			queue: flakyQueue, declared: declaredRetention{duration: flakyRetention},
			request: func() proto.Message { return &processorpb.FlakyRequest{Key: uuid.MustNewV7().String()} },
		},
		"forever": {
			queue: progressQueue, declared: declaredRetention{forever: true},
			request: func() proto.Message { return &processorpb.ProgressRequest{} },
		},
		"default": {
			queue:   echoQueue,
			request: func() proto.Message { return &processorpb.EchoRequest{Value: uuid.MustNewV7().String()} },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Stored as declared: an undeclared retention is not filled with the default.
			policy := getQueue(t, test.queue).GetPolicy()
			require.Equal(t, test.declared.duration, policy.GetRetentionDuration().AsDuration())
			require.Equal(t, test.declared.forever, policy.GetRetainForever())

			t.Run("succeeded", func(t *testing.T) {
				t.Parallel()
				job := waitForTerminal(t, createJob(t, test.request()).GetName())
				require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
				requirePurgeTime(t, job, test.declared)
			})
			t.Run("cancelled", func(t *testing.T) {
				t.Parallel()
				created := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: created.GetName()}
				cancelled, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
				require.NoError(t, err)
				requirePurgeTime(t, cancelled, test.declared)
			})
			t.Run("batch cancelled", func(t *testing.T) {
				t.Parallel()
				first := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				second := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				batchCancelJobsRequest := &schedulerservicepb.BatchCancelJobsRequest{Names: []string{first.GetName(), second.GetName()}}
				batchCancelJobsResponse, err := schedulerServiceClient.BatchCancelJobs(ctx, batchCancelJobsRequest)
				require.NoError(t, err)
				for _, job := range batchCancelJobsResponse.GetJobs() {
					requirePurgeTime(t, job, test.declared)
				}
			})
			t.Run("expired", func(t *testing.T) {
				t.Parallel()
				// Already past: never claimable, so the reaper fails it.
				created := createJob(t, test.request(), scheduler.WithExpireTime(time.Now().Add(-time.Second)))
				job := waitForTerminal(t, created.GetName())
				require.Equal(t, int32(codes.DeadlineExceeded), job.GetError().GetCode())
				requirePurgeTime(t, job, test.declared)
			})
		})
	}

	t.Run("failed", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.FlakyRequest{Key: uuid.MustNewV7().String(), Failures: 1, Code: int32(codes.InvalidArgument)})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
		requirePurgeTime(t, job, declaredRetention{duration: flakyRetention})
	})
}
