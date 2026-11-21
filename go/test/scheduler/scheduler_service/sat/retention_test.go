package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// requireRetention checks a terminal job is purged the retention after it
// completed, or never for a negative one.
func requireRetention(t *testing.T, job *schedulerpb.Job, retention time.Duration) {
	t.Helper()
	require.True(t, isTerminal(job.GetState()))
	if retention < 0 {
		require.Nil(t, job.GetPurgeTime())
		return
	}
	require.Equal(t, job.GetCompleteTime().AsTime().Add(retention), job.GetPurgeTime().AsTime())
}

func TestRetention(t *testing.T) {
	t.Parallel()
	// Each queue's retention is declared on its method's annotation, or not at all.
	for name, test := range map[string]struct {
		queue     string
		declared  *durationpb.Duration
		retention time.Duration
		// A request that succeeds.
		request func() proto.Message
	}{
		"declared": {
			queue: flakyQueue, declared: durationpb.New(flakyRetention), retention: flakyRetention,
			request: func() proto.Message { return &processorpb.FlakyRequest{Key: uuid.MustNewV7().String()} },
		},
		"declared forever": {
			queue: progressQueue, declared: durationpb.New(progressRetention), retention: progressRetention,
			request: func() proto.Message { return &processorpb.ProgressRequest{} },
		},
		"default": {
			queue: echoQueue, retention: defaultRetention,
			request: func() proto.Message { return &processorpb.EchoRequest{Value: uuid.MustNewV7().String()} },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Stored as declared: an undeclared retention is not filled with the default.
			require.True(t, proto.Equal(test.declared, getQueue(t, test.queue).GetPolicy().GetRetention()))

			t.Run("succeeded", func(t *testing.T) {
				t.Parallel()
				job := waitForTerminal(t, createJob(t, test.request()).GetName())
				require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
				requireRetention(t, job, test.retention)
			})
			t.Run("cancelled", func(t *testing.T) {
				t.Parallel()
				created := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: created.GetName()}
				cancelled, err := schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
				require.NoError(t, err)
				requireRetention(t, cancelled, test.retention)
			})
			t.Run("batch cancelled", func(t *testing.T) {
				t.Parallel()
				first := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				second := createJob(t, test.request(), scheduler.WithScheduleTime(farFuture))
				batchCancelJobsRequest := &schedulerservicepb.BatchCancelJobsRequest{Names: []string{first.GetName(), second.GetName()}}
				batchCancelJobsResponse, err := schedulerServiceClient.BatchCancelJobs(ctx, batchCancelJobsRequest)
				require.NoError(t, err)
				for _, job := range batchCancelJobsResponse.GetJobs() {
					requireRetention(t, job, test.retention)
				}
			})
			t.Run("expired", func(t *testing.T) {
				t.Parallel()
				// Already past: never claimable, so the reaper fails it.
				created := createJob(t, test.request(), scheduler.WithExpireTime(time.Now().Add(-time.Second)))
				job := waitForTerminal(t, created.GetName())
				require.Equal(t, int32(codes.DeadlineExceeded), job.GetError().GetCode())
				requireRetention(t, job, test.retention)
			})
		})
	}

	t.Run("failed", func(t *testing.T) {
		t.Parallel()
		created := createJob(t, &processorpb.FlakyRequest{Key: uuid.MustNewV7().String(), Failures: 1, Code: int32(codes.InvalidArgument)})
		job := waitForTerminal(t, created.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_FAILED, job.GetState())
		requireRetention(t, job, flakyRetention)
	})
}
