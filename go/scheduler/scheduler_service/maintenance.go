package scheduler_service

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/scheduler/transition"
)

// reapBatchSize bounds the jobs one reaper pass transitions.
const reapBatchSize = 100

// reap recovers jobs no worker will ever complete: those whose lease lapsed,
// and those that expired before starting. It also refreshes the backlog gauges.
func (s *Service) reap(ctx context.Context) error {
	if err := s.reapLapsedLeases(ctx); err != nil {
		return err
	}
	if err := s.reapExpiredJobs(ctx); err != nil {
		return err
	}
	return s.observeBacklog(ctx)
}

// reapLapsedLeases returns RUNNING jobs whose lease lapsed to PENDING: their
// worker died without recording an outcome.
func (s *Service) reapLapsedLeases(ctx context.Context) error {
	now := transition.Now()
	reaped, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND lock_time < $2",
		"ORDER BY lock_time",
		reapBatchSize,
		[]any{int16(schedulerpb.JobState_JOB_STATE_RUNNING), now},
		func(job *model.Job) error {
			return transition.Mutate(job, now, func(job *schedulerpb.Job) error {
				transition.RecordAttempt(job, now, grpcstatus.Error(codes.Unavailable, "lease lapsed"), nil)
				job.State = schedulerpb.JobState_JOB_STATE_PENDING
				job.LockTime = nil
				return nil
			})
		},
	)
	if err != nil {
		return err
	}
	for _, job := range reaped {
		s.log.WarnContext(ctx, "reaped job with lapsed lease", "job", job.JobID, "queue", job.Queue, "method", job.Method, "attempt", job.AttemptCount)
		reapedCounter.WithLabelValues(job.Queue, job.Method).Inc()
	}
	return nil
}

// reapExpiredJobs fails PENDING jobs that were not started by their expiry.
func (s *Service) reapExpiredJobs(ctx context.Context) error {
	now := transition.Now()
	expired, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND expire_time <= $2",
		"ORDER BY expire_time",
		reapBatchSize,
		[]any{int16(schedulerpb.JobState_JOB_STATE_PENDING), now},
		func(job *model.Job) error {
			return transition.Mutate(job, now, func(job *schedulerpb.Job) error {
				job.State = schedulerpb.JobState_JOB_STATE_FAILED
				job.CompleteTime = timestamppb.New(now)
				job.PurgeTime = transition.PurgeTime(now, s.opts.Retention)
				job.Error = grpcstatus.New(codes.DeadlineExceeded, "expired before starting").Proto()
				transition.Observe(job, job.State)
				return nil
			})
		},
	)
	if err != nil {
		return err
	}
	for _, job := range expired {
		s.log.InfoContext(ctx, "failed job that expired before starting", "job", job.JobID, "queue", job.Queue, "method", job.Method)
	}
	return nil
}

// observeBacklog refreshes the per-queue backlog gauges.
func (s *Service) observeBacklog(ctx context.Context) error {
	stats, err := s.schedulerPostgresStore.ListQueueStats(ctx, nil)
	if err != nil {
		return err
	}
	observeQueueStats(stats, transition.Now())
	return nil
}

// sweep deletes terminal jobs past their retention.
func (s *Service) sweep(ctx context.Context) error {
	deleted, err := s.schedulerPostgresStore.PurgeJobs(ctx, transition.Now())
	if err != nil {
		return err
	}
	if deleted > 0 {
		s.log.InfoContext(ctx, "swept expired jobs", "count", deleted)
	}
	return nil
}
