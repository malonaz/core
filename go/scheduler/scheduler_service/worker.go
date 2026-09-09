package scheduler_service

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/scheduler"
)

// Writes to a terminal state must not depend on the (possibly cancelled) job context.
const completionTimeout = 10 * time.Second

// maxRecordedAttempts bounds the attempt history kept in a job's metadata.
const maxRecordedAttempts = 20

// truncatedNow returns the current time at Postgres' timestamp precision, so a
// transition's in-memory row matches what a later read returns.
func truncatedNow() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// workerColumns are the columns a worker writes; leaving `labels` out keeps a
// client's concurrent UpdateJob from being overwritten.
var workerColumns = []string{
	"state", "schedule_time", "start_time", "complete_time", "lock_time",
	"attempt_count", "error", "response", "purge_time", "metadata", "update_time", "etag",
}

// mutate applies fn to the job's proto form, restamps update_time and etag, and
// writes the result back into the model.
func mutate(job *model.Job, now time.Time, fn func(*schedulerpb.Job) error) error {
	jobPb, err := job.ToPb()
	if err != nil {
		return err
	}
	if err := fn(jobPb); err != nil {
		return err
	}
	jobPb.UpdateTime = timestamppb.New(now)
	if jobPb.Etag, err = aip.ComputeETag(jobPb); err != nil {
		return err
	}
	mutated, err := model.JobFromPb(jobPb)
	if err != nil {
		return err
	}
	*job = *mutated
	return nil
}

// recordAttempt closes the job's current attempt in its history with the given
// outcome (nil on success) and releases the worker.
func recordAttempt(job *schedulerpb.Job, now time.Time, err error) {
	metadata := job.GetMetadata()
	if metadata == nil {
		metadata = &schedulerpb.JobMetadata{}
	}
	attempt := &schedulerpb.JobAttempt{
		Attempt:   job.GetAttemptCount(),
		StartTime: job.GetStartTime(),
		EndTime:   timestamppb.New(now),
		Worker:    metadata.GetWorker(),
	}
	if err != nil {
		// Details may carry types this binary cannot resolve, which the JSON column cannot hold.
		attemptStatus := grpcstatus.Convert(err)
		attempt.Error = &statuspb.Status{Code: int32(attemptStatus.Code()), Message: attemptStatus.Message()}
	}
	metadata.Attempts = append(metadata.Attempts, attempt)
	if len(metadata.Attempts) > maxRecordedAttempts {
		metadata.Attempts = metadata.Attempts[len(metadata.Attempts)-maxRecordedAttempts:]
	}
	metadata.Worker = ""
	job.Metadata = metadata
}

// claim moves due PENDING jobs to RUNNING, one per free slot, and hands each
// to a worker. A full batch signals another pass rather than waiting for the ticker.
func (s *Service) claim(ctx, workerCtx context.Context) error {
	free := cap(s.slots) - len(s.slots)
	if free == 0 {
		return nil
	}
	now := truncatedNow()
	lockTime := now.Add(s.opts.LeaseDuration)
	jobs, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND (schedule_time IS NULL OR schedule_time <= $2) AND (expire_time IS NULL OR expire_time > $2) AND NOT (job_type = ANY($3))",
		// Highest priority first; among equals, a job is due at its schedule time, or at creation when it has none.
		"ORDER BY priority DESC, COALESCE(schedule_time, create_time), create_time",
		free,
		[]any{int16(schedulerpb.JobState_JOB_STATE_PENDING), now, s.ignoredJobTypes},
		func(job *model.Job) error {
			return mutate(job, now, func(job *schedulerpb.Job) error {
				job.State = schedulerpb.JobState_JOB_STATE_RUNNING
				job.StartTime = timestamppb.New(now)
				job.LockTime = timestamppb.New(lockTime)
				job.AttemptCount++
				if job.Metadata == nil {
					job.Metadata = &schedulerpb.JobMetadata{}
				}
				job.Metadata.Worker = s.opts.WorkerID
				return nil
			})
		},
	)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		s.slots <- struct{}{}
		s.workers.Add(1)
		go s.process(workerCtx, job)
	}
	if len(jobs) == free {
		s.wakeClaim()
	}
	return nil
}

func (s *Service) wakeClaim() {
	select {
	case s.claimSignal <- struct{}{}:
	default:
	}
}

// process runs one claimed job: invokes the processor under the job type's
// timeout while renewing the lease, then records the outcome.
func (s *Service) process(ctx context.Context, job *model.Job) {
	defer s.workers.Done()
	defer func() { <-s.slots }()
	inflightGauge.Inc()
	defer inflightGauge.Dec()
	log := s.log.With("job", job.JobID, "job_type", job.JobType, "attempt", job.AttemptCount)

	jobTypeConfiguration, ok := s.jobTypeToConfiguration[job.JobType]
	if !ok {
		err := grpcstatus.Errorf(codes.FailedPrecondition, "no configuration for job type %q", job.JobType)
		s.complete(ctx, log, job, &pb.JobTypeConfiguration{MaxAttempts: 1}, nil, err)
		return
	}

	jobCtx, cancel := context.WithTimeout(ctx, jobTypeConfiguration.GetTimeout().AsDuration())
	defer cancel()
	s.inflight.add(job.JobID, cancel)
	defer s.inflight.remove(job.JobID)

	stopHeartbeat := s.heartbeat(jobCtx, cancel, log, job)
	start := time.Now()
	response, err := s.invoke(jobCtx, jobTypeConfiguration, job)
	stopHeartbeat()
	observeAttempt(job.JobType, time.Since(start), err)

	s.complete(ctx, log, job, jobTypeConfiguration, response, err)
}

// heartbeat renews the lease every third of its duration until stopped. Losing
// the RUNNING state (cancelled, or reaped) cancels the in-flight call.
func (s *Service) heartbeat(ctx context.Context, cancel context.CancelFunc, log *slog.Logger, job *model.Job) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(s.opts.LeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			// Work on a copy: the worker still reads its own model.
			now := truncatedNow()
			lease := *job
			if err := mutate(&lease, now, func(job *schedulerpb.Job) error {
				job.LockTime = timestamppb.New(now.Add(s.opts.LeaseDuration))
				return nil
			}); err != nil {
				log.ErrorContext(ctx, "computing lease", "error", err)
				continue
			}
			_, err := s.schedulerPostgresStore.UpdateRunningJob(ctx, &lease, "lock_time", "update_time", "etag")
			if errors.Is(err, store.ErrJobNotRunning) {
				log.InfoContext(ctx, "job no longer running, cancelling processor call")
				cancel()
				return
			}
			if err != nil {
				log.ErrorContext(ctx, "renewing lease", "error", err)
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

// invoke delivers the payload to the processor as the request body of the
// configured method, returning the raw response body.
func (s *Service) invoke(ctx context.Context, jobTypeConfiguration *pb.JobTypeConfiguration, job *model.Job) ([]byte, error) {
	processor := s.processorIDToProcessor[jobTypeConfiguration.GetProcessorId()]
	connection := s.processorIDToGRPCConnection[jobTypeConfiguration.GetProcessorId()]

	payload := &anypb.Any{}
	if err := pbutil.Unmarshal(job.Payload, payload); err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "unmarshaling payload: %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, scheduler.JobMetadataKey, jobName(job))
	for key, value := range processor.GetHeaders() {
		ctx = metadata.AppendToOutgoingContext(ctx, key, value)
	}
	// Attempts are the scheduler's to account for: no transparent retries.
	var response []byte
	if err := connection.Get().Invoke(ctx, jobTypeConfiguration.GetMethod(), payload.GetValue(), &response, grpc.WithRawCodec(), grpc_retry.Disable()); err != nil {
		return nil, err
	}
	return response, nil
}

// jobName returns the job's resource name under whichever parent it has.
func jobName(job *model.Job) string {
	switch {
	case job.UserID != nil:
		return (&schedulerpb.OrganizationsUsersJobResourceName{Organization: *job.OrganizationID, User: *job.UserID, Job: job.JobID}).String()
	case job.OrganizationID != nil:
		return (&schedulerpb.OrganizationsJobResourceName{Organization: *job.OrganizationID, Job: job.JobID}).String()
	}
	return (&schedulerpb.JobResourceName{Job: job.JobID}).String()
}

// complete records an attempt's outcome: SUCCEEDED, PENDING again after the
// backoff, FAILED once attempts are exhausted, or released untouched when the
// instance is shutting down. A job that left RUNNING meanwhile is left alone.
func (s *Service) complete(ctx context.Context, log *slog.Logger, job *model.Job, jobTypeConfiguration *pb.JobTypeConfiguration, response []byte, err error) {
	now := truncatedNow()
	transition := func(job *schedulerpb.Job) error {
		job.LockTime = nil
		if ctx.Err() != nil && err != nil {
			// Shutdown, not a failure: hand the job back without spending an attempt.
			job.State = schedulerpb.JobState_JOB_STATE_PENDING
			job.StartTime = nil
			job.AttemptCount--
			if job.Metadata != nil {
				job.Metadata.Worker = ""
			}
			observeTransition(job.JobType, job.State)
			return nil
		}
		recordAttempt(job, now, err)
		nextScheduleTime := retryTime(job, jobTypeConfiguration, now)
		switch {
		case err == nil:
			job.State = schedulerpb.JobState_JOB_STATE_SUCCEEDED
			job.CompleteTime = timestamppb.New(now)
			job.PurgeTime = s.purgeTime(now)
			job.Error = nil
			if responseTypeURL := jobTypeConfiguration.GetResponseTypeUrl(); responseTypeURL != "" {
				job.Response = &anypb.Any{TypeUrl: responseTypeURL, Value: response}
			}
		case nextScheduleTime != nil:
			job.State = schedulerpb.JobState_JOB_STATE_PENDING
			job.ScheduleTime = nextScheduleTime
			job.Error = grpcstatus.Convert(err).Proto()
		default:
			job.State = schedulerpb.JobState_JOB_STATE_FAILED
			job.CompleteTime = timestamppb.New(now)
			job.PurgeTime = s.purgeTime(now)
			job.Error = grpcstatus.Convert(err).Proto()
		}
		observeTransition(job.JobType, job.State)
		return nil
	}
	if err := mutate(job, now, transition); err != nil {
		log.ErrorContext(ctx, "computing job outcome", "error", err)
		return
	}

	completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completionTimeout)
	defer cancel()
	if _, err := s.schedulerPostgresStore.UpdateRunningJob(completionCtx, job, workerColumns...); err != nil {
		if errors.Is(err, store.ErrJobNotRunning) {
			log.InfoContext(ctx, "job left running before its outcome was recorded")
			return
		}
		log.ErrorContext(ctx, "recording job outcome", "error", err)
		return
	}
	log.InfoContext(ctx, "job attempt completed", "state", schedulerpb.JobState(job.State).String(), "error", err)
}

// purgeTime returns when a job completing now is deleted under the retention.
func (s *Service) purgeTime(now time.Time) *timestamppb.Timestamp {
	if s.opts.Retention <= 0 {
		return nil
	}
	return timestamppb.New(now.Add(s.opts.Retention))
}

// retryTime returns when the job's next attempt should run, or nil when the
// job must fail instead: attempts are exhausted, or the backoff would reach
// past the expiry.
func retryTime(job *schedulerpb.Job, jobTypeConfiguration *pb.JobTypeConfiguration, now time.Time) *timestamppb.Timestamp {
	if job.GetAttemptCount() >= jobTypeConfiguration.GetMaxAttempts() {
		return nil
	}
	next := now.Add(backoff(jobTypeConfiguration.GetRetryBackoff(), int(job.GetAttemptCount())))
	if job.ExpireTime != nil && !next.Before(job.GetExpireTime().AsTime()) {
		return nil
	}
	return timestamppb.New(next)
}

// backoff returns the wait after the given number of failed attempts.
func backoff(retryBackoff *pb.RetryBackoff, failedAttempts int) time.Duration {
	wait := float64(retryBackoff.GetInitial().AsDuration()) * math.Pow(retryBackoff.GetMultiplier(), float64(failedAttempts-1))
	return time.Duration(math.Min(wait, float64(retryBackoff.GetMax().AsDuration())))
}

// reap recovers jobs no worker will ever complete: those whose lease lapsed,
// and those that expired before starting.
func (s *Service) reap(ctx context.Context) error {
	if err := s.reapLapsedLeases(ctx); err != nil {
		return err
	}
	return s.reapExpiredJobs(ctx)
}

// reapLapsedLeases returns RUNNING jobs whose lease lapsed to PENDING: their
// worker died without recording an outcome.
func (s *Service) reapLapsedLeases(ctx context.Context) error {
	now := truncatedNow()
	reaped, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND lock_time < $2",
		"ORDER BY lock_time",
		cap(s.slots),
		[]any{int16(schedulerpb.JobState_JOB_STATE_RUNNING), now},
		func(job *model.Job) error {
			return mutate(job, now, func(job *schedulerpb.Job) error {
				recordAttempt(job, now, grpcstatus.Error(codes.Unavailable, "lease lapsed"))
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
		s.log.WarnContext(ctx, "reaped job with lapsed lease", "job", job.JobID, "job_type", job.JobType, "attempt", job.AttemptCount)
		reapedCounter.WithLabelValues(job.JobType).Inc()
	}
	if len(reaped) > 0 {
		s.wakeClaim()
	}
	return nil
}

// reapExpiredJobs fails PENDING jobs that were not started by their expiry.
func (s *Service) reapExpiredJobs(ctx context.Context) error {
	now := truncatedNow()
	expired, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND expire_time <= $2",
		"ORDER BY expire_time",
		cap(s.slots),
		[]any{int16(schedulerpb.JobState_JOB_STATE_PENDING), now},
		func(job *model.Job) error {
			return mutate(job, now, func(job *schedulerpb.Job) error {
				job.State = schedulerpb.JobState_JOB_STATE_FAILED
				job.CompleteTime = timestamppb.New(now)
				job.PurgeTime = s.purgeTime(now)
				job.Error = grpcstatus.New(codes.DeadlineExceeded, "expired before starting").Proto()
				observeTransition(job.GetJobType(), job.State)
				return nil
			})
		},
	)
	if err != nil {
		return err
	}
	for _, job := range expired {
		s.log.InfoContext(ctx, "failed job that expired before starting", "job", job.JobID, "job_type", job.JobType)
	}
	return nil
}

// sweep deletes terminal jobs past their retention.
func (s *Service) sweep(ctx context.Context) error {
	deleted, err := s.schedulerPostgresStore.PurgeJobs(ctx, truncatedNow())
	if err != nil {
		return err
	}
	if deleted > 0 {
		s.log.InfoContext(ctx, "swept expired jobs", "count", deleted)
	}
	return nil
}
