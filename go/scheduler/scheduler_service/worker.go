package scheduler_service

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
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
func recordAttempt(job *schedulerpb.Job, now time.Time, err error, retryDelay *durationpb.Duration) {
	metadata := job.GetMetadata()
	if metadata == nil {
		metadata = &schedulerpb.JobMetadata{}
	}
	attempt := &schedulerpb.JobAttempt{
		Attempt:    job.GetAttemptCount(),
		StartTime:  job.GetStartTime(),
		EndTime:    timestamppb.New(now),
		Worker:     metadata.GetWorker(),
		RetryDelay: retryDelay,
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
	jobs, err := s.schedulerPostgresStore.ClaimJobs(ctx, now, free, func(job *store.ClaimedJob) error {
		return mutate(&job.Job, now, func(job *schedulerpb.Job) error {
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
	})
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

// process runs one claimed job: invokes its handler under the queue's attempt
// timeout while renewing the lease, then records the outcome.
func (s *Service) process(ctx context.Context, claimed *store.ClaimedJob) {
	defer s.workers.Done()
	defer func() { <-s.slots }()
	inflightGauge.Inc()
	defer inflightGauge.Dec()
	job := &claimed.Job
	log := s.log.With("job", job.JobID, "queue", job.Queue, "method", job.Method, "attempt", job.AttemptCount)

	policy, handler, err := routing(claimed)
	if err != nil {
		// Nothing to retry against: the job is failed at once.
		s.complete(ctx, log, job, &schedulerpb.QueuePolicy{MaxAttempts: 1}, nil, err)
		return
	}

	jobCtx, cancel := context.WithTimeout(ctx, policy.GetAttemptTimeout().AsDuration())
	defer cancel()
	s.inflight.add(job.JobID, cancel)
	defer s.inflight.remove(job.JobID)

	stopHeartbeat := s.heartbeat(jobCtx, cancel, log, job)
	start := time.Now()
	response, err := s.invoke(jobCtx, handler, job)
	stopHeartbeat()
	observeAttempt(job, time.Since(start), err)

	var responseAny *anypb.Any
	if err == nil {
		responseAny = &anypb.Any{TypeUrl: handler.GetResponseType(), Value: response}
		if responseAny.GetTypeUrl() == operationTypeURL {
			responseAny, err = unwrapOperation(responseAny)
			if errors.Is(err, errUnfinishedOperation) {
				// A runner that hands back an unfinished operation will do so again: no retry.
				policy = &schedulerpb.QueuePolicy{MaxAttempts: 1}
			}
		}
	}
	s.complete(ctx, log, job, policy, responseAny, err)
}

// operationTypeURL is the response type of long-running operation handlers.
var operationTypeURL = typeURLPrefix + string((&longrunningpb.Operation{}).ProtoReflect().Descriptor().FullName())

// errUnfinishedOperation is a handler bug: a long-running operation handler
// must do the work and return the operation done.
var errUnfinishedOperation = grpcstatus.Error(codes.FailedPrecondition, "runner returned an unfinished operation")

// unwrapOperation records a done operation's outcome as if the handler had
// returned it directly: its response as the job's, its error as the attempt's.
func unwrapOperation(envelope *anypb.Any) (*anypb.Any, error) {
	operation := &longrunningpb.Operation{}
	if err := envelope.UnmarshalTo(operation); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "unmarshaling operation: %v", err)
	}
	if !operation.GetDone() {
		return nil, errUnfinishedOperation
	}
	if operation.GetError() != nil {
		return nil, grpcstatus.ErrorProto(operation.GetError())
	}
	return operation.GetResponse(), nil
}

// routing decodes the queue policy and handler the job was claimed with. A
// missing queue (deleted since the job was created) is a precondition failure.
func routing(claimed *store.ClaimedJob) (*schedulerpb.QueuePolicy, *schedulerpb.Handler, error) {
	if claimed.QueuePolicy == nil {
		return nil, nil, grpcstatus.Errorf(codes.FailedPrecondition, "queue %s does not exist", claimed.Queue)
	}
	policy := &schedulerpb.QueuePolicy{}
	if err := pbutil.JSONUnmarshal(claimed.QueuePolicy, policy); err != nil {
		return nil, nil, grpcstatus.Errorf(codes.Internal, "unmarshaling queue policy: %v", err)
	}
	handlers, err := pbutil.JSONUnmarshalSlice[schedulerpb.Handler](pbutil.JsonUnmarshalOptions, claimed.QueueHandlers)
	if err != nil {
		return nil, nil, grpcstatus.Errorf(codes.Internal, "unmarshaling queue handlers: %v", err)
	}
	index := slices.IndexFunc(handlers, func(handler *schedulerpb.Handler) bool { return handler.GetMethod() == claimed.Method })
	if index < 0 {
		return nil, nil, grpcstatus.Errorf(codes.FailedPrecondition, "queue %s no longer routes %s", claimed.Queue, claimed.Method)
	}
	return policy, handlers[index], nil
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
				log.InfoContext(ctx, "job no longer running, cancelling handler call")
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

// invoke delivers the payload to the handler's target as the request body of
// its method, returning the raw response body.
func (s *Service) invoke(ctx context.Context, handler *schedulerpb.Handler, job *model.Job) ([]byte, error) {
	targetID, err := model.ParseTargetName(handler.GetTarget())
	if err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "parsing target name: %v", err)
	}
	targetModel, err := s.schedulerPostgresStore.GetTarget(ctx, targetID)
	if err != nil {
		if errors.Is(err, model.ErrTargetNotExist) {
			return nil, grpcstatus.Errorf(codes.FailedPrecondition, "target %s does not exist", handler.GetTarget())
		}
		return nil, grpcstatus.Errorf(codes.Internal, "getting target: %v", err)
	}
	target, err := targetModel.ToPb()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "converting target from model to pb: %v", err)
	}
	connection, release, err := s.targets.acquire(ctx, target)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Unavailable, "connecting to target %s: %v", target.GetName(), err)
	}
	defer release()

	payload := &anypb.Any{}
	if err := pbutil.Unmarshal(job.Payload, payload); err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "unmarshaling payload: %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(outgoingContext(ctx, target), scheduler.JobMetadataKey, jobName(job))
	// Attempts are the scheduler's to account for: no transparent retries.
	var response []byte
	if err := connection.Get().Invoke(ctx, handler.GetMethod(), payload.GetValue(), &response, grpc.WithRawCodec(), grpc_retry.Disable()); err != nil {
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
// backoff, FAILED once attempts are exhausted or the error is not retryable,
// or released untouched when the instance is shutting down. A job that left
// RUNNING meanwhile is left alone.
func (s *Service) complete(ctx context.Context, log *slog.Logger, job *model.Job, policy *schedulerpb.QueuePolicy, response *anypb.Any, err error) {
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
			observeTransition(job, job.State)
			return nil
		}
		retryDelay := retryDelayOf(err)
		recordAttempt(job, now, err, retryDelay)
		nextScheduleTime := retryTime(job, policy, retryDelay, now, err)
		switch {
		case err == nil:
			job.State = schedulerpb.JobState_JOB_STATE_SUCCEEDED
			job.CompleteTime = timestamppb.New(now)
			job.PurgeTime = s.purgeTime(now)
			job.Error = nil
			job.Response = response
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
		observeTransition(job, job.State)
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

// retryDelayOf returns the wait a handler requested through a RetryInfo error
// detail, or nil.
func retryDelayOf(err error) *durationpb.Duration {
	for retryInfo := range status.ErrorDetails[*errdetails.RetryInfo](err) {
		return retryInfo.GetRetryDelay()
	}
	return nil
}

// retryTime returns when the job's next attempt should run, or nil when the
// job must fail instead: the error is not retryable, attempts are exhausted,
// or the wait would reach past the expiry.
func retryTime(job *schedulerpb.Job, policy *schedulerpb.QueuePolicy, retryDelay *durationpb.Duration, now time.Time, err error) *timestamppb.Timestamp {
	if err == nil || !slices.Contains(policy.GetRetryableCodes(), codepb.Code(grpcstatus.Code(err))) {
		return nil
	}
	if job.GetAttemptCount() >= policy.GetMaxAttempts() {
		return nil
	}
	wait := backoff(policy.GetRetryBackoff(), int(job.GetAttemptCount()))
	if retryDelay != nil {
		wait = retryDelay.AsDuration()
	}
	next := now.Add(wait)
	if job.ExpireTime != nil && !next.Before(job.GetExpireTime().AsTime()) {
		return nil
	}
	return timestamppb.New(next)
}

// backoff returns the wait after the given number of failed attempts.
func backoff(retryBackoff *schedulerpb.RetryBackoff, failedAttempts int) time.Duration {
	wait := float64(retryBackoff.GetInitial().AsDuration()) * math.Pow(retryBackoff.GetMultiplier(), float64(failedAttempts-1))
	return time.Duration(math.Min(wait, float64(retryBackoff.GetMax().AsDuration())))
}

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
	now := truncatedNow()
	reaped, err := s.schedulerPostgresStore.TransitionJobs(ctx,
		"state = $1 AND lock_time < $2",
		"ORDER BY lock_time",
		cap(s.slots),
		[]any{int16(schedulerpb.JobState_JOB_STATE_RUNNING), now},
		func(job *model.Job) error {
			return mutate(job, now, func(job *schedulerpb.Job) error {
				recordAttempt(job, now, grpcstatus.Error(codes.Unavailable, "lease lapsed"), nil)
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
				observeTransition(job, job.State)
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
	observeQueueStats(stats, truncatedNow())
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
