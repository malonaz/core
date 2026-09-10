package scheduler_dispatcher

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/transition"
)

// Writes to a terminal state must not depend on the (possibly cancelled) job context.
const completionTimeout = 10 * time.Second

// typeURLPrefix is the prefix anypb gives type URLs.
const typeURLPrefix = "type.googleapis.com/"

// claim moves due PENDING jobs to RUNNING, one per free slot, and hands each
// to a worker. A full batch signals another pass rather than waiting for the ticker.
func (s *Service) claim(ctx, workerCtx context.Context) error {
	free := cap(s.slots) - len(s.slots)
	if free == 0 {
		return nil
	}
	now := transition.Now()
	lockTime := now.Add(s.opts.LeaseDuration)
	jobs, err := s.schedulerPostgresStore.ClaimJobs(ctx, now, free, func(job *store.ClaimedJob) error {
		return transition.Mutate(&job.Job, now, func(job *schedulerpb.Job) error {
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

	route, err := routing(claimed)
	if err != nil {
		// Nothing to retry against: the job is failed at once.
		s.complete(ctx, log, job, &policypb.QueuePolicy{MaxAttempts: 1}, nil, err)
		return
	}
	policy := route.policy

	jobCtx, cancel := context.WithTimeout(ctx, policy.GetAttemptTimeout().AsDuration())
	defer cancel()

	stopHeartbeat := s.heartbeat(jobCtx, cancel, log, job)
	start := time.Now()
	response, err := s.invoke(jobCtx, route, job)
	stopHeartbeat()
	observeAttempt(job, time.Since(start), err)

	if err == nil && response.GetTypeUrl() == operationTypeURL {
		response, err = unwrapOperation(response)
		if errors.Is(err, errUnfinishedOperation) {
			// A runner that hands back an unfinished operation will do so again: no retry.
			policy = &policypb.QueuePolicy{MaxAttempts: 1}
		}
	}
	s.complete(ctx, log, job, policy, response, err)
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

// route is where a claimed job goes and under what policy, decoded from the
// queue columns the claim carried.
type route struct {
	endpoint     string
	responseType string
	policy       *policypb.QueuePolicy
}

// routing decodes the job's queue as of the claim. A missing queue (deleted
// since the job was created) is a precondition failure.
func routing(claimed *store.ClaimedJob) (*route, error) {
	if claimed.QueuePolicy == nil || claimed.QueueEndpoint == nil || claimed.QueueResponseType == nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "queue %s does not exist", claimed.Queue)
	}
	policy := &policypb.QueuePolicy{}
	if err := pbutil.JSONUnmarshal(claimed.QueuePolicy, policy); err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "unmarshaling queue policy: %v", err)
	}
	return &route{endpoint: *claimed.QueueEndpoint, responseType: *claimed.QueueResponseType, policy: policy}, nil
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
			now := transition.Now()
			lease := *job
			if err := transition.Mutate(&lease, now, func(job *schedulerpb.Job) error {
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

// invoke delivers the payload to the queue's method on its endpoint as the
// request body and returns the response under the queue's response type.
func (s *Service) invoke(ctx context.Context, route *route, job *model.Job) (*anypb.Any, error) {
	connection, err := s.endpoints.Acquire(ctx, route.endpoint)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Unavailable, "connecting to %s: %v", route.endpoint, err)
	}
	payload := &anypb.Any{}
	if err := pbutil.Unmarshal(job.Payload, payload); err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "unmarshaling payload: %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, scheduler.JobMetadataKey, transition.JobName(job))
	// Attempts are the scheduler's to account for: no transparent retries.
	var response []byte
	if err := connection.Get().Invoke(ctx, job.Method, payload.GetValue(), &response, grpc.WithRawCodec(), grpc_retry.Disable()); err != nil {
		return nil, err
	}
	return &anypb.Any{TypeUrl: route.responseType, Value: response}, nil
}

// complete records an attempt's outcome: SUCCEEDED, PENDING again after the
// backoff, FAILED once attempts are exhausted or the error is not retryable,
// or released untouched when the instance is shutting down. A job that left
// RUNNING meanwhile is left alone.
func (s *Service) complete(ctx context.Context, log *slog.Logger, job *model.Job, policy *policypb.QueuePolicy, response *anypb.Any, err error) {
	now := transition.Now()
	outcome := func(job *schedulerpb.Job) error {
		job.LockTime = nil
		if ctx.Err() != nil && err != nil {
			// Shutdown, not a failure: hand the job back without spending an attempt.
			job.State = schedulerpb.JobState_JOB_STATE_PENDING
			job.StartTime = nil
			job.AttemptCount--
			if job.Metadata != nil {
				job.Metadata.Worker = ""
			}
			transition.Observe(job, job.State)
			return nil
		}
		retryDelay := retryDelayOf(err)
		transition.RecordAttempt(job, now, err, retryDelay)
		nextScheduleTime := retryTime(job, policy, retryDelay, now, err)
		switch {
		case err == nil:
			job.State = schedulerpb.JobState_JOB_STATE_SUCCEEDED
			job.CompleteTime = timestamppb.New(now)
			job.PurgeTime = transition.PurgeTime(now, s.opts.Retention)
			job.Error = nil
			job.Response = response
		case nextScheduleTime != nil:
			job.State = schedulerpb.JobState_JOB_STATE_PENDING
			job.ScheduleTime = nextScheduleTime
			job.Error = grpcstatus.Convert(err).Proto()
		default:
			job.State = schedulerpb.JobState_JOB_STATE_FAILED
			job.CompleteTime = timestamppb.New(now)
			job.PurgeTime = transition.PurgeTime(now, s.opts.Retention)
			job.Error = grpcstatus.Convert(err).Proto()
		}
		transition.Observe(job, job.State)
		return nil
	}
	if err := transition.Mutate(job, now, outcome); err != nil {
		log.ErrorContext(ctx, "computing job outcome", "error", err)
		return
	}

	completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completionTimeout)
	defer cancel()
	if _, err := s.schedulerPostgresStore.UpdateRunningJob(completionCtx, job, transition.WorkerColumns...); err != nil {
		if errors.Is(err, store.ErrJobNotRunning) {
			log.InfoContext(ctx, "job left running before its outcome was recorded")
			return
		}
		log.ErrorContext(ctx, "recording job outcome", "error", err)
		return
	}
	log.InfoContext(ctx, "job attempt completed", "state", schedulerpb.JobState(job.State).String(), "error", err)
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
func retryTime(job *schedulerpb.Job, policy *policypb.QueuePolicy, retryDelay *durationpb.Duration, now time.Time, err error) *timestamppb.Timestamp {
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
func backoff(retryBackoff *policypb.RetryBackoff, failedAttempts int) time.Duration {
	wait := float64(retryBackoff.GetInitial().AsDuration()) * math.Pow(retryBackoff.GetMultiplier(), float64(failedAttempts-1))
	return time.Duration(math.Min(wait, float64(retryBackoff.GetMax().AsDuration())))
}
