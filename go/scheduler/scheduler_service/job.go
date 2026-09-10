package scheduler_service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/transition"
)

// statePreconditionError refuses a transition from the job's current state.
type statePreconditionError struct {
	state schedulerpb.JobState
}

func (e *statePreconditionError) Error() string {
	return fmt.Sprintf("job is %s", e.state)
}

// uniqueKeyCreateAttempts bounds the passes a keyed create makes when the
// key's PENDING job keeps being claimed between the lookup and the insert.
const uniqueKeyCreateAttempts = 3

// CreateJob accepts only the producer-owned fields and routes the job to the
// queue its payload type selects. A keyed job coalesces onto the key's PENDING
// job when there is one.
func (s *Service) CreateJob(ctx context.Context, request *pb.CreateJobRequest) (*schedulerpb.Job, error) {
	job := request.GetJob()
	queue, err := s.queueByRequestType(ctx, job.GetPayload().GetTypeUrl())
	if err != nil {
		if errors.Is(err, model.ErrQueueNotExist) {
			return nil, status.Errorf(codes.FailedPrecondition, "no queue accepts %s", job.GetPayload().GetTypeUrl()).Err()
		}
		return nil, err
	}
	request.Job = &schedulerpb.Job{
		Labels:       job.GetLabels(),
		Payload:      job.GetPayload(),
		Queue:        queue.GetName(),
		Method:       scheduler.MethodPath(queue.GetService(), queue.GetMethod()),
		Priority:     job.GetPriority(),
		UniqueKey:    job.GetUniqueKey(),
		ScheduleTime: job.GetScheduleTime(),
		ExpireTime:   job.GetExpireTime(),
		State:        schedulerpb.JobState_JOB_STATE_PENDING,
	}
	uniqueKey := job.GetUniqueKey()
	for attempt := 1; ; attempt++ {
		if uniqueKey != "" {
			pending, err := s.pendingJobByUniqueKey(ctx, uniqueKey)
			if err != nil {
				return nil, err
			}
			if pending != nil {
				return pending, nil
			}
		}
		created, err := s.SchedulerServiceServer.CreateJob(ctx, request)
		if err == nil {
			return created, nil
		}
		// A keyed insert conflicts when a PENDING job appeared since the lookup: pick it up on the next pass.
		if uniqueKey == "" || !status.HasCode(err, codes.AlreadyExists) || attempt == uniqueKeyCreateAttempts {
			return nil, err
		}
	}
}

// queueByRequestType returns the queue routing payloads of the type URL, or
// model.ErrQueueNotExist when none does; every other failure is a gRPC status.
func (s *Service) queueByRequestType(ctx context.Context, requestType string) (*schedulerpb.Queue, error) {
	if requestType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "payload.type_url must be set").Err()
	}
	queueModel, err := s.schedulerPostgresStore.GetQueueByRequestType(ctx, requestType)
	if err != nil {
		if errors.Is(err, model.ErrQueueNotExist) {
			return nil, err
		}
		return nil, status.FromError(err, "getting queue by request type").Err()
	}
	queue, err := queueModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting queue from model to pb: %v", err).Err()
	}
	return queue, nil
}

// pendingJobByUniqueKey returns the PENDING job holding the key, or nil.
func (s *Service) pendingJobByUniqueKey(ctx context.Context, uniqueKey string) (*schedulerpb.Job, error) {
	jobModel, err := s.schedulerPostgresStore.GetPendingJobByUniqueKey(ctx, uniqueKey)
	if err != nil {
		if errors.Is(err, model.ErrJobNotExist) {
			return nil, nil
		}
		return nil, status.FromError(err, "looking up unique key").Err()
	}
	job, err := jobModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting job from model to pb: %v", err).Err()
	}
	return job, nil
}

// pendingOnlyPaths are the update paths that only make sense before the first claim.
var pendingOnlyPaths = []string{"schedule_time", "priority"}

// UpdateJob only lets the scheduling fields change while the job is PENDING;
// the etag pins that check against a concurrent claim.
func (s *Service) UpdateJob(ctx context.Context, request *pb.UpdateJobRequest) (*schedulerpb.Job, error) {
	if slices.ContainsFunc(request.GetUpdateMask().GetPaths(), func(path string) bool { return slices.Contains(pendingOnlyPaths, path) }) {
		getJobRequest := &pb.GetJobRequest{Name: request.GetJob().GetName()}
		job, err := s.GetJob(ctx, getJobRequest)
		if err != nil {
			return nil, err
		}
		if job.GetState() != schedulerpb.JobState_JOB_STATE_PENDING {
			return nil, status.Errorf(codes.FailedPrecondition, "%s can only be updated on a PENDING job, job is %s", strings.Join(pendingOnlyPaths, " and "), job.GetState()).Err()
		}
		if slices.Contains(request.GetUpdateMask().GetPaths(), "schedule_time") && job.GetExpireTime() != nil && !request.GetJob().GetScheduleTime().AsTime().Before(job.GetExpireTime().AsTime()) {
			return nil, status.Errorf(codes.InvalidArgument, "schedule_time must be before expire_time %s", job.GetExpireTime().AsTime().Format(time.RFC3339)).Err()
		}
		if request.GetJob().GetEtag() == "" {
			request.Job.Etag = job.GetEtag()
		}
	}
	updated, err := s.SchedulerServiceServer.UpdateJob(ctx, request)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// DeleteJob refuses to delete a RUNNING job; cancel it first.
func (s *Service) DeleteJob(ctx context.Context, request *pb.DeleteJobRequest) (*emptypb.Empty, error) {
	getJobRequest := &pb.GetJobRequest{Name: request.GetName()}
	job, err := s.GetJob(ctx, getJobRequest)
	if err != nil {
		if request.GetAllowMissing() && status.HasCode(err, codes.NotFound) {
			return &emptypb.Empty{}, nil
		}
		return nil, err
	}
	if job.GetState() == schedulerpb.JobState_JOB_STATE_RUNNING {
		return nil, status.Errorf(codes.FailedPrecondition, "job is running; cancel it before deleting it").Err()
	}
	if request.GetEtag() == "" {
		request.Etag = job.GetEtag()
	}
	return s.SchedulerServiceServer.DeleteJob(ctx, request)
}

// RetryJob returns a terminal job to PENDING with a clean slate. The expiry is
// dropped: a retry asks for the job to run regardless. A keyed job whose key
// already has a PENDING job is refused: that job is the retry.
func (s *Service) RetryJob(ctx context.Context, request *pb.RetryJobRequest) (*schedulerpb.Job, error) {
	job, err := s.transitionJob(ctx, request.GetName(), func(job *schedulerpb.Job) error {
		if !transition.IsTerminal(job.GetState()) {
			return &statePreconditionError{state: job.GetState()}
		}
		job.State = schedulerpb.JobState_JOB_STATE_PENDING
		job.AttemptCount = 0
		job.ScheduleTime = nil
		job.StartTime = nil
		job.CompleteTime = nil
		job.LockTime = nil
		job.ExpireTime = nil
		job.PurgeTime = nil
		job.Error = nil
		job.Response = nil
		job.Progress = nil
		job.Metadata = nil
		aip.SetLabel(job, schedulerpb.Labels.Retried.GetKey(), schedulerpb.Labels.Retried.True)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// CancelJob moves a PENDING or RUNNING job to CANCELLED; the dispatcher running
// it notices on its next lease renewal and cancels the handler call.
func (s *Service) CancelJob(ctx context.Context, request *pb.CancelJobRequest) (*schedulerpb.Job, error) {
	now := transition.Now()
	job, err := s.transitionJob(ctx, request.GetName(), func(job *schedulerpb.Job) error {
		if transition.IsTerminal(job.GetState()) {
			return &statePreconditionError{state: job.GetState()}
		}
		cancelled := grpcstatus.New(codes.Canceled, "cancelled by client")
		if job.GetState() == schedulerpb.JobState_JOB_STATE_RUNNING {
			transition.RecordAttempt(job, now, cancelled.Err(), nil)
		}
		job.State = schedulerpb.JobState_JOB_STATE_CANCELLED
		job.CompleteTime = timestamppb.New(now)
		job.LockTime = nil
		job.PurgeTime = transition.PurgeTime(now, s.opts.Retention)
		job.Error = cancelled.Proto()
		transition.Observe(job, job.State)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// waitJobPollInterval is how often WaitJob re-reads the job. Terminal
// transitions are written by workers on any instance, so the row is the only
// place to observe them from.
const waitJobPollInterval = 250 * time.Millisecond

// WaitJob polls the job until it is terminal or the timeout elapses and
// returns it either way: timing out is the caller's to detect from `state`.
// The timeout is silently capped so a client cannot pin a server goroutine.
func (s *Service) WaitJob(ctx context.Context, request *pb.WaitJobRequest) (*schedulerpb.Job, error) {
	timeout := s.opts.WaitJobMaxTimeout
	if request.GetTimeout() != nil && request.GetTimeout().AsDuration() < timeout {
		timeout = request.GetTimeout().AsDuration()
	}
	deadline := time.Now().Add(timeout)
	getJobRequest := &pb.GetJobRequest{Name: request.GetName()}
	for {
		job, err := s.GetJob(ctx, getJobRequest)
		if err != nil {
			return nil, err
		}
		remaining := time.Until(deadline)
		if transition.IsTerminal(job.GetState()) || remaining <= 0 {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromError(ctx.Err(), "waiting for job").Err()
		case <-time.After(min(waitJobPollInterval, remaining)):
		}
	}
}

// ReportJobProgress records a RUNNING job's latest progress.
func (s *Service) ReportJobProgress(ctx context.Context, request *pb.ReportJobProgressRequest) (*schedulerpb.Job, error) {
	return s.transitionJob(ctx, request.GetName(), func(job *schedulerpb.Job) error {
		if job.GetState() != schedulerpb.JobState_JOB_STATE_RUNNING {
			return &statePreconditionError{state: job.GetState()}
		}
		job.Progress = request.GetProgress()
		return nil
	})
}

// transitionJob applies fn to the named job under a row lock, translating
// store and precondition errors to gRPC statuses.
func (s *Service) transitionJob(ctx context.Context, name string, fn func(*schedulerpb.Job) error) (*schedulerpb.Job, error) {
	_, _, jobID, err := model.ParseJobName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	now := transition.Now()
	jobModel, err := s.schedulerPostgresStore.TransitionJob(ctx, jobID, func(job *model.Job) error {
		return transition.Mutate(job, now, fn)
	})
	if err != nil {
		var preconditionErr *statePreconditionError
		switch {
		case errors.Is(err, model.ErrJobNotExist):
			return nil, status.Errorf(codes.NotFound, "job does not exist").Err()
		case errors.As(err, &preconditionErr):
			return nil, status.Errorf(codes.FailedPrecondition, "%v", preconditionErr).Err()
		case store.IsUniqueKeyConflict(err):
			return nil, status.Errorf(codes.AlreadyExists, "unique key already has a pending job").Err()
		}
		return nil, status.FromError(err, "transitioning job").Err()
	}
	job, err := jobModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting job from model to pb: %v", err).Err()
	}
	return job, nil
}
