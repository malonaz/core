package scheduler_service

import (
	"context"
	"errors"
	"slices"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/cron"
	"github.com/malonaz/core/go/scheduler/transition"
)

// defaultTimeZone is the zone a schedule's cron expression is evaluated in when none is given.
const defaultTimeZone = "UTC"

// CreateSchedule accepts only the producer-owned fields, routes the schedule
// to the queue its payload type selects and sets its first tick.
func (s *Service) CreateSchedule(ctx context.Context, request *pb.CreateScheduleRequest) (*schedulerpb.Schedule, error) {
	declared := request.GetSchedule()
	queue, err := s.queueByRequestType(ctx, declared.GetPayload().GetTypeUrl())
	if err != nil {
		if errors.Is(err, model.ErrQueueNotExist) {
			return nil, status.Errorf(codes.InvalidArgument, "no queue accepts %s", declared.GetPayload().GetTypeUrl()).Err()
		}
		return nil, err
	}
	timeZone := declared.GetTimeZone()
	if timeZone == "" {
		timeZone = defaultTimeZone
	}
	expression, err := cron.Parse(declared.GetCron(), timeZone)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	request.Schedule = &schedulerpb.Schedule{
		Labels:           declared.GetLabels(),
		Payload:          declared.GetPayload(),
		Queue:            queue.GetName(),
		Method:           scheduler.MethodPath(queue.GetService(), queue.GetMethod()),
		Cron:             declared.GetCron(),
		TimeZone:         timeZone,
		State:            schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED,
		Priority:         declared.GetPriority(),
		RunWindow:        declared.GetRunWindow(),
		NextScheduleTime: timestamppb.New(expression.Next(transition.Now())),
	}
	return s.SchedulerServiceServer.CreateSchedule(ctx, request)
}

var updateScheduleRequestParser = aip.MustNewUpdateRequestParser[*pb.UpdateScheduleRequest, *schedulerpb.Schedule]()

// recurrencePaths are the update paths that move the next tick.
var recurrencePaths = []string{"cron", "time_zone"}

// UpdateSchedule patches the schedule under its row lock rather than through
// the generated read-patch-write, so that a recurrence change and the
// `next_schedule_time` it implies land in one write.
func (s *Service) UpdateSchedule(ctx context.Context, request *pb.UpdateScheduleRequest) (*schedulerpb.Schedule, error) {
	parsedRequest, err := updateScheduleRequestParser.Parse(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing request: %v", err).Err()
	}
	recurrenceChanged := slices.ContainsFunc(request.GetUpdateMask().GetPaths(), func(path string) bool { return slices.Contains(recurrencePaths, path) })
	now := transition.Now()
	return s.transitionSchedule(ctx, request.GetSchedule().GetName(), request.GetSchedule().GetEtag(), func(schedule *schedulerpb.Schedule) (bool, error) {
		parsedRequest.ApplyFieldMask(schedule, request.GetSchedule())
		if schedule.GetTimeZone() == "" {
			schedule.TimeZone = defaultTimeZone
		}
		if err := protovalidate.Validate(schedule); err != nil {
			return false, status.Errorf(codes.InvalidArgument, "validating patched resource: %v", err).Err()
		}
		expression, err := cron.Parse(schedule.GetCron(), schedule.GetTimeZone())
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "%v", err).Err()
		}
		if recurrenceChanged && schedule.GetState() == schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED {
			schedule.NextScheduleTime = timestamppb.New(expression.Next(now))
		}
		return true, nil
	})
}

// PauseSchedule stops ticks; the jobs already created are untouched.
func (s *Service) PauseSchedule(ctx context.Context, request *pb.PauseScheduleRequest) (*schedulerpb.Schedule, error) {
	return s.setScheduleState(ctx, request.GetName(), request.GetEtag(), schedulerpb.ScheduleState_SCHEDULE_STATE_PAUSED)
}

// ResumeSchedule restarts ticks from the first occurrence after now.
func (s *Service) ResumeSchedule(ctx context.Context, request *pb.ResumeScheduleRequest) (*schedulerpb.Schedule, error) {
	return s.setScheduleState(ctx, request.GetName(), request.GetEtag(), schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED)
}

// setScheduleState moves the schedule to state under its row lock; a schedule
// already in that state is returned untouched.
func (s *Service) setScheduleState(ctx context.Context, name, etag string, state schedulerpb.ScheduleState) (*schedulerpb.Schedule, error) {
	now := transition.Now()
	return s.transitionSchedule(ctx, name, etag, func(schedule *schedulerpb.Schedule) (bool, error) {
		if schedule.GetState() == state {
			return false, nil
		}
		schedule.State = state
		schedule.NextScheduleTime = nil
		if state == schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED {
			expression, err := cron.Parse(schedule.GetCron(), schedule.GetTimeZone())
			if err != nil {
				return false, err
			}
			schedule.NextScheduleTime = timestamppb.New(expression.Next(now))
		}
		return true, nil
	})
}

// transitionSchedule applies fn to the named schedule's proto form under a
// row lock, restamping it when fn reports a change, and translates store and
// precondition errors to gRPC statuses. An empty etag skips the check.
func (s *Service) transitionSchedule(ctx context.Context, name, etag string, fn func(*schedulerpb.Schedule) (bool, error)) (*schedulerpb.Schedule, error) {
	_, _, scheduleID, err := model.ParseScheduleName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	now := transition.Now()
	scheduleModel, err := s.schedulerPostgresStore.TransitionSchedule(ctx, scheduleID, func(scheduleModel *model.Schedule) (bool, error) {
		if etag != "" && scheduleModel.Etag != etag {
			return false, model.ErrScheduleETagChanged
		}
		return mutateSchedule(scheduleModel, now, fn)
	})
	if err != nil {
		switch {
		case errors.Is(err, model.ErrScheduleNotExist):
			return nil, status.Errorf(codes.NotFound, "schedule does not exist").Err()
		case errors.Is(err, model.ErrScheduleETagChanged):
			return nil, status.Errorf(codes.Aborted, "ETag changed").Err()
		case status.HasCode(err, codes.InvalidArgument):
			return nil, err
		}
		return nil, status.FromError(err, "transitioning schedule").Err()
	}
	schedule, err := scheduleModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting schedule from model to pb: %v", err).Err()
	}
	return schedule, nil
}

// mutateSchedule applies fn to the schedule's proto form and, when fn reports
// a change, restamps update_time and etag and writes the result back into the model.
func mutateSchedule(scheduleModel *model.Schedule, now time.Time, fn func(*schedulerpb.Schedule) (bool, error)) (bool, error) {
	schedule, err := scheduleModel.ToPb()
	if err != nil {
		return false, err
	}
	changed, err := fn(schedule)
	if err != nil || !changed {
		return false, err
	}
	schedule.UpdateTime = timestamppb.New(now)
	if schedule.Etag, err = aip.ComputeETag(schedule); err != nil {
		return false, err
	}
	mutated, err := model.ScheduleFromPb(schedule)
	if err != nil {
		return false, err
	}
	*scheduleModel = *mutated
	return true, nil
}
