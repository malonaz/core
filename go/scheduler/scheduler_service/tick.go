package scheduler_service

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/scheduler/cron"
	"github.com/malonaz/core/go/scheduler/transition"
	"github.com/malonaz/core/go/uuid"
)

// tickBatchSize bounds the schedules one tick pass materializes.
const tickBatchSize = 100

// scheduleTickRequestIDNamespace derives a tick's job create request id from
// the schedule and the tick, so a pass interrupted after the insert replays
// into the same job instead of a twin.
var scheduleTickRequestIDNamespace = uuid.MustParse("928b12d9-af1a-4eb5-b1d2-b3d6742b4772")

// tick materializes the job of every due ENABLED schedule and advances it to
// its next occurrence, then refreshes the overdue gauge.
func (s *Service) tick(ctx context.Context) error {
	now := transition.Now()
	_, err := s.schedulerPostgresStore.TransitionSchedules(ctx,
		"state = $1 AND next_schedule_time <= $2",
		"ORDER BY next_schedule_time",
		tickBatchSize,
		[]any{int16(schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED), now},
		func(scheduleModel *model.Schedule) (bool, error) {
			return mutateSchedule(scheduleModel, now, func(schedule *schedulerpb.Schedule) (bool, error) {
				return s.tickSchedule(ctx, schedule, now)
			})
		},
	)
	if err != nil {
		return err
	}
	overdue, err := s.schedulerPostgresStore.CountOverdueSchedules(ctx, now.Add(-s.opts.TickInterval))
	if err != nil {
		return err
	}
	schedulesOverdueGauge.Set(float64(overdue))
	return nil
}

// tickSchedule creates the job of the schedule's due tick, or counts the tick
// missed when its run window has already closed, and moves the schedule to
// the first occurrence after now: ticks slept through are never caught up. A
// tick whose job cannot be created is left due and retried on the next pass
// until its window closes.
func (s *Service) tickSchedule(ctx context.Context, schedule *schedulerpb.Schedule, now time.Time) (bool, error) {
	expression, err := cron.Parse(schedule.GetCron(), schedule.GetTimeZone())
	if err != nil {
		return false, err
	}
	tick := schedule.GetNextScheduleTime().AsTime()
	expireTime := runWindowEnd(expression, schedule, tick)
	if !expireTime.After(now) {
		schedule.MissedTickCount++
		scheduleTicksCounter.WithLabelValues("missed").Inc()
		s.log.WarnContext(ctx, "skipped schedule tick whose run window had closed", "schedule", schedule.GetName(), "tick", tick, "expire_time", expireTime)
	} else {
		job, err := s.createScheduledJob(ctx, schedule, tick, expireTime)
		if err != nil {
			scheduleTicksCounter.WithLabelValues("failed").Inc()
			s.log.ErrorContext(ctx, "creating schedule tick job", "schedule", schedule.GetName(), "tick", tick, "error", err)
			return false, nil
		}
		schedule.LastScheduleTime = timestamppb.New(tick)
		schedule.LastJob = job.GetName()
		scheduleTicksCounter.WithLabelValues("created").Inc()
	}
	schedule.NextScheduleTime = timestamppb.New(expression.Next(now))
	return true, nil
}

// runWindowEnd returns when the tick's job may no longer start: the tick plus
// the schedule's run window, or the following tick when it has none.
func runWindowEnd(expression *cron.Expression, schedule *schedulerpb.Schedule, tick time.Time) time.Time {
	if schedule.GetRunWindow() != nil {
		return tick.Add(schedule.GetRunWindow().AsDuration())
	}
	return expression.Next(tick)
}

// createScheduledJob enqueues the tick's job through CreateJob, so it gets the
// routing and validation every job gets, under the schedule's parent.
func (s *Service) createScheduledJob(ctx context.Context, schedule *schedulerpb.Schedule, tick, expireTime time.Time) (*schedulerpb.Job, error) {
	organizationID, userID, _, err := model.ParseScheduleName(schedule.GetName())
	if err != nil {
		return nil, err
	}
	createJobRequest := &pb.CreateJobRequest{
		Parent: jobParent(organizationID, userID),
		Job: &schedulerpb.Job{
			Labels:       schedule.GetLabels(),
			Payload:      schedule.GetPayload(),
			Priority:     schedule.GetPriority(),
			ScheduleTime: timestamppb.New(tick),
			ExpireTime:   timestamppb.New(expireTime),
		},
		RequestId: uuid.NewV5(scheduleTickRequestIDNamespace, schedule.GetName()+"/"+tick.UTC().Format(time.RFC3339)).String(),
	}
	return s.createJob(ctx, createJobRequest, schedule.GetName())
}

// jobParent returns the parent a schedule's jobs live under: the schedule's own.
func jobParent(organizationID, userID string) string {
	switch {
	case userID != "":
		return (&schedulerpb.UserResourceName{Organization: organizationID, User: userID}).String()
	case organizationID != "":
		return (&schedulerpb.OrganizationResourceName{Organization: organizationID}).String()
	}
	return ""
}
