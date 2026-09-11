package sat

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler/cron"
	"github.com/malonaz/core/go/uuid"
)

const (
	// A tick that never falls within a test run, for schedules that must only fire when rewound.
	rareCron = "0 0 1 1 *"
	// A schedule rewound into the past fires on the next tick pass: long
	// enough to observe it under a busy suite, and to be sure it did not.
	tickWait   = 5 * time.Second
	tickSettle = 10 * tickInterval
)

// scheduleParents are the three shapes a schedule (and its jobs) may live under.
func scheduleParents() map[string]string {
	organization := (&schedulerpb.OrganizationResourceName{Organization: "org-" + uuid.MustNewV7().String()}).String()
	user := (&schedulerpb.UserResourceName{Organization: "org-" + uuid.MustNewV7().String(), User: "user-" + uuid.MustNewV7().String()}).String()
	return map[string]string{"root": "", "organization": organization, "user": user}
}

// newSchedule declares an Echo schedule with the given cron in UTC.
func newSchedule(t *testing.T, cronExpression string) *schedulerpb.Schedule {
	t.Helper()
	return &schedulerpb.Schedule{Payload: mustAny(t, &processorpb.EchoRequest{Value: uuid.MustNewV7().String()}), Cron: cronExpression}
}

func createSchedule(t *testing.T, parent string, schedule *schedulerpb.Schedule) *schedulerpb.Schedule {
	t.Helper()
	createScheduleRequest := &schedulerservicepb.CreateScheduleRequest{Parent: parent, Schedule: schedule}
	created, err := schedulerServiceClient.CreateSchedule(ctx, createScheduleRequest)
	require.NoError(t, err)
	t.Cleanup(func() {
		deleteScheduleRequest := &schedulerservicepb.DeleteScheduleRequest{Name: created.GetName(), AllowMissing: true}
		_, err := schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest)
		require.NoError(t, err)
	})
	return created
}

func getSchedule(t *testing.T, name string) *schedulerpb.Schedule {
	t.Helper()
	getScheduleRequest := &schedulerservicepb.GetScheduleRequest{Name: name}
	schedule, err := schedulerServiceClient.GetSchedule(ctx, getScheduleRequest)
	require.NoError(t, err)
	return schedule
}

// waitForSchedule polls the schedule until predicate holds and returns it.
func waitForSchedule(t *testing.T, name string, timeout time.Duration, predicate func(*schedulerpb.Schedule) bool) *schedulerpb.Schedule {
	t.Helper()
	var schedule *schedulerpb.Schedule
	require.Eventually(t, func() bool {
		schedule = getSchedule(t, name)
		return predicate(schedule)
	}, timeout, 20*time.Millisecond, "schedule %s never matched; last: %v", name, schedule)
	return schedule
}

// rewindSchedule places the schedule's next tick in the past behind the API's
// back, so a test observes a tick pass without waiting for a cron occurrence.
func rewindSchedule(t *testing.T, name string, tick time.Time) {
	t.Helper()
	postgresClient, err := satEnvironment.GetPostgresClient(ctx, "scheduler")
	require.NoError(t, err)
	_, err = postgresClient.Exec(ctx, "UPDATE scheduler.schedule SET next_schedule_time = $2 WHERE schedule_id = $1", resourceID(name), tick.UTC())
	require.NoError(t, err)
}

// pastTick is a whole minute in the past at Postgres' precision.
func pastTick(ago time.Duration) time.Time {
	return time.Now().UTC().Add(-ago).Truncate(time.Minute)
}

// scheduleJobs lists the jobs the schedule created, under its parent.
func scheduleJobs(t *testing.T, schedule *schedulerpb.Schedule) []*schedulerpb.Job {
	t.Helper()
	listJobsRequest := &schedulerservicepb.ListJobsRequest{
		Parent: parentOf(schedule.GetName()),
		Filter: fmt.Sprintf("schedule = %q", schedule.GetName()),
	}
	jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, schedulerServiceClient.ListJobs)
	require.NoError(t, err)
	return jobs
}

// parentOf strips the trailing collection and id from a resource name.
func parentOf(name string) string {
	segments := strings.Split(name, "/")
	return strings.Join(segments[:len(segments)-2], "/")
}

func nextTick(t *testing.T, schedule *schedulerpb.Schedule, after time.Time) time.Time {
	t.Helper()
	expression, err := cron.Parse(schedule.GetCron(), schedule.GetTimeZone())
	require.NoError(t, err)
	return expression.Next(after)
}

func TestSchedule_CRUD(t *testing.T) {
	t.Parallel()
	parent := scheduleParents()["organization"]
	declared := newSchedule(t, rareCron)
	declared.Labels = map[string]string{"team": "core"}
	declared.Priority = 5
	declared.RunWindow = durationpb.New(time.Hour)
	created := createSchedule(t, parent, declared)
	require.Regexp(t, `^organizations/[^/]+/schedules/[a-z0-9]+$`, created.GetName())
	require.Equal(t, parent, parentOf(created.GetName()))
	require.NotEmpty(t, created.GetEtag())
	require.Equal(t, created.GetCreateTime().AsTime(), created.GetUpdateTime().AsTime())
	require.Equal(t, echoQueue, created.GetQueue())
	require.Equal(t, processorPath+"Echo", created.GetMethod())
	require.Equal(t, schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED, created.GetState())
	require.Equal(t, rareCron, created.GetCron())
	require.Equal(t, "UTC", created.GetTimeZone(), "the time zone defaults to UTC")
	require.Equal(t, int32(5), created.GetPriority())
	require.Equal(t, time.Hour, created.GetRunWindow().AsDuration())
	require.Equal(t, map[string]string{"team": "core"}, created.GetLabels())
	require.True(t, nextTick(t, created, created.GetCreateTime().AsTime()).Equal(created.GetNextScheduleTime().AsTime()), "the first tick is the first occurrence after creation")
	require.Nil(t, created.GetLastScheduleTime())
	require.Empty(t, created.GetLastJob())
	require.Zero(t, created.GetMissedTickCount())
	grpcrequire.Equal(t, created, getSchedule(t, created.GetName()))

	batchGetSchedulesRequest := &schedulerservicepb.BatchGetSchedulesRequest{Names: []string{created.GetName()}}
	batchGetSchedulesResponse, err := schedulerServiceClient.BatchGetSchedules(ctx, batchGetSchedulesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetSchedulesResponse.GetSchedules(), 1)
	grpcrequire.Equal(t, created, batchGetSchedulesResponse.GetSchedules()[0])

	// Listed under its parent and by state; absent from the root.
	listSchedulesRequest := &schedulerservicepb.ListSchedulesRequest{Parent: parent, Filter: "state = SCHEDULE_STATE_ENABLED", OrderBy: "next_schedule_time"}
	schedules, err := aip.Paginate[*schedulerpb.Schedule](ctx, listSchedulesRequest, schedulerServiceClient.ListSchedules)
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	grpcrequire.Equal(t, created, schedules[0])
	listSchedulesRequest = &schedulerservicepb.ListSchedulesRequest{Parent: parent, Filter: "state = SCHEDULE_STATE_PAUSED"}
	schedules, err = aip.Paginate[*schedulerpb.Schedule](ctx, listSchedulesRequest, schedulerServiceClient.ListSchedules)
	require.NoError(t, err)
	require.Empty(t, schedules)
	listSchedulesRequest = &schedulerservicepb.ListSchedulesRequest{Filter: fmt.Sprintf(`create_time >= "%s"`, created.GetCreateTime().AsTime().Format(time.RFC3339Nano))}
	schedules, err = aip.Paginate[*schedulerpb.Schedule](ctx, listSchedulesRequest, schedulerServiceClient.ListSchedules)
	require.NoError(t, err)
	require.False(t, slices.ContainsFunc(schedules, func(schedule *schedulerpb.Schedule) bool { return schedule.GetName() == created.GetName() }))

	// Labels move without touching the recurrence.
	updateScheduleRequest := &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), Etag: created.GetEtag(), Labels: map[string]string{"team": "platform"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}
	updated, err := schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"team": "platform"}, updated.GetLabels())
	require.NotEqual(t, created.GetEtag(), updated.GetEtag())
	require.True(t, updated.GetUpdateTime().AsTime().After(created.GetUpdateTime().AsTime()))
	grpcrequire.Equal(t, created, updated, protocmp.IgnoreFields(&schedulerpb.Schedule{}, "etag", "update_time", "labels"))
	grpcrequire.Equal(t, updated, getSchedule(t, created.GetName()))

	// The stale etag is refused.
	_, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	grpcrequire.Error(t, codes.Aborted, err)

	// A new cron moves the next tick; a new zone too.
	updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), Cron: "0 0 1 7 *"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cron"}},
	}
	updated, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, "0 0 1 7 *", updated.GetCron())
	require.True(t, nextTick(t, updated, updated.GetUpdateTime().AsTime()).Equal(updated.GetNextScheduleTime().AsTime()))
	require.False(t, updated.GetNextScheduleTime().AsTime().Equal(created.GetNextScheduleTime().AsTime()))
	updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), TimeZone: "America/New_York"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_zone"}},
	}
	rezoned, err := schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, "America/New_York", rezoned.GetTimeZone())
	require.Equal(t, 4*time.Hour, rezoned.GetNextScheduleTime().AsTime().Sub(updated.GetNextScheduleTime().AsTime()), "midnight New York is 04:00 UTC in July")

	// What the scheduler owns is not updatable.
	for _, path := range []string{"state", "queue", "method", "payload", "next_schedule_time", "last_job", "missed_tick_count", "name"} {
		updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
			Schedule:   &schedulerpb.Schedule{Name: created.GetName(), State: schedulerpb.ScheduleState_SCHEDULE_STATE_PAUSED, Queue: sleepQueue, Method: "forged", Payload: created.GetPayload(), MissedTickCount: 3, LastJob: "jobs/x"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}},
		}
		_, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	}
	// Nor may an update break the resource.
	updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), Cron: "not cron"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cron"}},
	}
	_, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
	updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), Priority: 101},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	}
	_, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
	updateScheduleRequest = &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName() + "x", Cron: rareCron},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cron"}},
	}
	_, err = schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	grpcrequire.Error(t, codes.NotFound, err)

	deleteScheduleRequest := &schedulerservicepb.DeleteScheduleRequest{Name: created.GetName(), Etag: created.GetEtag()}
	_, err = schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	deleteScheduleRequest.Etag = ""
	_, err = schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.GetSchedule(ctx, &schedulerservicepb.GetScheduleRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestSchedule_Validation(t *testing.T) {
	t.Parallel()
	create := func(schedule *schedulerpb.Schedule) error {
		createScheduleRequest := &schedulerservicepb.CreateScheduleRequest{Schedule: schedule}
		_, err := schedulerServiceClient.CreateSchedule(ctx, createScheduleRequest)
		return err
	}
	t.Run("missing payload", func(t *testing.T) {
		t.Parallel()
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Schedule{Cron: rareCron}))
	})
	t.Run("payload no queue routes", func(t *testing.T) {
		t.Parallel()
		grpcrequire.Error(t, codes.InvalidArgument, create(&schedulerpb.Schedule{Payload: mustAny(t, &processorpb.UnroutedRequest{}), Cron: rareCron}))
	})
	t.Run("missing cron", func(t *testing.T) {
		t.Parallel()
		grpcrequire.Error(t, codes.InvalidArgument, create(newSchedule(t, "")))
	})
	t.Run("bad cron", func(t *testing.T) {
		t.Parallel()
		for _, expression := range []string{"* * * *", "* * * * * *", "@hourly", "61 * * * *", "TZ=UTC * * * * *"} {
			grpcrequire.Error(t, codes.InvalidArgument, create(newSchedule(t, expression)))
		}
	})
	t.Run("bad time zone", func(t *testing.T) {
		t.Parallel()
		schedule := newSchedule(t, rareCron)
		schedule.TimeZone = "Mars/Olympus_Mons"
		grpcrequire.Error(t, codes.InvalidArgument, create(schedule))
	})
	t.Run("zero run window", func(t *testing.T) {
		t.Parallel()
		schedule := newSchedule(t, rareCron)
		schedule.RunWindow = durationpb.New(0)
		grpcrequire.Error(t, codes.InvalidArgument, create(schedule))
	})
	t.Run("priority out of range", func(t *testing.T) {
		t.Parallel()
		schedule := newSchedule(t, rareCron)
		schedule.Priority = -101
		grpcrequire.Error(t, codes.InvalidArgument, create(schedule))
	})
	t.Run("output only fields are ignored", func(t *testing.T) {
		t.Parallel()
		schedule := newSchedule(t, rareCron)
		schedule.State = schedulerpb.ScheduleState_SCHEDULE_STATE_PAUSED
		schedule.Queue = sleepQueue
		schedule.Method = "forged"
		schedule.MissedTickCount = 4
		schedule.LastJob = "jobs/forged"
		created := createSchedule(t, "", schedule)
		require.Equal(t, schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED, created.GetState())
		require.Equal(t, echoQueue, created.GetQueue())
		require.Equal(t, processorPath+"Echo", created.GetMethod())
		require.Zero(t, created.GetMissedTickCount())
		require.Empty(t, created.GetLastJob())
	})
}

// requireTickJob asserts the job is the one the schedule's tick describes.
func requireTickJob(t *testing.T, schedule *schedulerpb.Schedule, job *schedulerpb.Job, tick, expireTime time.Time) {
	t.Helper()
	require.Equal(t, parentOf(schedule.GetName()), parentOf(job.GetName()), "the job lives under the schedule's parent")
	require.Equal(t, schedule.GetName(), job.GetSchedule())
	for key, value := range schedule.GetLabels() {
		require.Equal(t, value, job.GetLabels()[key])
	}
	require.Equal(t, schedule.GetQueue(), job.GetQueue())
	require.Equal(t, schedule.GetMethod(), job.GetMethod())
	require.Equal(t, schedule.GetPriority(), job.GetPriority())
	require.True(t, proto.Equal(schedule.GetPayload(), job.GetPayload()))
	require.True(t, tick.Equal(job.GetScheduleTime().AsTime()), "schedule_time %v is the tick %v", job.GetScheduleTime().AsTime(), tick)
	require.True(t, expireTime.Equal(job.GetExpireTime().AsTime()), "expire_time %v is the end of the run window %v", job.GetExpireTime().AsTime(), expireTime)
}

func TestSchedule_MaterializesEveryMinute(t *testing.T) {
	t.Parallel()
	for shape, parent := range scheduleParents() {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			declared := newSchedule(t, "* * * * *")
			declared.Labels = map[string]string{"team": "core"}
			declared.Priority = 7
			created := createSchedule(t, parent, declared)
			tick := created.GetNextScheduleTime().AsTime()
			require.Zero(t, tick.Second(), "ticks fall on whole minutes")
			require.True(t, tick.After(created.GetCreateTime().AsTime()))

			// The tick is at most a minute away; the pass follows within a tick interval.
			schedule := waitForSchedule(t, created.GetName(), time.Minute+10*time.Second, func(schedule *schedulerpb.Schedule) bool { return schedule.GetLastJob() != "" })
			require.True(t, tick.Equal(schedule.GetLastScheduleTime().AsTime()))
			require.True(t, tick.Add(time.Minute).Equal(schedule.GetNextScheduleTime().AsTime()))
			require.Zero(t, schedule.GetMissedTickCount())
			job := getJob(t, schedule.GetLastJob())
			// With no run window the job may start until the next tick.
			requireTickJob(t, schedule, job, tick, tick.Add(time.Minute))
			require.Len(t, scheduleJobs(t, schedule), 1)
			waitForState(t, job.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		})
	}
}

func TestSchedule_RewoundTickFiresPromptly(t *testing.T) {
	t.Parallel()
	declared := newSchedule(t, rareCron)
	declared.RunWindow = durationpb.New(time.Hour)
	created := createSchedule(t, "", declared)
	tick := pastTick(time.Minute)
	rewindSchedule(t, created.GetName(), tick)

	schedule := waitForSchedule(t, created.GetName(), tickWait, func(schedule *schedulerpb.Schedule) bool { return schedule.GetLastJob() != "" })
	require.True(t, tick.Equal(schedule.GetLastScheduleTime().AsTime()))
	require.True(t, nextTick(t, schedule, schedule.GetUpdateTime().AsTime()).Equal(schedule.GetNextScheduleTime().AsTime()), "the next tick is the first occurrence after the pass, not after the rewound tick")
	requireTickJob(t, schedule, getJob(t, schedule.GetLastJob()), tick, tick.Add(time.Hour))
}

func TestSchedule_MissedTick(t *testing.T) {
	t.Parallel()
	declared := newSchedule(t, rareCron)
	declared.RunWindow = durationpb.New(30 * time.Second)
	created := createSchedule(t, "", declared)
	rewindSchedule(t, created.GetName(), pastTick(2*time.Minute))

	schedule := waitForSchedule(t, created.GetName(), tickWait, func(schedule *schedulerpb.Schedule) bool { return schedule.GetMissedTickCount() == 1 })
	require.Empty(t, schedule.GetLastJob())
	require.Nil(t, schedule.GetLastScheduleTime())
	require.True(t, schedule.GetNextScheduleTime().AsTime().After(time.Now()))
	require.Empty(t, scheduleJobs(t, schedule))
}

func TestSchedule_TickFiresOnce(t *testing.T) {
	t.Parallel()
	// Two replicas run the tick routine against the same database.
	created := createSchedule(t, scheduleParents()["user"], newSchedule(t, rareCron))
	tick := pastTick(time.Minute)
	rewindSchedule(t, created.GetName(), tick)
	schedule := waitForSchedule(t, created.GetName(), tickWait, func(schedule *schedulerpb.Schedule) bool { return schedule.GetLastJob() != "" })
	time.Sleep(tickSettle)
	jobs := scheduleJobs(t, schedule)
	require.Len(t, jobs, 1)
	require.Equal(t, schedule.GetLastJob(), jobs[0].GetName())

	// A replayed pass (a crash between the insert and the advance) lands on the same job.
	rewindSchedule(t, created.GetName(), tick)
	replayed := waitForSchedule(t, created.GetName(), tickWait, func(schedule *schedulerpb.Schedule) bool { return schedule.GetNextScheduleTime().AsTime().After(tick) })
	require.Equal(t, schedule.GetLastJob(), replayed.GetLastJob())
	require.True(t, tick.Equal(replayed.GetLastScheduleTime().AsTime()))
	require.Len(t, scheduleJobs(t, replayed), 1)
}

func TestSchedule_PauseResume(t *testing.T) {
	t.Parallel()
	created := createSchedule(t, "", newSchedule(t, rareCron))

	pauseScheduleRequest := &schedulerservicepb.PauseScheduleRequest{Name: created.GetName(), Etag: created.GetEtag()}
	paused, err := schedulerServiceClient.PauseSchedule(ctx, pauseScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, schedulerpb.ScheduleState_SCHEDULE_STATE_PAUSED, paused.GetState())
	require.Nil(t, paused.GetNextScheduleTime(), "a paused schedule has no next tick")
	require.NotEqual(t, created.GetEtag(), paused.GetEtag())
	// The stale etag is refused; without one the pause is idempotent.
	_, err = schedulerServiceClient.PauseSchedule(ctx, pauseScheduleRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	pauseScheduleRequest.Etag = ""
	again, err := schedulerServiceClient.PauseSchedule(ctx, pauseScheduleRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, paused, again)

	// A due tick of a paused schedule is not materialized.
	tick := pastTick(time.Minute)
	rewindSchedule(t, created.GetName(), tick)
	time.Sleep(tickSettle)
	schedule := getSchedule(t, created.GetName())
	require.Empty(t, schedule.GetLastJob())
	require.True(t, tick.Equal(schedule.GetNextScheduleTime().AsTime()), "the tick routine leaves paused schedules alone")
	require.Empty(t, scheduleJobs(t, schedule))

	// Resuming picks up from the first occurrence after now, not from the missed tick.
	resumeScheduleRequest := &schedulerservicepb.ResumeScheduleRequest{Name: created.GetName(), Etag: paused.GetEtag()}
	resumed, err := schedulerServiceClient.ResumeSchedule(ctx, resumeScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED, resumed.GetState())
	require.True(t, nextTick(t, resumed, resumed.GetUpdateTime().AsTime()).Equal(resumed.GetNextScheduleTime().AsTime()))
	require.Zero(t, resumed.GetMissedTickCount())
	_, err = schedulerServiceClient.ResumeSchedule(ctx, resumeScheduleRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	resumeScheduleRequest.Etag = ""
	again, err = schedulerServiceClient.ResumeSchedule(ctx, resumeScheduleRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, resumed, again)
	time.Sleep(tickSettle)
	require.Empty(t, scheduleJobs(t, resumed))

	_, err = schedulerServiceClient.PauseSchedule(ctx, &schedulerservicepb.PauseScheduleRequest{Name: created.GetName() + "x"})
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestSchedule_UpdateWhilePausedKeepsNoTick(t *testing.T) {
	t.Parallel()
	created := createSchedule(t, "", newSchedule(t, rareCron))
	_, err := schedulerServiceClient.PauseSchedule(ctx, &schedulerservicepb.PauseScheduleRequest{Name: created.GetName()})
	require.NoError(t, err)
	updateScheduleRequest := &schedulerservicepb.UpdateScheduleRequest{
		Schedule:   &schedulerpb.Schedule{Name: created.GetName(), Cron: "* * * * *"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cron"}},
	}
	updated, err := schedulerServiceClient.UpdateSchedule(ctx, updateScheduleRequest)
	require.NoError(t, err)
	require.Equal(t, "* * * * *", updated.GetCron())
	require.Nil(t, updated.GetNextScheduleTime(), "a recurrence change on a paused schedule waits for the resume")
	resumed, err := schedulerServiceClient.ResumeSchedule(ctx, &schedulerservicepb.ResumeScheduleRequest{Name: created.GetName()})
	require.NoError(t, err)
	require.Zero(t, resumed.GetNextScheduleTime().AsTime().Second())
	// Pause again before it fires.
	_, err = schedulerServiceClient.PauseSchedule(ctx, &schedulerservicepb.PauseScheduleRequest{Name: created.GetName()})
	require.NoError(t, err)
}

func TestSchedule_DeleteLeavesJobs(t *testing.T) {
	t.Parallel()
	created := createSchedule(t, scheduleParents()["organization"], newSchedule(t, rareCron))
	rewindSchedule(t, created.GetName(), pastTick(time.Minute))
	schedule := waitForSchedule(t, created.GetName(), tickWait, func(schedule *schedulerpb.Schedule) bool { return schedule.GetLastJob() != "" })

	deleteScheduleRequest := &schedulerservicepb.DeleteScheduleRequest{Name: created.GetName()}
	_, err := schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.GetSchedule(ctx, &schedulerservicepb.GetScheduleRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.NotFound, err)

	job := waitForTerminal(t, schedule.GetLastJob())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Equal(t, created.GetName(), job.GetSchedule())
	jobs := scheduleJobs(t, schedule)
	require.True(t, slices.ContainsFunc(jobs, func(listed *schedulerpb.Job) bool { return listed.GetName() == job.GetName() }))
}
