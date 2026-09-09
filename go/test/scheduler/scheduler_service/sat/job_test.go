package sat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.einride.tech/aip/resourcename"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// farFuture keeps a job PENDING for the whole test. Postgres stores microseconds.
var farFuture = time.Now().Add(24 * time.Hour).Truncate(time.Microsecond)

func TestCreateJob_PopulatesServerFields(t *testing.T) {
	t.Parallel()
	job := createJob(t, &processorpb.EchoRequest{Value: uuid.MustNewV7().String()},
		scheduler.WithScheduleTime(farFuture),
		scheduler.WithLabels(map[string]string{"team": "core"}),
	)
	require.Regexp(t, `^jobs/[a-z0-9]+$`, job.GetName())
	require.NotEmpty(t, job.GetEtag())
	require.NotNil(t, job.GetCreateTime())
	require.Equal(t, job.GetCreateTime().AsTime(), job.GetUpdateTime().AsTime())
	require.Equal(t, jobType(&processorpb.EchoRequest{}), job.GetJobType())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, job.GetState())
	require.Equal(t, map[string]string{"team": "core"}, job.GetLabels())
	require.True(t, farFuture.Equal(job.GetScheduleTime().AsTime()))
	require.Zero(t, job.GetAttemptCount())

	grpcrequire.Equal(t, job, getJob(t, job.GetName()))
}

func TestCreateJob_IgnoresOutputOnlyFields(t *testing.T) {
	t.Parallel()
	createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"}, scheduler.WithScheduleTime(farFuture))
	require.NoError(t, err)
	createJobRequest.Job.State = schedulerpb.JobState_JOB_STATE_SUCCEEDED
	createJobRequest.Job.AttemptCount = 7
	createJobRequest.Job.JobType = "forged"
	createJobRequest.Job.StartTime = timestamppb.Now()
	job, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, job.GetState())
	require.Zero(t, job.GetAttemptCount())
	require.Equal(t, jobType(&processorpb.EchoRequest{}), job.GetJobType())
	require.Nil(t, job.GetStartTime())
}

func TestCreateJob_Validation(t *testing.T) {
	t.Parallel()
	t.Run("missing payload", func(t *testing.T) {
		t.Parallel()
		createJobRequest := &schedulerservicepb.CreateJobRequest{Job: &schedulerpb.Job{}}
		_, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
	t.Run("bad label value", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"},
			scheduler.WithLabels(map[string]string{"k": "Not Valid!"}))
		require.NoError(t, err)
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
	t.Run("bad request id", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"})
		require.NoError(t, err)
		createJobRequest.RequestId = "not-a-uuid"
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
	t.Run("validate only", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"})
		require.NoError(t, err)
		createJobRequest.ValidateOnly = true
		job, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
		require.NoError(t, err)
		_, err = schedulerServiceClient.GetJob(ctx, &schedulerservicepb.GetJobRequest{Name: job.GetName()})
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestCreateJob_Idempotent(t *testing.T) {
	t.Parallel()
	requestID := uuid.MustNewV7().String()
	newRequest := func(t *testing.T) *schedulerservicepb.CreateJobRequest {
		t.Helper()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithScheduleTime(farFuture))
		require.NoError(t, err)
		createJobRequest.RequestId = requestID
		return createJobRequest
	}
	first, err := schedulerServiceClient.CreateJob(ctx, newRequest(t))
	require.NoError(t, err)
	second, err := schedulerServiceClient.CreateJob(ctx, newRequest(t))
	require.NoError(t, err)
	grpcrequire.Equal(t, first, second)

	// The same request id with an explicit, different job id still resolves to the first job.
	createJobRequest := newRequest(t)
	createJobRequest.JobId = "other-" + uuid.MustNewV7().String()
	third, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	require.Equal(t, first.GetName(), third.GetName())

	// A job id collision with a fresh request id is a conflict.
	createJobRequest, err = scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithScheduleTime(farFuture))
	require.NoError(t, err)
	createJobRequest.JobId = first.GetName()[len("jobs/"):]
	_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
	grpcrequire.Error(t, codes.AlreadyExists, err)
}

func TestGetJob_NotFound(t *testing.T) {
	t.Parallel()
	_, err := schedulerServiceClient.GetJob(ctx, &schedulerservicepb.GetJobRequest{Name: "jobs/does-not-exist"})
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestBatchGetJobs(t *testing.T) {
	t.Parallel()
	first := createJob(t, &processorpb.EchoRequest{Value: "batch-1"}, scheduler.WithScheduleTime(farFuture))
	second := createJob(t, &processorpb.EchoRequest{Value: "batch-2"}, scheduler.WithScheduleTime(farFuture))

	batchGetJobsRequest := &schedulerservicepb.BatchGetJobsRequest{Names: []string{first.GetName(), second.GetName()}}
	batchGetJobsResponse, err := schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
	require.NoError(t, err)
	require.Len(t, batchGetJobsResponse.GetJobs(), 2)
	grpcrequire.Equal(t, first, batchGetJobsResponse.GetJobs()[0])
	grpcrequire.Equal(t, second, batchGetJobsResponse.GetJobs()[1])

	batchGetJobsRequest.Names = append(batchGetJobsRequest.Names, "jobs/does-not-exist")
	_, err = schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestUpdateJob(t *testing.T) {
	t.Parallel()

	t.Run("labels and etag", func(t *testing.T) {
		t.Parallel()
		job := createJob(t, &processorpb.EchoRequest{Value: "update"}, scheduler.WithScheduleTime(farFuture))

		updateJobRequest := &schedulerservicepb.UpdateJobRequest{
			Job:        &schedulerpb.Job{Name: job.GetName(), Labels: map[string]string{"owner": "sat"}, Etag: job.GetEtag()},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updated, err := schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"owner": "sat"}, updated.GetLabels())
		require.NotEqual(t, job.GetEtag(), updated.GetEtag())
		require.True(t, updated.GetUpdateTime().AsTime().After(job.GetUpdateTime().AsTime()))
		grpcrequire.Equal(t, job, updated, protocmp.IgnoreFields(&schedulerpb.Job{}, "labels", "etag", "update_time"))

		// The stale etag is refused.
		_, err = schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("output only fields are not updatable", func(t *testing.T) {
		t.Parallel()
		job := createJob(t, &processorpb.EchoRequest{Value: "update"}, scheduler.WithScheduleTime(farFuture))
		updateJobRequest := &schedulerservicepb.UpdateJobRequest{
			Job:        &schedulerpb.Job{Name: job.GetName(), State: schedulerpb.JobState_JOB_STATE_SUCCEEDED},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
		}
		_, err := schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("reschedule pending job", func(t *testing.T) {
		t.Parallel()
		value := uuid.MustNewV7().String()
		job := createJob(t, &processorpb.EchoRequest{Value: value}, scheduler.WithScheduleTime(farFuture))
		updateJobRequest := &schedulerservicepb.UpdateJobRequest{
			Job:        &schedulerpb.Job{Name: job.GetName(), ScheduleTime: timestamppb.Now()},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_time"}},
		}
		_, err := schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		require.NoError(t, err)
		// Pulling the schedule in gets the job run.
		waitForState(t, job.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		require.Len(t, testProcessor.calls(value), 1)

		// Once terminal, the schedule is frozen.
		_, err = schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})
}

func TestDeleteJob(t *testing.T) {
	t.Parallel()

	t.Run("pending", func(t *testing.T) {
		t.Parallel()
		job := createJob(t, &processorpb.EchoRequest{Value: "delete"}, scheduler.WithScheduleTime(farFuture))
		_, err := schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: job.GetName(), Etag: "stale"})
		grpcrequire.Error(t, codes.Aborted, err)
		_, err = schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: job.GetName()})
		require.NoError(t, err)
		_, err = schedulerServiceClient.GetJob(ctx, &schedulerservicepb.GetJobRequest{Name: job.GetName()})
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		_, err := schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: "jobs/does-not-exist"})
		grpcrequire.Error(t, codes.NotFound, err)
		_, err = schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: "jobs/does-not-exist", AllowMissing: true})
		require.NoError(t, err)
	})

	t.Run("running", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		job := createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleepTimeout)})
		waitForState(t, job.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
		_, err := schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: job.GetName()})
		grpcrequire.Error(t, codes.FailedPrecondition, err)
		_, err = schedulerServiceClient.CancelJob(ctx, &schedulerservicepb.CancelJobRequest{Name: job.GetName()})
		require.NoError(t, err)
		_, err = schedulerServiceClient.DeleteJob(ctx, &schedulerservicepb.DeleteJobRequest{Name: job.GetName()})
		require.NoError(t, err)
	})
}

func TestListJobs(t *testing.T) {
	t.Parallel()
	// Every job in this test carries a unique label so it can be told apart from the rest of the suite.
	run := uuid.MustNewV7().String()
	labels := func(extra map[string]string) map[string]string {
		labels := map[string]string{"run": run}
		for key, value := range extra {
			labels[key] = value
		}
		return labels
	}
	list := func(t *testing.T, filter, orderBy string) []*schedulerpb.Job {
		t.Helper()
		listJobsRequest := &schedulerservicepb.ListJobsRequest{
			Filter:  fmt.Sprintf(`labels.run = "%s"`, run),
			OrderBy: orderBy,
		}
		if filter != "" {
			listJobsRequest.Filter += " AND " + filter
		}
		jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, schedulerServiceClient.ListJobs)
		require.NoError(t, err)
		return jobs
	}

	// Two that will complete, one that stays pending far in the future, one pending soon.
	done1 := createJob(t, &processorpb.EchoRequest{Value: run + "-1"}, scheduler.WithLabels(labels(map[string]string{"kind": "done"})))
	done2 := createJob(t, &processorpb.EchoRequest{Value: run + "-2"}, scheduler.WithLabels(labels(map[string]string{"kind": "done"})))
	later := createJob(t, &processorpb.EchoRequest{Value: run + "-3"}, scheduler.WithLabels(labels(nil)), scheduler.WithScheduleTime(farFuture))
	sooner := createJob(t, &processorpb.EchoRequest{Value: run + "-4"}, scheduler.WithLabels(labels(nil)), scheduler.WithScheduleTime(farFuture.Add(-time.Hour)))
	waitForState(t, done1.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
	waitForState(t, done2.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)

	names := func(jobs []*schedulerpb.Job) []string {
		names := make([]string, len(jobs))
		for i, job := range jobs {
			names[i] = job.GetName()
		}
		return names
	}

	t.Run("default ordering is create_time desc", func(t *testing.T) {
		require.Equal(t, []string{sooner.GetName(), later.GetName(), done2.GetName(), done1.GetName()}, names(list(t, "", "")))
	})
	t.Run("filter on state", func(t *testing.T) {
		require.ElementsMatch(t, []string{done1.GetName(), done2.GetName()}, names(list(t, "state = JOB_STATE_SUCCEEDED", "")))
		require.ElementsMatch(t, []string{later.GetName(), sooner.GetName()}, names(list(t, "state = JOB_STATE_PENDING", "")))
		require.Empty(t, list(t, "state = JOB_STATE_FAILED", ""))
	})
	t.Run("filter on labels", func(t *testing.T) {
		require.ElementsMatch(t, []string{done1.GetName(), done2.GetName()}, names(list(t, `labels.kind = "done"`, "")))
		require.ElementsMatch(t, []string{later.GetName(), sooner.GetName()}, names(list(t, "NOT labels.kind:*", "")))
	})
	t.Run("filter on job_type", func(t *testing.T) {
		require.Len(t, list(t, fmt.Sprintf(`job_type = "%s"`, jobType(&processorpb.EchoRequest{})), ""), 4)
		require.Empty(t, list(t, fmt.Sprintf(`job_type = "%s"`, jobType(&processorpb.SleepRequest{})), ""))
	})
	t.Run("filter on presence and time", func(t *testing.T) {
		require.ElementsMatch(t, []string{done1.GetName(), done2.GetName()}, names(list(t, "complete_time:*", "")))
		require.ElementsMatch(t, []string{later.GetName(), sooner.GetName()}, names(list(t, "NOT complete_time:*", "")))
		require.ElementsMatch(t, []string{later.GetName(), sooner.GetName()}, names(list(t, fmt.Sprintf(`schedule_time > "%s"`, time.Now().UTC().Format(time.RFC3339)), "")))
		require.ElementsMatch(t, []string{done1.GetName(), done2.GetName()}, names(list(t, "attempt_count > 0", "")))
	})
	t.Run("order by schedule_time", func(t *testing.T) {
		require.Equal(t, []string{sooner.GetName(), later.GetName()}, names(list(t, "state = JOB_STATE_PENDING", "schedule_time asc")))
		require.Equal(t, []string{later.GetName(), sooner.GetName()}, names(list(t, "state = JOB_STATE_PENDING", "schedule_time desc")))
	})
	t.Run("pagination", func(t *testing.T) {
		listJobsRequest := &schedulerservicepb.ListJobsRequest{Filter: fmt.Sprintf(`labels.run = "%s"`, run), OrderBy: "create_time asc", PageSize: 3}
		firstPage, err := schedulerServiceClient.ListJobs(ctx, listJobsRequest)
		require.NoError(t, err)
		require.Len(t, firstPage.GetJobs(), 3)
		require.NotEmpty(t, firstPage.GetNextPageToken())
		listJobsRequest.PageToken = firstPage.GetNextPageToken()
		secondPage, err := schedulerServiceClient.ListJobs(ctx, listJobsRequest)
		require.NoError(t, err)
		require.Len(t, secondPage.GetJobs(), 1)
		require.Empty(t, secondPage.GetNextPageToken())
		require.Equal(t, []string{done1.GetName(), done2.GetName(), later.GetName(), sooner.GetName()}, names(append(firstPage.GetJobs(), secondPage.GetJobs()...)))
	})
	t.Run("invalid filter", func(t *testing.T) {
		listJobsRequest := &schedulerservicepb.ListJobsRequest{Filter: "no_such_field = 1"}
		_, err := schedulerServiceClient.ListJobs(ctx, listJobsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestJob_Parents(t *testing.T) {
	t.Parallel()
	run := uuid.MustNewV7().String()
	organization := (&schedulerpb.OrganizationResourceName{Organization: "org-" + run}).String()
	user := (&schedulerpb.UserResourceName{Organization: "org-" + run, User: "user-" + run}).String()
	otherOrganization := (&schedulerpb.OrganizationResourceName{Organization: "other-" + run}).String()
	labels := scheduler.WithLabels(map[string]string{"run": run})
	filter := fmt.Sprintf(`labels.run = "%s"`, run)

	names := func(jobs []*schedulerpb.Job) []string {
		names := make([]string, len(jobs))
		for i, job := range jobs {
			names[i] = job.GetName()
		}
		return names
	}
	list := func(t *testing.T, parent string) []string {
		t.Helper()
		listJobsRequest := &schedulerservicepb.ListJobsRequest{Parent: parent, Filter: filter}
		jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, schedulerServiceClient.ListJobs)
		require.NoError(t, err)
		return names(jobs)
	}

	systemJob := createJob(t, &processorpb.EchoRequest{Value: run + "-system"}, labels, scheduler.WithScheduleTime(farFuture))
	organizationJob := createJobUnder(t, organization, &processorpb.EchoRequest{Value: run + "-org"}, labels, scheduler.WithScheduleTime(farFuture))
	userJob := createJobUnder(t, user, &processorpb.EchoRequest{Value: run + "-user"}, labels, scheduler.WithScheduleTime(farFuture))
	otherOrganizationJob := createJobUnder(t, otherOrganization, &processorpb.EchoRequest{Value: run + "-other"}, labels, scheduler.WithScheduleTime(farFuture))

	t.Run("names follow the parent", func(t *testing.T) {
		require.Regexp(t, `^jobs/[a-z0-9]+$`, systemJob.GetName())
		require.True(t, resourcename.HasParent(organizationJob.GetName(), organization), organizationJob.GetName())
		require.True(t, resourcename.HasParent(userJob.GetName(), user), userJob.GetName())
		for _, job := range []*schedulerpb.Job{systemJob, organizationJob, userJob} {
			grpcrequire.Equal(t, job, getJob(t, job.GetName()))
		}
	})

	t.Run("list scopes to the parent", func(t *testing.T) {
		require.Equal(t, []string{systemJob.GetName()}, list(t, ""))
		require.Equal(t, []string{organizationJob.GetName()}, list(t, organization))
		require.Equal(t, []string{userJob.GetName()}, list(t, user))
		require.ElementsMatch(t, []string{organizationJob.GetName(), otherOrganizationJob.GetName()}, list(t, "organizations/-"))
		require.Equal(t, []string{userJob.GetName()}, list(t, "organizations/-/users/-"))
		require.Empty(t, list(t, otherOrganization+"/users/-"))
	})

	t.Run("invalid parent", func(t *testing.T) {
		_, err := schedulerServiceClient.ListJobs(ctx, &schedulerservicepb.ListJobsRequest{Parent: "teams/x"})
		grpcrequire.Error(t, codes.InvalidArgument, err)
		createJobRequest, err := scheduler.NewCreateJobRequest("teams/x", &processorpb.EchoRequest{Value: "x"})
		require.NoError(t, err)
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		createJobRequest.Parent = "organizations/-"
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("batch get", func(t *testing.T) {
		batchGetJobsRequest := &schedulerservicepb.BatchGetJobsRequest{Names: []string{systemJob.GetName(), organizationJob.GetName(), userJob.GetName()}}
		batchGetJobsResponse, err := schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
		require.NoError(t, err)
		require.Equal(t, batchGetJobsRequest.GetNames(), names(batchGetJobsResponse.GetJobs()))

		batchGetJobsRequest = &schedulerservicepb.BatchGetJobsRequest{Parent: organization, Names: []string{organizationJob.GetName()}}
		batchGetJobsResponse, err = schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
		require.NoError(t, err)
		require.Equal(t, batchGetJobsRequest.GetNames(), names(batchGetJobsResponse.GetJobs()))

		// A user's job is not a direct child of its organization.
		batchGetJobsRequest.Names = []string{userJob.GetName()}
		_, err = schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		batchGetJobsRequest.Names = []string{otherOrganizationJob.GetName()}
		_, err = schedulerServiceClient.BatchGetJobs(ctx, batchGetJobsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("processor receives the parented name", func(t *testing.T) {
		value := run + "-header"
		job := createJobUnder(t, user, &processorpb.EchoRequest{Value: value})
		waitForState(t, job.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		calls := testProcessor.calls(value)
		require.Len(t, calls, 1)
		require.Equal(t, job.GetName(), calls[0].job)
	})
}

func TestCreateJob_Priority(t *testing.T) {
	t.Parallel()
	t.Run("validation", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"}, scheduler.WithPriority(101))
		require.NoError(t, err)
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("update only while pending", func(t *testing.T) {
		t.Parallel()
		job := createJob(t, &processorpb.EchoRequest{Value: "priority"}, scheduler.WithScheduleTime(farFuture), scheduler.WithPriority(5))
		require.Equal(t, int32(5), job.GetPriority())
		updateJobRequest := &schedulerservicepb.UpdateJobRequest{
			Job:        &schedulerpb.Job{Name: job.GetName(), Priority: -5},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
		}
		updated, err := schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		require.NoError(t, err)
		require.Equal(t, int32(-5), updated.GetPriority())

		cancelJob(t, job.GetName())
		_, err = schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})
}

// Not parallel: it saturates the worker pool, which would stall every other
// test's jobs. Sequential tests run before the parallel ones start.
func TestProcess_PriorityOrder(t *testing.T) {
	// Hold every slot so that exactly one job can be claimed when one is released.
	run := uuid.MustNewV7().String()
	fillers := make([]*schedulerpb.Job, maxParallelJobs)
	for i := range fillers {
		fillers[i] = createJob(t, &processorpb.SleepRequest{Key: fmt.Sprintf("%s-filler-%d", run, i), Duration: durationpb.New(sleepTimeout)})
	}
	for _, filler := range fillers {
		waitForState(t, filler.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
	}
	low := createJob(t, &processorpb.EchoRequest{Value: run + "-low"}, scheduler.WithPriority(-10))
	high := createJob(t, &processorpb.SleepRequest{Key: run + "-high", Duration: durationpb.New(sleepTimeout)}, scheduler.WithPriority(10))
	defer func() {
		for _, filler := range fillers[1:] {
			cancelJob(t, filler.GetName())
		}
	}()

	cancelJob(t, fillers[0].GetName())
	waitForState(t, high.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)
	require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, getJob(t, low.GetName()).GetState(), "the older, lower priority job waits")
	cancelJob(t, high.GetName())
	waitForState(t, low.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
}

func TestCreateJob_UniqueKey(t *testing.T) {
	t.Parallel()

	t.Run("pending jobs coalesce", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		first := createJob(t, &processorpb.EchoRequest{Value: key}, scheduler.WithUniqueKey(key), scheduler.WithScheduleTime(farFuture))
		require.Equal(t, key, first.GetUniqueKey())
		second := createJob(t, &processorpb.EchoRequest{Value: key + "-ignored"}, scheduler.WithUniqueKey(key))
		grpcrequire.Equal(t, first, second)

		// A single run happens, with the first job's payload.
		updateJobRequest := &schedulerservicepb.UpdateJobRequest{
			Job:        &schedulerpb.Job{Name: first.GetName(), ScheduleTime: timestamppb.Now()},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_time"}},
		}
		_, err := schedulerServiceClient.UpdateJob(ctx, updateJobRequest)
		require.NoError(t, err)
		waitForState(t, first.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		require.Len(t, testProcessor.calls(key), 1)
		require.Empty(t, testProcessor.calls(key+"-ignored"))

		// Once terminal, the key is free again.
		third := createJob(t, &processorpb.EchoRequest{Value: key}, scheduler.WithUniqueKey(key), scheduler.WithScheduleTime(farFuture))
		require.NotEqual(t, first.GetName(), third.GetName())
	})

	t.Run("running job gets one trailing run", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		running := createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleepTimeout)}, scheduler.WithUniqueKey(key))
		waitForState(t, running.GetName(), schedulerpb.JobState_JOB_STATE_RUNNING)

		trailing := createJob(t, &processorpb.EchoRequest{Value: key + "-trailing"}, scheduler.WithUniqueKey(key))
		require.NotEqual(t, running.GetName(), trailing.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, trailing.GetState())
		coalesced := createJob(t, &processorpb.EchoRequest{Value: key + "-coalesced"}, scheduler.WithUniqueKey(key))
		require.Equal(t, trailing.GetName(), coalesced.GetName())

		// The trailing run waits for the running one.
		time.Sleep(5 * pollInterval)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, getJob(t, trailing.GetName()).GetState())
		cancelJob(t, running.GetName())
		waitForState(t, trailing.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		require.Len(t, testProcessor.calls(key+"-trailing"), 1)
		require.Empty(t, testProcessor.calls(key+"-coalesced"))
	})

	t.Run("retry conflicts with a pending job", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		done := createJob(t, &processorpb.EchoRequest{Value: key}, scheduler.WithUniqueKey(key))
		waitForState(t, done.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)
		pending := createJob(t, &processorpb.EchoRequest{Value: key}, scheduler.WithUniqueKey(key), scheduler.WithScheduleTime(farFuture))

		_, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: done.GetName()})
		grpcrequire.Error(t, codes.AlreadyExists, err)
		cancelJob(t, pending.GetName())
		retried, err := schedulerServiceClient.RetryJob(ctx, &schedulerservicepb.RetryJobRequest{Name: done.GetName()})
		require.NoError(t, err)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, retried.GetState())
	})

	t.Run("request id is independent", func(t *testing.T) {
		t.Parallel()
		key := uuid.MustNewV7().String()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: key}, scheduler.WithUniqueKey(key), scheduler.WithScheduleTime(farFuture))
		require.NoError(t, err)
		createJobRequest.RequestId = uuid.MustNewV7().String()
		first, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
		require.NoError(t, err)
		cancelJob(t, first.GetName())

		// Replaying the request returns the (now cancelled) job rather than creating a new one under the free key.
		replayed, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
		require.NoError(t, err)
		require.Equal(t, first.GetName(), replayed.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_CANCELLED, replayed.GetState())
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest("", &processorpb.EchoRequest{Value: "x"}, scheduler.WithUniqueKey(strings.Repeat("k", 257)))
		require.NoError(t, err)
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
