package sat

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	createJobRequest, err := scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "x"}, scheduler.WithScheduleTime(farFuture))
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
		createJobRequest, err := scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "x"},
			scheduler.WithLabels(map[string]string{"k": "Not Valid!"}))
		require.NoError(t, err)
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
	t.Run("bad request id", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "x"})
		require.NoError(t, err)
		createJobRequest.RequestId = "not-a-uuid"
		_, err = schedulerServiceClient.CreateJob(ctx, createJobRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
	t.Run("validate only", func(t *testing.T) {
		t.Parallel()
		createJobRequest, err := scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "x"})
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
	key := uuid.MustNewV7().String()
	first := createJob(t, &processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithScheduleTime(farFuture), scheduler.WithIdempotencyKey(key))
	second := createJob(t, &processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithScheduleTime(farFuture), scheduler.WithIdempotencyKey(key))
	grpcrequire.Equal(t, first, second)

	// The same request id with an explicit, different job id still resolves to the first job.
	createJobRequest, err := scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithIdempotencyKey(key))
	require.NoError(t, err)
	createJobRequest.JobId = "other-" + uuid.MustNewV7().String()
	third, err := schedulerServiceClient.CreateJob(ctx, createJobRequest)
	require.NoError(t, err)
	require.Equal(t, first.GetName(), third.GetName())

	// A job id collision with a fresh request id is a conflict.
	createJobRequest, err = scheduler.NewCreateJobRequest(&processorpb.EchoRequest{Value: "idempotent"}, scheduler.WithScheduleTime(farFuture))
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
