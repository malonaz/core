package sat

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// createTarget creates a target private to a test, deleted once the test ends.
func createTarget(t *testing.T, url string) *schedulerpb.Target {
	t.Helper()
	createTargetRequest := &schedulerservicepb.CreateTargetRequest{Target: &schedulerpb.Target{Url: url}}
	target, err := schedulerServiceClient.CreateTarget(ctx, createTargetRequest)
	require.NoError(t, err)
	t.Cleanup(func() {
		deleteTargetRequest := &schedulerservicepb.DeleteTargetRequest{Name: target.GetName(), AllowMissing: true}
		_, err := schedulerServiceClient.DeleteTarget(ctx, deleteTargetRequest)
		require.NoError(t, err)
	})
	return target
}

func TestTarget_CRUD(t *testing.T) {
	t.Parallel()
	created := createTarget(t, processorURL)
	require.Regexp(t, `^targets/[a-z0-9]+$`, created.GetName())
	require.NotEmpty(t, created.GetEtag())
	require.Equal(t, created.GetCreateTime().AsTime(), created.GetUpdateTime().AsTime())
	require.Equal(t, processorURL, created.GetUrl())

	getTargetRequest := &schedulerservicepb.GetTargetRequest{Name: created.GetName()}
	got, err := schedulerServiceClient.GetTarget(ctx, getTargetRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, created, got)

	batchGetTargetsRequest := &schedulerservicepb.BatchGetTargetsRequest{Names: []string{created.GetName(), targetName}}
	batchGetTargetsResponse, err := schedulerServiceClient.BatchGetTargets(ctx, batchGetTargetsRequest)
	require.NoError(t, err)
	require.Len(t, batchGetTargetsResponse.GetTargets(), 2)
	grpcrequire.Equal(t, created, batchGetTargetsResponse.GetTargets()[0])

	listTargetsRequest := &schedulerservicepb.ListTargetsRequest{Filter: fmt.Sprintf(`create_time >= "%s"`, created.GetCreateTime().AsTime().Format(time.RFC3339Nano))}
	targets, err := aip.Paginate[*schedulerpb.Target](ctx, listTargetsRequest, schedulerServiceClient.ListTargets)
	require.NoError(t, err)
	index := slices.IndexFunc(targets, func(target *schedulerpb.Target) bool { return target.GetName() == created.GetName() })
	require.GreaterOrEqual(t, index, 0)
	grpcrequire.Equal(t, created, targets[index])

	updateTargetRequest := &schedulerservicepb.UpdateTargetRequest{
		Target:     &schedulerpb.Target{Name: created.GetName(), Headers: map[string]string{"x-team": "core"}, Etag: created.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"headers"}},
	}
	updated, err := schedulerServiceClient.UpdateTarget(ctx, updateTargetRequest)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"x-team": "core"}, updated.GetHeaders())
	require.NotEqual(t, created.GetEtag(), updated.GetEtag())
	grpcrequire.Equal(t, created, updated, protocmp.IgnoreFields(&schedulerpb.Target{}, "headers", "etag", "update_time"))

	// The stale etag is refused, on update and delete alike.
	_, err = schedulerServiceClient.UpdateTarget(ctx, updateTargetRequest)
	grpcrequire.Error(t, codes.Aborted, err)
	deleteTargetRequest := &schedulerservicepb.DeleteTargetRequest{Name: created.GetName(), Etag: created.GetEtag()}
	_, err = schedulerServiceClient.DeleteTarget(ctx, deleteTargetRequest)
	grpcrequire.Error(t, codes.Aborted, err)

	deleteTargetRequest.Etag = updated.GetEtag()
	_, err = schedulerServiceClient.DeleteTarget(ctx, deleteTargetRequest)
	require.NoError(t, err)
	_, err = schedulerServiceClient.GetTarget(ctx, getTargetRequest)
	grpcrequire.Error(t, codes.NotFound, err)
	_, err = schedulerServiceClient.DeleteTarget(ctx, &schedulerservicepb.DeleteTargetRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestTarget_Validation(t *testing.T) {
	t.Parallel()
	for _, url := range []string{"", "localhost:9091", "ftp://localhost:9091", "http://localhost:notaport"} {
		t.Run(url, func(t *testing.T) {
			createTargetRequest := &schedulerservicepb.CreateTargetRequest{Target: &schedulerpb.Target{Url: url}}
			_, err := schedulerServiceClient.CreateTarget(ctx, createTargetRequest)
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}
	target := createTarget(t, processorURL)
	updateTargetRequest := &schedulerservicepb.UpdateTargetRequest{
		Target:     &schedulerpb.Target{Name: target.GetName(), Url: "http://localhost:notaport"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"url"}},
	}
	_, err := schedulerServiceClient.UpdateTarget(ctx, updateTargetRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestTarget_DeleteWhileReferenced(t *testing.T) {
	t.Parallel()
	target := createTarget(t, processorURL)
	createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: &schedulerpb.Queue{
		Policy:   &schedulerpb.QueuePolicy{AttemptTimeout: durationpb.New(time.Second), MaxAttempts: 1},
		Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: target.GetName()}},
	}}
	queue, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err)

	deleteTargetRequest := &schedulerservicepb.DeleteTargetRequest{Name: target.GetName()}
	_, err = schedulerServiceClient.DeleteTarget(ctx, deleteTargetRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)

	_, err = schedulerServiceClient.DeleteQueue(ctx, &schedulerservicepb.DeleteQueueRequest{Name: queue.GetName()})
	require.NoError(t, err)
	_, err = schedulerServiceClient.DeleteTarget(ctx, deleteTargetRequest)
	require.NoError(t, err)
}

func TestTarget_UpdateRedials(t *testing.T) {
	t.Parallel()
	// A private target and queue, so the shared processor route is untouched.
	target := createTarget(t, processorURL)
	createQueueRequest := &schedulerservicepb.CreateQueueRequest{Queue: &schedulerpb.Queue{
		Policy: &schedulerpb.QueuePolicy{
			AttemptTimeout: durationpb.New(2 * time.Second),
			MaxAttempts:    100,
			RetryBackoff:   &schedulerpb.RetryBackoff{Initial: durationpb.New(200 * time.Millisecond), Max: durationpb.New(200 * time.Millisecond), Multiplier: 1},
		},
		Handlers: []*schedulerpb.Handler{{Method: processorPath + "Echo", Target: target.GetName()}},
	}}
	queue, err := schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err)
	t.Cleanup(func() { deleteQueueOnceIdle(t, queue.GetName()) })

	setURL := func(t *testing.T, url string) {
		t.Helper()
		updateTargetRequest := &schedulerservicepb.UpdateTargetRequest{
			Target:     &schedulerpb.Target{Name: target.GetName(), Url: url},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"url"}},
		}
		_, err := schedulerServiceClient.UpdateTarget(ctx, updateTargetRequest)
		require.NoError(t, err)
	}

	live := createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: uuid.MustNewV7().String()})
	waitForState(t, live.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)

	// Pointed at a dead port, attempts fail UNAVAILABLE and are retried.
	setURL(t, deadURL)
	value := uuid.MustNewV7().String()
	dead := createJobIn(t, "", queue.GetName(), &processorpb.EchoRequest{Value: value})
	failing := waitForJob(t, dead.GetName(), func(job *schedulerpb.Job) bool { return job.GetAttemptCount() >= 2 })
	require.Equal(t, int32(codes.Unavailable), failing.GetMetadata().GetAttempts()[0].GetError().GetCode())
	require.Empty(t, testProcessor.calls(value))

	// Back on the live port, the next attempt goes through.
	setURL(t, processorURL)
	job := waitForTerminal(t, dead.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Len(t, testProcessor.calls(value), 1)
}
