package sat

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/binary"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/go/uuid"
)

// The discovery tests boot short-lived replicas that claim jobs like any
// other, so they run before the parallel tests rather than among them. Each
// replica needs grpc, health and metrics ports of its own.
var replicaPorts atomic.Int32

func init() { replicaPorts.Store(9200) }

// replicaOutput collects a replica's stdout and stderr, piped concurrently.
type replicaOutput struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (o *replicaOutput) Write(p []byte) (int, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.buffer.Write(p)
}

func (o *replicaOutput) String() string {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.buffer.String()
}

// runReplica boots a replica dispatching to the endpoints and stops it once it
// serves, returning its output and the error it exited with, if any.
func runReplica(t *testing.T, endpoints ...string) (string, error) {
	t.Helper()
	port := int(replicaPorts.Add(3))
	sut := schedulerSUT(fmt.Sprintf("%s-replica-%d", schedulerServiceName, port), port, port+1, port+2, endpoints...)
	output := &replicaOutput{}
	replica, err := binary.New(sut.Path, append(sut.Args, "--logging.format", logging.FormatRaw)...)
	require.NoError(t, err)
	replica = replica.WithName(sut.Name).WithPort(sut.Port).WithLogger(slog.New(logging.NewRawHandler(output, nil)))
	if err := replica.RunAsync(); err != nil {
		return output.String(), err
	}
	replica.Stop()
	return output.String(), nil
}

func TestDiscover_Idempotent(t *testing.T) {
	// The two boot replicas already converged; a third changes nothing.
	before := getQueue(t, echoQueue)
	output, err := runReplica(t, processorURL)
	require.NoError(t, err, output)
	require.NotContains(t, output, "updated queue")
	grpcrequire.Equal(t, before, getQueue(t, echoQueue), protocmp.IgnoreFields(&schedulerpb.Queue{}, "stats"))
}

func TestDiscover_RevertsDrift(t *testing.T) {
	// A policy changed through the API is put back to the declaration, its
	// state kept; a queue no endpoint serves is reported and left alone.
	queue := getQueue(t, tunableQueue)
	updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: queue.GetName(), Policy: &policypb.QueuePolicy{MaxAttempts: 7}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
	}
	drifted, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
	require.EqualValues(t, 7, drifted.GetPolicy().GetMaxAttempts())
	paused, err := schedulerServiceClient.PauseQueue(ctx, &schedulerservicepb.PauseQueueRequest{Name: queue.GetName()})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = schedulerServiceClient.ResumeQueue(ctx, &schedulerservicepb.ResumeQueueRequest{Name: queue.GetName()})
	})
	undeclared := declareQueue(t, deadURL, newPolicy())

	output, err := runReplica(t, processorURL)
	require.NoError(t, err, output)
	require.Contains(t, output, "updated queue")
	require.Contains(t, output, undeclared.GetName())

	reverted := getQueue(t, queue.GetName())
	grpcrequire.Equal(t, queue.GetPolicy(), reverted.GetPolicy())
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_PAUSED, reverted.GetState())
	require.NotEqual(t, paused.GetEtag(), reverted.GetEtag())
}

func TestDiscover_BadEndpointFailsStartup(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		endpoint string
		message  string
	}{
		{name: "unreachable", endpoint: deadURL, message: "Unavailable"},
		{name: "no reflection", endpoint: bareURL, message: "does not serve gRPC reflection"},
		{name: "malformed", endpoint: "nonsense://", message: "parsing url"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			output, err := runReplica(t, processorURL, testCase.endpoint)
			require.Error(t, err)
			require.Contains(t, output, "starting scheduler-dispatcher")
			require.Contains(t, output, testCase.message)
		})
	}
}

func TestDiscover_EndpointMoves(t *testing.T) {
	// The redial queue is this test's alone. Pointed at a dead port, attempts
	// fail UNAVAILABLE and are retried; back on the live one, the next attempt
	// goes through on the new connection.
	setEndpoint := func(t *testing.T, endpoint string) {
		t.Helper()
		updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: redialQueue, Endpoint: endpoint},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"endpoint"}},
		}
		_, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		require.NoError(t, err)
	}
	t.Cleanup(func() { setEndpoint(t, processorURL) })

	live := createJob(t, &processorpb.RedialRequest{Value: uuid.MustNewV7().String()})
	waitForState(t, live.GetName(), schedulerpb.JobState_JOB_STATE_SUCCEEDED)

	setEndpoint(t, deadURL)
	value := uuid.MustNewV7().String()
	dead := createJob(t, &processorpb.RedialRequest{Value: value})
	failing := waitForJob(t, dead.GetName(), func(job *schedulerpb.Job) bool { return job.GetAttemptCount() >= 2 })
	require.Equal(t, int32(codes.Unavailable), failing.GetMetadata().GetAttempts()[0].GetError().GetCode())
	require.Empty(t, testProcessor.calls(value))

	setEndpoint(t, processorURL)
	job := waitForTerminal(t, dead.GetName())
	require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
	require.Len(t, testProcessor.calls(value), 1)
	require.Less(t, time.Since(job.GetCompleteTime().AsTime()), waitTimeout)
}
