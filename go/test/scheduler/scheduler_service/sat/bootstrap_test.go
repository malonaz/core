package sat

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/binary"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/logging"
)

// The bootstrap tests boot short-lived replicas that claim jobs like any
// other, so they run before the parallel tests rather than among them. Each
// replica needs grpc, health and metrics ports of its own.
var bootstrapPorts atomic.Int32

func init() { bootstrapPorts.Store(9200) }

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

// runBootstrap boots a replica on the files and stops it once it serves,
// returning its output and the error it exited with, if any.
func runBootstrap(t *testing.T, files ...string) (string, error) {
	t.Helper()
	port := int(bootstrapPorts.Add(3))
	sut := schedulerSUT(fmt.Sprintf("%s-bootstrap-%d", schedulerServiceName, port), port, port+1, port+2, files...)
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

// writeBootstrap writes a jsonnet file in which `base` is the shared fixtures file.
func writeBootstrap(t *testing.T, snippet string) string {
	t.Helper()
	base, err := filepath.Abs(bootstrapPath)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bootstrap.jsonnet")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("local base = import '%s';\n%s", base, snippet)), 0o600))
	return path
}

func getTarget(t *testing.T, name string) *schedulerpb.Target {
	t.Helper()
	target, err := schedulerServiceClient.GetTarget(ctx, &schedulerservicepb.GetTargetRequest{Name: name})
	require.NoError(t, err)
	return target
}

// Both replicas booted on the same file concurrently; a third leaves everything as is.
func TestBootstrap_Idempotent(t *testing.T) {
	queue := getQueue(t, echoQueue)
	target := getTarget(t, targetName)

	output, err := runBootstrap(t, bootstrapPath)
	require.NoError(t, err, output)
	require.NotContains(t, output, "bootstrap updated")

	grpcrequire.Equal(t, queue, getQueue(t, echoQueue), protocmp.IgnoreFields(&schedulerpb.Queue{}, "stats"))
	grpcrequire.Equal(t, target, getTarget(t, targetName))
}

func TestBootstrap_AppliesChanges(t *testing.T) {
	// Something outside the file, to be reported and left alone.
	unmanaged := createTarget(t, processorURL, nil)

	const target, queue = "targets/bootstrap-target", "queues/bootstrap-policy"
	initial := writeBootstrap(t, `base {
  targets+: [{ target_id: 'bootstrap-target', target: { url: '`+processorURL+`', headers: { '`+testHeader+`': 'hello' } } }],
  queues+: [{ queue_id: 'bootstrap-policy', queue: { policy: { attempt_timeout: '5s', max_attempts: 1 }, handlers: [{ method: '`+processorPath+`Echo', target: '`+target+`' }] } }],
}`)
	output, err := runBootstrap(t, initial)
	require.NoError(t, err, output)
	require.Contains(t, output, unmanaged.GetName())
	created := getQueue(t, queue)
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, created.GetState())

	changed := writeBootstrap(t, `local initial = import '`+initial+`';
initial {
  targets: [if target.target_id == 'bootstrap-target' then target { target+: { headers: { '`+testHeader+`': 'changed' } } } else target for target in initial.targets],
  queues: [if queue.queue_id == 'bootstrap-policy' then queue { queue+: { policy+: { max_attempts: 2 } } } else queue for queue in initial.queues],
}`)
	output, err = runBootstrap(t, changed)
	require.NoError(t, err, output)
	require.Contains(t, output, "bootstrap updated target")
	require.Contains(t, output, "bootstrap updated queue")

	require.Equal(t, map[string]string{testHeader: "changed"}, getTarget(t, target).GetHeaders())
	updated := getQueue(t, queue)
	require.EqualValues(t, 2, updated.GetPolicy().GetMaxAttempts())
	// The rest of the policy, its defaults included, and the state carry over.
	grpcrequire.Equal(t, created.GetPolicy().GetRetryBackoff(), updated.GetPolicy().GetRetryBackoff())
	require.Equal(t, created.GetPolicy().GetRetryableCodes(), updated.GetPolicy().GetRetryableCodes())
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_RUNNING, updated.GetState())
	require.Equal(t, created.GetHandlers()[0].GetRequestType(), updated.GetHandlers()[0].GetRequestType())
}

func TestBootstrap_PreservesPausedState(t *testing.T) {
	paused, err := schedulerServiceClient.PauseQueue(ctx, &schedulerservicepb.PauseQueueRequest{Name: bootstrapPausedQueue})
	require.NoError(t, err)
	require.Equal(t, schedulerpb.QueueState_QUEUE_STATE_PAUSED, paused.GetState())

	output, err := runBootstrap(t, bootstrapPath)
	require.NoError(t, err, output)
	grpcrequire.Equal(t, paused, getQueue(t, bootstrapPausedQueue), protocmp.IgnoreFields(&schedulerpb.Queue{}, "stats"))
}

func TestBootstrap_InvalidFileFailsStartup(t *testing.T) {
	// Created through the API, so its request id is not the one bootstrap mints.
	foreign := createTarget(t, processorURL, nil)

	for _, testCase := range []struct {
		name     string
		snippet  string
		messages []string
	}{
		{
			name:     "unknown method",
			snippet:  `base { queues+: [{ queue_id: 'bad-method', queue: { policy: { attempt_timeout: '5s', max_attempts: 1 }, handlers: [{ method: '` + processorPath + `Nope', target: '` + targetName + `' }] } }] }`,
			messages: []string{"bootstrapping queue bad-method", processorPath + "Nope"},
		},
		{
			name:     "missing target",
			snippet:  `base { queues+: [{ queue_id: 'bad-target', queue: { policy: { attempt_timeout: '5s', max_attempts: 1 }, handlers: [{ method: '` + processorPath + `Echo', target: 'targets/missing' }] } }] }`,
			messages: []string{"bootstrapping queue bad-target", `target "targets/missing" does not exist`},
		},
		{
			name:     "target created outside bootstrap",
			snippet:  `base { targets+: [{ target_id: '` + resourceID(foreign.GetName()) + `', target: { url: '` + processorURL + `' } }] }`,
			messages: []string{"bootstrapping target " + resourceID(foreign.GetName()), "not created by bootstrap"},
		},
		{
			name:     "request id set",
			snippet:  `base { targets+: [{ target_id: 'with-request-id', request_id: '3f2f6c1a-2c2a-4d2b-9d6e-6a2d5d1b8c4e', target: { url: '` + processorURL + `' } }] }`,
			messages: []string{"bootstrap.targets_identified"},
		},
		{
			name:     "unknown field",
			snippet:  `base { targets+: [{ target_id: 'typo', targt: { url: '` + processorURL + `' } }] }`,
			messages: []string{"unknown field"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			output, err := runBootstrap(t, writeBootstrap(t, testCase.snippet))
			require.Error(t, err)
			for _, message := range testCase.messages {
				require.Contains(t, output, message)
			}
		})
	}
}
