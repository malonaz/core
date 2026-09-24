package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/uuid"
)

// Boots a replica and fills every slot on every replica with sleeping jobs, so
// the replica is sure to hold some when it is stopped. Sequential, like the
// discovery tests: the parallel tests would compete for the slots.
func fillReplica(t *testing.T, sleep time.Duration, args ...string) (*binary.Binary, []*schedulerpb.Job) {
	t.Helper()
	replica, output, err := startReplica(t, args, processorURL)
	require.NoError(t, err, output.String())
	key := uuid.MustNewV7().String()
	names := make([]string, 0, (replicaCount+1)*maxParallelJobs)
	for range cap(names) {
		names = append(names, createJob(t, &processorpb.SleepRequest{Key: key, Duration: durationpb.New(sleep)}).GetName())
	}
	var held []*schedulerpb.Job
	require.Eventually(t, func() bool {
		held = held[:0]
		for _, name := range names {
			if job := getJob(t, name); job.GetState() == schedulerpb.JobState_JOB_STATE_RUNNING && job.GetMetadata().GetWorker() == replica.Name() {
				held = append(held, job)
			}
		}
		return len(held) == maxParallelJobs
	}, waitTimeout, 20*time.Millisecond, "the replica never filled its slots")
	return replica, held
}

func TestShutdown_DrainsInFlightJobs(t *testing.T) {
	sleep := 2 * time.Second
	replica, held := fillReplica(t, sleep)

	// From the signal on, jobs keep arriving; none may land on the stopping replica.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		replica.Stop()
	}()
	start := time.Now()
	var fresh []string
	for len(fresh) < 20 {
		fresh = append(fresh, createJob(t, &processorpb.EchoRequest{Value: uuid.MustNewV7().String()}).GetName())
		time.Sleep(50 * time.Millisecond)
	}
	<-stopped
	require.Less(t, time.Since(start), sleep+5*time.Second, "the replica exits once its jobs are done")

	for _, job := range held {
		job = waitForTerminal(t, job.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.Equal(t, int32(1), job.GetAttemptCount(), "finished where it started: no release")
		require.Equal(t, replica.Name(), job.GetMetadata().GetAttempts()[0].GetWorker())
	}
	for _, name := range fresh {
		job := waitForTerminal(t, name)
		for _, attempt := range job.GetMetadata().GetAttempts() {
			require.NotEqual(t, replica.Name(), attempt.GetWorker(), "claimed after the signal")
		}
	}
}

func TestShutdown_ReleasesJobsOutlivingDrain(t *testing.T) {
	drain := 500 * time.Millisecond
	sleep := 3 * time.Second
	replica, held := fillReplica(t, sleep, "--scheduler-dispatcher.drain-timeout", drain.String())

	start := time.Now()
	replica.Stop()
	require.Less(t, time.Since(start), sleep, "the drain gave up before the jobs ended")

	// Released untouched: another replica runs what counts as the first attempt.
	for _, job := range held {
		job = waitForTerminal(t, job.GetName())
		require.Equal(t, schedulerpb.JobState_JOB_STATE_SUCCEEDED, job.GetState())
		require.Equal(t, int32(1), job.GetAttemptCount())
		require.Len(t, job.GetMetadata().GetAttempts(), 1)
		require.NotEqual(t, replica.Name(), job.GetMetadata().GetAttempts()[0].GetWorker())
	}
}
