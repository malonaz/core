package scheduler_service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/gengo/scheduler/store"
	"github.com/malonaz/core/go/scheduler/histogram"
)

func hourStats(hour time.Time, succeeded int64, runSeconds ...float64) *store.QueueHourStats {
	run := &durationFold{}
	for _, seconds := range runSeconds {
		run.add(&seconds)
	}
	return &store.QueueHourStats{Queue: "queues/q", Hour: hour, SucceededCount: succeeded, AttemptCount: succeeded, RunHistogram: run.histogram, RunMaxSeconds: run.max}
}

func TestFoldHours(t *testing.T) {
	t.Run("nothing", func(t *testing.T) {
		totals, buckets := foldHours(nil, time.Hour)
		require.Zero(t, totals.GetSucceededCount())
		require.Nil(t, totals.GetRunDuration())
		require.Empty(t, buckets)
	})

	t.Run("hours fold into epoch-aligned buckets", func(t *testing.T) {
		start := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
		hours := []*store.QueueHourStats{
			hourStats(start, 1, 2),
			hourStats(start.Add(time.Hour), 2, 4, 8),
			hourStats(start.Add(3*time.Hour), 1, 100),
		}
		totals, buckets := foldHours(hours, 2*time.Hour)
		require.Equal(t, int64(4), totals.GetSucceededCount())
		require.Equal(t, 100*time.Second, totals.GetRunDuration().GetMax().AsDuration())
		require.Len(t, buckets, 3)
		require.True(t, buckets[0].GetInterval().GetStartTime().AsTime().Equal(start.Add(-time.Hour)))
		require.Equal(t, int64(1), buckets[0].GetSnapshot().GetSucceededCount())
		require.Equal(t, int64(2), buckets[1].GetSnapshot().GetSucceededCount())
		require.Equal(t, int64(1), buckets[2].GetSnapshot().GetSucceededCount())
		require.InEpsilon(t, 4, buckets[1].GetSnapshot().GetRunDuration().GetP50().AsDuration().Seconds(), 0.1)
		require.InEpsilon(t, 8, buckets[1].GetSnapshot().GetRunDuration().GetP95().AsDuration().Seconds(), 0.1)
	})

	t.Run("a percentile never reads past the exact longest", func(t *testing.T) {
		// 2ms opens its bucket, whose midpoint lies above it.
		totals, _ := foldHours([]*store.QueueHourStats{hourStats(time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC), 1, 0.002)}, 0)
		require.Equal(t, 2*time.Millisecond, totals.GetRunDuration().GetP50().AsDuration())
		require.Equal(t, 2*time.Millisecond, totals.GetRunDuration().GetP95().AsDuration())
	})

	t.Run("a measured hour without wait leaves wait unset", func(t *testing.T) {
		hour := hourStats(time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC), 1, 3)
		require.Nil(t, hour.WaitHistogram)
		totals, _ := foldHours([]*store.QueueHourStats{hour}, 0)
		require.Nil(t, totals.GetWaitDuration())
		require.Len(t, hour.RunHistogram, histogram.Size)
	})
}
