package histogram

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucket(t *testing.T) {
	require.Equal(t, 0, bucket(0))
	require.Equal(t, 0, bucket(-1))
	require.Equal(t, 0, bucket(0.0009))
	require.Equal(t, 1, bucket(0.001))
	require.Equal(t, 5, bucket(0.002), "four buckets per doubling")
	require.Equal(t, Size-1, bucket(365*24*3600))
}

func TestQuantile(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, ok := New().Quantile(0.5)
		require.False(t, ok)
	})

	t.Run("within 10% of the nearest rank", func(t *testing.T) {
		// Log-uniform from a millisecond to a day: every bucket in between is exercised.
		values := make([]float64, 10_000)
		for i := range values {
			values[i] = 1e-3 * math.Pow(86_400_000, float64(i)/float64(len(values)))
		}
		histogram := New()
		for _, value := range values {
			histogram.Add(value)
		}
		slices.Sort(values)
		for _, q := range []float64{0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 1} {
			exact := values[int(math.Ceil(q*float64(len(values))))-1]
			got, ok := histogram.Quantile(q)
			require.True(t, ok)
			require.InEpsilon(t, exact, got, 0.1, "q=%v", q)
		}
	})
}

func TestMerge(t *testing.T) {
	first, second, both := New(), New(), New()
	for _, value := range []float64{0.01, 0.5, 3} {
		first.Add(value)
		both.Add(value)
	}
	for _, value := range []float64{0.02, 40} {
		second.Add(value)
		both.Add(value)
	}
	first.Merge(second)
	require.Equal(t, both, first)
}
