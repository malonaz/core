// Package histogram counts durations in fixed log-scale buckets. Histograms merge by addition, so
// percentiles survive being rolled up and summed.
package histogram

import "math"

// Size is the number of buckets. Bucket 0 holds durations under a millisecond, bucket k those in
// [1ms·r^(k-1), 1ms·r^k) with r = 2^(1/4), and the last one everything from about 35 days on:
// between those, a percentile read off its bucket is within 10% of the true one. Histograms are
// stored (scheduler.queue_hour_stats): changing the buckets needs a migration.
const Size = 128

const (
	floorSeconds = 1e-3
	growth       = 1.189207115002721 // 2^(1/4)
)

// Histogram counts durations by bucket; its length is Size.
type Histogram []int64

// New returns an empty histogram.
func New() Histogram { return make(Histogram, Size) }

// Add counts a duration, in seconds.
func (h Histogram) Add(seconds float64) { h[bucket(seconds)]++ }

// Merge adds other's counts to h.
func (h Histogram) Merge(other Histogram) {
	for i, count := range other {
		h[i] += count
	}
}

// Quantile returns the q-quantile (0 < q ≤ 1) by nearest rank, in seconds, read as its bucket's
// geometric midpoint; false when nothing was counted.
func (h Histogram) Quantile(q float64) (float64, bool) {
	var total int64
	for _, count := range h {
		total += count
	}
	if total == 0 {
		return 0, false
	}
	rank := max(1, int64(math.Ceil(q*float64(total))))
	var seen int64
	for i, count := range h {
		if seen += count; seen >= rank {
			return midpoint(i), true
		}
	}
	return midpoint(len(h) - 1), true // unreachable: rank ≤ total
}

func bucket(seconds float64) int {
	if seconds < floorSeconds {
		return 0
	}
	return min(Size-1, 1+int(math.Log(seconds/floorSeconds)/math.Log(growth)))
}

// midpoint is the value a bucket reads as: the geometric middle of its bounds.
func midpoint(i int) float64 {
	if i == 0 {
		return floorSeconds / 2
	}
	return floorSeconds * math.Pow(growth, float64(i)-0.5)
}
