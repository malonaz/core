package scheduler_service

import (
	"context"
	"time"

	"go.einride.tech/aip/resourcename"
	intervalpb "google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler/histogram"
	"github.com/malonaz/core/go/scheduler/transition"
)

const (
	// maxQueueStatsBuckets bounds a time series: a day by the minute.
	maxQueueStatsBuckets = 1440
	// maxJobStatsSpan is the longest interval read off the jobs themselves: past it, stats come
	// from the hourly roll-ups, as a week of jobs is a million rows.
	maxJobStatsSpan = 24 * time.Hour
)

// ComputeQueueStats reads a queue's completed jobs into interval totals and,
// when a step is requested, a sparse time series: off the jobs up to a day,
// off the hourly roll-ups past it.
func (s *Service) ComputeQueueStats(ctx context.Context, request *pb.ComputeQueueStatsRequest) (*pb.ComputeQueueStatsResponse, error) {
	queueID, err := model.ParseQueueName(request.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	// A nil queue aggregates every queue.
	var queueName *string
	if queueID != resourcename.Wildcard {
		getQueueRequest := &pb.GetQueueRequest{Name: request.GetName()}
		queue, err := s.SchedulerServiceServer.GetQueue(ctx, getQueueRequest)
		if err != nil {
			return nil, err
		}
		queueName = &queue.Name
	}

	// An open interval runs from the epoch to now: complete_time stays a range
	// the index answers.
	startTime, endTime := time.Unix(0, 0).UTC(), transition.Now()
	if request.GetInterval().GetStartTime() != nil {
		startTime = request.GetInterval().GetStartTime().AsTime()
	}
	if request.GetInterval().GetEndTime() != nil {
		endTime = request.GetInterval().GetEndTime().AsTime()
	}
	if endTime.Before(startTime) {
		return nil, status.Errorf(codes.InvalidArgument, "interval ends before it starts").Err()
	}
	step := request.GetStep().AsDuration()
	if step > 0 && endTime.Sub(startTime) > maxQueueStatsBuckets*step {
		return nil, status.Errorf(codes.InvalidArgument, "step cuts the interval into more than %d buckets", maxQueueStatsBuckets).Err()
	}

	compute := s.computeJobStats
	if endTime.Sub(startTime) > maxJobStatsSpan {
		if step%time.Hour != 0 {
			return nil, status.Errorf(codes.InvalidArgument, "past %s, step must be a whole number of hours", maxJobStatsSpan).Err()
		}
		compute = s.computeHourStats
	}
	totals, buckets, err := compute(ctx, queueName, startTime, endTime, step)
	if err != nil {
		return nil, status.FromError(err, "computing queue stats").Err()
	}
	return &pb.ComputeQueueStatsResponse{
		Interval:    request.GetInterval(),
		ComputeTime: timestamppb.Now(),
		Totals:      totals,
		Buckets:     buckets,
	}, nil
}

// computeJobStats reads the stats off the jobs: exact percentiles.
func (s *Service) computeJobStats(ctx context.Context, queueName *string, startTime, endTime time.Time, step time.Duration) (*schedulerpb.QueueStatsSnapshot, []*schedulerpb.QueueStatsBucket, error) {
	rows, err := s.schedulerPostgresStore.ComputeQueueStats(ctx, queueName, startTime, endTime, step)
	if err != nil {
		return nil, nil, err
	}
	var totals *schedulerpb.QueueStatsSnapshot
	var buckets []*schedulerpb.QueueStatsBucket
	for _, row := range rows {
		snapshot := &schedulerpb.QueueStatsSnapshot{
			SucceededCount: row.SucceededCount,
			FailedCount:    row.FailedCount,
			CancelledCount: row.CancelledCount,
			AttemptCount:   row.AttemptCount,
			WaitDuration:   durationDistributionToPb(row.WaitP50, row.WaitP95, row.WaitMax),
			RunDuration:    durationDistributionToPb(row.RunP50, row.RunP95, row.RunMax),
		}
		if row.Bucket == nil {
			totals = snapshot
			continue
		}
		buckets = append(buckets, &schedulerpb.QueueStatsBucket{Interval: bucketInterval(*row.Bucket, step), Snapshot: snapshot})
	}
	return totals, buckets, nil
}

// computeHourStats folds the hourly roll-ups over the interval, widened to whole hours:
// percentiles within the histogram's 10%.
func (s *Service) computeHourStats(ctx context.Context, queueName *string, startTime, endTime time.Time, step time.Duration) (*schedulerpb.QueueStatsSnapshot, []*schedulerpb.QueueStatsBucket, error) {
	hours, err := s.schedulerPostgresStore.ListQueueHourStats(ctx, queueName, startTime.Truncate(time.Hour), endTime)
	if err != nil {
		return nil, nil, err
	}
	totals := &statsFold{}
	var bucketStarts []time.Time
	bucketStartToFold := map[time.Time]*statsFold{}
	for _, hour := range hours {
		totals.addHour(hour)
		if step == 0 {
			continue
		}
		// Hours come in order, so buckets do too.
		bucketStart := alignToEpoch(hour.Hour, step)
		if _, ok := bucketStartToFold[bucketStart]; !ok {
			bucketStarts = append(bucketStarts, bucketStart)
			bucketStartToFold[bucketStart] = &statsFold{}
		}
		bucketStartToFold[bucketStart].addHour(hour)
	}
	buckets := make([]*schedulerpb.QueueStatsBucket, len(bucketStarts))
	for i, bucketStart := range bucketStarts {
		buckets[i] = &schedulerpb.QueueStatsBucket{Interval: bucketInterval(bucketStart, step), Snapshot: bucketStartToFold[bucketStart].snapshot()}
	}
	return totals.snapshot(), buckets, nil
}

// queueHourStatsOf rolls the measures of the jobs completed in one hour up by queue.
func queueHourStatsOf(hour time.Time, measures []*store.JobMeasure) []*store.QueueHourStats {
	queueToFold := map[string]*statsFold{}
	for _, measure := range measures {
		fold, ok := queueToFold[measure.Queue]
		if !ok {
			fold = &statsFold{}
			queueToFold[measure.Queue] = fold
		}
		fold.addJob(measure)
	}
	hourStats := make([]*store.QueueHourStats, 0, len(queueToFold))
	for queue, fold := range queueToFold {
		hourStats = append(hourStats, &store.QueueHourStats{
			Queue:          queue,
			Hour:           hour,
			SucceededCount: fold.succeeded,
			FailedCount:    fold.failed,
			CancelledCount: fold.cancelled,
			AttemptCount:   fold.attempts,
			WaitHistogram:  fold.wait.histogram,
			WaitMaxSeconds: fold.wait.max,
			RunHistogram:   fold.run.histogram,
			RunMaxSeconds:  fold.run.max,
		})
	}
	return hourStats
}

// statsFold adds jobs into an hour's stats, or hours into an interval's.
type statsFold struct {
	succeeded, failed, cancelled, attempts int64
	wait, run                              durationFold
}

func (f *statsFold) addJob(measure *store.JobMeasure) {
	switch schedulerpb.JobState(measure.State) {
	case schedulerpb.JobState_JOB_STATE_SUCCEEDED:
		f.succeeded++
	case schedulerpb.JobState_JOB_STATE_FAILED:
		f.failed++
	case schedulerpb.JobState_JOB_STATE_CANCELLED:
		f.cancelled++
	}
	f.attempts += measure.AttemptCount
	f.wait.add(measure.WaitSeconds)
	f.run.add(measure.RunSeconds)
}

func (f *statsFold) addHour(hour *store.QueueHourStats) {
	f.succeeded += hour.SucceededCount
	f.failed += hour.FailedCount
	f.cancelled += hour.CancelledCount
	f.attempts += hour.AttemptCount
	f.wait.merge(hour.WaitHistogram, hour.WaitMaxSeconds)
	f.run.merge(hour.RunHistogram, hour.RunMaxSeconds)
}

func (f *statsFold) snapshot() *schedulerpb.QueueStatsSnapshot {
	return &schedulerpb.QueueStatsSnapshot{
		SucceededCount: f.succeeded,
		FailedCount:    f.failed,
		CancelledCount: f.cancelled,
		AttemptCount:   f.attempts,
		WaitDuration:   f.wait.distribution(),
		RunDuration:    f.run.distribution(),
	}
}

// durationFold counts durations, in seconds, and keeps the longest exactly; both stay nil until
// one is counted.
type durationFold struct {
	histogram histogram.Histogram
	max       *float64
}

func (f *durationFold) add(seconds *float64) {
	if seconds != nil {
		f.extend(*seconds)
		f.histogram.Add(*seconds)
	}
}

func (f *durationFold) merge(counts histogram.Histogram, max *float64) {
	if max != nil {
		f.extend(*max)
		f.histogram.Merge(counts)
	}
}

// extend makes room for a duration: the histogram on the first one, and the longest so far.
func (f *durationFold) extend(seconds float64) {
	if f.histogram == nil {
		f.histogram = histogram.New()
	}
	if f.max == nil || seconds > *f.max {
		f.max = &seconds
	}
}

// distribution reads the percentiles off the histogram, never past the exact longest.
func (f *durationFold) distribution() *schedulerpb.DurationDistribution {
	if f.max == nil {
		return nil
	}
	p50, _ := f.histogram.Quantile(0.5)
	p95, _ := f.histogram.Quantile(0.95)
	return durationDistributionToPb(new(min(p50, *f.max)), new(min(p95, *f.max)), f.max)
}

// durationDistributionToPb converts percentiles in seconds; all three are set
// or none is, as they aggregate the same durations.
func durationDistributionToPb(p50, p95, max *float64) *schedulerpb.DurationDistribution {
	if max == nil {
		return nil
	}
	return &schedulerpb.DurationDistribution{
		P50: secondsToPb(*p50),
		P95: secondsToPb(*p95),
		Max: secondsToPb(*max),
	}
}

func secondsToPb(seconds float64) *durationpb.Duration {
	return durationpb.New(time.Duration(seconds * float64(time.Second)))
}

// alignToEpoch returns the start of the step-wide bucket, aligned to the Unix epoch, holding t.
func alignToEpoch(t time.Time, step time.Duration) time.Time {
	return time.Unix(0, t.UnixNano()-t.UnixNano()%int64(step)).UTC()
}

func bucketInterval(start time.Time, step time.Duration) *intervalpb.Interval {
	return &intervalpb.Interval{StartTime: timestamppb.New(start), EndTime: timestamppb.New(start.Add(step))}
}
