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
	"github.com/malonaz/core/go/scheduler/transition"
)

// maxQueueStatsBuckets bounds a time series: a day by the minute.
const maxQueueStatsBuckets = 1440

// ComputeQueueStats reads a queue's completed jobs into interval totals and,
// when a step is requested, a sparse time series. Nothing is stored: the
// response is computed per request.
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

	rows, err := s.schedulerPostgresStore.ComputeQueueStats(ctx, queueName, startTime, endTime, step)
	if err != nil {
		return nil, status.FromError(err, "computing queue stats").Err()
	}
	response := &pb.ComputeQueueStatsResponse{
		Interval:    request.GetInterval(),
		ComputeTime: timestamppb.Now(),
	}
	for _, row := range rows {
		if row.Bucket == nil {
			response.Totals = queueStatsSnapshotToPb(row)
			continue
		}
		response.Buckets = append(response.Buckets, &schedulerpb.QueueStatsBucket{
			Interval: &intervalpb.Interval{
				StartTime: timestamppb.New(*row.Bucket),
				EndTime:   timestamppb.New(row.Bucket.Add(step)),
			},
			Snapshot: queueStatsSnapshotToPb(row),
		})
	}
	return response, nil
}

func queueStatsSnapshotToPb(row *store.QueueStatsRow) *schedulerpb.QueueStatsSnapshot {
	return &schedulerpb.QueueStatsSnapshot{
		SucceededCount: row.SucceededCount,
		FailedCount:    row.FailedCount,
		CancelledCount: row.CancelledCount,
		AttemptCount:   row.AttemptCount,
		WaitDuration:   durationDistributionToPb(row.WaitP50, row.WaitP95, row.WaitMax),
		RunDuration:    durationDistributionToPb(row.RunP50, row.RunP95, row.RunMax),
	}
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
