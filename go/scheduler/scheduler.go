// Package scheduler holds the producer-side helpers of the scheduler service:
// building CreateJob requests and reading the job name a processor is handed.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/uuid"
)

// JobMetadataKey is the request metadata key under which the scheduler sends a
// processor the resource name of the job it is executing.
const JobMetadataKey = "x-scheduler-job"

var jobUUIDNamespace = aip.MustGetUUIDNamespace(&schedulerpb.Job{})

// JobFromIncomingContext returns the name of the job a processor call executes.
func JobFromIncomingContext(ctx context.Context) (string, bool) {
	values := metadata.ValueFromIncomingContext(ctx, JobMetadataKey)
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// CreateJobOption customizes a CreateJob request.
type CreateJobOption func(*pb.CreateJobRequest)

// WithScheduleTime defers the job until the given time.
func WithScheduleTime(scheduleTime time.Time) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.ScheduleTime = timestamppb.New(scheduleTime)
	}
}

// WithIdempotencyKey derives the request ID from key, so that repeated
// requests with the same key create a single job.
func WithIdempotencyKey(key string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.RequestId = uuid.NewV5(jobUUIDNamespace, key).String()
	}
}

// WithLabels sets the job's labels.
func WithLabels(labels map[string]string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.Labels = labels
	}
}

// NewCreateJobRequest builds a CreateJob request delivering message to the
// processor configured for its type.
func NewCreateJobRequest(message proto.Message, options ...CreateJobOption) (*pb.CreateJobRequest, error) {
	payload, err := anypb.New(message)
	if err != nil {
		return nil, err
	}
	request := &pb.CreateJobRequest{
		Job: &schedulerpb.Job{Payload: payload},
	}
	for _, option := range options {
		option(request)
	}
	return request, nil
}
