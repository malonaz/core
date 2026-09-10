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
)

// JobMetadataKey is the request metadata key under which the scheduler sends a
// handler the resource name of the job it is executing.
const JobMetadataKey = "x-scheduler-job"

// JobFromIncomingContext returns the name of the job a handler call executes.
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

// WithExpireTime requires the job to have started by the given time.
func WithExpireTime(expireTime time.Time) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.ExpireTime = timestamppb.New(expireTime)
	}
}

// WithPriority sets the job's claim priority; higher runs first.
func WithPriority(priority int32) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.Priority = priority
	}
}

// WithUniqueKey coalesces the job with any PENDING job sharing the key. Keys
// are global: namespace them.
func WithUniqueKey(uniqueKey string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.UniqueKey = uniqueKey
	}
}

// WithOperation exposes the job as the named long-running operation. The
// operation's ID must be the job's ID; see Job.operation.
func WithOperation(operation string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.Operation = operation
	}
}

// WithLabels sets the job's labels.
func WithLabels(labels map[string]string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.Labels = labels
	}
}

// NewCreateJobRequest builds a CreateJob request delivering message to the
// method whose request type it is, under parent (empty for a system job).
func NewCreateJobRequest(parent string, message proto.Message, options ...CreateJobOption) (*pb.CreateJobRequest, error) {
	payload, err := anypb.New(message)
	if err != nil {
		return nil, err
	}
	request := &pb.CreateJobRequest{
		Parent: parent,
		Job:    &schedulerpb.Job{Payload: payload},
	}
	for _, option := range options {
		option(request)
	}
	return request, nil
}
