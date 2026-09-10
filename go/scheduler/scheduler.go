// Package scheduler holds the producer-side helpers of the scheduler service:
// creating jobs and reading the job name a processor is handed.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
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

// WithRequestID makes the create idempotent under id: repeating it returns the
// job it first created.
func WithRequestID(id string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.RequestId = id
	}
}

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

// WithLabels sets the job's labels.
func WithLabels(labels map[string]string) CreateJobOption {
	return func(request *pb.CreateJobRequest) {
		request.Job.Labels = labels
	}
}

// CreateJob delivers message to the method whose request type it is, as a job
// under parent: the user or organization the work belongs to, "" for a system
// job. The request carries a fresh request_id unless WithRequestID sets one,
// so a retried create never makes a second job.
func CreateJob(ctx context.Context, client pb.SchedulerServiceClient, parent string, message proto.Message, options ...CreateJobOption) (*schedulerpb.Job, error) {
	payload, err := anypb.New(message)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "packing %T: %v", message, err).Err()
	}
	request := &pb.CreateJobRequest{
		Parent: parent,
		Job:    &schedulerpb.Job{Payload: payload},
	}
	for _, option := range options {
		option(request)
	}
	if request.RequestId == "" {
		request.RequestId = uuid.MustNewV7().String()
	}
	return client.CreateJob(ctx, request)
}
