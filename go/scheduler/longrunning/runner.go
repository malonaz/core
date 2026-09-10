package longrunning

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
)

// StartRequest describes the operation a producer starts.
type StartRequest struct {
	// Resource is the resource the operation hangs off; the job's parent is
	// derived from it (see jobParentOf).
	Resource string
	// Request is the payload the scheduler delivers back to the runner.
	Request proto.Message
	// RequestID, when set, makes the start idempotent: repeating it returns the
	// operation it first created.
	RequestID string
}

// Start hands the request to the scheduler as a job under the parent the
// resource derives to, and returns it as an operation, not done.
func Start(ctx context.Context, client schedulerservicepb.SchedulerServiceClient, request *StartRequest) (*longrunningpb.Operation, error) {
	job, err := scheduler.CreateJob(ctx, client, jobParentOf(request.Resource), request.Request, scheduler.WithRequestID(request.RequestID))
	if err != nil {
		return nil, status.FromError(err, "creating job").Err()
	}
	return operationFromJob(job)
}

// operationFromJob is OperationFromJob with its error as a gRPC status.
func operationFromJob(job *schedulerpb.Job) (*longrunningpb.Operation, error) {
	operation, err := OperationFromJob(job)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "naming operation: %v", err).Err()
	}
	return operation, nil
}

// IsRun reports whether the call is the scheduler running a job, as opposed
// to a client starting an operation.
func IsRun(ctx context.Context) bool {
	_, ok := scheduler.JobFromIncomingContext(ctx)
	return ok
}

// ReportProgress records metadata on the operation the call is running. It
// fails with FAILED_PRECONDITION once the job is no longer running, which is
// how a runner notices a cancellation.
func ReportProgress(ctx context.Context, client schedulerservicepb.SchedulerServiceClient, metadata proto.Message) error {
	jobName, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "not running a job: no %s metadata", scheduler.JobMetadataKey).Err()
	}
	progress, err := anypb.New(metadata)
	if err != nil {
		return status.Errorf(codes.Internal, "packing progress: %v", err).Err()
	}
	reportJobProgressRequest := &schedulerservicepb.ReportJobProgressRequest{Name: jobName, Progress: progress}
	if _, err := client.ReportJobProgress(ctx, reportJobProgressRequest); err != nil {
		return status.FromError(err, "reporting progress").Err()
	}
	return nil
}

// runningOperationName returns the name of the operation the call is running,
// from the job the scheduler's metadata names.
func runningOperationName(ctx context.Context) (string, error) {
	jobName, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.FailedPrecondition, "not running a job: no %s metadata", scheduler.JobMetadataKey).Err()
	}
	name, err := OperationName(jobName)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "parsing %s metadata: %v", scheduler.JobMetadataKey, err).Err()
	}
	return name, nil
}

// Done wraps a runner's response as the finished operation.
func Done(ctx context.Context, response proto.Message) (*longrunningpb.Operation, error) {
	name, err := runningOperationName(ctx)
	if err != nil {
		return nil, err
	}
	packed, err := anypb.New(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "packing response: %v", err).Err()
	}
	return &longrunningpb.Operation{Name: name, Done: true, Result: &longrunningpb.Operation_Response{Response: packed}}, nil
}

// Failed wraps a runner's error as the finished operation; the scheduler
// applies its retry policy to the error's code as if the call had returned it.
func Failed(ctx context.Context, err error) (*longrunningpb.Operation, error) {
	name, nameErr := runningOperationName(ctx)
	if nameErr != nil {
		return nil, nameErr
	}
	return &longrunningpb.Operation{Name: name, Done: true, Result: &longrunningpb.Operation_Error{Error: grpcstatus.Convert(err).Proto()}}, nil
}
