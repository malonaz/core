package longrunning

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/authentication"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
)

// StartRequest describes the operation a producer starts.
type StartRequest struct {
	// Queue is the scheduler queue the job runs in.
	Queue string
	// Resource is the resource the operation hangs off; the job's parent is
	// derived from it (see JobParentOf).
	Resource string
	// Request is the payload the scheduler delivers back to the runner.
	Request proto.Message
	// RequestID, when set, makes the start idempotent: repeating it returns the
	// operation it first created.
	RequestID string
}

// Start hands the request to the scheduler as a job exposed as an operation
// on the resource, and returns that operation, not done. The operation's ID is
// the job's, so the operation name alone locates the job afterwards.
func Start(ctx context.Context, client schedulerservicepb.SchedulerServiceClient, request *StartRequest) (*longrunningpb.Operation, error) {
	jobID := aip.NewSystemGeneratedBase32ResourceID()
	createJobRequest, err := scheduler.NewCreateJobRequest(JobParentOf(request.Resource), request.Queue, request.Request,
		scheduler.WithOperationName(OperationName(request.Resource, jobID)))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building job: %v", err).Err()
	}
	createJobRequest.JobId = jobID
	createJobRequest.RequestId = request.RequestID
	job, err := client.CreateJob(ctx, createJobRequest)
	if err != nil {
		return nil, status.FromError(err, "creating job").Err()
	}
	return OperationFromJob(job), nil
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

// Done wraps a runner's response as a finished operation. The name is left
// empty: the scheduler records the outcome on the job, which names it.
func Done(response proto.Message) (*longrunningpb.Operation, error) {
	packed, err := anypb.New(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "packing response: %v", err).Err()
	}
	return &longrunningpb.Operation{Done: true, Result: &longrunningpb.Operation_Response{Response: packed}}, nil
}

// Failed wraps a runner's error as a finished operation; the scheduler
// applies its retry policy to the error's code as if the call had returned it.
func Failed(err error) *longrunningpb.Operation {
	return &longrunningpb.Operation{Done: true, Result: &longrunningpb.Operation_Error{Error: grpcstatus.Convert(err).Proto()}}
}

// RunnerAuthorizer decides whether a call carrying the scheduler's job
// metadata may run the work inline. The metadata is a privilege: without a
// check, any caller could run a job's worth of work synchronously.
type RunnerAuthorizer func(ctx context.Context) error

// AllowAllRunners trusts every caller. For tests and servers without authentication.
func AllowAllRunners() RunnerAuthorizer {
	return func(context.Context) error { return nil }
}

// ServiceAccountRunners admits sessions authenticated as a service account,
// any when none is named, else one of the given IDs.
func ServiceAccountRunners(serviceAccountIDs ...string) RunnerAuthorizer {
	serviceAccountIDSet := make(map[string]bool, len(serviceAccountIDs))
	for _, serviceAccountID := range serviceAccountIDs {
		serviceAccountIDSet[serviceAccountID] = true
	}
	return func(ctx context.Context) error {
		session, err := authentication.GetSession(ctx)
		if err != nil {
			return status.Errorf(codes.PermissionDenied, "running a job requires a session: %v", err).Err()
		}
		identity, ok := session.GetIdentity().(*authenticationpb.Session_ServiceAccountIdentity)
		if !ok {
			return status.Errorf(codes.PermissionDenied, "running a job requires a service account").Err()
		}
		if len(serviceAccountIDSet) > 0 && !serviceAccountIDSet[identity.ServiceAccountIdentity.GetServiceAccountId()] {
			return status.Errorf(codes.PermissionDenied, "service account %q may not run jobs", identity.ServiceAccountIdentity.GetServiceAccountId()).Err()
		}
		return nil
	}
}
