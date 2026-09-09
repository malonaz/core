package longrunning

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// Server implements longrunningpb.OperationsServer over the scheduler, scoped
// to one service's long-running methods: only jobs of those methods are
// visible through it, so services sharing a scheduler do not see each other's
// operations.
type Server struct {
	client  schedulerservicepb.SchedulerServiceClient
	methods []string
	// The (method = ...) disjunction every ListJobs filter carries.
	methodsFilter string
}

// NewServer returns an Operations server for the given fully qualified gRPC
// methods, e.g. "/library.v1.LibraryService/ImportBooks".
func NewServer(client schedulerservicepb.SchedulerServiceClient, methods []string) *Server {
	methodTerms := make([]string, len(methods))
	for i, method := range methods {
		methodTerms[i] = fmt.Sprintf("method = %q", method)
	}
	return &Server{
		client:        client,
		methods:       methods,
		methodsFilter: "(" + strings.Join(methodTerms, " OR ") + ")",
	}
}

// job returns the job backing the named operation, NOT_FOUND when there is
// none, when the name does not match the job's, or when the job belongs to
// another service's method.
func (s *Server) job(ctx context.Context, operationName string) (*schedulerpb.Job, error) {
	jobName, ok := jobNameOf(operationName)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not an operation name", operationName).Err()
	}
	getJobRequest := &schedulerservicepb.GetJobRequest{Name: jobName}
	job, err := s.client.GetJob(ctx, getJobRequest)
	if err != nil {
		if status.HasCode(err, codes.NotFound) {
			return nil, status.Errorf(codes.NotFound, "operation %q does not exist", operationName).Err()
		}
		return nil, status.FromError(err, "getting job").Err()
	}
	if job.GetOperationName() != operationName || !slices.Contains(s.methods, job.GetMethod()) {
		return nil, status.Errorf(codes.NotFound, "operation %q does not exist", operationName).Err()
	}
	return job, nil
}

// GetOperation implements longrunningpb.OperationsServer.
func (s *Server) GetOperation(ctx context.Context, request *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	job, err := s.job(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	return OperationFromJob(job), nil
}

// ListOperations lists the operations directly on the named resource
// (`{name}/operations/*`) of this server's methods; their jobs all live under
// the parent the name derives to. The supported filter subset is `done = true`
// and `done = false`; the page token is the scheduler's.
func (s *Server) ListOperations(ctx context.Context, request *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	if request.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name must be set").Err()
	}
	filter, err := jobsFilter(request.GetFilter())
	if err != nil {
		return nil, err
	}
	listJobsRequest := &schedulerservicepb.ListJobsRequest{
		Parent:    JobParentOf(request.GetName()),
		Filter:    fmt.Sprintf("operation_name = %q AND %s%s", OperationName(request.GetName(), "*"), s.methodsFilter, filter),
		PageSize:  request.GetPageSize(),
		PageToken: request.GetPageToken(),
	}
	listJobsResponse, err := s.client.ListJobs(ctx, listJobsRequest)
	if err != nil {
		return nil, status.FromError(err, "listing jobs").Err()
	}
	operations := make([]*longrunningpb.Operation, len(listJobsResponse.GetJobs()))
	for i, job := range listJobsResponse.GetJobs() {
		operations[i] = OperationFromJob(job)
	}
	return &longrunningpb.ListOperationsResponse{Operations: operations, NextPageToken: listJobsResponse.GetNextPageToken()}, nil
}

// jobsFilter translates an operations filter into a job filter clause. Only
// `done` is filterable: it is the one Operation field the job stores as a
// column (its state); response, error and metadata are opaque blobs.
func jobsFilter(filter string) (string, error) {
	switch strings.Join(strings.Fields(filter), " ") {
	case "":
		return "", nil
	case "done = true":
		return " AND " + statesFilter(terminalStates), nil
	case "done = false":
		return " AND " + statesFilter(liveStates), nil
	}
	return "", status.Errorf(codes.InvalidArgument, "unsupported filter %q: only `done = true` and `done = false` are supported", filter).Err()
}

var (
	terminalStates = []schedulerpb.JobState{schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED}
	liveStates     = []schedulerpb.JobState{schedulerpb.JobState_JOB_STATE_PENDING, schedulerpb.JobState_JOB_STATE_RUNNING}
)

// statesFilter returns the filter clause matching any of the states.
func statesFilter(states []schedulerpb.JobState) string {
	terms := make([]string, len(states))
	for i, state := range states {
		terms[i] = fmt.Sprintf("state = %s", state)
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

// CancelOperation implements longrunningpb.OperationsServer. A finished
// operation fails with FAILED_PRECONDITION.
func (s *Server) CancelOperation(ctx context.Context, request *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	job, err := s.job(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: job.GetName()}
	if _, err := s.client.CancelJob(ctx, cancelJobRequest); err != nil {
		return nil, status.FromError(err, "cancelling job").Err()
	}
	return &emptypb.Empty{}, nil
}

// DeleteOperation implements longrunningpb.OperationsServer. A running
// operation must be cancelled first.
func (s *Server) DeleteOperation(ctx context.Context, request *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	job, err := s.job(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	deleteJobRequest := &schedulerservicepb.DeleteJobRequest{Name: job.GetName()}
	if _, err := s.client.DeleteJob(ctx, deleteJobRequest); err != nil {
		return nil, status.FromError(err, "deleting job").Err()
	}
	return &emptypb.Empty{}, nil
}

// WaitOperation implements longrunningpb.OperationsServer; the timeout is
// capped by the scheduler, and elapsing is not an error.
func (s *Server) WaitOperation(ctx context.Context, request *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	job, err := s.job(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	waitJobRequest := &schedulerservicepb.WaitJobRequest{Name: job.GetName(), Timeout: request.GetTimeout()}
	job, err = s.client.WaitJob(ctx, waitJobRequest)
	if err != nil {
		return nil, status.FromError(err, "waiting for job").Err()
	}
	return OperationFromJob(job), nil
}
