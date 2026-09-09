package sat

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	processorpb "github.com/malonaz/core/genproto/test/scheduler/processor/v1"
	"github.com/malonaz/core/go/scheduler"
)

// call is one processor invocation as the test process saw it.
type call struct {
	time      time.Time
	job       string
	headers   metadata.MD
	cancelled bool
}

// processor is the scriptable gRPC server the scheduler routes jobs to. Calls
// are recorded by key so tests can assert on attempt counts and timings.
type processor struct {
	processorpb.UnimplementedProcessorServer
	schedulerServiceClient schedulerservicepb.SchedulerServiceClient

	mutex      sync.Mutex
	keyToCalls map[string][]call
}

func newProcessor() *processor {
	return &processor{keyToCalls: map[string][]call{}}
}

func (p *processor) serve(address string) (func(), error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	processorpb.RegisterProcessorServer(server, p)
	go server.Serve(listener)
	return server.Stop, nil
}

// record registers a call under key and returns a function recording whether
// the call ended cancelled.
func (p *processor) record(ctx context.Context, key string) func() {
	job, _ := scheduler.JobFromIncomingContext(ctx)
	headers, _ := metadata.FromIncomingContext(ctx)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	index := len(p.keyToCalls[key])
	p.keyToCalls[key] = append(p.keyToCalls[key], call{time: time.Now(), job: job, headers: headers})
	return func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		p.keyToCalls[key][index].cancelled = ctx.Err() != nil
	}
}

// calls returns a copy of the calls recorded under key.
func (p *processor) calls(key string) []call {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]call(nil), p.keyToCalls[key]...)
}

func (p *processor) Echo(ctx context.Context, request *processorpb.EchoRequest) (*processorpb.EchoResponse, error) {
	defer p.record(ctx, request.GetValue())()
	return &processorpb.EchoResponse{Value: request.GetValue()}, nil
}

func (p *processor) Flaky(ctx context.Context, request *processorpb.FlakyRequest) (*processorpb.FlakyResponse, error) {
	defer p.record(ctx, request.GetKey())()
	calls := int32(len(p.calls(request.GetKey())))
	if calls > request.GetFailures() {
		return &processorpb.FlakyResponse{Calls: calls}, nil
	}
	failure := status.Newf(codes.Code(request.GetCode()), "flaky failure %d/%d", calls, request.GetFailures())
	if request.GetRetryDelay() != nil {
		var err error
		if failure, err = failure.WithDetails(&errdetails.RetryInfo{RetryDelay: request.GetRetryDelay()}); err != nil {
			return nil, err
		}
	}
	return nil, failure.Err()
}

func (p *processor) Sleep(ctx context.Context, request *processorpb.SleepRequest) (*processorpb.SleepResponse, error) {
	defer p.record(ctx, request.GetKey())()
	if err := sleep(ctx, request.GetDuration().AsDuration()); err != nil {
		return nil, err
	}
	return &processorpb.SleepResponse{}, nil
}

func (p *processor) Deadline(ctx context.Context, request *processorpb.DeadlineRequest) (*processorpb.DeadlineResponse, error) {
	defer p.record(ctx, request.GetKey())()
	if err := sleep(ctx, request.GetDuration().AsDuration()); err != nil {
		return nil, err
	}
	return &processorpb.DeadlineResponse{}, nil
}

func (p *processor) Progress(ctx context.Context, request *processorpb.ProgressRequest) (*processorpb.ProgressResponse, error) {
	job, _ := scheduler.JobFromIncomingContext(ctx)
	defer p.record(ctx, job)()
	for index := int32(1); index <= request.GetSteps(); index++ {
		progress, err := anypb.New(&processorpb.Step{Index: index})
		if err != nil {
			return nil, err
		}
		reportJobProgressRequest := &schedulerservicepb.ReportJobProgressRequest{Name: job, Progress: progress}
		if _, err := p.schedulerServiceClient.ReportJobProgress(ctx, reportJobProgressRequest); err != nil {
			return nil, err
		}
	}
	return &processorpb.ProgressResponse{}, nil
}

func sleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-time.After(duration):
		return nil
	}
}
