package scheduler_dispatcher

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/endpoints"
	"github.com/malonaz/core/go/uuid"
)

// queueRequestIDNamespace derives a queue's create request id from its method,
// so replaying it (a restart, a concurrent replica) returns the existing queue
// instead of colliding on it.
var queueRequestIDNamespace = uuid.MustParse("6b1f3f0e-2f9c-4c1e-9b0a-8e2b5b0f3c71")

// Etag races between replicas converging concurrently resolve on the next
// replay, so a second loss means something else is mutating the queue.
const convergeAttempts = 3

// queueConvergeMask holds what a method's annotation and endpoint decide.
var queueConvergeMask = pbfieldmask.FromPaths("endpoint", "response_type", "policy").MustValidate(&schedulerpb.Queue{})

// discover reflects every endpoint and converges one queue per scheduler-run
// method onto what the endpoint declares. Queues of methods no endpoint serves
// any more are left alone and reported.
func (s *Service) discover(ctx context.Context) error {
	declared := map[string]struct{}{}
	for _, url := range s.opts.Endpoints {
		names, err := s.convergeEndpoint(ctx, url)
		if err != nil {
			return err
		}
		for _, name := range names {
			declared[name] = struct{}{}
		}
	}
	return s.logUndeclared(ctx, declared)
}

// rediscover converges every endpoint's queues again while running. An
// endpoint failing (mid-rollout) does not hold back the others; undeclared
// queues were reported at startup.
func (s *Service) rediscover(ctx context.Context) error {
	var errs []error
	for _, url := range s.opts.Endpoints {
		if _, err := s.convergeEndpoint(ctx, url); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// convergeEndpoint reflects the endpoint and converges the queue of each of
// its scheduler-run methods, returning their names.
func (s *Service) convergeEndpoint(ctx context.Context, url string) ([]string, error) {
	methods, err := endpoints.Discover(ctx, s.grpcDialer, url)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(methods))
	for _, method := range methods {
		queue, err := s.convergeQueue(ctx, url, method)
		if err != nil {
			return nil, fmt.Errorf("converging queue of %s.%s: %w", method.Service, method.Method, err)
		}
		names = append(names, queue.GetName())
	}
	return names, nil
}

// convergeQueue creates the method's queue, or updates it when its endpoint,
// response type or policy differ from the declaration.
func (s *Service) convergeQueue(ctx context.Context, url string, method *endpoints.Method) (*schedulerpb.Queue, error) {
	desired := &schedulerpb.Queue{
		Service:      method.Service,
		Method:       method.Method,
		Endpoint:     url,
		RequestType:  method.RequestType,
		ResponseType: method.ResponseType,
		Policy:       scheduler.NormalizePolicy(method.Policy, s.defaultRetryBackoff),
	}
	for range convergeAttempts {
		createQueueRequest := &pb.CreateQueueRequest{
			Queue:     proto.CloneOf(desired),
			RequestId: uuid.NewV5(queueRequestIDNamespace, scheduler.MethodPath(method.Service, method.Method)).String(),
		}
		existing, err := s.schedulerServiceClient.CreateQueue(ctx, createQueueRequest)
		if err != nil {
			if status.HasCode(err, codes.AlreadyExists) {
				return nil, fmt.Errorf("exists but was not created by a dispatcher; delete it: %w", err)
			}
			return nil, err
		}
		if proto.Equal(masked(desired, queueConvergeMask), masked(existing, queueConvergeMask)) {
			return existing, nil
		}
		updateQueueRequest := &pb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: existing.GetName(), Etag: existing.GetEtag(), Endpoint: desired.GetEndpoint(), ResponseType: desired.GetResponseType(), Policy: desired.GetPolicy()},
			UpdateMask: queueConvergeMask.Proto(),
		}
		updated, err := s.schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
		if err == nil {
			s.log.InfoContext(ctx, "updated queue", "name", updated.GetName(), "service", method.Service, "method", method.Method)
			return updated, nil
		}
		if !status.HasCode(err, codes.Aborted) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("lost %d etag races", convergeAttempts)
}

// logUndeclared names every queue no endpoint declares; they are left alone.
func (s *Service) logUndeclared(ctx context.Context, declared map[string]struct{}) error {
	for queues, err := range aip.PageIterator[*schedulerpb.Queue](ctx, &pb.ListQueuesRequest{}, s.schedulerServiceClient.ListQueues) {
		if err != nil {
			return fmt.Errorf("listing queues: %w", err)
		}
		for _, queue := range queues {
			if _, ok := declared[queue.GetName()]; !ok {
				s.log.WarnContext(ctx, "queue served by no endpoint", "name", queue.GetName(), "service", queue.GetService(), "method", queue.GetMethod())
			}
		}
	}
	return nil
}

// masked returns a copy of the message holding only the mask's fields.
func masked[M proto.Message](message M, mask *pbfieldmask.FieldMask) M {
	copied := proto.CloneOf(message)
	mask.Apply(copied)
	return copied
}
