package scheduler_service

import (
	"context"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/uuid"
)

// bootstrapRequestIDNamespace derives every bootstrap create's request id from
// the resource name, so replaying it (a restart, a concurrent replica) returns
// the existing resource instead of colliding on it.
var bootstrapRequestIDNamespace = uuid.MustParse("6b1f3f0e-2f9c-4c1e-9b0a-8e2b5b0f3c71")

// Etag races between replicas converging concurrently resolve on the next
// replay, so a second loss means something else is mutating the resource.
const bootstrapAttempts = 3

var (
	targetBootstrapMask = pbfieldmask.FromPaths("url", "headers").MustValidate(&schedulerpb.Target{})
	queueBootstrapMask  = pbfieldmask.FromPaths("policy", "handlers").MustValidate(&schedulerpb.Queue{})
)

// loadBootstrap evaluates the files into one Bootstrap, in order.
func loadBootstrap(paths []string) (*pb.Bootstrap, error) {
	bootstrap := &pb.Bootstrap{}
	for _, path := range paths {
		bytes, err := jsonnet.EvaluateFile(path, jsonnet.WithEnvVariables())
		if err != nil {
			return nil, fmt.Errorf("evaluating %s: %w", path, err)
		}
		fileBootstrap := &pb.Bootstrap{}
		if err := pbutil.JSONUnmarshalStrict(bytes, fileBootstrap); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
		if err := protovalidate.Validate(fileBootstrap); err != nil {
			return nil, fmt.Errorf("validating %s: %w", path, err)
		}
		bootstrap.Targets = append(bootstrap.Targets, fileBootstrap.GetTargets()...)
		bootstrap.Queues = append(bootstrap.Queues, fileBootstrap.GetQueues()...)
	}
	return bootstrap, nil
}

// bootstrap converges the targets, then the queues, onto the bootstrap files,
// and reports whatever exists that the files do not declare.
func (s *Service) bootstrap(ctx context.Context) error {
	if len(s.opts.Bootstrap) == 0 {
		return nil
	}
	bootstrap, err := loadBootstrap(s.opts.Bootstrap)
	if err != nil {
		return err
	}
	for _, createTargetRequest := range bootstrap.GetTargets() {
		if err := s.bootstrapTarget(ctx, createTargetRequest); err != nil {
			return fmt.Errorf("bootstrapping target %s: %w", createTargetRequest.GetTargetId(), err)
		}
	}
	for _, createQueueRequest := range bootstrap.GetQueues() {
		if err := s.bootstrapQueue(ctx, createQueueRequest); err != nil {
			return fmt.Errorf("bootstrapping queue %s: %w", createQueueRequest.GetQueueId(), err)
		}
	}
	return s.logUnmanaged(ctx, bootstrap)
}

func (s *Service) bootstrapTarget(ctx context.Context, createTargetRequest *pb.CreateTargetRequest) error {
	desired := &schedulerpb.Target{Url: createTargetRequest.GetTarget().GetUrl(), Headers: createTargetRequest.GetTarget().GetHeaders()}
	for range bootstrapAttempts {
		// Create mutates its request; replay from a fresh copy each time.
		request := proto.CloneOf(createTargetRequest)
		request.RequestId = bootstrapRequestID(request.GetTargetId())
		existing, err := s.CreateTarget(ctx, request)
		if err != nil {
			return bootstrapCreateError(err)
		}
		if proto.Equal(desired, masked(existing, targetBootstrapMask)) {
			return nil
		}
		updateTargetRequest := &pb.UpdateTargetRequest{
			Target:     &schedulerpb.Target{Name: existing.GetName(), Etag: existing.GetEtag(), Url: desired.GetUrl(), Headers: desired.GetHeaders()},
			UpdateMask: targetBootstrapMask.Proto(),
		}
		if _, err := s.UpdateTarget(ctx, updateTargetRequest); err == nil {
			s.log.InfoContext(ctx, "bootstrap updated target", "name", existing.GetName())
			return nil
		} else if !status.HasCode(err, codes.Aborted) {
			return err
		}
	}
	return fmt.Errorf("lost %d etag races", bootstrapAttempts)
}

func (s *Service) bootstrapQueue(ctx context.Context, createQueueRequest *pb.CreateQueueRequest) error {
	// What the store holds once the service has normalized the policy and
	// resolved the handlers, so an unchanged file compares equal.
	desired := &schedulerpb.Queue{Policy: normalizePolicy(createQueueRequest.GetQueue().GetPolicy())}
	for _, handler := range createQueueRequest.GetQueue().GetHandlers() {
		desired.Handlers = append(desired.Handlers, &schedulerpb.Handler{Method: handler.GetMethod(), Target: handler.GetTarget()})
	}
	for range bootstrapAttempts {
		request := proto.CloneOf(createQueueRequest)
		request.RequestId = bootstrapRequestID(request.GetQueueId())
		existing, err := s.CreateQueue(ctx, request)
		if err != nil {
			return bootstrapCreateError(err)
		}
		current := masked(existing, queueBootstrapMask)
		for _, handler := range current.GetHandlers() {
			handler.RequestType, handler.ResponseType = "", ""
		}
		if proto.Equal(desired, current) {
			return nil
		}
		updateQueueRequest := &pb.UpdateQueueRequest{
			Queue:      &schedulerpb.Queue{Name: existing.GetName(), Etag: existing.GetEtag(), Policy: desired.GetPolicy(), Handlers: desired.GetHandlers()},
			UpdateMask: queueBootstrapMask.Proto(),
		}
		if _, err := s.UpdateQueue(ctx, updateQueueRequest); err == nil {
			s.log.InfoContext(ctx, "bootstrap updated queue", "name", existing.GetName())
			return nil
		} else if !status.HasCode(err, codes.Aborted) {
			return err
		}
	}
	return fmt.Errorf("lost %d etag races", bootstrapAttempts)
}

// logUnmanaged names every target and queue the files do not declare; they
// are left alone.
func (s *Service) logUnmanaged(ctx context.Context, bootstrap *pb.Bootstrap) error {
	targets, err := aip.Paginate[*schedulerpb.Target](ctx, &pb.ListTargetsRequest{}, s.ListTargets)
	if err != nil {
		return fmt.Errorf("listing targets: %w", err)
	}
	queues, err := aip.Paginate[*schedulerpb.Queue](ctx, &pb.ListQueuesRequest{}, s.ListQueues)
	if err != nil {
		return fmt.Errorf("listing queues: %w", err)
	}
	targetNameSet := map[string]struct{}{}
	for _, createTargetRequest := range bootstrap.GetTargets() {
		targetNameSet[(&schedulerpb.TargetResourceName{Target: createTargetRequest.GetTargetId()}).String()] = struct{}{}
	}
	queueNameSet := map[string]struct{}{}
	for _, createQueueRequest := range bootstrap.GetQueues() {
		queueNameSet[(&schedulerpb.QueueResourceName{Queue: createQueueRequest.GetQueueId()}).String()] = struct{}{}
	}
	for _, target := range targets {
		if _, ok := targetNameSet[target.GetName()]; !ok {
			s.log.InfoContext(ctx, "target not in bootstrap", "name", target.GetName())
		}
	}
	for _, queue := range queues {
		if _, ok := queueNameSet[queue.GetName()]; !ok {
			s.log.InfoContext(ctx, "queue not in bootstrap", "name", queue.GetName())
		}
	}
	return nil
}

func bootstrapRequestID(id string) string {
	return uuid.NewV5(bootstrapRequestIDNamespace, id).String()
}

// bootstrapCreateError explains the one Create failure specific to bootstrap:
// the resource exists under a request id bootstrap did not mint.
func bootstrapCreateError(err error) error {
	if status.HasCode(err, codes.AlreadyExists) {
		return fmt.Errorf("exists but was not created by bootstrap; delete it or drop it from the file: %w", err)
	}
	return err
}

// masked returns a copy of the message holding only the mask's fields.
func masked[M proto.Message](message M, mask *pbfieldmask.FieldMask) M {
	copied := proto.CloneOf(message)
	mask.Apply(copied)
	return copied
}
