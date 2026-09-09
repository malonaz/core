package scheduler_service

import (
	"context"
	"errors"
	"strings"
	"time"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// typeURLPrefix is the prefix anypb gives type URLs; handler request and
// response types are recorded in the same form so payloads match them directly.
const typeURLPrefix = "type.googleapis.com/"

var defaultRetryBackoff = &schedulerpb.RetryBackoff{
	Initial:    durationpb.New(10 * time.Second),
	Max:        durationpb.New(10 * time.Minute),
	Multiplier: 2,
}

// defaultRetryableCodes are the transient failures worth another attempt;
// anything else means the request itself is wrong and would fail again.
var defaultRetryableCodes = []codepb.Code{
	codepb.Code_UNAVAILABLE, codepb.Code_INTERNAL, codepb.Code_UNKNOWN,
	codepb.Code_DEADLINE_EXCEEDED, codepb.Code_RESOURCE_EXHAUSTED, codepb.Code_ABORTED,
}

// normalizePolicy fills the defaults in, so the stored policy is what runs.
func normalizePolicy(policy *schedulerpb.QueuePolicy) *schedulerpb.QueuePolicy {
	policy = proto.CloneOf(policy)
	if policy.RetryBackoff == nil {
		policy.RetryBackoff = defaultRetryBackoff
	}
	if len(policy.RetryableCodes) == 0 {
		policy.RetryableCodes = defaultRetryableCodes
	}
	return policy
}

// resolveHandlers checks every handler routes to a known method on an
// existing target and stamps the method's request and response types.
func (s *Service) resolveHandlers(ctx context.Context, handlers []*schedulerpb.Handler) ([]*schedulerpb.Handler, error) {
	resolved := make([]*schedulerpb.Handler, 0, len(handlers))
	requestTypeToMethod := map[string]string{}
	for _, handler := range handlers {
		method, err := s.resolveMethod(handler.GetMethod())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "handler %s: %v", handler.GetMethod(), err).Err()
		}
		targetID, err := model.ParseTargetName(handler.GetTarget())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "handler %s: parsing target: %v", handler.GetMethod(), err).Err()
		}
		if _, err := s.schedulerPostgresStore.GetTarget(ctx, targetID); err != nil {
			if errors.Is(err, model.ErrTargetNotExist) {
				return nil, status.Errorf(codes.InvalidArgument, "handler %s: target %q does not exist", handler.GetMethod(), handler.GetTarget()).Err()
			}
			return nil, status.FromError(err, "getting target").Err()
		}
		requestType := typeURLPrefix + string(method.Input().FullName())
		if other, ok := requestTypeToMethod[requestType]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "handlers %s and %s both accept %s", other, handler.GetMethod(), requestType).Err()
		}
		requestTypeToMethod[requestType] = handler.GetMethod()
		resolved = append(resolved, &schedulerpb.Handler{
			Method:       handler.GetMethod(),
			Target:       handler.GetTarget(),
			RequestType:  requestType,
			ResponseType: typeURLPrefix + string(method.Output().FullName()),
		})
	}
	return resolved, nil
}

// CreateQueue accepts only the producer-owned fields; the queue starts RUNNING.
func (s *Service) CreateQueue(ctx context.Context, request *pb.CreateQueueRequest) (*schedulerpb.Queue, error) {
	handlers, err := s.resolveHandlers(ctx, request.GetQueue().GetHandlers())
	if err != nil {
		return nil, err
	}
	request.Queue = &schedulerpb.Queue{
		State:    schedulerpb.QueueState_QUEUE_STATE_RUNNING,
		Policy:   normalizePolicy(request.GetQueue().GetPolicy()),
		Handlers: handlers,
	}
	queue, err := s.SchedulerServiceServer.CreateQueue(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.withQueueStats(ctx, queue)
}

func (s *Service) GetQueue(ctx context.Context, request *pb.GetQueueRequest) (*schedulerpb.Queue, error) {
	queue, err := s.SchedulerServiceServer.GetQueue(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.withQueueStats(ctx, queue)
}

func (s *Service) ListQueues(ctx context.Context, request *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	listQueuesResponse, err := s.SchedulerServiceServer.ListQueues(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.attachQueueStats(ctx, listQueuesResponse.GetQueues()); err != nil {
		return nil, err
	}
	return listQueuesResponse, nil
}

func (s *Service) BatchGetQueues(ctx context.Context, request *pb.BatchGetQueuesRequest) (*pb.BatchGetQueuesResponse, error) {
	batchGetQueuesResponse, err := s.SchedulerServiceServer.BatchGetQueues(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.attachQueueStats(ctx, batchGetQueuesResponse.GetQueues()); err != nil {
		return nil, err
	}
	return batchGetQueuesResponse, nil
}

// UpdateQueue applies the mask itself so that the whole policy and handler
// list can be normalized and validated, whichever sub-paths were sent.
func (s *Service) UpdateQueue(ctx context.Context, request *pb.UpdateQueueRequest) (*schedulerpb.Queue, error) {
	getQueueRequest := &pb.GetQueueRequest{Name: request.GetQueue().GetName()}
	existing, err := s.SchedulerServiceServer.GetQueue(ctx, getQueueRequest)
	if err != nil {
		return nil, err
	}
	fieldMask := pbfieldmask.New(request.GetUpdateMask())
	if err := fieldMask.Validate(existing); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid update_mask: %v", err).Err()
	}
	patched := proto.CloneOf(existing)
	fieldMask.Update(patched, request.GetQueue())

	rootPathSet := map[string]struct{}{}
	for _, path := range request.GetUpdateMask().GetPaths() {
		root, _, _ := strings.Cut(path, ".")
		rootPathSet[root] = struct{}{}
	}
	if _, ok := rootPathSet["policy"]; ok {
		patched.Policy = normalizePolicy(patched.GetPolicy())
	}
	if _, ok := rootPathSet["handlers"]; ok {
		if patched.Handlers, err = s.resolveHandlers(ctx, patched.GetHandlers()); err != nil {
			return nil, err
		}
	}
	etag := request.GetQueue().GetEtag()
	if etag == "" {
		etag = existing.GetEtag()
	}
	request.Queue = &schedulerpb.Queue{Name: existing.GetName(), Etag: etag, Policy: patched.GetPolicy(), Handlers: patched.GetHandlers()}
	request.UpdateMask = &fieldmaskpb.FieldMask{}
	for root := range rootPathSet {
		request.UpdateMask.Paths = append(request.UpdateMask.Paths, root)
	}
	queue, err := s.SchedulerServiceServer.UpdateQueue(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.withQueueStats(ctx, queue)
}

// DeleteQueue refuses to delete a queue that still has PENDING or RUNNING jobs.
func (s *Service) DeleteQueue(ctx context.Context, request *pb.DeleteQueueRequest) (*emptypb.Empty, error) {
	live, err := s.schedulerPostgresStore.QueueHasLiveJobs(ctx, request.GetName())
	if err != nil {
		return nil, status.FromError(err, "checking queue jobs").Err()
	}
	if live {
		return nil, status.Errorf(codes.FailedPrecondition, "queue has pending or running jobs").Err()
	}
	return s.SchedulerServiceServer.DeleteQueue(ctx, request)
}

// PauseQueue stops claims on the queue; running jobs finish.
func (s *Service) PauseQueue(ctx context.Context, request *pb.PauseQueueRequest) (*schedulerpb.Queue, error) {
	return s.setQueueState(ctx, request.GetName(), request.GetEtag(), schedulerpb.QueueState_QUEUE_STATE_PAUSED)
}

// ResumeQueue restarts claims on the queue at once.
func (s *Service) ResumeQueue(ctx context.Context, request *pb.ResumeQueueRequest) (*schedulerpb.Queue, error) {
	queue, err := s.setQueueState(ctx, request.GetName(), request.GetEtag(), schedulerpb.QueueState_QUEUE_STATE_RUNNING)
	if err != nil {
		return nil, err
	}
	s.wakeClaim()
	return queue, nil
}

// setQueueState moves the queue to state under a row lock; a queue already in
// that state is returned untouched.
func (s *Service) setQueueState(ctx context.Context, name, etag string, state schedulerpb.QueueState) (*schedulerpb.Queue, error) {
	queueID, err := model.ParseQueueName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	now := truncatedNow()
	queueModel, err := s.schedulerPostgresStore.TransitionQueue(ctx, queueID, func(queueModel *model.Queue) (bool, error) {
		if etag != "" && queueModel.Etag != etag {
			return false, model.ErrQueueETagChanged
		}
		if queueModel.State == int16(state) {
			return false, nil
		}
		queue, err := queueModel.ToPb()
		if err != nil {
			return false, err
		}
		queue.State = state
		queue.UpdateTime = timestamppb.New(now)
		if queue.Etag, err = aip.ComputeETag(queue); err != nil {
			return false, err
		}
		mutated, err := model.QueueFromPb(queue)
		if err != nil {
			return false, err
		}
		*queueModel = *mutated
		return true, nil
	})
	if err != nil {
		switch {
		case errors.Is(err, model.ErrQueueNotExist):
			return nil, status.Errorf(codes.NotFound, "queue does not exist").Err()
		case errors.Is(err, model.ErrQueueETagChanged):
			return nil, status.Errorf(codes.Aborted, "ETag changed").Err()
		}
		return nil, status.FromError(err, "transitioning queue").Err()
	}
	queue, err := queueModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting queue from model to pb: %v", err).Err()
	}
	return s.withQueueStats(ctx, queue)
}

func (s *Service) withQueueStats(ctx context.Context, queue *schedulerpb.Queue) (*schedulerpb.Queue, error) {
	if err := s.attachQueueStats(ctx, []*schedulerpb.Queue{queue}); err != nil {
		return nil, err
	}
	return queue, nil
}

// attachQueueStats fills every queue's backlog from one grouped query.
func (s *Service) attachQueueStats(ctx context.Context, queues []*schedulerpb.Queue) error {
	if len(queues) == 0 {
		return nil
	}
	queueIDs := make([]string, len(queues))
	for i, queue := range queues {
		queueID, err := model.ParseQueueName(queue.GetName())
		if err != nil {
			return status.Errorf(codes.Internal, "parsing queue name: %v", err).Err()
		}
		queueIDs[i] = queueID
	}
	stats, err := s.schedulerPostgresStore.ListQueueStats(ctx, queueIDs)
	if err != nil {
		return status.FromError(err, "listing queue stats").Err()
	}
	queueIDToStats := make(map[string]*schedulerpb.QueueStats, len(stats))
	for _, queueStats := range stats {
		queueIDToStats[queueStats.QueueID] = queueStatsToPb(queueStats)
	}
	for i, queue := range queues {
		queue.Stats = queueIDToStats[queueIDs[i]]
	}
	return nil
}

func queueStatsToPb(stats *store.QueueStats) *schedulerpb.QueueStats {
	queueStats := &schedulerpb.QueueStats{
		PendingCount: int32(stats.PendingCount),
		RunningCount: int32(stats.RunningCount),
	}
	if stats.OldestPendingScheduleTime != nil {
		queueStats.OldestPendingScheduleTime = timestamppb.New(*stats.OldestPendingScheduleTime)
	}
	return queueStats
}
