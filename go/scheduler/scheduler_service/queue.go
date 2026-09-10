package scheduler_service

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
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
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/transition"
)

// CreateQueue accepts only the dispatcher-owned fields; the queue starts
// RUNNING. A queue already serving the method, or already routing the request
// type, is an ALREADY_EXISTS.
func (s *Service) CreateQueue(ctx context.Context, request *pb.CreateQueueRequest) (*schedulerpb.Queue, error) {
	declared := request.GetQueue()
	request.Queue = &schedulerpb.Queue{
		State:        schedulerpb.QueueState_QUEUE_STATE_RUNNING,
		Service:      declared.GetService(),
		Method:       declared.GetMethod(),
		Endpoint:     declared.GetEndpoint(),
		RequestType:  declared.GetRequestType(),
		ResponseType: declared.GetResponseType(),
		Policy:       scheduler.NormalizePolicy(declared.GetPolicy()),
	}
	queue, err := s.SchedulerServiceServer.CreateQueue(ctx, request)
	if err != nil {
		return nil, queueConflictError(err, declared)
	}
	return s.withQueueStats(ctx, queue)
}

// queueConflictError names the uniqueness a create or update broke.
func queueConflictError(err error, queue *schedulerpb.Queue) error {
	switch {
	case store.IsQueueMethodConflict(err):
		return status.Errorf(codes.AlreadyExists, "a queue already serves %s.%s", queue.GetService(), queue.GetMethod()).Err()
	case store.IsQueueRequestTypeConflict(err):
		return status.Errorf(codes.AlreadyExists, "a queue already routes %s", queue.GetRequestType()).Err()
	}
	return err
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

// UpdateQueue applies the mask itself so that the whole policy can be
// normalized, whichever sub-paths were sent.
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
		patched.Policy = scheduler.NormalizePolicy(patched.GetPolicy())
	}
	etag := request.GetQueue().GetEtag()
	if etag == "" {
		etag = existing.GetEtag()
	}
	request.Queue = &schedulerpb.Queue{Name: existing.GetName(), Etag: etag, Endpoint: patched.GetEndpoint(), ResponseType: patched.GetResponseType(), Policy: patched.GetPolicy()}
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
	return queue, nil
}

// setQueueState moves the queue to state under a row lock; a queue already in
// that state is returned untouched.
func (s *Service) setQueueState(ctx context.Context, name, etag string, state schedulerpb.QueueState) (*schedulerpb.Queue, error) {
	queueID, err := model.ParseQueueName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	now := transition.Now()
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
