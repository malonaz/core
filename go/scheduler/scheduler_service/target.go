package scheduler_service

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
)

// validateURL rejects what buf.validate's scheme check lets through, e.g. a
// bad port.
func validateURL(url string) error {
	if _, err := grpc.ParseOpts(url); err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing url: %v", err).Err()
	}
	return nil
}

// CreateTarget accepts only the producer-owned fields.
func (s *Service) CreateTarget(ctx context.Context, request *pb.CreateTargetRequest) (*schedulerpb.Target, error) {
	target := request.GetTarget()
	if err := validateURL(target.GetUrl()); err != nil {
		return nil, err
	}
	request.Target = &schedulerpb.Target{Url: target.GetUrl(), Headers: target.GetHeaders()}
	return s.SchedulerServiceServer.CreateTarget(ctx, request)
}

// UpdateTarget validates a new url; workers pick the change up through the
// new etag on their next call.
func (s *Service) UpdateTarget(ctx context.Context, request *pb.UpdateTargetRequest) (*schedulerpb.Target, error) {
	if slices.Contains(request.GetUpdateMask().GetPaths(), "url") {
		if err := validateURL(request.GetTarget().GetUrl()); err != nil {
			return nil, err
		}
	}
	return s.SchedulerServiceServer.UpdateTarget(ctx, request)
}

// DeleteTarget refuses to delete a target a queue handler still routes to.
func (s *Service) DeleteTarget(ctx context.Context, request *pb.DeleteTargetRequest) (*emptypb.Empty, error) {
	referenced, err := s.schedulerPostgresStore.TargetIsReferenced(ctx, request.GetName())
	if err != nil {
		return nil, status.FromError(err, "checking target references").Err()
	}
	if referenced {
		return nil, status.Errorf(codes.FailedPrecondition, "target is referenced by a queue handler").Err()
	}
	deleted, err := s.SchedulerServiceServer.DeleteTarget(ctx, request)
	if err != nil {
		return nil, err
	}
	s.targets.evict(request.GetName())
	s.schemas.evict(request.GetName())
	return deleted, nil
}

// outgoingContext carries the target's headers on every call made to it,
// reflection included.
func outgoingContext(ctx context.Context, target *schedulerpb.Target) context.Context {
	for key, value := range target.GetHeaders() {
		ctx = metadata.AppendToOutgoingContext(ctx, key, value)
	}
	return ctx
}

// targetConnection is a dialled target. Once its target is updated it is
// stale: closed as soon as the last call using it returns.
type targetConnection struct {
	etag       string
	connection *grpc.Connection
	inflight   int
	stale      bool
}

func (c *targetConnection) closeIfRetired() {
	if c.stale && c.inflight == 0 {
		c.connection.Close()
	}
}

// targetConnections caches one connection per target, keyed by the target's
// etag so an update re-dials.
type targetConnections struct {
	mutex            sync.Mutex
	nameToConnection map[string]*targetConnection
}

func newTargetConnections() *targetConnections {
	return &targetConnections{nameToConnection: map[string]*targetConnection{}}
}

// acquire returns a connection to the target as it currently stands, dialling
// on first use or when the target changed. release must be called once the
// call ends.
func (c *targetConnections) acquire(ctx context.Context, target *schedulerpb.Target) (*grpc.Connection, func(), error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	current, ok := c.nameToConnection[target.GetName()]
	if !ok || current.etag != target.GetEtag() {
		connection, err := dial(ctx, target)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			current.stale = true
			current.closeIfRetired()
		}
		current = &targetConnection{etag: target.GetEtag(), connection: connection}
		c.nameToConnection[target.GetName()] = current
	}
	current.inflight++
	release := func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		current.inflight--
		current.closeIfRetired()
	}
	return current.connection, release, nil
}

// evict forgets a deleted target's connection.
func (c *targetConnections) evict(name string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if current, ok := c.nameToConnection[name]; ok {
		delete(c.nameToConnection, name)
		current.stale = true
		current.closeIfRetired()
	}
}

func (c *targetConnections) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for name, current := range c.nameToConnection {
		delete(c.nameToConnection, name)
		current.connection.Close()
	}
}

func dial(ctx context.Context, target *schedulerpb.Target) (*grpc.Connection, error) {
	opts, err := grpc.ParseOpts(target.GetUrl())
	if err != nil {
		return nil, fmt.Errorf("parsing url %q: %w", target.GetUrl(), err)
	}
	connection, err := grpc.NewConnection(opts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	// The dial is non-blocking; the connection outlives the call that opened it.
	if err := connection.Connect(context.WithoutCancel(ctx)); err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	return connection, nil
}
