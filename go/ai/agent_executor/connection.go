package agent_executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// errConnectionClosed fails the sends of a connection that ended or is closing.
var errConnectionClosed = errors.New("connection closed")

// permanentRegistrationCodes refuse a registration for good: no retry fixes the
// executor's configuration or credentials.
var permanentRegistrationCodes = []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition}

// Run connects to the AgentService and runs the agents it leases, reconnecting
// with backoff whenever the connection is lost. It returns nil once ctx is
// done, after the running turns had a chance to report their cancellation, and
// an error if the service refuses the registration for good.
func (e *Executor) Run(ctx context.Context) error {
	if e.opts.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive, got %s", e.opts.HeartbeatInterval)
	}
	if e.opts.MaxAgents <= 0 {
		return fmt.Errorf("max agents must be positive, got %d", e.opts.MaxAgents)
	}
	reconnectBackOff := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(e.opts.ReconnectInitialInterval),
		backoff.WithMaxInterval(e.opts.ReconnectMaxInterval),
		backoff.WithMaxElapsedTime(0),
	)
	for {
		registered, err := e.connect(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if !registered && status.HasCode(err, permanentRegistrationCodes...) {
			return fmt.Errorf("registration refused: %w", err)
		}
		// Only failures in a row back off further.
		if registered {
			reconnectBackOff.Reset()
		}
		delay := reconnectBackOff.NextBackOff()
		e.log.Warn("disconnected", "error", err, "reconnect_delay", delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// connect runs one connection until it fails or ctx is done, reporting whether
// the executor registered.
func (e *Executor) connect(ctx context.Context) (bool, error) {
	// The stream outlives ctx, so a shutdown can still report cancelled turns.
	streamCtx, cancelStream := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelStream()
	stream, err := e.agentServiceClient.ConnectExecutor(streamCtx)
	if err != nil {
		return false, fmt.Errorf("connecting: %w", err)
	}
	executor, err := e.register(ctx, stream, cancelStream)
	if err != nil {
		return false, err
	}
	c := &connection{
		executor:         e,
		log:              e.log.With("executor", executor.GetName()),
		stream:           stream,
		requests:         make(chan *pb.ConnectExecutorRequest),
		closing:          make(chan struct{}),
		agentNameToAgent: map[string]*agent{},
	}
	c.log.Info("connected")
	return true, c.serve(ctx, streamCtx, cancelStream)
}

// register registers the executor on a new stream and returns it as registered.
func (e *Executor) register(ctx context.Context, stream pb.AgentService_ConnectExecutorClient, cancelStream context.CancelFunc) (*agentpb.Executor, error) {
	// Until registered, there is nothing to shut down gracefully.
	stop := context.AfterFunc(ctx, cancelStream)
	defer stop()
	registerRequest := &pb.ConnectExecutorRequest{
		Content: &pb.ConnectExecutorRequest_Register_{
			Register: &pb.ConnectExecutorRequest_Register{
				Parent:     e.userRn.String(),
				ExecutorId: e.opts.ExecutorID,
				Executor: &agentpb.Executor{
					DisplayName: e.opts.DisplayName,
					Labels:      e.opts.Labels,
					MaxAgents:   e.opts.MaxAgents,
					Tools:       slices.Sorted(maps.Keys(e.toolNameToTool)),
				},
			},
		},
	}
	if err := stream.Send(registerRequest); err != nil {
		return nil, fmt.Errorf("registering: %w", err)
	}
	connectExecutorResponse, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("awaiting registration: %w", err)
	}
	executor := connectExecutorResponse.GetRegistered()
	if executor == nil {
		return nil, fmt.Errorf("expected registered, got %T", connectExecutorResponse.GetContent())
	}
	return executor, nil
}

// connection is one registered ConnectExecutor stream and the agents leased on it.
type connection struct {
	executor *Executor
	log      *slog.Logger
	stream   pb.AgentService_ConnectExecutorClient

	// ctx ends with the connection, and with it every lease taken on it.
	ctx context.Context
	// requests feeds the one writer: streams forbid concurrent sends.
	requests chan *pb.ConnectExecutorRequest
	// closing is closed once a shutdown half-closes the stream.
	closing chan struct{}

	// agentNameToAgent holds the agents leased on the connection. Touched by
	// serve only.
	agentNameToAgent map[string]*agent
	// agents tracks every actor started on the connection, revoked ones included.
	agents sync.WaitGroup
}

// serve dispatches what the service sends until the connection fails or ctx is
// done, then ends every lease and waits for their actors.
func (c *connection) serve(ctx, streamCtx context.Context, cancelStream context.CancelFunc) error {
	eg, ctxEg := errgroup.WithContext(streamCtx)
	c.ctx = ctxEg
	responses := make(chan *pb.ConnectExecutorResponse)
	eg.Go(func() error { return c.write(ctxEg) })
	eg.Go(func() error { return c.read(ctxEg, responses) })
	eg.Go(func() error { return c.heartbeat(ctxEg) })

loop:
	for {
		select {
		case <-ctx.Done():
			c.shutdown()
			break loop
		case <-ctxEg.Done():
			break loop
		case connectExecutorResponse := <-responses:
			c.dispatch(connectExecutorResponse)
		}
	}
	// Leases derive from the stream's context: this stops every actor.
	cancelStream()
	c.agents.Wait()
	err := eg.Wait()
	if ctx.Err() != nil {
		c.log.Info("disconnected on shutdown")
		return nil
	}
	return err
}

// shutdown cancels the running turns and, once they reported their end,
// half-closes the stream and waits for the service to close it, all within
// the shutdown timeout.
func (c *connection) shutdown() {
	ctx, cancel := context.WithTimeout(c.ctx, c.executor.opts.ShutdownTimeout)
	defer cancel()
	for _, actor := range c.agentNameToAgent {
		actor.drain()
	}
	for _, actor := range c.agentNameToAgent {
		select {
		case <-actor.done:
		case <-ctx.Done():
		}
	}
	close(c.closing)
	// The reader's end of stream ends c.ctx, and with it ctx.
	<-ctx.Done()
}

// write sends the queued requests up, one at a time.
func (c *connection) write(ctx context.Context) error {
	for {
		select {
		case request := <-c.requests:
			if err := c.stream.Send(request); err != nil {
				return fmt.Errorf("sending: %w", err)
			}
		case <-c.closing:
			return c.stream.CloseSend()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// read forwards what the service sends to serve, until the stream ends.
func (c *connection) read(ctx context.Context, responses chan<- *pb.ConnectExecutorResponse) error {
	for {
		connectExecutorResponse, err := c.stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("stream closed by the service")
		}
		if err != nil {
			return fmt.Errorf("receiving: %w", err)
		}
		select {
		case responses <- connectExecutorResponse:
		case <-c.closing:
			// Shutting down: drop it and read on until the service closes the stream.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// heartbeat proves the executor alive until the connection ends.
func (c *connection) heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(c.executor.opts.HeartbeatInterval)
	defer ticker.Stop()
	heartbeatRequest := &pb.ConnectExecutorRequest{
		Content: &pb.ConnectExecutorRequest_Heartbeat_{Heartbeat: &pb.ConnectExecutorRequest_Heartbeat{}},
	}
	for {
		select {
		case <-ticker.C:
			// A failed send means the connection is ending: nothing to report.
			if err := c.send(ctx, heartbeatRequest); err != nil {
				return nil
			}
		case <-c.closing:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// send queues a request for the writer, blocking until it takes it.
func (c *connection) send(ctx context.Context, request *pb.ConnectExecutorRequest) error {
	// Checked first: select picks among ready cases at random.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.requests <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return errConnectionClosed
	case <-c.closing:
		return errConnectionClosed
	}
}

// dispatch acts on one message from the service.
func (c *connection) dispatch(connectExecutorResponse *pb.ConnectExecutorResponse) {
	switch content := connectExecutorResponse.GetContent().(type) {
	case *pb.ConnectExecutorResponse_Lease:
		c.lease(content.Lease)
	case *pb.ConnectExecutorResponse_Input:
		c.input(content.Input)
	case *pb.ConnectExecutorResponse_Cancel_:
		if actor, ok := c.agentNameToAgent[content.Cancel.GetAgent()]; ok {
			c.log.Debug("cancelling turn", "agent", content.Cancel.GetAgent())
			actor.cancel()
		}
	case *pb.ConnectExecutorResponse_Revoke_:
		if actor, ok := c.agentNameToAgent[content.Revoke.GetAgent()]; ok {
			c.log.Info("agent revoked", "agent", content.Revoke.GetAgent())
			actor.stop()
			delete(c.agentNameToAgent, content.Revoke.GetAgent())
		}
	default:
		c.log.Warn("unexpected message", "content", fmt.Sprintf("%T", content))
	}
}

// lease starts an actor for a leased agent, or releases the agent if the
// executor cannot serve it.
func (c *connection) lease(leasedAgent *agentpb.Agent) {
	log := c.log.With("agent", leasedAgent.GetName())
	if _, ok := c.agentNameToAgent[leasedAgent.GetName()]; ok {
		log.Debug("ignoring the lease of a held agent")
		return
	}
	if missingToolNames := c.executor.missingToolNames(leasedAgent); len(missingToolNames) > 0 {
		log.Warn("releasing an agent needing tools the executor lacks", "tools", missingToolNames)
		c.release(leasedAgent.GetName())
		return
	}
	if len(c.agentNameToAgent) >= int(c.executor.opts.MaxAgents) {
		log.Warn("releasing an agent beyond capacity", "max_agents", c.executor.opts.MaxAgents)
		c.release(leasedAgent.GetName())
		return
	}
	actor := newAgent(c.ctx, c.executor, leasedAgent, c.send)
	actor.predecessorDone = c.executor.adopt(actor)
	c.agentNameToAgent[leasedAgent.GetName()] = actor
	c.agents.Add(1)
	go func() {
		defer c.agents.Done()
		actor.run()
		c.executor.forget(actor)
	}()
	log.Info("agent leased")
}

// release gives a leased agent back to the service.
func (c *connection) release(agentName string) {
	releaseRequest := &pb.ConnectExecutorRequest{
		Content: &pb.ConnectExecutorRequest_Release_{
			Release: &pb.ConnectExecutorRequest_Release{Agent: agentName},
		},
	}
	if err := c.send(c.ctx, releaseRequest); err != nil {
		c.log.Debug("releasing agent", "agent", agentName, "error", err)
	}
}

// input queues an input on the held agent it is for.
func (c *connection) input(input *agentpb.Input) {
	inputRn, err := agentpb.ParseInputRn(input.GetName())
	if err != nil {
		c.log.Warn("ignoring an input with an invalid name", "input", input.GetName(), "error", err)
		return
	}
	actor, ok := c.agentNameToAgent[inputRn.AgentRn().String()]
	if !ok {
		c.log.Debug("ignoring an input for an agent not held", "input", input.GetName())
		return
	}
	actor.addInput(input)
}
