package agent_service

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

var (
	// errTurnCancelled cancels a turn on CancelAgent or DeleteAgent: it ends CANCELLED.
	errTurnCancelled = errors.New("the turn was cancelled")
	// errLeaseLost and errShuttingDown stop a turn without ending it: another replica takes it over.
	errLeaseLost    = errors.New("the turn's lease was lost")
	errShuttingDown = errors.New("the replica is shutting down")
)

// turn is one run of an agent's turns on this replica, from the claim of its lease until it goes idle or stops.
type turn struct {
	agent     string
	ctx       context.Context
	cancel    context.CancelCauseFunc
	startTime time.Time
	// done is closed once the run ended.
	done chan struct{}
	// rerun is set when the agent's turn is claimed again while it runs: the run then goes on.
	rerun bool
}

// turns is the registry of the turns this replica runs.
type turns struct {
	mutex       sync.Mutex
	agentToTurn map[string]*turn
	stopped     bool
	// draining is closed when the replica starts shutting down: turns start no new generation.
	draining chan struct{}
}

func newTurns() *turns {
	return &turns{agentToTurn: map[string]*turn{}, draining: make(chan struct{})}
}

// hasRoom reports whether the replica may claim one more turn.
func (t *turns) hasRoom(maxConcurrentTurns int) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return !t.stopped && len(t.agentToTurn) < maxConcurrentTurns
}

func (t *turns) count() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return len(t.agentToTurn)
}

// agents returns the names of the agents whose turns run.
func (t *turns) agents() []string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return slices.Collect(maps.Keys(t.agentToTurn))
}

// start registers a run of the agent's turns, returning it, or marks the agent's running one for a rerun.
func (t *turns) start(ctx context.Context, agent string) *turn {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.stopped {
		return nil
	}
	if running, ok := t.agentToTurn[agent]; ok {
		running.rerun = true
		return nil
	}
	return t.register(ctx, agent)
}

// register adds a run. Callers hold the mutex.
func (t *turns) register(ctx context.Context, agent string) *turn {
	turnCtx, cancel := context.WithCancelCause(ctx)
	turn := &turn{agent: agent, ctx: turnCtx, cancel: cancel, startTime: time.Now(), done: make(chan struct{})}
	t.agentToTurn[agent] = turn
	return turn
}

// finish ends a run, returning the next one if it was claimed again meanwhile.
func (t *turns) finish(ctx context.Context, finished *turn) *turn {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	finished.cancel(nil)
	close(finished.done)
	delete(t.agentToTurn, finished.agent)
	if !finished.rerun || t.stopped {
		return nil
	}
	return t.register(ctx, finished.agent)
}

// cancel cancels the agent's running turn, if any, returning its done channel.
func (t *turns) cancel(agent string, cause error) (<-chan struct{}, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	running, ok := t.agentToTurn[agent]
	if !ok {
		return nil, false
	}
	running.cancel(cause)
	return running.done, true
}

// loseLeases cancels the turns started before since whose agent is not in heldAgentSet.
func (t *turns) loseLeases(heldAgentSet map[string]struct{}, since time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for agent, running := range t.agentToTurn {
		if _, ok := heldAgentSet[agent]; !ok && running.startTime.Before(since) {
			running.cancel(errLeaseLost)
		}
	}
}

// stop prevents any new turn and returns the running ones.
func (t *turns) stop() []*turn {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.draining)
	}
	running := make([]*turn, 0, len(t.agentToTurn))
	for _, turn := range t.agentToTurn {
		running = append(running, turn)
	}
	return running
}

// start runs the replica's claim and heartbeat loops. The returned func shuts the replica down: it stops claiming,
// gives in-flight tool calls the shutdown grace, then cancels the turns and releases their leases.
func (s *Service) start(ctx context.Context) (func(), error) {
	s.runCtx = context.WithoutCancel(ctx)
	claimCtx, stopClaiming := context.WithCancel(s.runCtx)
	heartbeatCtx, stopHeartbeats := context.WithCancel(s.runCtx)
	var claimWaitGroup, heartbeatWaitGroup sync.WaitGroup
	claimWaitGroup.Go(func() { s.runClaimLoop(claimCtx) })
	heartbeatWaitGroup.Go(func() { s.runHeartbeatLoop(heartbeatCtx) })
	return func() {
		stopClaiming()
		claimWaitGroup.Wait()
		s.stopTurns()
		// Heartbeats run until the turns stopped, so no lease expires during the grace.
		stopHeartbeats()
		heartbeatWaitGroup.Wait()
		if err := s.agentPostgresStore.ReleaseLeases(s.runCtx, s.replica); err != nil {
			s.log.Error("releasing leases", "error", err)
		}
		s.connections.close()
	}, nil
}

// stopTurns lets the running turns finish their tool calls within the shutdown grace, then cancels them.
func (s *Service) stopTurns() {
	running := s.turns.stop()
	waitAll := func(timeout time.Duration) bool {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for _, turn := range running {
			select {
			case <-turn.done:
			case <-timer.C:
				return false
			}
		}
		return true
	}
	if waitAll(s.opts.ShutdownGrace) {
		return
	}
	for _, turn := range running {
		turn.cancel(errShuttingDown)
	}
	if !waitAll(s.opts.ShutdownGrace) {
		s.log.Warn("turns still running after their cancellation")
	}
}

// startTurn runs the agent's turns on this replica, whose lease was just claimed.
func (s *Service) startTurn(agent string) {
	turn := s.turns.start(s.runCtx, agent)
	if turn == nil {
		return
	}
	go func() {
		for turn != nil {
			s.runTurns(turn)
			turn = s.turns.finish(s.runCtx, turn)
		}
	}()
}

// runClaimLoop claims the turns needing a replica, up to the replica's room: RUNNING agents whose lease expired
// (takeover) and IDLE agents with pending inputs (a safety net: inputs claim their agent's turn themselves).
func (s *Service) runClaimLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.ClaimInterval)
	defer ticker.Stop()
	for {
		if err := s.claimTurns(ctx); err != nil && ctx.Err() == nil {
			s.log.WarnContext(ctx, "claiming turns", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) claimTurns(ctx context.Context) error {
	room := s.opts.MaxConcurrentTurns - s.turns.count()
	if room <= 0 {
		return nil
	}
	return s.transact(ctx, func(ctx context.Context, t *txn) error {
		agentModels, err := t.ListClaimableAgents(ctx, room)
		if err != nil {
			return fmt.Errorf("listing claimable agents: %w", err)
		}
		for _, agentModel := range agentModels {
			agent, err := agentModel.ToPb()
			if err != nil {
				return status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
			}
			if err := t.claimTurn(ctx, agent); err != nil {
				return err
			}
		}
		return nil
	})
}

// runHeartbeatLoop renews the leases of the replica's turns, cancelling the turns whose lease was lost, or all of them once
// renewals failed for a whole lease duration: another replica may have taken them over. A lease no turn runs under,
// left by a turn that failed to end, expires: the claim loops take its turn over.
func (s *Service) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.HeartbeatInterval)
	defer ticker.Stop()
	lastRenewTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		renewTime := time.Now()
		heldAgents, err := s.agentPostgresStore.RenewLeases(ctx, s.replica, s.turns.agents(), s.opts.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.WarnContext(ctx, "renewing leases", "error", err)
			if time.Since(lastRenewTime) >= s.opts.LeaseDuration {
				s.turns.loseLeases(nil, renewTime)
			}
			continue
		}
		lastRenewTime = renewTime
		heldAgentSet := make(map[string]struct{}, len(heldAgents))
		for _, agent := range heldAgents {
			heldAgentSet[agent] = struct{}{}
		}
		s.turns.loseLeases(heldAgentSet, renewTime)
	}
}

// turnEnd is how a turn ended.
type turnEnd struct {
	outcome agentpb.Agent_TurnOutcome
	// The error a FAILED turn ended on.
	err error
	// The report queued on the parent agent, if any, and its request id.
	report          string
	reportRequestID string
}
