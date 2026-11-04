package agent_executor

import (
	"context"
	"sync"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// agent is the actor holding one leased agent: it runs the agent's turns one
// at a time, woken by its inputs.
type agent struct {
	executor *Executor
	agent    *agentpb.Agent
	// send writes to the connection the agent was leased on.
	send func(ctx context.Context, request *pb.ConnectExecutorRequest) error

	// ctx spans the lease: cancelled on revoke, disconnect or shutdown.
	ctx  context.Context
	stop context.CancelFunc
	done chan struct{}
	wake chan struct{}

	mutex         sync.Mutex
	pendingInputs []*agentpb.Input
	// inputNameSet dedupes inputs the service redelivers.
	inputNameSet map[string]struct{}
	// cancelTurn cancels the running turn. Nil while idle.
	cancelTurn context.CancelFunc
	// draining stops the agent once its running turn, if any, ended.
	draining bool

	// predecessorDone, if set, closes once the agent's previous actor is done:
	// no turn runs before, so two actors never run one agent.
	predecessorDone <-chan struct{}

	// instructionsEnsured is set once the lease's first turn persisted the
	// system message. Touched by the run loop only.
	instructionsEnsured bool
}

func newAgent(ctx context.Context, executor *Executor, leasedAgent *agentpb.Agent, send func(context.Context, *pb.ConnectExecutorRequest) error) *agent {
	a := &agent{
		executor:     executor,
		agent:        leasedAgent,
		send:         send,
		done:         make(chan struct{}),
		wake:         make(chan struct{}, 1),
		inputNameSet: map[string]struct{}{},
	}
	a.ctx, a.stop = context.WithCancel(ctx)
	return a
}

// run runs the agent's turns until its lease ends or it drained. Closes done on
// return.
func (a *agent) run() {
	defer close(a.done)
	if a.predecessorDone != nil {
		select {
		case <-a.ctx.Done():
			return
		case <-a.predecessorDone:
		}
	}
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.wake:
		}
		turnCtx, inputs, ok := a.startTurn()
		if !ok {
			return
		}
		if len(inputs) == 0 {
			continue
		}
		a.runTurn(turnCtx, inputs)
		a.endTurn()
	}
}

// startTurn drains the pending inputs and arms the turn's cancellation in one
// step, so a cancel never falls between them. Not ok once the agent must stop.
func (a *agent) startTurn() (context.Context, []*agentpb.Input, bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.draining || a.ctx.Err() != nil {
		return nil, nil, false
	}
	inputs := a.pendingInputs
	a.pendingInputs = nil
	if len(inputs) == 0 {
		return nil, nil, true
	}
	turnCtx, cancelTurn := context.WithCancel(a.ctx)
	a.cancelTurn = cancelTurn
	return turnCtx, inputs, true
}

// endTurn disarms the turn's cancellation.
func (a *agent) endTurn() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.cancelTurn()
	a.cancelTurn = nil
	// Inputs that arrived during the final generation start the next turn.
	if a.draining || len(a.pendingInputs) > 0 {
		a.signal()
	}
}

// addInput queues an input, waking the agent.
func (a *agent) addInput(input *agentpb.Input) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if _, ok := a.inputNameSet[input.GetName()]; ok {
		return
	}
	a.inputNameSet[input.GetName()] = struct{}{}
	a.pendingInputs = append(a.pendingInputs, input)
	a.signal()
}

// takeInputs drains the inputs queued since the turn last looked, for a
// running turn to pick up between tool rounds.
func (a *agent) takeInputs() []*agentpb.Input {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	inputs := a.pendingInputs
	a.pendingInputs = nil
	return inputs
}

// cancel aborts the running turn, if any. Pending inputs stay queued.
func (a *agent) cancel() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.cancelTurn != nil {
		a.cancelTurn()
	}
}

// drain cancels the running turn, if any, and starts no other: the agent stops
// once idle, closing done, while its lease still lets it report the turn's end.
func (a *agent) drain() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.draining = true
	if a.cancelTurn != nil {
		a.cancelTurn()
	}
	a.signal()
}

// signal wakes the run loop without blocking. Callers hold the mutex.
func (a *agent) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// report sends one of the agent's events up. It outlives a cancelled turn, so
// the turn can still report how it ended.
func (a *agent) report(event *agentpb.Event) error {
	request := &pb.ConnectExecutorRequest{
		Content: &pb.ConnectExecutorRequest_Report_{
			Report: &pb.ConnectExecutorRequest_Report{
				Agent: a.agent.GetName(),
				Event: event,
			},
		},
	}
	return a.send(a.ctx, request)
}

// delta sends a block of an in-flight generation up.
func (a *agent) delta(block *aipb.Block) error {
	request := &pb.ConnectExecutorRequest{
		Content: &pb.ConnectExecutorRequest_Delta{
			Delta: &agentpb.Delta{
				Agent: a.agent.GetName(),
				Block: block,
			},
		},
	}
	return a.send(a.ctx, request)
}
