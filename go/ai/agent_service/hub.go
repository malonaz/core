package agent_service

import (
	"sync"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
)

// subscriptionBufferSize bounds the activity a subscription queues: a stream falling further behind is ended
// rather than slowing the turns publishing it.
const subscriptionBufferSize = 1024

// hub fans this replica's live agent activity out to its StreamAgent calls.
type hub struct {
	mutex                sync.RWMutex
	agentToSubscriptions map[string]map[*subscription]struct{}
}

// subscription receives the activity of the agents it is subscribed to.
type subscription struct {
	responses chan *agentservicepb.StreamAgentResponse
	// overflowed is closed once the subscription dropped activity: its stream must end.
	overflowed   chan struct{}
	overflowOnce sync.Once
}

func newHub() *hub {
	return &hub{agentToSubscriptions: map[string]map[*subscription]struct{}{}}
}

// subscribe returns a subscription to no agent yet.
func (h *hub) subscribe() *subscription {
	return &subscription{
		responses:  make(chan *agentservicepb.StreamAgentResponse, subscriptionBufferSize),
		overflowed: make(chan struct{}),
	}
}

// add subscribes a subscription to an agent's activity.
func (h *hub) add(agentSubscription *subscription, agent string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	subscriptionSet, ok := h.agentToSubscriptions[agent]
	if !ok {
		subscriptionSet = map[*subscription]struct{}{}
		h.agentToSubscriptions[agent] = subscriptionSet
	}
	subscriptionSet[agentSubscription] = struct{}{}
}

// unsubscribe removes a subscription from the given agents.
func (h *hub) unsubscribe(subscription *subscription, agents ...string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	for _, agent := range agents {
		subscriptionSet := h.agentToSubscriptions[agent]
		delete(subscriptionSet, subscription)
		if len(subscriptionSet) == 0 {
			delete(h.agentToSubscriptions, agent)
		}
	}
}

// publish sends a piece of an agent's activity, keyed by response.Agent, to the subscriptions to it. It never
// blocks: a subscription with a full buffer overflows.
func (h *hub) publish(response *agentservicepb.StreamAgentResponse) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for subscription := range h.agentToSubscriptions[response.Agent] {
		select {
		case subscription.responses <- response:
		default:
			subscription.overflowOnce.Do(func() { close(subscription.overflowed) })
		}
	}
}
