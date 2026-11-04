package agent_service

import (
	"context"
	"sync"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
)

// subscriptionBufferSize bounds what a slow subscriber may lag behind before traffic to it is dropped.
const subscriptionBufferSize = 1024

// broker carries live traffic: committed events and deltas to the streams watching an agent tree, and commands
// to the executor sessions they are for. Postgres is the source of truth, so everything a broker carries is a
// best-effort hint: streams fill gaps from the event log and sessions reconcile with the database.
type broker interface {
	// publishEvents fans out committed events of one agent of the tree rooted at rootAgent.
	publishEvents(ctx context.Context, rootAgent string, events []*agentpb.Event)
	// publishDelta fans out a delta of one agent of the tree rooted at rootAgent.
	publishDelta(ctx context.Context, rootAgent string, delta *agentpb.Delta)
	// subscribeTree subscribes to the events of every agent of the tree rooted at rootAgent, and to their deltas if withDeltas.
	subscribeTree(rootAgent string, withDeltas bool) *subscription[*agentservicepb.StreamAgentResponse]
	// sendToSession sends a command to an executor session, dropped if no such session is subscribed.
	sendToSession(ctx context.Context, session string, response *agentservicepb.ConnectExecutorResponse)
	// subscribeSession subscribes to the commands sent to an executor session.
	subscribeSession(session string) *subscription[*agentservicepb.ConnectExecutorResponse]
}

// subscription is a stream of broker traffic. Close it once done.
type subscription[T any] struct {
	C     <-chan T
	close func()
}

// Close stops the subscription.
func (s *subscription[T]) Close() { s.close() }

// localBroker is an in-process broker: it serves a single replica.
type localBroker struct {
	mutex                       sync.Mutex
	rootAgentToTreeSubscribers  map[string]map[*treeSubscriber]struct{}
	sessionToSessionSubscribers map[string]map[chan *agentservicepb.ConnectExecutorResponse]struct{}
}

type treeSubscriber struct {
	channel    chan *agentservicepb.StreamAgentResponse
	withDeltas bool
}

func newLocalBroker() *localBroker {
	return &localBroker{
		rootAgentToTreeSubscribers:  map[string]map[*treeSubscriber]struct{}{},
		sessionToSessionSubscribers: map[string]map[chan *agentservicepb.ConnectExecutorResponse]struct{}{},
	}
}

func (b *localBroker) publishEvents(ctx context.Context, rootAgent string, events []*agentpb.Event) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for treeSubscriber := range b.rootAgentToTreeSubscribers[rootAgent] {
		for _, event := range events {
			response := &agentservicepb.StreamAgentResponse{Content: &agentservicepb.StreamAgentResponse_Event{Event: event}}
			trySend(treeSubscriber.channel, response)
		}
	}
}

func (b *localBroker) publishDelta(ctx context.Context, rootAgent string, delta *agentpb.Delta) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for treeSubscriber := range b.rootAgentToTreeSubscribers[rootAgent] {
		if treeSubscriber.withDeltas {
			response := &agentservicepb.StreamAgentResponse{Content: &agentservicepb.StreamAgentResponse_Delta{Delta: delta}}
			trySend(treeSubscriber.channel, response)
		}
	}
}

func (b *localBroker) subscribeTree(rootAgent string, withDeltas bool) *subscription[*agentservicepb.StreamAgentResponse] {
	subscriber := &treeSubscriber{
		channel:    make(chan *agentservicepb.StreamAgentResponse, subscriptionBufferSize),
		withDeltas: withDeltas,
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	subscribe(b.rootAgentToTreeSubscribers, rootAgent, subscriber)
	return &subscription[*agentservicepb.StreamAgentResponse]{
		C: subscriber.channel,
		close: func() {
			b.mutex.Lock()
			defer b.mutex.Unlock()
			unsubscribe(b.rootAgentToTreeSubscribers, rootAgent, subscriber)
		},
	}
}

func (b *localBroker) sendToSession(ctx context.Context, session string, response *agentservicepb.ConnectExecutorResponse) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for channel := range b.sessionToSessionSubscribers[session] {
		trySend(channel, response)
	}
}

func (b *localBroker) subscribeSession(session string) *subscription[*agentservicepb.ConnectExecutorResponse] {
	channel := make(chan *agentservicepb.ConnectExecutorResponse, subscriptionBufferSize)
	b.mutex.Lock()
	defer b.mutex.Unlock()
	subscribe(b.sessionToSessionSubscribers, session, channel)
	return &subscription[*agentservicepb.ConnectExecutorResponse]{
		C: channel,
		close: func() {
			b.mutex.Lock()
			defer b.mutex.Unlock()
			unsubscribe(b.sessionToSessionSubscribers, session, channel)
		},
	}
}

// trySend never blocks the publisher: a subscriber too slow to keep up loses traffic, which it recovers from Postgres.
func trySend[T any](channel chan T, value T) {
	select {
	case channel <- value:
	default:
	}
}

func subscribe[K comparable, V comparable](keyToSubscribers map[K]map[V]struct{}, key K, subscriber V) {
	subscribers, ok := keyToSubscribers[key]
	if !ok {
		subscribers = map[V]struct{}{}
		keyToSubscribers[key] = subscribers
	}
	subscribers[subscriber] = struct{}{}
}

func unsubscribe[K comparable, V comparable](keyToSubscribers map[K]map[V]struct{}, key K, subscriber V) {
	delete(keyToSubscribers[key], subscriber)
	if len(keyToSubscribers[key]) == 0 {
		delete(keyToSubscribers, key)
	}
}
