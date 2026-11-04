package agent_service

import (
	"container/heap"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// streamPageSize bounds how many events a stream reads from the store at once, per agent.
const streamPageSize = 100

var caughtUpResponse = &agentservicepb.StreamAgentResponse{
	Content: &agentservicepb.StreamAgentResponse_CaughtUp_{CaughtUp: &agentservicepb.StreamAgentResponse_CaughtUp{}},
}

// StreamAgent replays an agent's log, and its descendants' if requested, then streams them live.
func (s *Service) StreamAgent(request *agentservicepb.StreamAgentRequest, server grpc.ServerStreamingServer[agentservicepb.StreamAgentResponse]) error {
	ctx := server.Context()
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: request.Name}
	agent, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return err
	}

	// Subscribe before reading the store, so nothing committed after the replay's reads is missed.
	subscription := s.broker.subscribeTree(agent.RootAgent, request.View == agentservicepb.AgentView_AGENT_VIEW_FULL)
	defer subscription.Close()

	stream := &agentStream{
		service:             s,
		server:              server,
		agent:               agent.Name,
		includeDescendants:  request.IncludeDescendants,
		agentToLastSequence: map[string]int64{agent.Name: request.AfterSequence},
		nonMemberAgentSet:   map[string]struct{}{},
	}
	if err := stream.replay(ctx, agent); err != nil {
		return err
	}
	if err := server.Send(caughtUpResponse); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case response := <-subscription.C:
			if err := stream.forward(ctx, response); err != nil {
				return err
			}
		}
	}
}

// agentStream is the state of one StreamAgent call.
type agentStream struct {
	service            *Service
	server             grpc.ServerStreamingServer[agentservicepb.StreamAgentResponse]
	agent              string
	includeDescendants bool
	// The streamed agents, to the sequence of the last event sent of each, and the agents of the tree known not to be.
	agentToLastSequence map[string]int64
	nonMemberAgentSet   map[string]struct{}
	// The replay's merge of the members' logs, nil once live.
	replayHeap *replayHeap
}

// replay sends the members' logs in one chronological order across agents, each agent's in sequence order.
func (s *agentStream) replay(ctx context.Context, agent *agentpb.Agent) error {
	s.replayHeap = &replayHeap{}
	defer func() { s.replayHeap = nil }()
	if s.includeDescendants {
		if err := s.addPresentDescendants(ctx, agent); err != nil {
			return err
		}
	}
	for member, lastSequence := range s.agentToLastSequence {
		if err := s.pushReplayCursor(ctx, member, lastSequence); err != nil {
			return err
		}
	}
	for s.replayHeap.Len() > 0 {
		cursor := (*s.replayHeap)[0]
		event := cursor.events[0]
		cursor.events = cursor.events[1:]
		if len(cursor.events) == 0 && cursor.more {
			if err := cursor.fetch(ctx, s.service, event.Sequence); err != nil {
				return err
			}
		}
		if len(cursor.events) == 0 {
			heap.Pop(s.replayHeap)
		} else {
			heap.Fix(s.replayHeap, 0)
		}
		if err := s.send(ctx, cursor.agent, event, nil); err != nil {
			return err
		}
	}
	return nil
}

// addPresentDescendants makes members of the agent's descendants, and non-members of the rest of its tree.
func (s *agentStream) addPresentDescendants(ctx context.Context, agent *agentpb.Agent) error {
	treeAgents, err := s.service.agentPostgresStore.ListTreeAgents(ctx, agent.RootAgent)
	if err != nil {
		return status.FromError(err, "listing tree agents").Err()
	}
	parentToChildren := make(map[string][]string, len(treeAgents))
	for _, treeAgent := range treeAgents {
		agentRn := &agentpb.AgentRn{Organization: treeAgent.OrganizationID, User: treeAgent.UserID, Agent: treeAgent.AgentID}
		name := agentRn.String()
		parentToChildren[treeAgent.ParentAgent] = append(parentToChildren[treeAgent.ParentAgent], name)
		s.nonMemberAgentSet[name] = struct{}{}
	}
	delete(s.nonMemberAgentSet, agent.Name)
	pending := parentToChildren[agent.Name]
	for len(pending) > 0 {
		child := pending[len(pending)-1]
		pending = append(pending[:len(pending)-1], parentToChildren[child]...)
		delete(s.nonMemberAgentSet, child)
		s.agentToLastSequence[child] = 0
	}
	return nil
}

func (s *agentStream) pushReplayCursor(ctx context.Context, agent string, lastSequence int64) error {
	agentRn, err := agentpb.ParseAgentRn(agent)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	cursor := &replayCursor{agent: agent, agentRn: agentRn}
	if err := cursor.fetch(ctx, s.service, lastSequence); err != nil {
		return err
	}
	if len(cursor.events) > 0 {
		heap.Push(s.replayHeap, cursor)
	}
	return nil
}

// forward relays live broker traffic: a member's events densely, deduplicated and gap-filled from the store, and its deltas.
func (s *agentStream) forward(ctx context.Context, response *agentservicepb.StreamAgentResponse) error {
	switch content := response.Content.(type) {
	case *agentservicepb.StreamAgentResponse_Delta:
		member, err := s.isMember(ctx, content.Delta.Agent)
		if err != nil || !member {
			return err
		}
		return s.server.Send(response)

	case *agentservicepb.StreamAgentResponse_Event:
		event := content.Event
		eventRn, err := agentpb.ParseEventRn(event.Name)
		if err != nil {
			return status.Errorf(codes.Internal, "parsing event name: %v", err).Err()
		}
		agent := eventRn.Parent()
		if member, err := s.isMember(ctx, agent); err != nil || !member {
			return err
		}
		lastSequence := s.agentToLastSequence[agent]
		if event.Sequence <= lastSequence {
			return nil
		}
		if event.Sequence > lastSequence+1 {
			if err := s.fill(ctx, agent, eventRn.AgentRn(), lastSequence, event.Sequence-1); err != nil {
				return err
			}
		}
		return s.send(ctx, agent, event, response)
	}
	return nil
}

// fill sends a member's events in (afterSequence, throughSequence] from the store.
func (s *agentStream) fill(ctx context.Context, agent string, agentRn *agentpb.AgentRn, afterSequence, throughSequence int64) error {
	for afterSequence < throughSequence {
		limit := int(min(streamPageSize, throughSequence-afterSequence))
		events, err := s.service.agentPostgresStore.ListEvents(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, afterSequence, limit)
		if err != nil {
			return status.FromError(err, "listing events").Err()
		}
		if len(events) == 0 {
			return status.Errorf(codes.Internal, "events %d to %d of %s are missing", afterSequence+1, throughSequence, agent).Err()
		}
		for _, event := range events {
			if err := s.send(ctx, agent, event, nil); err != nil {
				return err
			}
		}
		afterSequence = events[len(events)-1].Sequence
	}
	return nil
}

// send sends a member's next event, reusing its broker response if any, and adopts the sub-agents it creates.
func (s *agentStream) send(ctx context.Context, agent string, event *agentpb.Event, response *agentservicepb.StreamAgentResponse) error {
	if response == nil {
		response = &agentservicepb.StreamAgentResponse{Content: &agentservicepb.StreamAgentResponse_Event{Event: event}}
	}
	if err := s.server.Send(response); err != nil {
		return err
	}
	s.agentToLastSequence[agent] = event.Sequence
	subAgentCreated := event.GetSubAgentCreated()
	if !s.includeDescendants || subAgentCreated == nil {
		return nil
	}
	if _, ok := s.agentToLastSequence[subAgentCreated.Agent]; ok {
		return nil
	}
	delete(s.nonMemberAgentSet, subAgentCreated.Agent)
	s.agentToLastSequence[subAgentCreated.Agent] = 0
	// Live, the sub-agent's first event gap-fills its log; during replay, merge it now.
	if s.replayHeap != nil {
		return s.pushReplayCursor(ctx, subAgentCreated.Agent, 0)
	}
	return nil
}

// isMember reports whether an agent of the tree is streamed, resolving an unknown one through its parent chain.
func (s *agentStream) isMember(ctx context.Context, agent string) (bool, error) {
	if _, ok := s.agentToLastSequence[agent]; ok {
		return true, nil
	}
	if !s.includeDescendants {
		return false, nil
	}
	if _, ok := s.nonMemberAgentSet[agent]; ok {
		return false, nil
	}

	var chain []string
	member := false
	for ancestor := agent; ancestor != ""; {
		if _, ok := s.agentToLastSequence[ancestor]; ok {
			member = true
			break
		}
		if _, ok := s.nonMemberAgentSet[ancestor]; ok {
			break
		}
		chain = append(chain, ancestor)
		getAgentRequest := &agentservicepb.GetAgentRequest{Name: ancestor}
		ancestorAgent, err := s.service.GetAgent(ctx, getAgentRequest)
		if err != nil {
			return false, status.FromError(err, "getting agent").Err()
		}
		ancestor = ancestorAgent.ParentAgent
	}
	for _, unknown := range chain {
		if member {
			s.agentToLastSequence[unknown] = 0
		} else {
			s.nonMemberAgentSet[unknown] = struct{}{}
		}
	}
	return member, nil
}

// replayCursor walks one member's log in bounded pages.
type replayCursor struct {
	agent   string
	agentRn *agentpb.AgentRn
	// The fetched events not yet sent, and whether the log may hold more after them.
	events []*agentpb.Event
	more   bool
}

func (c *replayCursor) fetch(ctx context.Context, service *Service, afterSequence int64) error {
	events, err := service.agentPostgresStore.ListEvents(ctx, c.agentRn.Organization, c.agentRn.User, c.agentRn.Agent, afterSequence, streamPageSize)
	if err != nil {
		return status.FromError(err, "listing events").Err()
	}
	c.events = events
	c.more = len(events) == streamPageSize
	return nil
}

// replayHeap orders cursors by their next event's create_time, ties by agent.
type replayHeap []*replayCursor

func (h replayHeap) Len() int { return len(h) }

func (h replayHeap) Less(i, j int) bool {
	left, right := h[i].events[0], h[j].events[0]
	if left.CreateTime.Seconds != right.CreateTime.Seconds {
		return left.CreateTime.Seconds < right.CreateTime.Seconds
	}
	if left.CreateTime.Nanos != right.CreateTime.Nanos {
		return left.CreateTime.Nanos < right.CreateTime.Nanos
	}
	// A cursor per agent: same-agent ties never meet here, each cursor yielding its events in sequence order.
	return h[i].agent < h[j].agent
}

func (h replayHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *replayHeap) Push(cursor any) { *h = append(*h, cursor.(*replayCursor)) }

func (h *replayHeap) Pop() any {
	old := *h
	cursor := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return cursor
}
