package agent_service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/gengo/ai/agent/store"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

var (
	errStreamOverflowed = status.Errorf(codes.ResourceExhausted, "the stream fell behind its agents' activity: reopen it").Err()
	errStreamDraining   = status.Errorf(codes.Unavailable, "the replica is shutting down: reopen the stream").Err()
)

// StreamAgent streams an agent's live activity, and its descendants' if asked. Each agent's activity comes from the
// replica running its turn: this one's hub, or the owner's StreamAgent, proxied. A poll re-reads the agents for
// changes made elsewhere: snapshots, turns moving between replicas, new descendants.
func (s *Service) StreamAgent(request *agentservicepb.StreamAgentRequest, srv agentservicepb.AgentService_StreamAgentServer) error {
	if _, err := agentpb.ParseAgentRn(request.Name); err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	if isForwarded(srv.Context()) {
		return s.streamLocalAgent(request, srv)
	}
	agentStream := &agentStream{
		service:         s,
		request:         request,
		srv:             srv,
		subscription:    s.hub.subscribe(),
		proxyEvents:     make(chan *proxyEvent, subscriptionBufferSize),
		agentToStreamed: map[string]*streamedAgent{},
	}
	defer agentStream.close()
	return agentStream.run(srv.Context())
}

// streamLocalAgent serves another replica proxying this one: the agent's activity on this replica's hub only.
func (s *Service) streamLocalAgent(request *agentservicepb.StreamAgentRequest, srv agentservicepb.AgentService_StreamAgentServer) error {
	ctx := srv.Context()
	subscription := s.hub.subscribe()
	s.hub.add(subscription, request.Name)
	defer s.hub.unsubscribe(subscription, request.Name)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.turns.draining:
			return errStreamDraining
		case <-subscription.overflowed:
			return errStreamOverflowed
		case response := <-subscription.responses:
			if !inView(request.View, response) {
				continue
			}
			if err := srv.Send(response); err != nil {
				return err
			}
		}
	}
}

// inView reports whether a view streams a piece of activity.
func inView(view agentservicepb.AgentView, response *agentservicepb.StreamAgentResponse) bool {
	switch response.Content.(type) {
	case *agentservicepb.StreamAgentResponse_Snapshot, *agentservicepb.StreamAgentResponse_ToolCall_, *agentservicepb.StreamAgentResponse_ToolCallEnded_:
		return true
	default:
		return view == agentservicepb.AgentView_AGENT_VIEW_FULL
	}
}

// agentStream is one StreamAgent call. Its run loop is the only goroutine sending on the stream.
type agentStream struct {
	service         *Service
	request         *agentservicepb.StreamAgentRequest
	srv             agentservicepb.AgentService_StreamAgentServer
	subscription    *subscription
	proxyEvents     chan *proxyEvent
	agentToStreamed map[string]*streamedAgent
}

// streamedAgent is the state of one agent of a stream.
type streamedAgent struct {
	// The last snapshot sent.
	etag       string
	updateTime time.Time
	// The address of the replica proxied for the agent's activity, empty while it is this replica's.
	owner       string
	stopProxy   context.CancelFunc
	proxyClosed bool
	// generating is set while a generation's deltas were streamed and neither its message nor a reset followed.
	generating bool
	// The number of messages sent to the agent when last polled, and the queued messages streamed.
	sentCount            int64
	queuedMessageNameSet map[string]struct{}
}

// proxyEvent is a piece of activity proxied from an owner, or the end of its proxy.
type proxyEvent struct {
	agent    string
	owner    string
	response *agentservicepb.StreamAgentResponse
	closed   bool
}

func (a *agentStream) run(ctx context.Context) error {
	ticker := time.NewTicker(a.service.opts.StreamPollInterval)
	defer ticker.Stop()
	for {
		if err := a.poll(ctx); err != nil {
			return err
		}
	wait:
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-a.service.turns.draining:
				return errStreamDraining
			case <-a.subscription.overflowed:
				return errStreamOverflowed
			case response := <-a.subscription.responses:
				streamedAgent, ok := a.agentToStreamed[response.Agent]
				if !ok {
					continue
				}
				// Only the replica running a turn publishes its activity: the turn moved here.
				if streamedAgent.owner != "" && response.GetSnapshot() == nil {
					if err := a.follow(ctx, response.Agent, streamedAgent, ""); err != nil {
						return err
					}
				}
				if err := a.send(streamedAgent, response); err != nil {
					return err
				}
			case proxyEvent := <-a.proxyEvents:
				streamedAgent, ok := a.agentToStreamed[proxyEvent.agent]
				if !ok || streamedAgent.owner != proxyEvent.owner {
					continue
				}
				if proxyEvent.closed {
					// Reattached by the next poll.
					streamedAgent.proxyClosed = true
					continue
				}
				if err := a.send(streamedAgent, proxyEvent.response); err != nil {
					return err
				}
			case <-ticker.C:
				break wait
			}
		}
	}
}

// poll re-reads the streamed agents: it attaches new ones, sends snapshots changed elsewhere and follows each
// agent's turn to the replica running it.
func (a *agentStream) poll(ctx context.Context) error {
	polledAgents, err := a.readAgents(ctx)
	if err != nil {
		return err
	}
	firstPoll := len(a.agentToStreamed) == 0
	for _, agent := range polledAgents.agents {
		sentCount := polledAgents.agentToSentCount[agent.Name]
		streamed, ok := a.agentToStreamed[agent.Name]
		if !ok {
			streamed = &streamedAgent{queuedMessageNameSet: map[string]struct{}{}}
			// Messages queued before the stream opened are history; an agent attached later streams its own.
			if firstPoll {
				streamed.sentCount = sentCount
			}
			a.agentToStreamed[agent.Name] = streamed
			// Subscribed before its snapshot is sent: later activity follows it, older snapshots are dropped.
			a.service.hub.add(a.subscription, agent.Name)
		}
		snapshot := &agentservicepb.StreamAgentResponse{
			Agent:   agent.Name,
			Content: &agentservicepb.StreamAgentResponse_Snapshot{Snapshot: agent},
		}
		if err := a.send(streamed, snapshot); err != nil {
			return err
		}
		if err := a.follow(ctx, agent.Name, streamed, a.owner(polledAgents.agentToLease[agent.Name])); err != nil {
			return err
		}
		if sentCount > streamed.sentCount {
			streamed.sentCount = sentCount
			if err := a.sendQueuedMessages(ctx, agent, streamed); err != nil {
				return err
			}
		}
	}
	return nil
}

// sendQueuedMessages streams the messages queued for an agent not streamed yet: those sent through other replicas
// reach this one only through the poll.
func (a *agentStream) sendQueuedMessages(ctx context.Context, agent *agentpb.Agent, streamed *streamedAgent) error {
	if a.request.View != agentservicepb.AgentView_AGENT_VIEW_FULL {
		return nil
	}
	listMessagesRequest := &aiservicepb.ListMessagesRequest{
		Parent:  agent.Chat,
		Filter:  pendingFilter,
		OrderBy: "create_time asc",
	}
	pendingMessages, err := aip.Paginate[*aipb.Message](ctx, listMessagesRequest, a.service.aiServiceClient.ListMessages)
	if err != nil {
		return status.FromError(err, "listing pending messages").Err()
	}
	for _, pendingMessage := range pendingMessages {
		message := &agentservicepb.StreamAgentResponse{
			Agent:   agent.Name,
			Content: &agentservicepb.StreamAgentResponse_Message{Message: pendingMessage},
		}
		if err := a.send(streamed, message); err != nil {
			return err
		}
	}
	return nil
}

// polledAgents are the streamed agents as a poll read them.
type polledAgents struct {
	agents           []*agentpb.Agent
	agentToLease     map[string]*store.Lease
	agentToSentCount map[string]int64
}

// readAgents reads the streamed agent, and its descendants if asked, with their leases and sent message counts.
func (a *agentStream) readAgents(ctx context.Context) (*polledAgents, error) {
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: a.request.Name}
	agent, err := a.service.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if !a.request.IncludeDescendants {
		agentRn, err := agentpb.ParseAgentRn(agent.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
		}
		lease, err := a.service.agentPostgresStore.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return nil, status.FromError(err, "getting lease").Err()
		}
		sentCount, err := a.service.agentPostgresStore.GetSentCount(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return nil, status.FromError(err, "getting sent count").Err()
		}
		return &polledAgents{
			agents:           []*agentpb.Agent{agent},
			agentToLease:     map[string]*store.Lease{agent.Name: lease},
			agentToSentCount: map[string]int64{agent.Name: sentCount},
		}, nil
	}

	agentModels, err := a.service.agentPostgresStore.ListTreeAgents(ctx, agent.RootAgent)
	if err != nil {
		return nil, status.FromError(err, "listing tree agents").Err()
	}
	agentToLease, err := a.service.agentPostgresStore.ListTreeLeases(ctx, agent.RootAgent)
	if err != nil {
		return nil, status.FromError(err, "listing tree leases").Err()
	}
	agentToSentCount, err := a.service.agentPostgresStore.ListTreeSentCounts(ctx, agent.RootAgent)
	if err != nil {
		return nil, status.FromError(err, "listing tree sent counts").Err()
	}
	treeAgents := make([]*agentpb.Agent, 0, len(agentModels))
	agentToParent := make(map[string]string, len(agentModels))
	for _, agentModel := range agentModels {
		treeAgent, err := agentModel.ToPb()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
		}
		treeAgents = append(treeAgents, treeAgent)
		agentToParent[treeAgent.Name] = treeAgent.ParentAgent
	}
	// Oldest first, so a parent is attached before its children.
	agents := []*agentpb.Agent{agent}
	for _, treeAgent := range treeAgents {
		if treeAgent.Name != agent.Name && descendsFrom(agentToParent, treeAgent.Name, agent.Name) {
			agents = append(agents, treeAgent)
		}
	}
	return &polledAgents{agents: agents, agentToLease: agentToLease, agentToSentCount: agentToSentCount}, nil
}

// descendsFrom reports whether an agent descends from an ancestor, following parent links.
func descendsFrom(agentToParent map[string]string, agent, ancestor string) bool {
	for parent := agentToParent[agent]; parent != ""; parent = agentToParent[parent] {
		if parent == ancestor {
			return true
		}
	}
	return false
}

// owner returns the address of the replica to proxy for an agent's activity: the live holder of its lease, unless
// that is this replica.
func (a *agentStream) owner(lease *store.Lease) string {
	if lease == nil || lease.Expired || lease.Replica == a.service.replica || lease.Address == a.service.address {
		return ""
	}
	return lease.Address
}

// follow points an agent's activity at its owner, reattaching a closed proxy. A generation cut by the move is reset.
func (a *agentStream) follow(ctx context.Context, agent string, streamedAgent *streamedAgent, owner string) error {
	if owner == streamedAgent.owner && !streamedAgent.proxyClosed {
		return nil
	}
	if streamedAgent.stopProxy != nil {
		streamedAgent.stopProxy()
		streamedAgent.stopProxy = nil
	}
	if streamedAgent.generating {
		generationReset := &agentservicepb.StreamAgentResponse{
			Agent:   agent,
			Content: &agentservicepb.StreamAgentResponse_GenerationReset_{GenerationReset: &agentservicepb.StreamAgentResponse_GenerationReset{}},
		}
		if err := a.send(streamedAgent, generationReset); err != nil {
			return err
		}
	}
	streamedAgent.owner = owner
	streamedAgent.proxyClosed = false
	if owner == "" {
		return nil
	}
	proxyCtx, stopProxy := context.WithCancel(ctx)
	streamedAgent.stopProxy = stopProxy
	go a.proxy(proxyCtx, agent, owner)
	return nil
}

// proxy relays an agent's activity from the replica running its turn until it ends or is stopped.
func (a *agentStream) proxy(ctx context.Context, agent, owner string) {
	emit := func(proxyEvent *proxyEvent) bool {
		select {
		case a.proxyEvents <- proxyEvent:
			return true
		case <-ctx.Done():
			return false
		}
	}
	defer emit(&proxyEvent{agent: agent, owner: owner, closed: true})
	peerClient, err := a.service.peerClient(ctx, owner)
	if err != nil {
		a.service.log.WarnContext(ctx, "connecting to the replica running a turn", "address", owner, "error", err)
		return
	}
	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent, View: a.request.View}
	stream, err := peerClient.StreamAgent(forwardedContext(ctx), streamAgentRequest)
	if err != nil {
		return
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			return
		}
		if !emit(&proxyEvent{agent: agent, owner: owner, response: response}) {
			return
		}
	}
}

// send streams a piece of an agent's activity, if the view streams it. Snapshots are sent only when they change
// and never older than the last one sent.
func (a *agentStream) send(streamedAgent *streamedAgent, response *agentservicepb.StreamAgentResponse) error {
	switch content := response.Content.(type) {
	case *agentservicepb.StreamAgentResponse_Snapshot:
		updateTime := content.Snapshot.GetUpdateTime().AsTime()
		if content.Snapshot.GetEtag() == streamedAgent.etag || updateTime.Before(streamedAgent.updateTime) {
			return nil
		}
		streamedAgent.etag = content.Snapshot.GetEtag()
		streamedAgent.updateTime = updateTime
	case *agentservicepb.StreamAgentResponse_Delta:
		streamedAgent.generating = true
	case *agentservicepb.StreamAgentResponse_Message:
		// A queued message reaches a stream from its sender's replica, the owner's or the poll: stream it once.
		if aip.HasLabel(content.Message, agentpb.Labels.Pending.GetKey()) {
			if _, ok := streamedAgent.queuedMessageNameSet[content.Message.GetName()]; ok {
				return nil
			}
			streamedAgent.queuedMessageNameSet[content.Message.GetName()] = struct{}{}
		} else {
			streamedAgent.generating = false
		}
	case *agentservicepb.StreamAgentResponse_GenerationReset_:
		streamedAgent.generating = false
	}
	if !inView(a.request.View, response) {
		return nil
	}
	return a.srv.Send(response)
}

// close stops the stream's proxies and subscription.
func (a *agentStream) close() {
	agents := make([]string, 0, len(a.agentToStreamed))
	for agent, streamedAgent := range a.agentToStreamed {
		agents = append(agents, agent)
		if streamedAgent.stopProxy != nil {
			streamedAgent.stopProxy()
		}
	}
	a.service.hub.unsubscribe(a.subscription, agents...)
}
