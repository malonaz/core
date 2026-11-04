package agent_service

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/agent/model"
	"github.com/malonaz/core/gengo/ai/agent/store"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/uuid"
)

// newTestUser returns a user of a fresh organization.
func newTestUser(t *testing.T) *agentpb.UserRn {
	organizationRn, err := agentpb.ParseOrganizationRn(newTestOrganization(t))
	require.NoError(t, err)
	return organizationRn.UserRn("user")
}

func newTestAgentModel(t *testing.T, agentRn *agentpb.AgentRn, mutate func(agent *agentpb.Agent)) *model.Agent {
	now := timestamppb.Now()
	agent := &agentpb.Agent{
		Name:       agentRn.String(),
		CreateTime: now,
		UpdateTime: now,
		RootAgent:  agentRn.String(),
		State:      agentpb.Agent_STATE_PENDING,
		Spec:       &agentpb.AgentSpec{Model: "providers/mock/models/mock"},
	}
	if mutate != nil {
		mutate(agent)
	}
	agentModel, err := model.AgentFromPb(agent)
	require.NoError(t, err)
	return agentModel
}

// insertTestAgent inserts an agent in its own transaction.
func insertTestAgent(t *testing.T, agentRn *agentpb.AgentRn, mutate func(agent *agentpb.Agent)) *model.Agent {
	agentModel := newTestAgentModel(t, agentRn, mutate)
	err := newTestStore().Transaction(context.Background(), func(tx *store.Tx) error {
		var err error
		agentModel, _, err = tx.InsertAgent(context.Background(), uuid.MustNewV7().String(), agentModel)
		return err
	})
	require.NoError(t, err)
	return agentModel
}

// registerTestExecutor registers an ONLINE executor on a fresh session, which it returns.
func registerTestExecutor(t *testing.T, executorRn *agentpb.ExecutorRn, mutate func(executor *agentpb.Executor)) (*model.Executor, string) {
	now := timestamppb.Now()
	executor := &agentpb.Executor{
		Name:          executorRn.String(),
		CreateTime:    now,
		UpdateTime:    now,
		MaxAgents:     2,
		State:         agentpb.Executor_STATE_ONLINE,
		ConnectTime:   now,
		HeartbeatTime: now,
	}
	if mutate != nil {
		mutate(executor)
	}
	executorModel, err := model.ExecutorFromPb(executor)
	require.NoError(t, err)
	session := uuid.MustNewV7().String()
	executorModel, err = newTestStore().RegisterExecutor(context.Background(), executorModel, session)
	require.NoError(t, err)
	return executorModel, session
}

func findTestExecutor(t *testing.T, executorQuery *store.ExecutorQuery) string {
	var executorID string
	err := newTestStore().Transaction(context.Background(), func(tx *store.Tx) error {
		executor, err := tx.FindExecutor(context.Background(), executorQuery)
		if executor != nil {
			executorID = executor.ExecutorID
		}
		return err
	})
	require.NoError(t, err)
	return executorID
}

func refreshTestExecutorAgentCount(t *testing.T, executorRn *agentpb.ExecutorRn) {
	err := newTestStore().Transaction(context.Background(), func(tx *store.Tx) error {
		return tx.RefreshExecutorAgentCount(context.Background(), executorRn.Organization, executorRn.User, executorRn.Executor, time.Now().UTC())
	})
	require.NoError(t, err)
}

func TestStore_TransactionRetriesDeadlocks(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	firstAgentRn, secondAgentRn := userRn.AgentRn("first"), userRn.AgentRn("second")
	insertTestAgent(t, firstAgentRn, nil)
	insertTestAgent(t, secondAgentRn, nil)

	// Each transaction locks one agent, waits for the other to lock the other, then locks it: a deadlock.
	var attempts atomic.Int32
	var bothLocked sync.WaitGroup
	bothLocked.Add(2)
	lockBoth := func(firstRn, secondRn *agentpb.AgentRn) error {
		var waited bool
		return newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			attempts.Add(1)
			if _, err := tx.GetAgentForUpdate(ctx, firstRn.Organization, firstRn.User, firstRn.Agent); err != nil {
				return err
			}
			if !waited {
				waited = true
				bothLocked.Done()
				bothLocked.Wait()
			}
			_, err := tx.GetAgentForUpdate(ctx, secondRn.Organization, secondRn.User, secondRn.Agent)
			return err
		})
	}
	errs := make(chan error, 2)
	go func() { errs <- lockBoth(firstAgentRn, secondAgentRn) }()
	go func() { errs <- lockBoth(secondAgentRn, firstAgentRn) }()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, int32(3), attempts.Load())
}

func TestStore_TransactionReturnsErrorUnwrapped(t *testing.T) {
	sentinel := errors.New("sentinel")
	var attempts int
	err := newTestStore().Transaction(context.Background(), func(tx *store.Tx) error {
		attempts++
		return sentinel
	})
	require.Same(t, sentinel, err)
	require.Equal(t, 1, attempts)
}

func TestStore_InsertAgentReplay(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	agentModel := newTestAgentModel(t, userRn.AgentRn("agent"), nil)
	requestID := uuid.MustNewV7().String()

	insert := func(requestID string, agentModel *model.Agent) (*model.Agent, bool, error) {
		var inserted *model.Agent
		var created bool
		err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			var err error
			inserted, created, err = tx.InsertAgent(ctx, requestID, agentModel)
			return err
		})
		return inserted, created, err
	}

	inserted, created, err := insert(requestID, agentModel)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "agent", inserted.AgentID)

	// Same request: the agent it inserted, even under other (server-generated) ids.
	otherIDs := newTestAgentModel(t, userRn.AgentRn("other"), nil)
	replayed, created, err := insert(requestID, otherIDs)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, "agent", replayed.AgentID)

	// Same ids, other request.
	_, _, err = insert(uuid.MustNewV7().String(), agentModel)
	require.ErrorIs(t, err, model.ErrAgentAlreadyExists)
}

func TestStore_InsertInputReplay(t *testing.T) {
	ctx := context.Background()
	agentRn := newTestUser(t).AgentRn("agent")
	newInput := func(inputID string) *model.Input {
		input := &agentpb.Input{
			Name:       agentRn.InputRn(inputID).String(),
			CreateTime: timestamppb.Now(),
			Message:    &aipb.Message{Role: aipb.Role_ROLE_USER},
		}
		inputModel, err := model.InputFromPb(input)
		require.NoError(t, err)
		return inputModel
	}
	insert := func(requestID string, inputModel *model.Input) (*model.Input, bool, error) {
		var inserted *model.Input
		var created bool
		err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			var err error
			inserted, created, err = tx.InsertInput(ctx, requestID, inputModel)
			return err
		})
		return inserted, created, err
	}
	requestID := uuid.MustNewV7().String()

	inserted, created, err := insert(requestID, newInput("first"))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "first", inserted.InputID)

	replayed, created, err := insert(requestID, newInput("second"))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, "first", replayed.InputID)

	_, _, err = insert(uuid.MustNewV7().String(), newInput("first"))
	require.ErrorIs(t, err, model.ErrInputAlreadyExists)
}

func TestStore_Inputs(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	executorRn := userRn.ExecutorRn("executor")
	agentRn := userRn.AgentRn("agent")
	insertTestAgent(t, agentRn, func(agent *agentpb.Agent) { agent.LeasedExecutor = executorRn.String() })
	otherAgentRn := userRn.AgentRn("other")
	insertTestAgent(t, otherAgentRn, nil)

	createTime := time.Now().UTC().Truncate(time.Microsecond)
	err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		for i, inputRn := range []*agentpb.InputRn{agentRn.InputRn("b"), agentRn.InputRn("a"), agentRn.InputRn("c"), otherAgentRn.InputRn("d")} {
			input := &agentpb.Input{
				Name:       inputRn.String(),
				CreateTime: timestamppb.New(createTime.Add(time.Duration(i/2) * time.Second)),
				Message:    &aipb.Message{Role: aipb.Role_ROLE_USER},
			}
			inputModel, err := model.InputFromPb(input)
			require.NoError(t, err)
			if _, _, err := tx.InsertInput(ctx, uuid.MustNewV7().String(), inputModel); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	inputIDs := func(inputs []*model.Input) []string {
		var ids []string
		for _, input := range inputs {
			ids = append(ids, input.InputID)
		}
		return ids
	}
	listPending := func(agentRn *agentpb.AgentRn) []string {
		var inputs []*model.Input
		err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			var err error
			inputs, err = tx.ListPendingInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
			return err
		})
		require.NoError(t, err)
		return inputIDs(inputs)
	}
	// Ties on create_time break on input_id.
	require.Equal(t, []string{"a", "b", "c"}, listPending(agentRn))

	// Only this agent's pending inputs are consumed.
	err = newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		return tx.ConsumeInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, []string{"a", "d"}, time.Now().UTC())
	})
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, listPending(agentRn))
	require.Equal(t, []string{"d"}, listPending(otherAgentRn))

	inputs, err := newTestStore().ListPendingInputsOfExecutor(ctx, executorRn.String())
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, inputIDs(inputs))
}

func TestStore_FindExecutor(t *testing.T) {
	userRn := newTestUser(t)
	now := time.Now().UTC()
	query := func(mutate func(executorQuery *store.ExecutorQuery)) *store.ExecutorQuery {
		executorQuery := &store.ExecutorQuery{OrganizationID: userRn.Organization, UserID: userRn.User, Now: now}
		if mutate != nil {
			mutate(executorQuery)
		}
		return executorQuery
	}

	require.Empty(t, findTestExecutor(t, query(nil)))

	busyRn := userRn.ExecutorRn("busy")
	registerTestExecutor(t, busyRn, func(executor *agentpb.Executor) {
		executor.Labels = map[string]string{"os": "linux", "gpu": "true"}
		executor.Tools = []string{"exec_shell", "read_files"}
	})
	insertTestAgent(t, userRn.AgentRn("leased"), func(agent *agentpb.Agent) { agent.LeasedExecutor = busyRn.String() })
	refreshTestExecutorAgentCount(t, busyRn)
	idleRn := userRn.ExecutorRn("idle")
	registerTestExecutor(t, idleRn, func(executor *agentpb.Executor) {
		executor.Labels = map[string]string{"os": "linux"}
	})
	// Another user's executor is never returned.
	registerTestExecutor(t, userRn.OrganizationRn().UserRn("other").ExecutorRn("idle"), nil)

	// Least loaded first.
	require.Equal(t, "idle", findTestExecutor(t, query(nil)))
	require.Equal(t, "busy", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.ExecutorID = "busy" })))
	// Selector.
	require.Equal(t, "idle", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.Selector = map[string]string{"os": "linux"} })))
	require.Equal(t, "busy", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.Selector = map[string]string{"gpu": "true"} })))
	require.Empty(t, findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.Selector = map[string]string{"os": "darwin"} })))
	// Tools.
	require.Equal(t, "busy", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.Tools = []string{"read_files"} })))
	require.Empty(t, findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.Tools = []string{"read_files", "write_file"} })))

	// Refusals: live ones exclude, expired ones do not.
	refusingAgent := userRn.AgentRn("refused").String()
	err := newTestStore().Transaction(context.Background(), func(tx *store.Tx) error {
		if err := tx.InsertRefusal(context.Background(), refusingAgent, userRn.Organization, userRn.User, "idle", now.Add(time.Minute)); err != nil {
			return err
		}
		return tx.InsertRefusal(context.Background(), refusingAgent, userRn.Organization, userRn.User, "busy", now.Add(-time.Minute))
	})
	require.NoError(t, err)
	require.Equal(t, "busy", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.RefusingAgent = refusingAgent })))
	require.Equal(t, "idle", findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.RefusingAgent = userRn.AgentRn("other").String() })))

	// Capacity.
	insertTestAgent(t, userRn.AgentRn("leased-2"), func(agent *agentpb.Agent) { agent.LeasedExecutor = busyRn.String() })
	refreshTestExecutorAgentCount(t, busyRn)
	require.Empty(t, findTestExecutor(t, query(func(q *store.ExecutorQuery) { q.ExecutorID = "busy" })))
}

func TestStore_FindExecutorSkipsDisconnected(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	executorRn := userRn.ExecutorRn("executor")
	_, session := registerTestExecutor(t, executorRn, nil)
	executorQuery := &store.ExecutorQuery{OrganizationID: userRn.Organization, UserID: userRn.User, Now: time.Now().UTC()}
	require.Equal(t, "executor", findTestExecutor(t, executorQuery))

	// Another session does not detach it.
	detached, err := newTestStore().DetachExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, uuid.MustNewV7().String(), time.Now().UTC())
	require.NoError(t, err)
	require.False(t, detached)

	detached, err = newTestStore().DetachExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, session, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, detached)
	require.Empty(t, findTestExecutor(t, executorQuery))
	executor, err := newTestStore().GetExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor)
	require.NoError(t, err)
	require.Equal(t, int16(agentpb.Executor_STATE_ONLINE), executor.State)

	// A detached executor no longer heartbeats on its old session.
	heartbeated, err := newTestStore().HeartbeatExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, session, time.Now().UTC())
	require.NoError(t, err)
	require.False(t, heartbeated)

	// Registering again binds the new session; a clean close takes it OFFLINE.
	_, session = registerTestExecutor(t, executorRn, nil)
	heartbeated, err = newTestStore().HeartbeatExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, session, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, heartbeated)
	disconnected, err := newTestStore().DisconnectExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor, session, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, disconnected)
	require.Empty(t, findTestExecutor(t, executorQuery))
	err = newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		session, err := tx.GetExecutorSession(ctx, executorRn.Organization, executorRn.User, executorRn.Executor)
		require.Empty(t, session)
		return err
	})
	require.NoError(t, err)
}

func TestStore_RefreshExecutorAgentCount(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	executorRn := userRn.ExecutorRn("executor")
	registerTestExecutor(t, executorRn, nil)
	leasedTo := func(executor string) func(agent *agentpb.Agent) {
		return func(agent *agentpb.Agent) { agent.LeasedExecutor = executor }
	}
	insertTestAgent(t, userRn.AgentRn("first"), leasedTo(executorRn.String()))
	insertTestAgent(t, userRn.AgentRn("second"), leasedTo(executorRn.String()))
	insertTestAgent(t, userRn.AgentRn("deleted"), func(agent *agentpb.Agent) {
		agent.LeasedExecutor = executorRn.String()
		agent.DeleteTime = timestamppb.Now()
	})
	insertTestAgent(t, userRn.AgentRn("elsewhere"), leasedTo(userRn.ExecutorRn("other").String()))

	refreshTestExecutorAgentCount(t, executorRn)
	executor, err := newTestStore().GetExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor)
	require.NoError(t, err)
	require.Equal(t, int32(2), executor.AgentCount)

	// Registering recounts too.
	executor, _ = registerTestExecutor(t, executorRn, nil)
	require.Equal(t, int32(2), executor.AgentCount)
}

func TestStore_ExpireExecutors(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	longAgo := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	registerTestExecutor(t, userRn.ExecutorRn("stale"), func(executor *agentpb.Executor) {
		executor.HeartbeatTime = timestamppb.New(longAgo)
	})
	registerTestExecutor(t, userRn.ExecutorRn("fresh"), nil)
	registerTestExecutor(t, userRn.ExecutorRn("offline"), func(executor *agentpb.Executor) {
		executor.HeartbeatTime = timestamppb.New(longAgo)
		executor.State = agentpb.Executor_STATE_OFFLINE
	})

	expire := func() []string {
		executors, err := newTestStore().ExpireExecutors(ctx, longAgo.Add(time.Hour), time.Now().UTC())
		require.NoError(t, err)
		var executorIDs []string
		for _, executor := range executors {
			if executor.OrganizationID == userRn.Organization {
				executorIDs = append(executorIDs, executor.ExecutorID)
				require.Equal(t, int16(agentpb.Executor_STATE_OFFLINE), executor.State)
			}
		}
		return executorIDs
	}
	require.Equal(t, []string{"stale"}, expire())
	require.Empty(t, expire())

	err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		session, err := tx.GetExecutorSession(ctx, userRn.Organization, userRn.User, "stale")
		require.Empty(t, session)
		return err
	})
	require.NoError(t, err)
}

func TestStore_ListTurnEvents(t *testing.T) {
	ctx := context.Background()
	agentRn := newTestUser(t).AgentRn("agent")
	newEvent := func(sequence int64, turnStarted bool) *agentpb.Event {
		event := &agentpb.Event{
			Name:       agentRn.EventRn(strconv.FormatInt(sequence, 10)).String(),
			CreateTime: timestamppb.Now(),
			Sequence:   sequence,
			Kind:       &agentpb.Event_StateChanged_{StateChanged: &agentpb.Event_StateChanged{State: agentpb.Agent_STATE_IDLE}},
		}
		if turnStarted {
			event.Kind = &agentpb.Event_TurnStarted_{TurnStarted: &agentpb.Event_TurnStarted{}}
		}
		return event
	}
	listTurnEvents := func() []int64 {
		var sequences []int64
		err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			events, err := tx.ListTurnEvents(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
			for _, event := range events {
				sequences = append(sequences, event.Sequence)
			}
			return err
		})
		require.NoError(t, err)
		return sequences
	}
	insertEvents := func(events ...*agentpb.Event) {
		err := newTestStore().Transaction(ctx, func(tx *store.Tx) error { return tx.InsertEvents(ctx, events) })
		require.NoError(t, err)
	}

	insertEvents(newEvent(1, false), newEvent(2, false))
	require.Equal(t, []int64{1, 2}, listTurnEvents())

	insertEvents(newEvent(3, true), newEvent(4, false), newEvent(5, true), newEvent(6, false), newEvent(7, false))
	require.Equal(t, []int64{6, 7}, listTurnEvents())

	events, err := newTestStore().ListEvents(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, 2, 2)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, int64(3), events[0].Sequence)
	require.NotNil(t, events[0].GetTurnStarted())

	// An event whose name does not carry its sequence is refused.
	mismatched := newEvent(8, false)
	mismatched.Sequence = 9
	err = newTestStore().Transaction(ctx, func(tx *store.Tx) error { return tx.InsertEvents(ctx, []*agentpb.Event{mismatched}) })
	require.Error(t, err)
}

func TestStore_ListOrphanedAgents(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	onlineRn, offlineRn := userRn.ExecutorRn("online"), userRn.ExecutorRn("offline")
	registerTestExecutor(t, onlineRn, nil)
	registerTestExecutor(t, offlineRn, func(executor *agentpb.Executor) { executor.State = agentpb.Executor_STATE_OFFLINE })
	leasedTo := func(executor string) func(agent *agentpb.Agent) {
		return func(agent *agentpb.Agent) { agent.LeasedExecutor = executor }
	}
	insertTestAgent(t, userRn.AgentRn("on-online"), leasedTo(onlineRn.String()))
	insertTestAgent(t, userRn.AgentRn("on-offline"), leasedTo(offlineRn.String()))
	insertTestAgent(t, userRn.AgentRn("on-missing"), leasedTo(userRn.ExecutorRn("missing").String()))
	insertTestAgent(t, userRn.AgentRn("unleased"), nil)
	insertTestAgent(t, userRn.AgentRn("deleted"), func(agent *agentpb.Agent) {
		agent.LeasedExecutor = userRn.ExecutorRn("missing").String()
		agent.DeleteTime = timestamppb.Now()
	})

	agents, err := newTestStore().ListOrphanedAgents(ctx, 10000)
	require.NoError(t, err)
	var agentIDs []string
	for _, agent := range agents {
		if agent.OrganizationID == userRn.Organization {
			agentIDs = append(agentIDs, agent.AgentID)
		}
	}
	require.ElementsMatch(t, []string{"on-offline", "on-missing"}, agentIDs)
}

func TestStore_ListAgents(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	rootRn := userRn.AgentRn("root")
	insertTestAgent(t, rootRn, func(agent *agentpb.Agent) { agent.State = agentpb.Agent_STATE_IDLE })
	child := func(agent *agentpb.Agent) {
		agent.ParentAgent = rootRn.String()
		agent.RootAgent = rootRn.String()
	}
	insertTestAgent(t, userRn.AgentRn("child"), child)
	insertTestAgent(t, userRn.AgentRn("deleted-child"), func(agent *agentpb.Agent) {
		child(agent)
		agent.DeleteTime = timestamppb.Now()
	})

	agentIDs := func(agents []*model.Agent, err error) []string {
		require.NoError(t, err)
		var ids []string
		for _, agent := range agents {
			ids = append(ids, agent.AgentID)
		}
		return ids
	}
	require.Equal(t, []string{"child"}, agentIDs(newTestStore().ListChildAgents(ctx, rootRn.String())))
	require.ElementsMatch(t, []string{"root", "child", "deleted-child"}, agentIDs(newTestStore().ListTreeAgents(ctx, rootRn.String())))
	require.Equal(t, []string{"child"}, agentIDs(newTestStore().ListPendingAgents(ctx, userRn.Organization, userRn.User, 10)))
	require.Equal(t, []string{"child"}, agentIDs(newTestStore().ListPendingAgents(ctx, userRn.Organization, "", 10)))
}

func TestStore_SaveAgent(t *testing.T) {
	ctx := context.Background()
	agentRn := newTestUser(t).AgentRn("agent")
	insertTestAgent(t, agentRn, nil)

	err := newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		agentModel, err := tx.GetAgentForUpdate(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return err
		}
		agentModel.State = int16(agentpb.Agent_STATE_IDLE)
		agentModel.LeasedExecutor = "organizations/o/users/u/executors/e"
		agentModel.LastEventSequence = 7
		return tx.SaveAgent(ctx, agentModel)
	})
	require.NoError(t, err)

	agentModel, err := newTestStore().GetAgent(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	require.NoError(t, err)
	require.Equal(t, int16(agentpb.Agent_STATE_IDLE), agentModel.State)
	require.Equal(t, "organizations/o/users/u/executors/e", agentModel.LeasedExecutor)
	require.Equal(t, int64(7), agentModel.LastEventSequence)

	err = newTestStore().Transaction(ctx, func(tx *store.Tx) error {
		_, err := tx.GetAgentForUpdate(ctx, agentRn.Organization, agentRn.User, "missing")
		return err
	})
	require.ErrorIs(t, err, model.ErrAgentNotExist)
}

func TestStore_RefreshExecutorAgentCountSeesTheLeaseItWaitedOn(t *testing.T) {
	ctx := context.Background()
	userRn := newTestUser(t)
	executorRn := userRn.ExecutorRn("executor")
	registerTestExecutor(t, executorRn, nil)

	// A lease holds the executor's row while it inserts the agent; a recount waits on it, then must count the agent.
	leasing, leased := make(chan struct{}), make(chan struct{})
	leaseErr := make(chan error, 1)
	go func() {
		leaseErr <- newTestStore().Transaction(ctx, func(tx *store.Tx) error {
			executorQuery := &store.ExecutorQuery{OrganizationID: userRn.Organization, UserID: userRn.User, Now: time.Now().UTC()}
			if _, err := tx.FindExecutor(ctx, executorQuery); err != nil {
				return err
			}
			agentModel := newTestAgentModel(t, userRn.AgentRn("agent"), func(agent *agentpb.Agent) { agent.LeasedExecutor = executorRn.String() })
			if _, _, err := tx.InsertAgent(ctx, uuid.MustNewV7().String(), agentModel); err != nil {
				return err
			}
			close(leasing)
			<-leased
			return nil
		})
	}()
	<-leasing
	recounted := make(chan struct{})
	go func() {
		refreshTestExecutorAgentCount(t, executorRn)
		close(recounted)
	}()
	time.Sleep(200 * time.Millisecond) // Let the recount block on the executor's row.
	close(leased)
	require.NoError(t, <-leaseErr)
	<-recounted

	executor, err := newTestStore().GetExecutor(ctx, executorRn.Organization, executorRn.User, executorRn.Executor)
	require.NoError(t, err)
	require.Equal(t, int32(1), executor.AgentCount)
}
