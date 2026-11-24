package agent_service

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

const (
	// testServerSecret signs the tokens the replicas, and the tests calling as service accounts, present each other.
	testServerSecret = "agent-service-test-server-secret-0123456789"
	// serviceAccountID is the account the service runs as, dispatcherAccountID the scheduler dispatcher's, and
	// otherAccountID another service's.
	serviceAccountID    = "agent-service"
	dispatcherAccountID = "scheduler-dispatcher"
	otherAccountID      = "other-service"
	dispatcherAccount   = "serviceAccounts/" + dispatcherAccountID
	// serverTokenKey carries the token a server presents another.
	serverTokenKey = "x-server-token"
	// ownerTokenIssuer issues the owners' tokens the replicas' tool calls carry.
	ownerTokenIssuer = "owner"
)

// testOwnerTokenKey signs the owners' tokens.
var testOwnerTokenKey = func() string {
	key, err := authentication.SymmetricKey("agent-service-test-owner-token-secret-0123")
	if err != nil {
		panic(err)
	}
	return key
}()

// testInterceptor authenticates the calls to every test replica and tool target.
var testInterceptor *authentication.Interceptor

// newTestConfiguration lets anyone call anything, the tests exercising what the service decides itself, and trusts
// the owners' tokens the test token service exchanges.
func newTestConfiguration() *authenticationpb.Configuration {
	return &authenticationpb.Configuration{
		Issuers: []*authenticationpb.Issuer{{
			Id:               ownerTokenIssuer,
			Issuer:           ownerTokenIssuer,
			Audience:         ownerTokenIssuer,
			Keys:             &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(testOwnerTokenKey)},
			GrantsCel:        "[]",
			ExchangeLifetime: durationpb.New(10 * time.Minute),
		}},
		ServiceAccounts: []*authenticationpb.ServiceAccount{{Id: serviceAccountID}, {Id: dispatcherAccountID}, {Id: otherAccountID}},
		Roles:           []*authenticationpb.Role{{Id: "public", Rules: []*authenticationpb.Rule{{Methods: []string{"*"}}}}},
		PublicRoles:     []string{"public"},
	}
}

// newTestInterceptor returns the interceptor of every test replica, serving the AgentService as the service's account.
func newTestInterceptor(configuration *authenticationpb.Configuration) (*authentication.Interceptor, error) {
	configurationBytes, err := pbutil.JSONMarshal(configuration)
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "authentication")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "authentication.jsonnet")
	if err := os.WriteFile(path, configurationBytes, 0o600); err != nil {
		return nil, err
	}
	opts := &authentication.Opts{Config: path, APIKeyHeader: "x-api-key", CookieName: "access_token", ServerSecret: testServerSecret}
	interceptor, err := authentication.NewInterceptor(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	if err := interceptor.Serve(serviceAccountID, agentservicepb.AgentService_ServiceDesc.ServiceName); err != nil {
		return nil, err
	}
	return interceptor, nil
}

// testTokenService exchanges the owners' tokens of every test replica.
var testTokenService authenticationpb.TokenServiceClient

// newTestTokenService serves the TokenService behind the test interceptor, on its own unix socket.
func newTestTokenService(configuration *authenticationpb.Configuration) (authenticationpb.TokenServiceClient, error) {
	tokenService, err := authentication.NewTokenService(context.Background(), configuration, testSubjects, map[string]string{ownerTokenIssuer: testOwnerTokenKey})
	if err != nil {
		return nil, err
	}
	socketDirectory, err := os.MkdirTemp("", "token")
	if err != nil {
		return nil, err
	}
	socketPath := filepath.Join(socketDirectory, "grpc.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	server := grpcgo.NewServer(grpcgo.ChainUnaryInterceptor(testInterceptor.Unary()))
	authenticationpb.RegisterTokenServiceServer(server, tokenService)
	go server.Serve(listener)
	clientOpts, err := grpc.ParseClientOpts("unix:" + socketPath)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewConnection(clientOpts, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := connection.Connect(context.Background()); err != nil {
		return nil, err
	}
	return authenticationpb.NewTokenServiceClient(connection.Get()), nil
}

// strangerUserID names the users the test token service knows nothing of.
const strangerUserID = "stranger"

// testSubjects are the owners the test token service exchanges for: every user but strangers. It counts the
// exchanges.
var testSubjects = &fakeSubjects{subjectToExchanges: map[string]int{}}

type fakeSubjects struct {
	mutex              sync.Mutex
	subjectToExchanges map[string]int
}

func (s *fakeSubjects) Claims(ctx context.Context, issuer, subject string) (map[string]any, error) {
	userRn := &agentpb.UserRn{}
	if err := userRn.UnmarshalString(subject); err != nil || userRn.User == strangerUserID {
		return nil, status.Errorf(codes.NotFound, "unknown user %s", subject).Err()
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.subjectToExchanges[subject]++
	return map[string]any{}, nil
}

// exchanges returns how many tokens were exchanged for a user.
func (s *fakeSubjects) exchanges(subject string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.subjectToExchanges[subject]
}

// asAccount returns what makes calls as a service account, as onyx hands it to a service's Start.
func asAccount(t *testing.T, account string) func(context.Context) context.Context {
	t.Helper()
	as, err := testInterceptor.As(account)
	require.NoError(t, err)
	return as
}

// serverClient returns an AgentService client on a replica's server, through its interceptor.
func serverClient(t *testing.T, replica *testReplica) agentservicepb.AgentServiceClient {
	t.Helper()
	agentServiceClient, err := replica.peerClient(context.Background(), replica.address)
	require.NoError(t, err)
	return agentServiceClient
}

// sessionContext returns the context of a call the interceptor let in with a session, as its handler sees it.
func sessionContext(principal, actor, caller string) context.Context {
	session := &authenticationpb.Session{}
	if principal != "" {
		session.Principal = &authenticationpb.Principal{Subject: principal}
	}
	if actor != "" {
		session.Actor = &authenticationpb.Principal{Subject: actor}
	}
	if caller != "" {
		session.Caller = &authenticationpb.Principal{Subject: caller}
	}
	return authenticationtest.NewContext(context.Background(), session)
}

// ownerContext returns the context of a call made by the user owning an agent.
func ownerContext(t *testing.T, agent string) context.Context {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	return sessionContext(agentRn.UserRn().String(), "", "")
}

// dispatcherContext returns the context of a job the scheduler dispatcher runs.
func dispatcherContext(jobName string) context.Context {
	ctx := sessionContext(dispatcherAccount, "", dispatcherAccount)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(scheduler.JobMetadataKey, jobName))
}

const (
	agentServiceName  = "malonaz.ai.agent.agent_service.v1.AgentService"
	messageAgentGrant = "toolSets/" + agentServiceName + "/tools/MessageAgent"
	// reflectionFullMethod is the call reading a tool target's schema.
	reflectionFullMethod = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
)

// checkCall returns a call of the health tool checking a service.
func checkCall(service string) *aipb.ToolCall {
	return newToolCall(service, healthCheckTool, map[string]any{"tool_call_title": "Check " + service, "service": service})
}

func TestTools_CalledAsTheOwner(t *testing.T) {
	// The replica's own AgentService is a tool target: a message records who sent it.
	healthServer, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{healthService: endpoint, agentServiceName: opts.AdvertisedAddress}
	})
	owner := newTestUser(t)
	spec := &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel, Tools: ungatedTools(healthCheckGrant, messageAgentGrant)}
	agent := createTestAgentWithSpec(t, service, owner, spec)
	recipient := createTestAgentWithSpec(t, service, owner, spec)
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)

	messageCall := newToolCall("message", "AgentService_MessageAgent", map[string]any{
		"tool_call_title": "Say hello",
		"name":            recipient.Name,
		"message":         map[string]any{"role": "ROLE_USER", "blocks": []any{map[string]any{"text": "hello"}}},
	})
	for _, toolCall := range []*aipb.ToolCall{checkCall("library"), messageCall, checkCall("shelf")} {
		require.Nil(t, toolbox.call(context.Background(), toolCall).GetError(), toolCall.GetId())
	}

	queuedMessages := getAgent(t, service, recipient.Name).GetQueuedMessages()
	require.Len(t, queuedMessages, 1)
	sender, _ := agentpb.Annotations.Sender.Get(queuedMessages[0])
	actor, _ := agentpb.Annotations.Actor.Get(queuedMessages[0])
	require.Equal(t, owner, sender)
	require.Equal(t, agent.Name, actor)
	require.Equal(t, []string{"library", "shelf"}, healthServer.checkedServices())
	// A schema is the same for every owner: the service reads it.
	require.Equal(t, "serviceAccounts/"+serviceAccountID, healthServer.session(reflectionFullMethod).GetPrincipal().GetSubject())
	// One token serves the turn.
	require.Equal(t, 1, testSubjects.exchanges(owner))
}

func TestTools_FailWithoutTheOwnersToken(t *testing.T) {
	healthServer, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	stranger := (&agentpb.UserRn{Organization: aip.NewSystemGeneratedBase32ResourceID(), User: strangerUserID}).String()
	agent := createTestAgentWithSpec(t, service, stranger, &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel, Tools: ungatedTools(healthCheckGrant)})
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)

	toolResult := toolbox.call(context.Background(), checkCall("library"))

	require.Equal(t, int32(codes.NotFound), toolResult.GetError().GetCode())
	require.Empty(t, healthServer.checkedServices())
}

func TestStreamAgent_ForwardedOnlyFromAnotherReplica(t *testing.T) {
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) { opts.MaxConcurrentTurns = 0 })
	agent := createTestAgent(t, service)
	// A client's stream opens on the agent's snapshot; a forwarded one relays only the activity of the turn this
	// replica runs, here none, so it is waited on for a while only.
	opensOnSnapshot := func(ctx context.Context, timeout time.Duration) bool {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		agentServiceClient, err := service.peerClient(ctx, service.address)
		require.NoError(t, err)
		streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent.Name}
		stream, err := agentServiceClient.StreamAgent(ctx, streamAgentRequest)
		require.NoError(t, err)
		response, err := stream.Recv()
		if status.HasCode(err, codes.DeadlineExceeded) {
			return false
		}
		require.NoError(t, err)
		return response.GetSnapshot() != nil
	}

	require.False(t, opensOnSnapshot(asAccount(t, serviceAccountID)(context.Background()), 500*time.Millisecond))
	require.True(t, opensOnSnapshot(asAccount(t, otherAccountID)(context.Background()), waitTimeout))
	require.True(t, opensOnSnapshot(context.Background(), waitTimeout))
	// A header naming the call forwarded proves nothing.
	require.True(t, opensOnSnapshot(metadata.AppendToOutgoingContext(context.Background(), "x-agent-service-forwarded", "true"), waitTimeout))
}

func TestWakeAgent_RunsOnlyForTheDispatcher(t *testing.T) {
	// No turn runs: a delivery in flight would show a message both delivered and queued.
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) { opts.MaxConcurrentTurns = 0 })
	agent := createTestAgent(t, service)
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{Name: agent.Name, Wake: &agentservicepb.WakeAgentRequest_Message{Message: "hi"}}
	withJob := func(ctx context.Context) context.Context {
		return metadata.NewIncomingContext(ctx, metadata.Pairs(scheduler.JobMetadataKey, "jobs/1"))
	}

	for name, ctx := range map[string]context.Context{
		"anonymous":           withJob(context.Background()),
		"another service":     withJob(sessionContext("serviceAccounts/"+otherAccountID, "", "serviceAccounts/"+otherAccountID)),
		"dispatcher as owner": withJob(sessionContext(dispatcherAccount, "", "")),
		"carried by another":  withJob(sessionContext(dispatcherAccount, "", "serviceAccounts/"+otherAccountID)),
	} {
		_, err := service.WakeAgent(ctx, wakeAgentRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%s: %v", name, err)
	}
	require.Empty(t, sentMessages(t, service, agent.Name))

	// Through the server, as the dispatcher calls.
	agentServiceClient := serverClient(t, service)
	asJob := func(account string) context.Context {
		return metadata.AppendToOutgoingContext(asAccount(t, account)(context.Background()), scheduler.JobMetadataKey, "jobs/"+account)
	}
	_, err := agentServiceClient.WakeAgent(asJob(otherAccountID), wakeAgentRequest)
	require.True(t, status.HasCode(err, codes.PermissionDenied), err)
	_, err = agentServiceClient.WakeAgent(asJob(dispatcherAccountID), wakeAgentRequest)
	require.NoError(t, err)
	queuedMessages := getAgent(t, service, agent.Name).QueuedMessages
	require.Len(t, queuedMessages, 1)
	require.Equal(t, "hi", lastText(queuedMessages[0]))
}

// harper and operator are a caller other than the user owning an agent, and someone acting for it.
const (
	harper   = "serviceAccounts/harper"
	operator = "users/operator"
)

func TestAwaited_SettledByTheCallerAndTheirActor(t *testing.T) {
	service := newTestReplica(t, newFakeAiService(), targetSelf, (&fakeHealthServer{}).register)
	agent := createGatedAgent(t, service)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "approved"), healthCheckCall("call-2", "refused")), textGeneration("done"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	approveToolCallRequest := &agentservicepb.ApproveToolCallRequest{Name: agent.Name, ToolCallId: "call-1"}
	for _, ctx := range []context.Context{context.Background(), sessionContext("", "", "")} {
		_, err := service.ApproveToolCall(ctx, approveToolCallRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), err)
	}
	approvedAgent, err := service.ApproveToolCall(sessionContext(harper, operator, ""), approveToolCallRequest)
	require.NoError(t, err)
	resolution := approvedAgent.GetLatestTurn().GetAwaitedToolCalls()[0].GetResolution()
	require.Equal(t, harper, resolution.GetResolver())
	require.Equal(t, operator, resolution.GetActor())
	resolveToolCallRequest := &agentservicepb.ResolveToolCallRequest{
		Name:       agent.Name,
		ToolCallId: "call-2",
		Result:     ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "no").Err()),
	}
	_, err = service.ResolveToolCall(sessionContext(harper, "", ""), resolveToolCallRequest)
	require.NoError(t, err)

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	approvedBy, _ := agentpb.Annotations.ApprovedBy.Get(toolCallIDToToolResult["call-1"])
	require.Equal(t, harper, approvedBy)
	actor, _ := agentpb.Annotations.Actor.Get(toolCallIDToToolResult["call-1"])
	require.Equal(t, operator, actor)
	resolvedBy, _ := agentpb.Annotations.ResolvedBy.Get(toolCallIDToToolResult["call-2"])
	require.Equal(t, harper, resolvedBy)
	require.False(t, agentpb.Annotations.Actor.Has(toolCallIDToToolResult["call-2"]))
}

func TestCancelAgent_ResolvesAwaitedCallsAsTheCanceller(t *testing.T) {
	service := newTestReplica(t, newFakeAiService(), targetSelf, (&fakeHealthServer{}).register)
	agent := createGatedAgent(t, service)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("done"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	_, err := service.CancelAgent(context.Background(), cancelAgentRequest)
	require.True(t, status.HasCode(err, codes.Unauthenticated), err)
	cancelledAgent, err := service.CancelAgent(sessionContext(harper, operator, ""), cancelAgentRequest)
	require.NoError(t, err)

	resolution := cancelledAgent.GetLatestTurn().GetAwaitedToolCalls()[0].GetResolution()
	require.Equal(t, harper, resolution.GetResolver())
	require.Equal(t, operator, resolution.GetActor())
	resolvedBy, _ := agentpb.Annotations.ResolvedBy.Get(resolution.GetResult())
	require.Equal(t, harper, resolvedBy)
	require.Equal(t, int32(codes.Canceled), resolution.GetResult().GetError().GetCode())
	actor, _ := agentpb.Annotations.Actor.Get(resolution.GetResult())
	require.Equal(t, operator, actor)
}

func TestMessageAgent_SentByTheCallerAndTheirActor(t *testing.T) {
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) { opts.MaxConcurrentTurns = 0 })
	agent := createTestAgent(t, service)
	// A client's annotations name no one: the service stamps who called.
	message := ai.NewUserMessage(ai.NewTextBlock("hello"))
	agentpb.Annotations.Actor.Set(message, "users/forged")
	messageAgentRequest := &agentservicepb.MessageAgentRequest{Name: agent.Name, Message: message}

	for _, ctx := range []context.Context{context.Background(), sessionContext("", "", "")} {
		_, err := service.MessageAgent(ctx, messageAgentRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), err)
	}
	messagedAgent, err := service.MessageAgent(sessionContext(harper, operator, ""), messageAgentRequest)
	require.NoError(t, err)
	messageAgentRequest.RequestId = uuid.MustNewV7().String()
	messagedAgent, err = service.MessageAgent(sessionContext(harper, "", ""), messageAgentRequest)
	require.NoError(t, err)

	require.Len(t, messagedAgent.QueuedMessages, 2)
	for i, wantActor := range []string{operator, ""} {
		sender, _ := agentpb.Annotations.Sender.Get(messagedAgent.QueuedMessages[i])
		require.Equal(t, harper, sender)
		actor, _ := agentpb.Annotations.Actor.Get(messagedAgent.QueuedMessages[i])
		require.Equal(t, wantActor, actor)
	}
	// A sender the client names must be the caller.
	agentpb.Annotations.Sender.Set(message, userOf(t, agent))
	messageAgentRequest.RequestId = uuid.MustNewV7().String()
	_, err = service.MessageAgent(sessionContext(harper, "", ""), messageAgentRequest)
	require.True(t, status.HasCode(err, codes.InvalidArgument), err)
}
