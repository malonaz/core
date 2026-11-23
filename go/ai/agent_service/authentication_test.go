package agent_service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/scheduler"
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
)

// testInterceptor authenticates the calls to every test replica.
var testInterceptor *authentication.Interceptor

// newTestInterceptor returns the interceptor of every test replica, serving the AgentService as the service's account.
// Anyone may call anything: the tests exercise what the service decides itself.
func newTestInterceptor() (*authentication.Interceptor, error) {
	configuration := &authenticationpb.Configuration{
		ServiceAccounts: []*authenticationpb.ServiceAccount{{Id: serviceAccountID}, {Id: dispatcherAccountID}, {Id: otherAccountID}},
		Roles:           []*authenticationpb.Role{{Id: "public", Rules: []*authenticationpb.Rule{{Methods: []string{"*"}}}}},
		PublicRoles:     []string{"public"},
	}
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

// asAccount returns what makes calls as a service account, as onyx hands it to a service's Start.
func asAccount(t *testing.T, account string) func(context.Context) context.Context {
	t.Helper()
	as, err := testInterceptor.As(account)
	require.NoError(t, err)
	return as
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

func TestTools_TargetsReceiveNoIdentity(t *testing.T) {
	healthServer, endpoint := newTestToolTarget(t)
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.ToolTargets = map[string]string{healthService: endpoint}
	})
	// Turns run as the service's account: a call on their context presents its token.
	callMetadata, err := grpc.CallMetadata(service.runCtx)
	require.NoError(t, err)
	require.Contains(t, callMetadata, serverTokenKey)
	agent := createTestAgent(t, service, healthCheckMethod)
	service.aiService.script(agent.Chat,
		toolCallGeneration(newToolCall("call-1", healthCheckTool, map[string]any{"tool_call_title": "Check the health", "service": "library"})),
		textGeneration("checked"),
	)

	sendText(t, service, agent.Name, "check")

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"library"}, healthServer.checkedServices())
	// Neither the reflection resolving the tools nor the call carry anything but the transport's own headers.
	transportKeySet := map[string]struct{}{":authority": {}, "content-type": {}, "user-agent": {}, "grpc-accept-encoding": {}, "grpc-timeout": {}}
	require.NotEmpty(t, healthServer.metadataKeySet)
	for key := range healthServer.metadataKeySet {
		require.Contains(t, transportKeySet, key)
	}
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
	agentServiceClient, err := service.peerClient(context.Background(), service.address)
	require.NoError(t, err)
	asJob := func(account string) context.Context {
		return metadata.AppendToOutgoingContext(asAccount(t, account)(context.Background()), scheduler.JobMetadataKey, "jobs/"+account)
	}
	_, err = agentServiceClient.WakeAgent(asJob(otherAccountID), wakeAgentRequest)
	require.True(t, status.HasCode(err, codes.PermissionDenied), err)
	_, err = agentServiceClient.WakeAgent(asJob(dispatcherAccountID), wakeAgentRequest)
	require.NoError(t, err)
	queuedMessages := getAgent(t, service, agent.Name).QueuedMessages
	require.Len(t, queuedMessages, 1)
	require.Equal(t, "hi", lastText(queuedMessages[0]))
}
