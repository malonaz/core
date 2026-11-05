package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/uuid"
)

func newCreateInputRequest(agent, text string) *agentservicepb.CreateInputRequest {
	return &agentservicepb.CreateInputRequest{
		Parent:    agent,
		Input:     &agentpb.Input{Message: ai.NewUserMessage(ai.NewTextBlock(text))},
		RequestId: uuid.MustNewV7().String(),
	}
}

func TestCreateInput_RecordsInputQueued(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)

	var inputs []*agentpb.Input
	for _, text := range []string{"one", "two", "three"} {
		createInputRequest := newCreateInputRequest(agent.Name, text)
		input, err := service.CreateInput(context.Background(), createInputRequest)
		require.NoError(t, err)
		require.True(t, proto.Equal(createInputRequest.Input.Message, input.Message))
		require.Nil(t, input.ConsumeTime)
		inputs = append(inputs, input)
	}
	require.Equal(t, []string{"state_changed:STATE_PENDING", "input_queued", "input_queued", "input_queued"}, requireEventLog(t, service, agent.Name))
	events := listTestEvents(t, service, agent.Name)
	for i, input := range inputs {
		require.Equal(t, input.Name, events[i+1].GetInputQueued().Input)
		require.True(t, proto.Equal(input.Message, events[i+1].GetInputQueued().Message))
	}
}

func TestCreateInput_ReplayIsIdempotent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)
	createInputRequest := newCreateInputRequest(agent.Name, "hello")
	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)
	replayedInput, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(input, replayedInput))
	require.Equal(t, []string{"state_changed:STATE_PENDING", "input_queued"}, requireEventLog(t, service, agent.Name))
}

func TestCreateInput_MissingAgent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	createInputRequest := newCreateInputRequest(userRn.AgentRn("missing").String(), "hello")
	_, err := service.CreateInput(context.Background(), createInputRequest)
	requireCode(t, err, codes.NotFound)
}

func TestCreateInput_ForwardedToTheLeasedExecutor(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgentRPC(t, service, userRn, nil)
	testExecutor.recvLease()
	createInputRequest := newCreateInputRequest(agent.Name, "hello")
	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(input, testExecutor.recvInput()))
}
