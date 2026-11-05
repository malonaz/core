package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
)

// listAllTestEvents pages through ListEvents, returning every page's sequences.
func listAllTestEvents(t *testing.T, service *Service, request *agentservicepb.ListEventsRequest) [][]int64 {
	t.Helper()
	var pages [][]int64
	for {
		// Parsing defaults the order_by in place, which would then fail the page token's checksum.
		listEventsResponse, err := service.ListEvents(context.Background(), proto.CloneOf(request))
		require.NoError(t, err)
		var sequences []int64
		for _, event := range listEventsResponse.Events {
			sequences = append(sequences, event.Sequence)
		}
		pages = append(pages, sequences)
		if listEventsResponse.NextPageToken == "" {
			return pages
		}
		request.PageToken = listEventsResponse.NextPageToken
	}
}

func TestListEvents(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)
	for _, text := range []string{"one", "two", "three", "four"} {
		_, err := service.CreateInput(context.Background(), newCreateInputRequest(agent.Name, text))
		require.NoError(t, err)
	}
	requireEventLog(t, service, agent.Name)

	listEventsRequest := &agentservicepb.ListEventsRequest{Parent: agent.Name, PageSize: 2}
	require.Equal(t, [][]int64{{1, 2}, {3, 4}, {5}}, listAllTestEvents(t, service, listEventsRequest))

	listEventsRequest = &agentservicepb.ListEventsRequest{Parent: agent.Name, PageSize: 3, OrderBy: "sequence desc"}
	require.Equal(t, [][]int64{{5, 4, 3}, {2, 1}}, listAllTestEvents(t, service, listEventsRequest))

	listEventsRequest = &agentservicepb.ListEventsRequest{Parent: agent.Name, Filter: "sequence > 3"}
	require.Equal(t, [][]int64{{4, 5}}, listAllTestEvents(t, service, listEventsRequest))

	// Another agent's log is its own.
	otherAgent := createTestAgentRPC(t, service, userRn, nil)
	listEventsRequest = &agentservicepb.ListEventsRequest{Parent: otherAgent.Name}
	require.Equal(t, [][]int64{{1}}, listAllTestEvents(t, service, listEventsRequest))
}

func TestListEvents_RejectsFieldsOutsideTheTable(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)
	for _, filter := range []string{
		`name = "x"`,
		"state_changed.state = 1",
		`sequence > 0 AND input_queued.input = "x"`,
	} {
		listEventsRequest := &agentservicepb.ListEventsRequest{Parent: agent.Name, Filter: filter}
		_, err := service.ListEvents(context.Background(), listEventsRequest)
		// Refused by the parser, before reaching the database.
		requireCode(t, err, codes.InvalidArgument)
		require.ErrorContains(t, err, "parsing request")
	}
	listEventsRequest := &agentservicepb.ListEventsRequest{Parent: agent.Name, Filter: `create_time > "2000-01-01T00:00:00Z"`}
	listEventsResponse, err := service.ListEvents(context.Background(), listEventsRequest)
	require.NoError(t, err)
	require.Len(t, listEventsResponse.Events, 1)
}
