package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// createSearchableAgent creates an agent under a user with the given title, labels and instructions.
func createSearchableAgent(t *testing.T, service *testReplica, user, title string, labels map[string]string, instructions string) *agentpb.Agent {
	t.Helper()
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: user,
		Agent: &agentpb.Agent{
			Title:  title,
			Labels: labels,
			Spec:   &agentpb.AgentSpec{Instructions: instructions, Model: testModel},
		},
	}
	agent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	return agent
}

// searchAgents returns the names of the agents a search finds, in rank order.
func searchAgents(t *testing.T, service *testReplica, searchAgentsRequest *agentservicepb.SearchAgentsRequest) []string {
	t.Helper()
	searchAgentsResponse, err := service.SearchAgents(context.Background(), searchAgentsRequest)
	require.NoError(t, err)
	names := make([]string, 0, len(searchAgentsResponse.Agents))
	for _, agent := range searchAgentsResponse.Agents {
		names = append(names, agent.Name)
	}
	return names
}

func TestSearchAgents(t *testing.T) {
	service := newTestService(t)
	user := newTestUser(t)
	userRn, err := agentpb.ParseUserRn(user)
	require.NoError(t, err)
	otherUser := userRn.OrganizationRn().UserRn("other").String()
	titled := createSearchableAgent(t, service, user, "Invoice reconciler", nil, "You match payments.")
	labelled := createSearchableAgent(t, service, user, "Helper", map[string]string{"team": "invoicing"}, "You help.")
	instructed := createSearchableAgent(t, service, user, "Clerk", nil, "You chase every unpaid invoice.")
	othersAgent := createSearchableAgent(t, service, otherUser, "Invoice auditor", nil, "You audit.")

	t.Run("RanksTitlesOverLabelsOverInstructions", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "invoic"}
		require.Equal(t, []string{titled.Name, labelled.Name, instructed.Name}, searchAgents(t, service, searchAgentsRequest))
	})

	t.Run("SearchesAcrossUsers", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: userRn.OrganizationRn().UserRn("-").String(), Query: "invoice"}
		require.ElementsMatch(t, []string{titled.Name, othersAgent.Name, instructed.Name}, searchAgents(t, service, searchAgentsRequest))
	})

	t.Run("ComposesWithFilters", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "invoic", Filter: `title = "Clerk"`}
		require.Equal(t, []string{instructed.Name}, searchAgents(t, service, searchAgentsRequest))
	})

	t.Run("Paginates", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "invoic", PageSize: 2}
		searchAgentsResponse, err := service.SearchAgents(context.Background(), searchAgentsRequest)
		require.NoError(t, err)
		require.Len(t, searchAgentsResponse.Agents, 2)
		searchAgentsRequest.PageToken = searchAgentsResponse.NextPageToken
		require.Equal(t, []string{instructed.Name}, searchAgents(t, service, searchAgentsRequest))
	})

	t.Run("HighlightsSnippets", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "invoicing", IncludeSnippets: true}
		searchAgentsResponse, err := service.SearchAgents(context.Background(), searchAgentsRequest)
		require.NoError(t, err)
		require.Len(t, searchAgentsResponse.Snippets, 1)
		require.Equal(t, "labels", searchAgentsResponse.Snippets[0].Matches[0].Path)
		require.Equal(t, "**invoicing**", searchAgentsResponse.Snippets[0].Matches[0].Match)
	})

	t.Run("ExcludesDeletedAgentsByDefault", func(t *testing.T) {
		deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: titled.Name}
		_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
		require.NoError(t, err)
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "reconciler"}
		require.Empty(t, searchAgents(t, service, searchAgentsRequest))
		searchAgentsRequest.ShowDeleted = true
		require.Equal(t, []string{titled.Name}, searchAgents(t, service, searchAgentsRequest))
	})

	t.Run("RefusesQueriesWithoutTerms", func(t *testing.T) {
		searchAgentsRequest := &agentservicepb.SearchAgentsRequest{Parent: user, Query: "!!"}
		_, err := service.SearchAgents(context.Background(), searchAgentsRequest)
		require.True(t, status.HasCode(err, codes.InvalidArgument))
	})
}
