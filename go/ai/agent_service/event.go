package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

var listEventsRequestParser = aip.MustNewListRequestParser[*agentservicepb.ListEventsRequest, *agentpb.Event]()

// ListEvents lists an agent's events, in sequence order by default.
func (s *Service) ListEvents(ctx context.Context, request *agentservicepb.ListEventsRequest) (*agentservicepb.ListEventsResponse, error) {
	agentRn, err := agentpb.ParseAgentRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	parsedRequest, err := listEventsRequestParser.Parse(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing request: %v", err).Err()
	}
	whereClause, whereParams := parsedRequest.GetSQLWhereClause()
	events, err := s.agentPostgresStore.ListEventsQuery(
		ctx, agentRn.Organization, agentRn.User, agentRn.Agent,
		whereClause, parsedRequest.GetSQLOrderByClause(), parsedRequest.GetSQLPaginationClause(), whereParams...,
	)
	if err != nil {
		return nil, status.FromError(err, "listing events").Err()
	}
	nextPageToken := parsedRequest.GetNextPageToken(len(events))
	if nextPageToken != "" {
		events = events[:len(events)-1]
	}
	return &agentservicepb.ListEventsResponse{Events: events, NextPageToken: nextPageToken}, nil
}
