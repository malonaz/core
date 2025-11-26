package aip

import (
	"fmt"

	"go.einride.tech/aip/pagination"
)

// ParsedPaginatedRequest is a request that is parsed.
type ParsedPaginatedRequest interface {
	// Returns an SQL limit/offset clause. The limit is 0 if the request's page size is 0, or pageSize + 1 otherwise. Offset is the page token's offset if it exists.
	GetSQLPaginationClause() string
	// Returns "" if the request's page size is 0 or if we found `GetLimit` objects, indicating there is no more pages.
	// Otherwise returns the next page token.
	GetNextPageToken(itemsFetchedWithLimit int) string
}

type parsedPaginatedRequest struct {
	request   pagination.Request
	pageToken pagination.PageToken
}

func ParsePaginatedRequest(request pagination.Request) (ParsedPaginatedRequest, error) {
	// Parse page token.
	pageToken, err := pagination.ParsePageToken(request)
	if err != nil {
		return nil, fmt.Errorf("parsing page token: %w", err)
	}
	return &parsedPaginatedRequest{
		request:   request,
		pageToken: pageToken,
	}, nil
}

// GetSQLLimitClause implements the ParsedRequest interface.
func (p *parsedPaginatedRequest) GetSQLPaginationClause() string {
	if p.request.GetPageSize() == 0 {
		return ""
	}
	return fmt.Sprintf("OFFSET %d LIMIT %d", p.pageToken.Offset, p.request.GetPageSize()+1)
}

// GetNextPageToken implements the ParsedRequest interface.
func (p *parsedPaginatedRequest) GetNextPageToken(itemsFetched int) string {
	if p.request.GetPageSize() == 0 || itemsFetched <= int(p.request.GetPageSize()) {
		return ""
	}
	return p.pageToken.Next(p.request).String()
}
