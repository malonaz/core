package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/model"
)

// buildStatsClauses assembles the shared bucket column, where clause and
// params for stats aggregation queries. Columns are qualified with alias
// (the chat or message table). Empty organizationID/userID are treated as
// wildcards.
func buildStatsClauses(alias, organizationID, userID string, startTime, endTime *time.Time, dateTruncField string) (string, string, []any) {
	params := []any{}
	whereClauses := []string{fmt.Sprintf("%s.delete_time IS NULL", alias)}
	if organizationID != "" {
		params = append(params, organizationID)
		whereClauses = append(whereClauses, fmt.Sprintf("%s.organization_id = $%d", alias, len(params)))
	}
	if userID != "" {
		params = append(params, userID)
		whereClauses = append(whereClauses, fmt.Sprintf("%s.user_id = $%d", alias, len(params)))
	}

	// A constant NULL bucket keeps a single query shape whether or not a time
	// series is requested.
	bucketColumn := "NULL::timestamp"
	if dateTruncField != "" {
		params = append(params, dateTruncField)
		bucketColumn = fmt.Sprintf("date_trunc($%d, %s.create_time)", len(params), alias)
	}

	if startTime != nil {
		params = append(params, *startTime)
		whereClauses = append(whereClauses, fmt.Sprintf("%s.create_time >= $%d", alias, len(params)))
	}
	if endTime != nil {
		params = append(params, *endTime)
		whereClauses = append(whereClauses, fmt.Sprintf("%s.create_time < $%d", alias, len(params)))
	}
	return bucketColumn, strings.Join(whereClauses, " AND "), params
}

// ComputeChatStats aggregates chat counts and price, grouped by bucket.
func (s *Store) ComputeChatStats(
	ctx context.Context,
	organizationID, userID string,
	startTime, endTime *time.Time,
	dateTruncField string,
) ([]*model.ChatStatsRow, error) {
	bucketColumn, whereClause, params := buildStatsClauses("c", organizationID, userID, startTime, endTime, dateTruncField)
	query := fmt.Sprintf(`
    SELECT
      %s AS bucket,
      COUNT(*)::int AS count,
      COALESCE(SUM(c.price), 0) AS price
    FROM chat c
    WHERE %s
    GROUP BY 1`,
		bucketColumn, whereClause,
	)

	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("computing chat stats: %w", err)
	}
	return v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.ChatStatsRow])
}

// resourceConsumptionCategories are the disjoint priced categories of
// [ModelUsage][malonaz.ai.v1.ModelUsage], in field order. Each is aggregated
// separately: the server never folds categories together, so clients keep the
// full picture (cache hit rates, reasoning spend, image and audio usage).
var resourceConsumptionCategories = []string{
	"input_token",
	"output_token",
	"output_reasoning_token",
	"input_token_cache_read",
	"input_token_cache_write",
	"input_second",
	"output_second",
	"input_character",
	"input_image_token",
	"output_image_token",
	"input_image_token_cache_read",
	"input_image_token_cache_write",
}

// resourceConsumptionColumns builds the SUM columns for every consumption
// category, reading quantity and price out of the message's model_usage JSON —
// the same numbers the providers reported, so usage and pricing never drift
// apart. Column aliases match the db tags of model.MessageStatsRow.
func resourceConsumptionColumns() string {
	columns := make([]string, 0, 2*len(resourceConsumptionCategories))
	for _, category := range resourceConsumptionCategories {
		columns = append(columns, fmt.Sprintf(
			"COALESCE(SUM(COALESCE((m.model_usage->'%s'->>'quantity')::bigint, 0)), 0) AS %s_quantity",
			category, category,
		))
		columns = append(columns, fmt.Sprintf(
			"COALESCE(SUM(COALESCE((m.model_usage->'%s'->>'price')::double precision, 0)), 0) AS %s_price",
			category, category,
		))
	}
	return strings.Join(columns, ",\n      ")
}

// ComputeMessageStats aggregates message counts, price and per-category
// resource consumption, grouped by (bucket, model). Messages with no model
// (user, system, tool) group under an empty model.
func (s *Store) ComputeMessageStats(
	ctx context.Context,
	organizationID, userID string,
	startTime, endTime *time.Time,
	dateTruncField string,
) ([]*model.MessageStatsRow, error) {
	bucketColumn, whereClause, params := buildStatsClauses("m", organizationID, userID, startTime, endTime, dateTruncField)
	query := fmt.Sprintf(`
    SELECT
      %s AS bucket,
      COALESCE(m.model, '') AS model,
      COUNT(*)::int AS count,
      COALESCE(SUM(m.price), 0) AS price,
      %s
    FROM message m
    WHERE %s
    GROUP BY 1, 2`,
		bucketColumn, resourceConsumptionColumns(), whereClause,
	)

	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("computing message stats: %w", err)
	}
	return v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.MessageStatsRow])
}
