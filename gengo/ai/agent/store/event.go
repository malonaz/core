package store

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/postgres"
)

// eventRow is an event as stored: its agent's ids, its sequence (its id) and the event as binary proto.
type eventRow struct {
	OrganizationID string    `db:"organization_id"`
	UserID         string    `db:"user_id"`
	AgentID        string    `db:"agent_id"`
	Sequence       int64     `db:"sequence"`
	CreateTime     time.Time `db:"create_time"`
	Kind           string    `db:"kind"`
	Event          []byte    `db:"event"`
}

// turnStartedKind is the kind of the event starting a turn.
const turnStartedKind = "turn_started"

var (
	eventRowColumns    = postgres.GetDBColumns(eventRow{})
	eventKindOneof     = (&agentpb.Event{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	eventSelectQuery   = "SELECT event.event FROM event "
	eventAgentWhere    = "event.organization_id = $1 AND event.user_id = $2 AND event.agent_id = $3"
)

// InsertEvents appends events to their agents' logs. Each event's name must end in its sequence.
func (t *Tx) InsertEvents(ctx context.Context, events []*agentpb.Event) error {
	if len(events) == 0 {
		return nil
	}
	rows := make([]*eventRow, 0, len(events))
	for _, event := range events {
		eventRn, err := agentpb.ParseEventRn(event.Name)
		if err != nil {
			return fmt.Errorf("parsing event name: %w", err)
		}
		if eventRn.Event != strconv.FormatInt(event.Sequence, 10) {
			return fmt.Errorf("event %s does not carry sequence %d", event.Name, event.Sequence)
		}
		kindField := event.ProtoReflect().WhichOneof(eventKindOneof)
		if kindField == nil {
			return fmt.Errorf("event %s has no kind", event.Name)
		}
		eventBytes, err := pbutil.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshaling event: %w", err)
		}
		rows = append(rows, &eventRow{
			OrganizationID: eventRn.Organization,
			UserID:         eventRn.User,
			AgentID:        eventRn.Agent,
			Sequence:       event.Sequence,
			CreateTime:     event.CreateTime.AsTime(),
			Kind:           string(kindField.Name()),
			Event:          eventBytes,
		})
	}
	query, params := postgres.BatchInsertQuery("INSERT INTO event %s VALUES %s", rows, eventRowColumns...)
	if _, err := t.tx.Exec(ctx, query, params...); err != nil {
		return fmt.Errorf("inserting events: %w", err)
	}
	return nil
}

// ListTurnEvents lists the events after the agent's latest turn_started (all of them if none), oldest first.
func (t *Tx) ListTurnEvents(ctx context.Context, organizationID, userID, agentID string) ([]*agentpb.Event, error) {
	query := eventSelectQuery + "WHERE " + eventAgentWhere + ` AND event.sequence > COALESCE(
		(SELECT MAX(sequence) FROM event WHERE ` + eventAgentWhere + ` AND kind = $4), 0
	) ORDER BY event.sequence`
	rows, err := t.tx.Query(ctx, query, organizationID, userID, agentID, turnStartedKind)
	if err != nil {
		return nil, fmt.Errorf("selecting turn events: %w", err)
	}
	return collectEvents(rows)
}

// ListEvents lists up to limit events of an agent after a sequence, oldest first.
func (s *Store) ListEvents(ctx context.Context, organizationID, userID, agentID string, afterSequence int64, limit int) ([]*agentpb.Event, error) {
	query := eventSelectQuery + "WHERE " + eventAgentWhere + " AND event.sequence > $4 ORDER BY event.sequence LIMIT $5"
	rows, err := s.client.Query(ctx, query, organizationID, userID, agentID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("selecting events: %w", err)
	}
	return collectEvents(rows)
}

// ListEventsQuery lists an agent's events with clauses built by the aip list request parser over the columns
// sequence and create_time; whereParams are the where clause's parameters.
func (s *Store) ListEventsQuery(
	ctx context.Context, organizationID, userID, agentID, whereClause, orderByClause, paginationClause string, whereParams ...any,
) ([]*agentpb.Event, error) {
	params := append(slices.Clip(whereParams), organizationID, userID, agentID)
	agentWhere := fmt.Sprintf("event.organization_id = $%d AND event.user_id = $%d AND event.agent_id = $%d", len(params)-2, len(params)-1, len(params))
	whereClause = postgres.AddToWhereClause(whereClause, agentWhere)
	query := eventSelectQuery + whereClause + " " + orderByClause + " " + paginationClause
	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("selecting events: %w", err)
	}
	return collectEvents(rows)
}

func collectEvents(rows pgx.Rows) ([]*agentpb.Event, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*agentpb.Event, error) {
		var eventBytes []byte
		if err := row.Scan(&eventBytes); err != nil {
			return nil, err
		}
		event := &agentpb.Event{}
		if err := pbutil.Unmarshal(eventBytes, event); err != nil {
			return nil, fmt.Errorf("unmarshaling event: %w", err)
		}
		return event, nil
	})
}
