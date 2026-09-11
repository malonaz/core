// Package outbox implements the transactional outbox of a resource service: a
// journal table written in the same transaction as the write that caused an
// event, and a relay that hands each journaled event to the scheduler, which
// delivers it to the service's outbox method.
//
// The journal is one table per database schema, holding the resource as it was
// written. A committed write and its events therefore cannot diverge, and the
// event a delivery carries is the resource as of the write rather than as of
// the delivery, however long the two are apart.
package outbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/uuid"
)

// Columns of the journal table, in insert order.
var columns = []string{"id", "create_time", "resource_name", "event_type", "event"}

// Entry is one journaled resource event awaiting delivery.
type Entry struct {
	// ID is a UUIDv7, so ordering by it is ordering by write time. It is also
	// the request id of the job the relay creates, which makes a redelivered
	// entry land on the job it already created rather than on a second one.
	ID string `db:"id"`
	// CreateTime is when the write that caused the event committed.
	CreateTime time.Time `db:"create_time"`
	// ResourceName is the name of the resource the event is about, for reading
	// the journal; delivery reads it off the event itself.
	ResourceName string `db:"resource_name"`
	// EventType is the aippb.ResourceEventType of the event, for the same reason.
	EventType int16 `db:"event_type"`
	// Event is the marshaled malonaz.aip.v1.ResourceEvent.
	Event []byte `db:"event"`
}

// ParseEvent unmarshals the entry's event.
func (e *Entry) ParseEvent() (*aippb.ResourceEvent, error) {
	event := &aippb.ResourceEvent{}
	if err := proto.Unmarshal(e.Event, event); err != nil {
		return nil, fmt.Errorf("unmarshaling event of journal entry %s: %w", e.ID, err)
	}
	return event, nil
}

// EventFn builds the events a write journals. It is handed the rows the write
// committed, inside its transaction, so the events carry the resource as
// stored rather than as requested.
type EventFn[M any] func(rows []M) ([]*aippb.ResourceEvent, error)

// Execer is what a journal is written through: the transaction of the write
// that caused the events, or the pool for an event about a row that is already
// committed.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Write inserts the events into the journal table. Table is a generated
// constant, never user input.
func Write(ctx context.Context, execer Execer, table string, events []*aippb.ResourceEvent) error {
	if len(events) == 0 {
		return nil
	}
	now := time.Now().UTC()
	params := make([]any, 0, len(events)*len(columns))
	rows := make([]string, 0, len(events))
	for i, event := range events {
		bytes, err := proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshaling %s event of %s: %w", event.GetType(), event.GetName(), err)
		}
		placeholders := make([]string, len(columns))
		for j := range columns {
			placeholders[j] = fmt.Sprintf("$%d", i*len(columns)+j+1)
		}
		rows = append(rows, "("+strings.Join(placeholders, ", ")+")")
		params = append(params, uuid.MustNewV7().String(), now, event.GetName(), int16(event.GetType()), bytes)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ", "), strings.Join(rows, ", "))
	if _, err := execer.Exec(ctx, query, params...); err != nil {
		return fmt.Errorf("journaling %d events: %w", len(events), err)
	}
	return nil
}

// List returns the oldest undelivered entries of the journal, up to limit.
func List(ctx context.Context, client *postgres.Client, table string, limit int) ([]*Entry, error) {
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY id LIMIT $1", strings.Join(columns, ", "), table)
	rows, err := client.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("listing journal entries: %w", err)
	}
	entries, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[Entry])
	if err != nil {
		return nil, fmt.Errorf("collecting journal entries: %w", err)
	}
	return entries, nil
}

// Delete removes the delivered entries from the journal.
func Delete(ctx context.Context, client *postgres.Client, table string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", table)
	if _, err := client.Exec(ctx, query, ids); err != nil {
		return fmt.Errorf("deleting %d journal entries: %w", len(ids), err)
	}
	return nil
}
