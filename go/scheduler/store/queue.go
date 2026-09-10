package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	v5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/postgres"
)

var (
	// queue_id identifies the row; every other column is written by a transition.
	queueTransitionColumns = postgres.GetDBColumns(model.Queue{}, postgres.ExceptColumns("queue_id"))
	queueTransitionQuery   = updateQuery("scheduler.queue", queueTransitionColumns) + fmt.Sprintf(" WHERE queue_id = $%d", len(queueTransitionColumns)+1)
	queueSelectForUpdate   = postgres.SelectQuery("SELECT %s FROM scheduler.queue WHERE queue_id = $1 FOR UPDATE", QueuePostgresColumns)
)

// TransitionQueue locks one queue and applies transition to it, persisting the
// result when transition reports a change. The transition may return an error
// to refuse the change. Returns model.ErrQueueNotExist when there is no such queue.
func (s *Store) TransitionQueue(ctx context.Context, queueID string, transition func(*model.Queue) (bool, error)) (*model.Queue, error) {
	var queue *model.Queue
	transactionFN := func(tx postgres.Tx) error {
		rows, err := tx.Query(ctx, queueSelectForUpdate, queueID)
		if err != nil {
			return fmt.Errorf("selecting queue: %w", err)
		}
		queue, err = v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Queue])
		if err != nil {
			if errors.Is(err, v5.ErrNoRows) {
				return model.ErrQueueNotExist
			}
			return fmt.Errorf("collecting row: %w", err)
		}
		changed, err := transition(queue)
		if err != nil || !changed {
			return err
		}
		params := append(postgres.GetParams(queue, queueTransitionColumns...), queue.QueueID)
		if _, err := tx.Exec(ctx, queueTransitionQuery, params...); err != nil {
			return fmt.Errorf("updating queue: %w", err)
		}
		return nil
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return queue, nil
}

// QueueStats is a queue's live backlog.
type QueueStats struct {
	QueueID                   string     `db:"queue_id"`
	PendingCount              int64      `db:"pending_count"`
	RunningCount              int64      `db:"running_count"`
	OldestPendingScheduleTime *time.Time `db:"oldest_pending_schedule_time"`
}

const queueStatsQuery = `
SELECT queue.queue_id,
    count(job.job_id) FILTER (WHERE job.state = $1) AS pending_count,
    count(job.job_id) FILTER (WHERE job.state = $2) AS running_count,
    min(COALESCE(job.schedule_time, job.create_time)) FILTER (WHERE job.state = $1) AS oldest_pending_schedule_time
FROM scheduler.queue
LEFT JOIN scheduler.job ON job.queue = 'queues/' || queue.queue_id AND job.state IN ($1, $2)
WHERE $3::text[] IS NULL OR queue.queue_id = ANY($3)
GROUP BY queue.queue_id`

// ListQueueStats returns the backlog of the given queues, or of every queue
// when queueIDs is nil. Queues without live jobs are reported with zero counts.
func (s *Store) ListQueueStats(ctx context.Context, queueIDs []string) ([]*QueueStats, error) {
	rows, err := s.client.Query(ctx, queueStatsQuery, int16(schedulerpb.JobState_JOB_STATE_PENDING), int16(schedulerpb.JobState_JOB_STATE_RUNNING), queueIDs)
	if err != nil {
		return nil, fmt.Errorf("listing queue stats: %w", err)
	}
	stats, err := v5.CollectRows(rows, v5.RowToAddrOfStructByName[QueueStats])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return stats, nil
}

// QueueHasLiveJobs reports whether a PENDING or RUNNING job references the
// queue, which is what refuses its deletion.
func (s *Store) QueueHasLiveJobs(ctx context.Context, queueName string) (bool, error) {
	var exists bool
	if err := s.client.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM scheduler.job WHERE queue = $1 AND state IN ($2, $3))",
		queueName, int16(schedulerpb.JobState_JOB_STATE_PENDING), int16(schedulerpb.JobState_JOB_STATE_RUNNING)).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking queue jobs: %w", err)
	}
	return exists, nil
}

var queueGetByRequestTypeQuery = postgres.SelectQuery("SELECT %s FROM scheduler.queue WHERE request_type = $1", QueuePostgresColumns)

// GetQueueByRequestType returns the queue routing payloads of the given type
// URL, or model.ErrQueueNotExist.
func (s *Store) GetQueueByRequestType(ctx context.Context, requestType string) (*model.Queue, error) {
	rows, err := s.client.Query(ctx, queueGetByRequestTypeQuery, requestType)
	if err != nil {
		return nil, fmt.Errorf("getting queue by request type: %w", err)
	}
	queue, err := v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Queue])
	if err != nil {
		if errors.Is(err, v5.ErrNoRows) {
			return nil, model.ErrQueueNotExist
		}
		return nil, fmt.Errorf("collecting row: %w", err)
	}
	return queue, nil
}

const (
	queueMethodConstraint      = "queue_method_unique"
	queueRequestTypeConstraint = "queue_request_type_unique"
)

// IsQueueMethodConflict reports whether err is another queue already serving the method.
func IsQueueMethodConflict(err error) bool { return isConstraintViolation(err, queueMethodConstraint) }

// IsQueueRequestTypeConflict reports whether err is another queue already routing the request type.
func IsQueueRequestTypeConflict(err error) bool {
	return isConstraintViolation(err, queueRequestTypeConstraint)
}

func isConstraintViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == constraint
}
