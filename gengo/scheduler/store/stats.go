package store

import (
	"context"
	"fmt"
	"time"

	v5 "github.com/jackc/pgx/v5"

	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/postgres"
)

// jobMeasures is what a completed job contributes to its queue's stats, defined once for the
// exact stats and the hourly roll-ups. Wait is measured on the jobs that ran once and were never
// retried by hand, from due to start; run on the outcomes of an attempt (SUCCEEDED, FAILED).
var jobMeasures = fmt.Sprintf(`queue, state, attempt_count, complete_time,
    CASE WHEN attempt_count = 1 AND COALESCE(labels->>'%[1]s', '') <> '%[2]s'
        THEN extract(epoch FROM start_time - COALESCE(schedule_time, create_time))::float8 END AS wait_seconds,
    CASE WHEN state IN (%[3]d, %[4]d)
        THEN extract(epoch FROM complete_time - start_time)::float8 END AS run_seconds`,
	schedulerpb.Labels.Retried.GetKey(), schedulerpb.Labels.Retried.True,
	schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED)

// QueueStatsRow is what the jobs that completed in a bucket, or in the whole
// interval when Bucket is nil, did. Durations are in seconds, nil when no job
// was measured.
type QueueStatsRow struct {
	Bucket         *time.Time `db:"bucket"`
	SucceededCount int64      `db:"succeeded_count"`
	FailedCount    int64      `db:"failed_count"`
	CancelledCount int64      `db:"cancelled_count"`
	AttemptCount   int64      `db:"attempt_count"`
	WaitP50        *float64   `db:"wait_p50"`
	WaitP95        *float64   `db:"wait_p95"`
	WaitMax        *float64   `db:"wait_max"`
	RunP50         *float64   `db:"run_p50"`
	RunP95         *float64   `db:"run_p95"`
	RunMax         *float64   `db:"run_max"`
}

// The jobs completed in [$2, $3), of queue $1 or of every queue when it is NULL.
var computeQueueStatsTemplate = fmt.Sprintf(`
SELECT %%s AS bucket,
    count(*) FILTER (WHERE job.state = %[1]d) AS succeeded_count,
    count(*) FILTER (WHERE job.state = %[2]d) AS failed_count,
    count(*) FILTER (WHERE job.state = %[3]d) AS cancelled_count,
    COALESCE(sum(job.attempt_count), 0) AS attempt_count,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY job.wait_seconds) AS wait_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY job.wait_seconds) AS wait_p95,
    max(job.wait_seconds) AS wait_max,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY job.run_seconds) AS run_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY job.run_seconds) AS run_p95,
    max(job.run_seconds) AS run_max
FROM (
    SELECT %[4]s
    FROM scheduler.job
    WHERE complete_time IS NOT NULL AND complete_time >= $2 AND complete_time < $3
        AND ($1::text IS NULL OR queue = $1)
) job
%%s`, schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED, jobMeasures)

var (
	// One row over the whole interval.
	computeQueueStatsTotalsQuery = fmt.Sprintf(computeQueueStatsTemplate, "NULL::timestamp", "")
	// The totals' row, with a NULL bucket, then a row per $4-second bucket
	// aligned to the epoch: percentiles cannot be folded from the buckets'.
	computeQueueStatsBucketsQuery = fmt.Sprintf(computeQueueStatsTemplate,
		"date_bin(make_interval(secs => $4), job.complete_time, timestamp 'epoch')",
		"GROUP BY GROUPING SETS ((bucket), ()) ORDER BY bucket NULLS FIRST")
)

// ComputeQueueStats returns the stats of the jobs completed in
// [startTime, endTime), of the queue or of every queue when queueName is nil:
// the totals' row (no bucket) then, when step is set, one row per bucket with
// a completed job, in order.
func (s *Store) ComputeQueueStats(ctx context.Context, queueName *string, startTime, endTime time.Time, step time.Duration) ([]*QueueStatsRow, error) {
	query := computeQueueStatsTotalsQuery
	params := []any{queueName, startTime, endTime}
	if step > 0 {
		query = computeQueueStatsBucketsQuery
		params = append(params, step.Seconds())
	}
	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("computing queue stats: %w", err)
	}
	statsRows, err := v5.CollectRows(rows, v5.RowToAddrOfStructByName[QueueStatsRow])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return statsRows, nil
}

// JobMeasure is what one completed job contributes to its queue's stats.
type JobMeasure struct {
	Queue        string    `db:"queue"`
	State        int16     `db:"state"`
	AttemptCount int64     `db:"attempt_count"`
	CompleteTime time.Time `db:"complete_time"`
	WaitSeconds  *float64  `db:"wait_seconds"`
	RunSeconds   *float64  `db:"run_seconds"`
}

var listJobMeasuresQuery = "SELECT " + jobMeasures + `
FROM scheduler.job
WHERE complete_time IS NOT NULL AND complete_time >= $1 AND complete_time < $2`

// ListJobMeasures returns the measures of every job completed in [startTime, endTime).
func (s *Store) ListJobMeasures(ctx context.Context, startTime, endTime time.Time) ([]*JobMeasure, error) {
	rows, err := s.client.Query(ctx, listJobMeasuresQuery, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("listing job measures: %w", err)
	}
	measures, err := v5.CollectRows(rows, v5.RowToAddrOfStructByName[JobMeasure])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return measures, nil
}

// EarliestCompleteTime returns when the oldest job kept completed, or nil when none has.
func (s *Store) EarliestCompleteTime(ctx context.Context) (*time.Time, error) {
	var earliest *time.Time
	if err := s.client.QueryRow(ctx, "SELECT min(complete_time) FROM scheduler.job WHERE complete_time IS NOT NULL").Scan(&earliest); err != nil {
		return nil, fmt.Errorf("reading earliest complete time: %w", err)
	}
	return earliest, nil
}

// QueueHourStats is what a queue's jobs that completed in one hour did. Histograms count seconds by
// go/scheduler/histogram's buckets; a histogram and its maximum are nil when no job was measured.
type QueueHourStats struct {
	Queue          string    `db:"queue"`
	Hour           time.Time `db:"hour"`
	SucceededCount int64     `db:"succeeded_count"`
	FailedCount    int64     `db:"failed_count"`
	CancelledCount int64     `db:"cancelled_count"`
	AttemptCount   int64     `db:"attempt_count"`
	WaitHistogram  []int64   `db:"wait_histogram"`
	WaitMaxSeconds *float64  `db:"wait_max_seconds"`
	RunHistogram   []int64   `db:"run_histogram"`
	RunMaxSeconds  *float64  `db:"run_max_seconds"`
}

var (
	queueHourStatsColumns = postgres.GetDBColumns(QueueHourStats{})
	queueHourStatsSelect  = postgres.SelectQuery(`SELECT %s FROM scheduler.queue_hour_stats
WHERE hour >= $2 AND hour < $3 AND ($1::text IS NULL OR queue = $1)
ORDER BY hour`, queueHourStatsColumns)
)

// queueHourStatsAdvisoryLockKey serializes replacements of hours across scheduler replicas.
const queueHourStatsAdvisoryLockKey int64 = 0x7175_6575_655f_6872

// ReplaceQueueHourStats replaces every queue's stats over the hours in [startTime, endTime) with
// stats, in one transaction.
func (s *Store) ReplaceQueueHourStats(ctx context.Context, startTime, endTime time.Time, stats []*QueueHourStats) error {
	transactionFN := func(tx postgres.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", queueHourStatsAdvisoryLockKey); err != nil {
			return fmt.Errorf("taking roll-up lock: %w", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM scheduler.queue_hour_stats WHERE hour >= $1 AND hour < $2", startTime, endTime); err != nil {
			return fmt.Errorf("deleting hours: %w", err)
		}
		if len(stats) == 0 {
			return nil
		}
		query, params := postgres.BatchInsertQuery("INSERT INTO scheduler.queue_hour_stats %s VALUES %s", stats, queueHourStatsColumns...)
		if _, err := tx.Exec(ctx, query, params...); err != nil {
			return fmt.Errorf("inserting hours: %w", err)
		}
		return nil
	}
	return s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN)
}

// ListQueueHourStats returns the hours in [startTime, endTime) of the queue, or of every queue
// when queueName is nil, in order.
func (s *Store) ListQueueHourStats(ctx context.Context, queueName *string, startTime, endTime time.Time) ([]*QueueHourStats, error) {
	rows, err := s.client.Query(ctx, queueHourStatsSelect, queueName, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("listing queue hour stats: %w", err)
	}
	stats, err := v5.CollectRows(rows, v5.RowToAddrOfStructByName[QueueHourStats])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return stats, nil
}

// LatestQueueHour returns the latest hour rolled up, or nil when none is.
func (s *Store) LatestQueueHour(ctx context.Context) (*time.Time, error) {
	var latest *time.Time
	if err := s.client.QueryRow(ctx, "SELECT max(hour) FROM scheduler.queue_hour_stats").Scan(&latest); err != nil {
		return nil, fmt.Errorf("reading latest queue hour: %w", err)
	}
	return latest, nil
}

// PurgeQueueHourStats deletes the hours before cutoff.
func (s *Store) PurgeQueueHourStats(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.client.Exec(ctx, "DELETE FROM scheduler.queue_hour_stats WHERE hour < $1", cutoff)
	if err != nil {
		return 0, fmt.Errorf("purging queue hour stats: %w", err)
	}
	return tag.RowsAffected(), nil
}
