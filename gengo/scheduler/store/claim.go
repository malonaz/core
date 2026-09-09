package store

import (
	"context"
	"fmt"
	"time"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/postgres"
)

// claimAdvisoryLockKey serializes claim transactions across scheduler
// instances, making every queue's max_concurrency exact rather than eventual.
// "schedule" in ASCII; any constant shared by every instance would do.
const claimAdvisoryLockKey int64 = 0x736368656475_6c65

// ClaimedJob is a job selected by the claim scan, carrying the policy and
// handlers of its queue as of the claim. Both are nil when the queue row is
// gone.
type ClaimedJob struct {
	model.Job
	QueuePolicy   []byte `db:"queue_policy"`
	QueueHandlers []byte `db:"queue_handlers"`
}

// jobClaimCandidatesQuery ranks due PENDING jobs within their queue and keeps
// those fitting under the queue's remaining concurrency. Jobs of PAUSED queues
// are skipped; jobs whose queue is gone are claimed so the worker can fail
// them. FOR UPDATE is illegal beside window functions, so the chosen rows are
// locked by a second statement.
const jobClaimCandidatesQuery = `
WITH running AS (
    SELECT queue, count(*) AS count FROM job WHERE state = $2 GROUP BY queue
), candidate AS (
    SELECT job.job_id, job.priority, COALESCE(job.schedule_time, job.create_time) AS due_time, job.create_time,
        row_number() OVER (PARTITION BY job.queue ORDER BY job.priority DESC, COALESCE(job.schedule_time, job.create_time), job.create_time) AS rank,
        COALESCE((queue.policy->>'max_concurrency')::int, 0) AS max_concurrency,
        COALESCE(running.count, 0) AS running_count
    FROM job
    LEFT JOIN queue ON 'queues/' || queue.queue_id = job.queue
    LEFT JOIN running ON running.queue = job.queue
    WHERE job.state = $1
        AND (job.schedule_time IS NULL OR job.schedule_time <= $3)
        AND (job.expire_time IS NULL OR job.expire_time > $3)
        AND (queue.queue_id IS NULL OR queue.state = $4)
)
SELECT job_id FROM candidate
WHERE max_concurrency = 0 OR rank <= max_concurrency - running_count
ORDER BY priority DESC, due_time, create_time
LIMIT $5`

var jobClaimLockQuery = "SELECT " + postgres.QualifyColumns(JobPostgresColumns, "job") + `, queue.policy AS queue_policy, queue.handlers AS queue_handlers
FROM job LEFT JOIN queue ON 'queues/' || queue.queue_id = job.queue
WHERE job.job_id = ANY($1) AND job.state = $2
ORDER BY array_position($1, job.job_id)
FOR UPDATE OF job SKIP LOCKED`

// ClaimJobs moves up to limit due PENDING jobs to RUNNING through transition,
// honouring each queue's state and max_concurrency, and returns them with
// their queue's policy and handlers. Instances claim one at a time under an
// advisory lock so the concurrency limits hold exactly across replicas.
func (s *Store) ClaimJobs(ctx context.Context, now time.Time, limit int, transition func(*ClaimedJob) error) ([]*ClaimedJob, error) {
	var jobs []*ClaimedJob
	transactionFN := func(tx postgres.Tx) error {
		jobs = nil
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", claimAdvisoryLockKey); err != nil {
			return fmt.Errorf("taking claim lock: %w", err)
		}
		rows, err := tx.Query(ctx, jobClaimCandidatesQuery,
			int16(schedulerpb.JobState_JOB_STATE_PENDING), int16(schedulerpb.JobState_JOB_STATE_RUNNING), now, int16(schedulerpb.QueueState_QUEUE_STATE_RUNNING), limit)
		if err != nil {
			return fmt.Errorf("selecting claim candidates: %w", err)
		}
		jobIDs, err := v5.CollectRows(rows, v5.RowTo[string])
		if err != nil {
			return fmt.Errorf("collecting claim candidates: %w", err)
		}
		if len(jobIDs) == 0 {
			return nil
		}
		rows, err = tx.Query(ctx, jobClaimLockQuery, jobIDs, int16(schedulerpb.JobState_JOB_STATE_PENDING))
		if err != nil {
			return fmt.Errorf("locking jobs: %w", err)
		}
		jobs, err = v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[ClaimedJob])
		if err != nil {
			return fmt.Errorf("collecting jobs: %w", err)
		}
		batch := &v5.Batch{}
		for _, job := range jobs {
			if err := transition(job); err != nil {
				return err
			}
			params := append(postgres.GetParams(&job.Job, jobTransitionColumns...), job.JobID)
			batch.Queue(jobTransitionQuery, params...)
		}
		return tx.SendBatch(ctx, batch).Close()
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return jobs, nil
}
