package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	v5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/postgres"
)

// ErrJobNotRunning is returned by UpdateRunningJob when the job left RUNNING
// under the worker's feet (cancelled, or reaped after its lease lapsed).
var ErrJobNotRunning = errors.New("job is not running")

// jobUniqueKeyLiveIndex allows one PENDING and one RUNNING job per unique key;
// see the job migration.
const jobUniqueKeyLiveIndex = "job_unique_key_live_idx"

// IsUniqueKeyConflict reports whether err is a violation of the unique key
// index, i.e. a transition that would give a key a second PENDING or RUNNING job.
func IsUniqueKeyConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == jobUniqueKeyLiveIndex
}

var (
	// job_id identifies the row; every other column is written by a transition.
	jobTransitionColumns = postgres.GetDBColumns(model.Job{}, postgres.ExceptColumns("job_id"))
	jobTransitionQuery   = updateQuery("scheduler.job", jobTransitionColumns) + fmt.Sprintf(" WHERE job_id = $%d", len(jobTransitionColumns)+1)
	jobSelectForUpdate   = postgres.SelectQuery("SELECT %s FROM scheduler.job WHERE #where# #order_by# LIMIT #limit# FOR UPDATE #locking#", JobPostgresColumns)
)

func updateQuery(table string, columns []string) string {
	assignments := make([]string, len(columns))
	for i, column := range columns {
		assignments[i] = fmt.Sprintf("%s = $%d", column, i+1)
	}
	return "UPDATE " + table + " SET " + strings.Join(assignments, ", ")
}

// TransitionJobs locks up to limit jobs matching whereClause, applies
// transition to each and persists them, all in one transaction. SKIP LOCKED
// lets concurrent workers claim disjoint sets without contending. Rows are
// selected in orderBy order (e.g. "ORDER BY schedule_time NULLS FIRST").
func (s *Store) TransitionJobs(ctx context.Context, whereClause, orderBy string, limit int, params []any, transition func(*model.Job) error) ([]*model.Job, error) {
	return s.transitionJobs(ctx, whereClause, orderBy, limit, true, params, transition)
}

func (s *Store) transitionJobs(ctx context.Context, whereClause, orderBy string, limit int, skipLocked bool, params []any, transition func(*model.Job) error) ([]*model.Job, error) {
	locking := ""
	if skipLocked {
		locking = "SKIP LOCKED"
	}
	query := strings.NewReplacer(
		"#where#", whereClause,
		"#order_by#", orderBy,
		"#limit#", fmt.Sprint(limit),
		"#locking#", locking,
	).Replace(jobSelectForUpdate)

	var jobs []*model.Job
	transactionFN := func(tx postgres.Tx) error {
		jobs = nil
		rows, err := tx.Query(ctx, query, params...)
		if err != nil {
			return fmt.Errorf("selecting jobs: %w", err)
		}
		jobs, err = v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.Job])
		if err != nil {
			return fmt.Errorf("collecting rows: %w", err)
		}
		if len(jobs) == 0 {
			return nil
		}
		batch := &v5.Batch{}
		for _, job := range jobs {
			if err := transition(job); err != nil {
				return err
			}
			params := append(postgres.GetParams(job, jobTransitionColumns...), job.JobID)
			batch.Queue(jobTransitionQuery, params...)
		}
		return tx.SendBatch(ctx, batch).Close()
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return jobs, nil
}

// TransitionJob locks one job and applies transition to it. The transition
// may return an error to refuse the change (e.g. a state precondition). The
// row is waited for, not skipped: a lease renewal holding it must not turn a
// cancel into NotFound.
func (s *Store) TransitionJob(ctx context.Context, jobID string, transition func(*model.Job) error) (*model.Job, error) {
	jobs, err := s.transitionJobs(ctx, "job_id = $1", "", 1, false, []any{jobID}, transition)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, model.ErrJobNotExist
	}
	return jobs[0], nil
}

// UpdateRunningJob writes the given columns of a job the caller believes to be
// RUNNING and returns the fresh row. It is the worker's write path: the state
// predicate makes a cancelled or reaped job's late writes no-ops, surfaced as
// ErrJobNotRunning.
func (s *Store) UpdateRunningJob(ctx context.Context, job *model.Job, columns ...string) (*model.Job, error) {
	params := append(postgres.GetParams(job, columns...), job.JobID, int16(schedulerpb.JobState_JOB_STATE_RUNNING))
	query := updateQuery("scheduler.job", columns) + fmt.Sprintf(" WHERE job_id = $%d AND state = $%d RETURNING ", len(columns)+1, len(columns)+2) +
		postgres.SelectQuery("%s", JobPostgresColumns)
	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("updating running job: %w", err)
	}
	row, err := v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Job])
	if err != nil {
		if errors.Is(err, v5.ErrNoRows) {
			return nil, ErrJobNotRunning
		}
		return nil, fmt.Errorf("collecting row: %w", err)
	}
	return row, nil
}

var jobGetPendingByUniqueKeyQuery = postgres.SelectQuery("SELECT %s FROM scheduler.job WHERE unique_key = $1 AND state = $2", JobPostgresColumns)

// GetPendingJobByUniqueKey returns the PENDING job holding the unique key, the
// one a keyed create coalesces onto. Returns model.ErrJobNotExist when there is
// none.
func (s *Store) GetPendingJobByUniqueKey(ctx context.Context, uniqueKey string) (*model.Job, error) {
	rows, err := s.client.Query(ctx, jobGetPendingByUniqueKeyQuery, uniqueKey, int16(schedulerpb.JobState_JOB_STATE_PENDING))
	if err != nil {
		return nil, fmt.Errorf("getting pending job by unique key: %w", err)
	}
	job, err := v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Job])
	if err != nil {
		if errors.Is(err, v5.ErrNoRows) {
			return nil, model.ErrJobNotExist
		}
		return nil, fmt.Errorf("collecting row: %w", err)
	}
	return job, nil
}

// PurgeJobs deletes jobs whose retention lapsed before now.
func (s *Store) PurgeJobs(ctx context.Context, now time.Time) (int64, error) {
	tag, err := s.client.Exec(ctx, "DELETE FROM scheduler.job WHERE purge_time < $1", now)
	if err != nil {
		return 0, fmt.Errorf("purging jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}
