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

// jobUniqueKeyLiveIndex allows one live job not yet started and one started
// (RUNNING, or PENDING a retry) per unique key; see the job migrations.
const jobUniqueKeyLiveIndex = "job_unique_key_live_idx"

// IsUniqueKeyConflict reports whether err is a violation of the unique key
// index, i.e. a transition that would give a key a second job not yet started,
// or a second started one.
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
		var err error
		if jobs, err = lockJobs(ctx, tx, query, params...); err != nil {
			return err
		}
		return writeJobs(ctx, tx, jobs, transition)
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return jobs, nil
}

func lockJobs(ctx context.Context, tx postgres.Tx, query string, params ...any) ([]*model.Job, error) {
	rows, err := tx.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("selecting jobs: %w", err)
	}
	jobs, err := v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.Job])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return jobs, nil
}

func writeJobs(ctx context.Context, tx postgres.Tx, jobs []*model.Job, transition func(*model.Job) error) error {
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

// jobsByNameForUpdate locks the jobs named by parallel organization, user and
// job id arrays ("" for no parent segment). Locking in job_id order makes
// concurrent batches over overlapping jobs queue instead of deadlocking.
var jobsByNameForUpdate = postgres.SelectQuery(`SELECT %s FROM scheduler.job
JOIN unnest($1::text[], $2::text[], $3::text[]) AS name(organization, usr, job)
ON job_id = name.job AND organization_id IS NOT DISTINCT FROM NULLIF(name.organization, '') AND user_id IS NOT DISTINCT FROM NULLIF(name.usr, '')
ORDER BY job_id FOR UPDATE OF job`, JobPostgresColumns)

// JobNames holds parsed job names as parallel arrays, as BatchGetJobs takes them.
type JobNames struct {
	OrganizationIDs, UserIDs, JobIDs []string
}

func (n JobNames) lock(ctx context.Context, tx postgres.Tx) ([]*model.Job, error) {
	return lockJobs(ctx, tx, jobsByNameForUpdate, n.OrganizationIDs, n.UserIDs, n.JobIDs)
}

// missing names the requested jobs absent from the locked ones, so a batch
// failure says which job it was.
func (n JobNames) missing(jobs []*model.Job) error {
	found := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		found[job.JobID] = true
	}
	var missing []string
	for _, jobID := range n.JobIDs {
		if !found[jobID] {
			missing = append(missing, jobID)
		}
	}
	return fmt.Errorf("%w: %s", model.ErrJobNotExist, strings.Join(missing, ", "))
}

// TransitionJobsByName locks the jobs, waiting on held rows, and applies
// transition to each in one transaction: every job transitions or none does.
// Returns model.ErrJobNotExist when any job is missing.
func (s *Store) TransitionJobsByName(ctx context.Context, names JobNames, transition func(*model.Job) error) ([]*model.Job, error) {
	var jobs []*model.Job
	transactionFN := func(tx postgres.Tx) error {
		var err error
		if jobs, err = names.lock(ctx, tx); err != nil {
			return err
		}
		if len(jobs) != len(names.JobIDs) {
			return names.missing(jobs)
		}
		return writeJobs(ctx, tx, jobs, transition)
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return jobs, nil
}

// DeleteJobsByName locks the jobs and deletes them in one transaction once
// check accepts each: every job is deleted or none is. Returns
// model.ErrJobNotExist when any job is missing, unless allowMissing.
func (s *Store) DeleteJobsByName(ctx context.Context, names JobNames, allowMissing bool, check func(*model.Job) error) error {
	transactionFN := func(tx postgres.Tx) error {
		jobs, err := names.lock(ctx, tx)
		if err != nil {
			return err
		}
		if !allowMissing && len(jobs) != len(names.JobIDs) {
			return names.missing(jobs)
		}
		jobIDs := make([]string, len(jobs))
		for i, job := range jobs {
			if err := check(job); err != nil {
				return err
			}
			jobIDs[i] = job.JobID
		}
		if _, err := tx.Exec(ctx, "DELETE FROM scheduler.job WHERE job_id = ANY($1)", jobIDs); err != nil {
			return fmt.Errorf("deleting jobs: %w", err)
		}
		return nil
	}
	return s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN)
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

var jobGetUnstartedByUniqueKeyQuery = postgres.SelectQuery("SELECT %s FROM scheduler.job WHERE unique_key = $1 AND state = $2 AND start_time IS NULL", JobPostgresColumns)

// GetUnstartedJobByUniqueKey returns the PENDING job holding the unique key
// that has not started, the one a keyed create coalesces onto. Returns
// model.ErrJobNotExist when there is none.
func (s *Store) GetUnstartedJobByUniqueKey(ctx context.Context, uniqueKey string) (*model.Job, error) {
	rows, err := s.client.Query(ctx, jobGetUnstartedByUniqueKeyQuery, uniqueKey, int16(schedulerpb.JobState_JOB_STATE_PENDING))
	if err != nil {
		return nil, fmt.Errorf("getting unstarted job by unique key: %w", err)
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
