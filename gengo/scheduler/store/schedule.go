package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/postgres"
)

var (
	// schedule_id identifies the row; every other column is written by a transition.
	scheduleTransitionColumns = postgres.GetDBColumns(model.Schedule{}, postgres.ExceptColumns("schedule_id"))
	scheduleTransitionQuery   = updateQuery("scheduler.schedule", scheduleTransitionColumns) + fmt.Sprintf(" WHERE schedule_id = $%d", len(scheduleTransitionColumns)+1)
	scheduleSelectForUpdate   = postgres.SelectQuery("SELECT %s FROM scheduler.schedule WHERE #where# #order_by# LIMIT #limit# FOR UPDATE #locking#", SchedulePostgresColumns)
)

// TransitionSchedules locks up to limit schedules matching whereClause, applies
// transition to each and persists those it reports changed, all in one
// transaction. SKIP LOCKED lets concurrent instances tick disjoint sets. The
// transition runs while the row is locked, so whatever it does outside the
// transaction (creating a tick's job) cannot race another instance's pass on
// the same schedule. Returns the locked schedules as transition left them.
func (s *Store) TransitionSchedules(ctx context.Context, whereClause, orderBy string, limit int, params []any, transition func(*model.Schedule) (bool, error)) ([]*model.Schedule, error) {
	return s.transitionSchedules(ctx, whereClause, orderBy, limit, true, params, transition)
}

func (s *Store) transitionSchedules(ctx context.Context, whereClause, orderBy string, limit int, skipLocked bool, params []any, transition func(*model.Schedule) (bool, error)) ([]*model.Schedule, error) {
	locking := ""
	if skipLocked {
		locking = "SKIP LOCKED"
	}
	query := strings.NewReplacer(
		"#where#", whereClause,
		"#order_by#", orderBy,
		"#limit#", fmt.Sprint(limit),
		"#locking#", locking,
	).Replace(scheduleSelectForUpdate)

	var schedules []*model.Schedule
	transactionFN := func(tx postgres.Tx) error {
		schedules = nil
		rows, err := tx.Query(ctx, query, params...)
		if err != nil {
			return fmt.Errorf("selecting schedules: %w", err)
		}
		schedules, err = v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.Schedule])
		if err != nil {
			return fmt.Errorf("collecting rows: %w", err)
		}
		batch := &v5.Batch{}
		for _, schedule := range schedules {
			changed, err := transition(schedule)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			params := append(postgres.GetParams(schedule, scheduleTransitionColumns...), schedule.ScheduleID)
			batch.Queue(scheduleTransitionQuery, params...)
		}
		if batch.Len() == 0 {
			return nil
		}
		return tx.SendBatch(ctx, batch).Close()
	}
	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return nil, err
	}
	return schedules, nil
}

// TransitionSchedule locks one schedule and applies transition to it,
// persisting the result when transition reports a change. The transition may
// return an error to refuse the change. The row is waited for, not skipped: a
// tick holding it must not turn a pause into NotFound. Returns
// model.ErrScheduleNotExist when there is no such schedule.
func (s *Store) TransitionSchedule(ctx context.Context, scheduleID string, transition func(*model.Schedule) (bool, error)) (*model.Schedule, error) {
	schedules, err := s.transitionSchedules(ctx, "schedule_id = $1", "", 1, false, []any{scheduleID}, transition)
	if err != nil {
		return nil, err
	}
	if len(schedules) == 0 {
		return nil, model.ErrScheduleNotExist
	}
	return schedules[0], nil
}

// CountOverdueSchedules returns the number of ENABLED schedules whose tick was
// due before the given time and has not been taken: the tick routine's lag.
func (s *Store) CountOverdueSchedules(ctx context.Context, before time.Time) (int64, error) {
	var count int64
	if err := s.client.QueryRow(ctx, "SELECT count(*) FROM scheduler.schedule WHERE state = $1 AND next_schedule_time < $2",
		int16(schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED), before).Scan(&count); err != nil {
		return 0, fmt.Errorf("counting overdue schedules: %w", err)
	}
	return count, nil
}
