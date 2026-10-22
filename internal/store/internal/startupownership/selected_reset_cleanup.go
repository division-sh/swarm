package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

type selectedResetHistory struct {
	apply func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) (destructivereset.SelectedHistoryDisposition, error)
}

func BindPostgresSelectedResetHistory(s *StartupPostgresOwner, apply func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) (destructivereset.SelectedHistoryDisposition, error)) error {
	if s == nil {
		return errors.New("selected reset history requires its startup owner")
	}
	return s.selectedResetHistory.bindSelectedResetHistory(apply)
}

func BindSQLiteSelectedResetHistory(s *StartupSQLiteOwner, apply func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) (destructivereset.SelectedHistoryDisposition, error)) error {
	if s == nil {
		return errors.New("selected reset history requires its startup owner")
	}
	return s.selectedResetHistory.bindSelectedResetHistory(apply)
}

func (s *selectedResetHistory) bindSelectedResetHistory(apply func(context.Context, *sql.Tx, destructivereset.CleanupRequest, bool) (destructivereset.SelectedHistoryDisposition, error)) error {
	if apply == nil || s.apply != nil {
		return errors.New("selected reset history must be bound exactly once")
	}
	s.apply = apply
	return nil
}

func (s *selectedResetHistory) applySelectedResetHistoryTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) (destructivereset.SelectedHistoryDisposition, error) {
	var absent destructivereset.SelectedHistoryDisposition
	runIDs, err := adminpersistence.ValidateDestructiveResetCleanupRequest(req, req.RequestedAt)
	if err != nil {
		return absent, err
	}
	planned := make(map[string]bool, len(runIDs))
	for _, id := range runIDs {
		planned[id] = true
	}
	seen := make(map[string]bool, len(req.SelectedHistory))
	for _, prepared := range req.SelectedHistory {
		id := prepared.Binding.ForkRunID
		if !planned[id] || seen[id] {
			return absent, errors.New("selected reset preparation repeats or crosses the planned run set")
		}
		seen[id] = true
	}
	if s.apply == nil {
		if len(req.SelectedHistory) != 0 {
			return absent, errors.New("selected reset history reader is not composed")
		}
		return absent, nil
	}
	return s.apply(ctx, tx, req, sqlite)
}

func attachSelectedResetDisposition(result *destructivereset.CleanupResult, history destructivereset.SelectedHistoryDisposition) error {
	result.SelectedHistory = history
	if !history.Projected {
		return nil
	}
	for i := range result.Tables {
		row := &result.Tables[i]
		if row.Table != "run_fork_operations" && row.Table != "activity_attempts" && row.Table != "agent_turns" {
			continue
		}
		for _, retained := range history.Tables {
			if retained.Table == row.Table {
				if row.MatchedRows < retained.PreservedRows {
					return errors.New("selected dry-run retention exceeds its planned mutable cohort")
				}
				row.MatchedRows -= retained.PreservedRows
				row.PreservedRows += retained.PreservedRows
			}
		}
	}
	return nil
}
