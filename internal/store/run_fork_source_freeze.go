package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

var ErrRunForkSourceFreezeConfirmationRequired = errors.New("run fork source freeze confirmation required")
var ErrRunForkSourceFreezeBusy = errors.New("run fork source has in-flight execution authority")

type RunForkSourceFreezeConfirmationError struct {
	SourceRunID string
	ForkRunID   string
}

func (e *RunForkSourceFreezeConfirmationError) Error() string {
	if e == nil {
		return ErrRunForkSourceFreezeConfirmationRequired.Error()
	}
	return fmt.Sprintf("%s: source_run_id=%s fork_run_id=%s", ErrRunForkSourceFreezeConfirmationRequired, strings.TrimSpace(e.SourceRunID), strings.TrimSpace(e.ForkRunID))
}

func (e *RunForkSourceFreezeConfirmationError) Unwrap() error {
	return ErrRunForkSourceFreezeConfirmationRequired
}

type RunForkSourceFreezeBusyError struct {
	SourceRunID string
	Blockers    []string
}

func (e *RunForkSourceFreezeBusyError) Error() string {
	if e == nil {
		return ErrRunForkSourceFreezeBusy.Error()
	}
	return fmt.Sprintf("%s: source_run_id=%s blockers=%s", ErrRunForkSourceFreezeBusy, strings.TrimSpace(e.SourceRunID), strings.Join(e.Blockers, ","))
}

func (e *RunForkSourceFreezeBusyError) Unwrap() error {
	return ErrRunForkSourceFreezeBusy
}

// applyRunForkSourceFreeze is the only writer of the terminal forked source
// state. The caller owns the surrounding serializable transaction.
func applyRunForkSourceFreeze(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, now time.Time, confirmed bool) error {
	if tx == nil {
		return fmt.Errorf("run fork source freeze transaction is required")
	}
	if err := storerunlifecycle.RequireActive(ctx, tx, lineage.SourceRunID, storerunlifecycle.DialectPostgres); err != nil {
		return fmt.Errorf("admit run fork source freeze: %w", err)
	}
	if !confirmed {
		return &RunForkSourceFreezeConfirmationError{SourceRunID: lineage.SourceRunID, ForkRunID: lineage.ForkRunID}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if err := requireRunForkSourceFreezeReady(ctx, tx, lineage.SourceRunID, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    ended_at = COALESCE(ended_at, $3),
		    continued_as_run_id = $4::uuid
		WHERE run_id = $1::uuid
		  AND status IN ('running', 'paused')
		  AND (continued_as_run_id IS NULL OR continued_as_run_id = $4::uuid)
	`, lineage.SourceRunID, RunForkSourceFrozenStatus, now, lineage.ForkRunID)
	if err != nil {
		return fmt.Errorf("freeze source run: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm source freeze: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("fork activation blocked: source_run_freeze_not_applied")
	}
	if err := supersedeDecisionCardsForRun(ctx, tx, lineage.SourceRunID, "run_forked", now, true, true); err != nil {
		return fmt.Errorf("supersede frozen source decision authority: %w", err)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2, ended_at = NULL
		WHERE run_id = $1::uuid
		  AND status = $3
	`, lineage.ForkRunID, RunForkActivatedStatus, RunForkMaterializedStatus)
	if err != nil {
		return fmt.Errorf("activate fork run: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm fork activation: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("fork activation blocked: fork_run_activation_not_applied")
	}
	if err := recordRunForkActivationAuthorActivity(ctx, lineage, now, true); err != nil {
		return err
	}
	return nil
}

func requireRunForkSourceFreezeReady(ctx context.Context, tx *sql.Tx, sourceRunID string, now time.Time) error {
	checks := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name: "claimed_delivery",
			query: `SELECT EXISTS (
				SELECT 1 FROM event_deliveries
				WHERE run_id = $1::uuid AND status = 'in_progress'
			)`,
			args: []any{sourceRunID},
		},
		{
			name: "leased_session",
			query: `SELECT EXISTS (
				SELECT 1 FROM agent_sessions
				WHERE run_id = $1::uuid AND status = 'active'
				  AND NULLIF(lease_holder, '') IS NOT NULL
				  AND lease_expires_at > $2
			)`,
			args: []any{sourceRunID, now},
		},
		{
			name: "started_activity",
			query: `SELECT EXISTS (
				SELECT 1 FROM activity_attempts
				WHERE run_id = $1::uuid AND status = 'started'
			)`,
			args: []any{sourceRunID},
		},
		{
			name: "directive_operation",
			query: `SELECT EXISTS (
				SELECT 1 FROM agent_directive_operations
				WHERE resolved_run_id = $1::uuid
				  AND state IN ('prepared', 'executing', 'executed')
			)`,
			args: []any{sourceRunID},
		},
		{
			name: "managed_external_attempt",
			query: `SELECT EXISTS (
				SELECT 1
				FROM runtime_external_effect_attempts a
				JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
				WHERE a.state IN ('authorized', 'launched', 'response_observed')
				  AND a.lease_expires_at > $2
				  AND COALESCE(NULLIF(o.lineage->>'run_id', ''), NULLIF(o.authority_evidence #>> '{usage_target,run_id}', '')) = $1::text
			)`,
			args: []any{sourceRunID, now},
		},
	}
	blockers := make([]string, 0, len(checks))
	for _, check := range checks {
		var blocked bool
		if err := tx.QueryRowContext(ctx, check.query, check.args...).Scan(&blocked); err != nil {
			return fmt.Errorf("inspect source freeze blocker %s: %w", check.name, err)
		}
		if blocked {
			blockers = append(blockers, check.name)
		}
	}
	if len(blockers) > 0 {
		return &RunForkSourceFreezeBusyError{SourceRunID: sourceRunID, Blockers: blockers}
	}
	return nil
}
