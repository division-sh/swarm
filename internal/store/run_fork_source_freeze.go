package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
func (s *PostgresStore) applyRunForkSourceFreeze(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, now time.Time, confirmed bool) error {
	if tx == nil {
		return fmt.Errorf("run fork source freeze transaction is required")
	}
	ctx = s.bindRunLifecycleMutation(ctx, tx)
	if err := requirePostgresRunActive(ctx, tx, lineage.SourceRunID); err != nil {
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
	if _, _, err := runtimerunlifecycle.ForkSource(ctx, runtimerunlifecycle.ForkSourceRequest{
		RunID:            lineage.SourceRunID,
		ContinuedAsRunID: lineage.ForkRunID,
		EndedAt:          now,
	}); err != nil {
		return fmt.Errorf("freeze source run lifecycle: %w", err)
	}
	if _, err := runtimerunlifecycle.TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: lineage.ForkRunID,
		State: runtimerunlifecycle.StateRunning,
	}); err != nil {
		return fmt.Errorf("activate fork run lifecycle: %w", err)
	}
	if err := recordRunForkActivationAuthorActivity(ctx, lineage, now); err != nil {
		return err
	}
	return nil
}

func requireRunForkSourceFreezeReady(ctx context.Context, tx *sql.Tx, sourceRunID string, now time.Time) error {
	deliveries, err := postgresDeliveryAdapter.ActiveRunSnapshots(ctx, tx, sourceRunID)
	if err != nil {
		return fmt.Errorf("inspect source freeze delivery authority: %w", err)
	}
	blockers := make([]string, 0, 1)
	for _, delivery := range deliveries {
		if delivery.Status == runtimedelivery.StatusInProgress {
			blockers = append(blockers, "claimed_delivery")
			break
		}
	}
	checks := []struct {
		name  string
		query string
		args  []any
	}{
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
