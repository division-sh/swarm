package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimeingress "swarm/internal/runtime/ingress"
)

func (s *PostgresStore) EnsureRuntimeIngressState(ctx context.Context, now time.Time) (runtimeingress.State, error) {
	if s == nil || s.DB == nil {
		return runtimeingress.State{}, fmt.Errorf("postgres store is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, err := s.DB.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (id, status, controlled_by, updated_at)
		VALUES (1, 'running', 'runtime', $1)
		ON CONFLICT (id) DO NOTHING
	`, now.UTC()); err != nil {
		return runtimeingress.State{}, fmt.Errorf("ensure runtime ingress state: %w", err)
	}
	return s.LoadRuntimeIngressState(ctx)
}

func (s *PostgresStore) LoadRuntimeIngressState(ctx context.Context) (runtimeingress.State, error) {
	if s == nil || s.DB == nil {
		return runtimeingress.State{}, fmt.Errorf("postgres store is required")
	}
	state, err := scanRuntimeIngressState(s.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
		FROM runtime_ingress_state
		WHERE id = 1
	`))
	if err == nil {
		return state, nil
	}
	if err == sql.ErrNoRows {
		return runtimeingress.State{}, fmt.Errorf("runtime ingress state is not initialized")
	}
	return runtimeingress.State{}, fmt.Errorf("load runtime ingress state: %w", err)
}

func (s *PostgresStore) TransitionRuntimeIngressState(ctx context.Context, target runtimeingress.Status, reason, controlledBy string, now time.Time) (runtimeingress.State, bool, error) {
	if s == nil || s.DB == nil {
		return runtimeingress.State{}, false, fmt.Errorf("postgres store is required")
	}
	if target != runtimeingress.StatusRunning && target != runtimeingress.StatusPaused {
		return runtimeingress.State{}, false, fmt.Errorf("unsupported runtime ingress status: %s", target)
	}
	reason = strings.TrimSpace(reason)
	controlledBy = strings.TrimSpace(controlledBy)
	if controlledBy == "" {
		controlledBy = "runtime"
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("begin runtime ingress transition: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (id, status, controlled_by, updated_at)
		VALUES (1, 'running', 'runtime', $1)
		ON CONFLICT (id) DO NOTHING
	`, now.UTC()); err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("ensure runtime ingress state: %w", err)
	}
	state, err := scanRuntimeIngressState(tx.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
		FROM runtime_ingress_state
		WHERE id = 1
		FOR UPDATE
	`))
	if err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("lock runtime ingress state: %w", err)
	}
	if state.Status == target {
		if err := tx.Commit(); err != nil {
			return runtimeingress.State{}, false, fmt.Errorf("commit runtime ingress no-op: %w", err)
		}
		committed = true
		return state, false, nil
	}
	state, err = scanRuntimeIngressState(tx.QueryRowContext(ctx, `
		UPDATE runtime_ingress_state
		SET status = $1,
		    reason = NULLIF($2, ''),
		    controlled_by = $3,
		    transition_event_id = NULL,
		    updated_at = $4
		WHERE id = 1
		RETURNING status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
	`, string(target), reason, controlledBy, now.UTC()))
	if err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("update runtime ingress state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("commit runtime ingress transition: %w", err)
	}
	committed = true
	return state, true, nil
}

func (s *PostgresStore) SetRuntimeIngressTransitionEvent(ctx context.Context, target runtimeingress.Status, eventID string, transitionAt time.Time) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	if target != runtimeingress.StatusRunning && target != runtimeingress.StatusPaused {
		return false, fmt.Errorf("unsupported runtime ingress status: %s", target)
	}
	if transitionAt.IsZero() {
		return false, fmt.Errorf("runtime ingress transition timestamp is required")
	}
	res, err := s.DB.ExecContext(ctx, `
			UPDATE runtime_ingress_state
			SET transition_event_id = $1::uuid,
			    updated_at = $3
			WHERE id = 1
			  AND status = $2
			  AND updated_at = $3
		`, eventID, string(target), transitionAt.UTC())
	if err != nil {
		return false, fmt.Errorf("set runtime ingress transition event: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("set runtime ingress transition event rows: %w", err)
	}
	return rows > 0, nil
}

type runtimeIngressStateScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeIngressState(row runtimeIngressStateScanner) (runtimeingress.State, error) {
	var state runtimeingress.State
	var status string
	var updatedAt any
	if err := row.Scan(&status, &state.Reason, &state.ControlledBy, &state.TransitionEventID, &updatedAt); err != nil {
		return runtimeingress.State{}, err
	}
	state.Status = runtimeingress.Status(strings.TrimSpace(status))
	if at, ok, err := sqliteTimeValue(updatedAt); err != nil {
		return runtimeingress.State{}, err
	} else if ok {
		state.UpdatedAt = at
	}
	return state, nil
}
