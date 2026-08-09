package ingresspersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type RuntimeIngressPostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent func() error
}

type RuntimeIngressSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	requireCurrent func() error
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error) (*RuntimeIngressPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, errors.New("runtime ingress PostgreSQL owner dependencies are required")
	}
	return &RuntimeIngressPostgresOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error) (*RuntimeIngressSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, errors.New("runtime ingress SQLite owner dependencies are required")
	}
	return &RuntimeIngressSQLiteOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func (s *RuntimeIngressPostgresOwner) EnsureRuntimeIngressState(ctx context.Context, now time.Time) (runtimeingress.State, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, fmt.Errorf("runtime ingress PostgreSQL owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, err := s.backend.ExecContext(ctx, `
		INSERT INTO runtime_ingress_state (id, status, controlled_by, updated_at)
		VALUES (1, 'running', 'runtime', $1)
		ON CONFLICT (id) DO NOTHING
	`, now.UTC()); err != nil {
		return runtimeingress.State{}, fmt.Errorf("ensure runtime ingress state: %w", err)
	}
	return s.LoadRuntimeIngressState(ctx)
}

func (s *RuntimeIngressPostgresOwner) LoadRuntimeIngressState(ctx context.Context) (runtimeingress.State, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, fmt.Errorf("runtime ingress PostgreSQL owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, err
	}
	state, err := scanRuntimeIngressState(s.backend.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
		FROM runtime_ingress_state
		WHERE id = 1
	`))
	if err == sql.ErrNoRows {
		return runtimeingress.State{}, runtimeingress.ErrStateNotInitialized
	}
	if err != nil {
		return runtimeingress.State{}, fmt.Errorf("load runtime ingress state: %w", err)
	}
	return state, nil
}

func (s *RuntimeIngressPostgresOwner) TransitionRuntimeIngressState(ctx context.Context, target runtimeingress.Status, reason, controlledBy string, now time.Time) (runtimeingress.State, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, false, fmt.Errorf("runtime ingress PostgreSQL owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, false, err
	}
	if err := validateTransition(target); err != nil {
		return runtimeingress.State{}, false, err
	}
	reason, controlledBy, now = normalizeTransition(reason, controlledBy, now)
	var state runtimeingress.State
	changed := false
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txctx, `
			INSERT INTO runtime_ingress_state (id, status, controlled_by, updated_at)
			VALUES (1, 'running', 'runtime', $1)
			ON CONFLICT (id) DO NOTHING
		`, now); err != nil {
			return fmt.Errorf("ensure runtime ingress state: %w", err)
		}
		current, err := scanRuntimeIngressState(tx.QueryRowContext(txctx, `
			SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
			FROM runtime_ingress_state
			WHERE id = 1
			FOR UPDATE
		`))
		if err != nil {
			return fmt.Errorf("lock runtime ingress state: %w", err)
		}
		if current.Status == target {
			state = current
			return nil
		}
		state, err = scanRuntimeIngressState(tx.QueryRowContext(txctx, `
			UPDATE runtime_ingress_state
			SET status = $1, reason = NULLIF($2, ''), controlled_by = $3,
			    transition_event_id = NULL, updated_at = $4
			WHERE id = 1
			RETURNING status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id::text, ''), updated_at
		`, string(target), reason, controlledBy, now))
		if err != nil {
			return fmt.Errorf("update runtime ingress state: %w", err)
		}
		changed = true
		return nil
	})
	return state, changed, err
}

func (s *RuntimeIngressPostgresOwner) SetRuntimeIngressTransitionEvent(ctx context.Context, target runtimeingress.Status, eventID string, transitionAt time.Time) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("runtime ingress PostgreSQL owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return false, err
	}
	if err := validateTransitionEvent(target, eventID, transitionAt); err != nil || strings.TrimSpace(eventID) == "" {
		return false, err
	}
	res, err := s.backend.ExecContext(ctx, `
		UPDATE runtime_ingress_state
		SET transition_event_id = $1::uuid, updated_at = $3
		WHERE id = 1 AND status = $2 AND updated_at = $3
	`, strings.TrimSpace(eventID), string(target), transitionAt.UTC())
	if err != nil {
		return false, fmt.Errorf("set runtime ingress transition event: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("set runtime ingress transition event rows: %w", err)
	}
	return rows > 0, nil
}

func (s *RuntimeIngressSQLiteOwner) EnsureRuntimeIngressState(ctx context.Context, now time.Time) (runtimeingress.State, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, fmt.Errorf("runtime ingress SQLite owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := s.backend.RunTransaction(ctx, "sqlite runtime ingress ensure", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			INSERT OR IGNORE INTO runtime_ingress_state (id, status, controlled_by, updated_at)
			VALUES (1, 'running', 'runtime', ?)
		`, now.UTC())
		return err
	}); err != nil {
		return runtimeingress.State{}, fmt.Errorf("ensure sqlite runtime ingress state: %w", err)
	}
	return s.LoadRuntimeIngressState(ctx)
}

func (s *RuntimeIngressSQLiteOwner) LoadRuntimeIngressState(ctx context.Context) (runtimeingress.State, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, fmt.Errorf("runtime ingress SQLite owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, err
	}
	state, err := scanRuntimeIngressState(s.backend.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id, ''), updated_at
		FROM runtime_ingress_state
		WHERE id = 1
	`))
	if err == sql.ErrNoRows {
		return runtimeingress.State{}, runtimeingress.ErrStateNotInitialized
	}
	if err != nil {
		return runtimeingress.State{}, fmt.Errorf("load sqlite runtime ingress state: %w", err)
	}
	return state, nil
}

func (s *RuntimeIngressSQLiteOwner) TransitionRuntimeIngressState(ctx context.Context, target runtimeingress.Status, reason, controlledBy string, now time.Time) (runtimeingress.State, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeingress.State{}, false, fmt.Errorf("runtime ingress SQLite owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return runtimeingress.State{}, false, err
	}
	if err := validateTransition(target); err != nil {
		return runtimeingress.State{}, false, err
	}
	reason, controlledBy, now = normalizeTransition(reason, controlledBy, now)
	var state runtimeingress.State
	changed := false
	err := s.backend.RunTransaction(ctx, "sqlite runtime ingress transition", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txctx, `
			INSERT OR IGNORE INTO runtime_ingress_state (id, status, controlled_by, updated_at)
			VALUES (1, 'running', 'runtime', ?)
		`, now); err != nil {
			return fmt.Errorf("ensure sqlite runtime ingress state: %w", err)
		}
		current, err := scanRuntimeIngressState(tx.QueryRowContext(txctx, `
			SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id, ''), updated_at
			FROM runtime_ingress_state WHERE id = 1
		`))
		if err != nil {
			return fmt.Errorf("load sqlite runtime ingress state: %w", err)
		}
		if current.Status == target {
			state = current
			return nil
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE runtime_ingress_state
			SET status = ?, reason = ?, controlled_by = ?, transition_event_id = NULL, updated_at = ?
			WHERE id = 1
		`, string(target), nullableString(reason), controlledBy, now); err != nil {
			return fmt.Errorf("update sqlite runtime ingress state: %w", err)
		}
		state, err = scanRuntimeIngressState(tx.QueryRowContext(txctx, `
			SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id, ''), updated_at
			FROM runtime_ingress_state WHERE id = 1
		`))
		if err != nil {
			return fmt.Errorf("load updated sqlite runtime ingress state: %w", err)
		}
		changed = true
		return nil
	})
	return state, changed, err
}

func (s *RuntimeIngressSQLiteOwner) SetRuntimeIngressTransitionEvent(ctx context.Context, target runtimeingress.Status, eventID string, transitionAt time.Time) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("runtime ingress SQLite owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return false, err
	}
	if err := validateTransitionEvent(target, eventID, transitionAt); err != nil || strings.TrimSpace(eventID) == "" {
		return false, err
	}
	updated := false
	err := s.backend.RunTransaction(ctx, "sqlite runtime ingress transition event", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `
			UPDATE runtime_ingress_state
			SET transition_event_id = ?, updated_at = ?
			WHERE id = 1 AND status = ? AND updated_at = ?
		`, strings.TrimSpace(eventID), transitionAt.UTC(), string(target), transitionAt.UTC())
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		updated = rows > 0
		return err
	})
	if err != nil {
		return false, fmt.Errorf("set sqlite runtime ingress transition event: %w", err)
	}
	return updated, nil
}

func validateTransition(target runtimeingress.Status) error {
	if target != runtimeingress.StatusRunning && target != runtimeingress.StatusPaused {
		return fmt.Errorf("unsupported runtime ingress status: %s", target)
	}
	return nil
}

func validateTransitionEvent(target runtimeingress.Status, eventID string, transitionAt time.Time) error {
	if strings.TrimSpace(eventID) == "" {
		return nil
	}
	if err := validateTransition(target); err != nil {
		return err
	}
	if transitionAt.IsZero() {
		return fmt.Errorf("runtime ingress transition timestamp is required")
	}
	return nil
}

func normalizeTransition(reason, controlledBy string, now time.Time) (string, string, time.Time) {
	reason = strings.TrimSpace(reason)
	controlledBy = strings.TrimSpace(controlledBy)
	if controlledBy == "" {
		controlledBy = "runtime"
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return reason, controlledBy, now.UTC()
}

type stateScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeIngressState(row stateScanner) (runtimeingress.State, error) {
	var state runtimeingress.State
	var status string
	var updatedAt any
	if err := row.Scan(&status, &state.Reason, &state.ControlledBy, &state.TransitionEventID, &updatedAt); err != nil {
		return runtimeingress.State{}, err
	}
	state.Status = runtimeingress.Status(strings.TrimSpace(status))
	if at, ok, err := timeValue(updatedAt); err != nil {
		return runtimeingress.State{}, err
	} else if ok {
		state.UpdatedAt = at
	}
	return state, nil
}

func timeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseTime(value)
	case []byte:
		return parseTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05"}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}

func nullableString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}
