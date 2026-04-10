package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Snapshot struct {
	RunID        string
	Status       string
	EventCount   int
	EntityCount  int
	ErrorSummary string
	StartedAt    time.Time
	EndedAt      *time.Time
}

func CanonicalTerminalStatus(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "completed":
		return "completed", nil
	case "failed":
		return "failed", nil
	case "cancelled":
		return "cancelled", nil
	case "forked":
		return "forked", nil
	default:
		return "", fmt.Errorf("unsupported terminal run status %q", raw)
	}
}

func EnsureActive(ctx context.Context, db DBTX, runID, triggerEventID, triggerEventType string) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	triggerEventID = strings.TrimSpace(triggerEventID)
	triggerEventType = strings.TrimSpace(triggerEventType)
	_, err := db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, trigger_event_id, trigger_event_type, started_at
		)
		VALUES (
			$1::uuid, 'running', NULLIF($2,'')::uuid, NULLIF($3,''), now()
		)
		ON CONFLICT (run_id) DO UPDATE SET
			status = CASE
				WHEN runs.status = 'completed' THEN 'running'
				ELSE runs.status
			END,
			error_summary = CASE
				WHEN runs.status = 'completed' THEN NULL
				ELSE runs.error_summary
			END,
			ended_at = CASE
				WHEN runs.status = 'completed' THEN NULL
				ELSE runs.ended_at
			END,
			trigger_event_id = COALESCE(runs.trigger_event_id, NULLIF($2,'')::uuid),
			trigger_event_type = COALESCE(NULLIF(runs.trigger_event_type, ''), NULLIF($3, ''))
	`, runID, triggerEventID, triggerEventType)
	if err != nil {
		return fmt.Errorf("ensure run row: %w", err)
	}
	return nil
}

func SyncCounts(ctx context.Context, db DBTX, runID string) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	_, err := db.ExecContext(ctx, `
		UPDATE runs
		SET
			event_count = counts.event_count,
			entity_count = counts.entity_count
		FROM (
			SELECT
				COUNT(*)::integer AS event_count,
				COUNT(DISTINCT entity_id)::integer AS entity_count
			FROM events
			WHERE run_id = $1::uuid
		) AS counts
		WHERE runs.run_id = $1::uuid
	`, runID)
	if err != nil {
		return fmt.Errorf("sync run counters: %w", err)
	}
	return nil
}

func HasActiveDeliveries(ctx context.Context, db DBTX, runID string) (bool, error) {
	if db == nil {
		return false, nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	var active bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND status IN ('pending', 'in_progress')
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("load active deliveries: %w", err)
	}
	return active, nil
}

func LoadSnapshot(ctx context.Context, db DBTX, runID string) (Snapshot, error) {
	runID = strings.TrimSpace(runID)
	if db == nil || runID == "" {
		return Snapshot{}, fmt.Errorf("run_id is required")
	}
	var (
		snap  Snapshot
		ended sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			COALESCE(event_count, 0),
			COALESCE(entity_count, 0),
			COALESCE(error_summary, ''),
			started_at,
			ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(
		&snap.RunID,
		&snap.Status,
		&snap.EventCount,
		&snap.EntityCount,
		&snap.ErrorSummary,
		&snap.StartedAt,
		&ended,
	); err != nil {
		if err == sql.ErrNoRows {
			return Snapshot{}, fmt.Errorf("run %s not found", runID)
		}
		return Snapshot{}, fmt.Errorf("load run snapshot: %w", err)
	}
	snap.RunID = strings.TrimSpace(snap.RunID)
	snap.Status = strings.TrimSpace(snap.Status)
	snap.ErrorSummary = strings.TrimSpace(snap.ErrorSummary)
	if ended.Valid {
		tm := ended.Time
		snap.EndedAt = &tm
	}
	return snap, nil
}

func MarkTerminal(ctx context.Context, db DBTX, runID, status, errorSummary string, endedAt time.Time) (Snapshot, error) {
	if db == nil {
		return Snapshot{}, fmt.Errorf("run lifecycle persistence is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return Snapshot{}, fmt.Errorf("run_id is required")
	}
	var err error
	status, err = CanonicalTerminalStatus(status)
	if err != nil {
		return Snapshot{}, err
	}
	errorSummary = strings.TrimSpace(errorSummary)
	if status != "failed" {
		errorSummary = ""
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	if err := SyncCounts(ctx, db, runID); err != nil {
		return Snapshot{}, err
	}
	if status == "completed" {
		active, err := HasActiveDeliveries(ctx, db, runID)
		if err != nil {
			return Snapshot{}, err
		}
		if active {
			return Snapshot{}, fmt.Errorf("run %s still has active deliveries", runID)
		}
	}
	result, err := db.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    error_summary = NULLIF($3, ''),
		    ended_at = COALESCE(ended_at, $4)
		WHERE run_id = $1::uuid
		  AND (status IN ('running', 'paused') OR status = $2)
	`, runID, status, errorSummary, endedAt.UTC())
	if err != nil {
		return Snapshot{}, fmt.Errorf("mark run terminal: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		current, loadErr := LoadSnapshot(ctx, db, runID)
		if loadErr != nil {
			return Snapshot{}, loadErr
		}
		if current.Status != status {
			return Snapshot{}, fmt.Errorf("run %s already terminal with status %s", runID, current.Status)
		}
		return current, nil
	}
	return LoadSnapshot(ctx, db, runID)
}
