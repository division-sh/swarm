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

type EnsureActiveOptions struct {
	ReopenCompleted         bool
	HasStartedAtCol         bool
	HasTriggerCols          bool
	HasCounterCols          bool
	HasTerminalCols         bool
	HasBundleHashCol        bool
	HasBundleSourceCol      bool
	HasBundleFingerprintCol bool
	BundleHash              string
	BundleSource            string
	BundleFingerprint       string
}

const (
	BundleSourcePersisted = "persisted"
	BundleSourceEphemeral = "ephemeral"
	BundleSourceDeleted   = "deleted"
	BundleSourceLegacy    = "legacy"
	defaultBundleSource   = BundleSourceLegacy
)

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

func CanonicalBundleSource(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return defaultBundleSource, nil
	case BundleSourcePersisted:
		return BundleSourcePersisted, nil
	case BundleSourceEphemeral:
		return BundleSourceEphemeral, nil
	case BundleSourceDeleted:
		return BundleSourceDeleted, nil
	case BundleSourceLegacy:
		return BundleSourceLegacy, nil
	default:
		return "", fmt.Errorf("unsupported bundle source %q", raw)
	}
}

func EnsureActive(ctx context.Context, db DBTX, runID, triggerEventID, triggerEventType string, opts EnsureActiveOptions) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	triggerEventID = strings.TrimSpace(triggerEventID)
	triggerEventType = strings.TrimSpace(triggerEventType)
	bundleHash := strings.TrimSpace(opts.BundleHash)
	bundleFingerprint := strings.TrimSpace(opts.BundleFingerprint)
	bundleSource, err := CanonicalBundleSource(opts.BundleSource)
	if err != nil {
		return err
	}
	if bundleSource == BundleSourceLegacy && bundleHash != "" {
		return fmt.Errorf("ensure run row: legacy bundle_source cannot carry canonical bundle_hash")
	}
	if bundleSource != BundleSourceLegacy && bundleHash == "" {
		return fmt.Errorf("ensure run row: bundle_hash is required for bundle_source=%s", bundleSource)
	}
	reopenStatus := "runs.status"
	reopenErrorSummary := ""
	reopenEndedAt := ""
	if opts.ReopenCompleted {
		reopenStatus = "CASE WHEN runs.status = 'completed' THEN 'running' ELSE runs.status END"
		if opts.HasTerminalCols {
			reopenErrorSummary = "CASE WHEN runs.status = 'completed' THEN NULL ELSE runs.error_summary END"
			reopenEndedAt = "CASE WHEN runs.status = 'completed' THEN NULL ELSE runs.ended_at END"
		}
	} else if opts.HasTerminalCols {
		reopenErrorSummary = "runs.error_summary"
		reopenEndedAt = "runs.ended_at"
	}
	insertCols := []string{"run_id", "status"}
	insertVals := []string{"$1::uuid", "'running'"}
	args := []any{runID}
	addParam := func(col, expr string, value any) {
		args = append(args, value)
		insertCols = append(insertCols, col)
		insertVals = append(insertVals, fmt.Sprintf(expr, len(args)))
	}
	if opts.HasTriggerCols {
		addParam("trigger_event_id", "NULLIF($%d,'')::uuid", triggerEventID)
		addParam("trigger_event_type", "NULLIF($%d,'')", triggerEventType)
	}
	if opts.HasBundleHashCol {
		addParam("bundle_hash", "NULLIF($%d,'')", bundleHash)
	}
	if opts.HasBundleSourceCol {
		addParam("bundle_source", "$%d", bundleSource)
	}
	if opts.HasBundleFingerprintCol {
		addParam("bundle_fingerprint", "NULLIF($%d,'')", bundleFingerprint)
	}
	if opts.HasStartedAtCol {
		insertCols = append(insertCols, "started_at")
		insertVals = append(insertVals, "now()")
	}
	query := `
		INSERT INTO runs (` + strings.Join(insertCols, ", ") + `)
		VALUES (` + strings.Join(insertVals, ", ") + `)
		ON CONFLICT (run_id) DO UPDATE SET
			status = ` + reopenStatus
	if opts.HasTerminalCols {
		query += `,
			error_summary = ` + reopenErrorSummary + `,
			ended_at = ` + reopenEndedAt
	}
	if opts.HasBundleHashCol {
		query += `,
			bundle_hash = COALESCE(runs.bundle_hash, EXCLUDED.bundle_hash)`
	}
	if opts.HasBundleSourceCol {
		query += `,
			bundle_source = COALESCE(runs.bundle_source, EXCLUDED.bundle_source)`
	}
	if opts.HasBundleFingerprintCol {
		query += `,
			bundle_fingerprint = COALESCE(runs.bundle_fingerprint, EXCLUDED.bundle_fingerprint)`
	}
	if opts.HasTriggerCols {
		query += `,
			trigger_event_id = COALESCE(runs.trigger_event_id, NULLIF($2,'')::uuid),
			trigger_event_type = COALESCE(NULLIF(runs.trigger_event_type, ''), NULLIF($3, ''))`
	}
	_, err = db.ExecContext(ctx, query, args...)
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

func LoadSnapshot(ctx context.Context, db DBTX, runID string, opts EnsureActiveOptions) (Snapshot, error) {
	runID = strings.TrimSpace(runID)
	if db == nil || runID == "" {
		return Snapshot{}, fmt.Errorf("run_id is required")
	}
	var (
		snap      Snapshot
		startedAt sql.NullTime
		endedAt   sql.NullTime
	)
	query := `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			0,
			0,
			'',
			NULL::timestamptz,
			NULL::timestamptz
		FROM runs
		WHERE run_id = $1::uuid
	`
	if opts.HasCounterCols || opts.HasTerminalCols || opts.HasStartedAtCol {
		query = `
		SELECT
			run_id::text,
			COALESCE(status, ''), `
		if opts.HasCounterCols {
			query += `
			COALESCE(event_count, 0),
			COALESCE(entity_count, 0),`
		} else {
			query += `
			0,
			0,`
		}
		if opts.HasTerminalCols {
			query += `
			COALESCE(error_summary, ''),`
		} else {
			query += `
			'',`
		}
		if opts.HasStartedAtCol {
			query += `
			started_at,`
		} else {
			query += `
			NULL::timestamptz,`
		}
		if opts.HasTerminalCols {
			query += `
			ended_at`
		} else {
			query += `
			NULL::timestamptz`
		}
		query += `
		FROM runs
		WHERE run_id = $1::uuid
	`
	}
	if err := db.QueryRowContext(ctx, query, runID).Scan(
		&snap.RunID,
		&snap.Status,
		&snap.EventCount,
		&snap.EntityCount,
		&snap.ErrorSummary,
		&startedAt,
		&endedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return Snapshot{}, fmt.Errorf("run %s not found", runID)
		}
		return Snapshot{}, fmt.Errorf("load run snapshot: %w", err)
	}
	snap.RunID = strings.TrimSpace(snap.RunID)
	snap.Status = strings.TrimSpace(snap.Status)
	snap.ErrorSummary = strings.TrimSpace(snap.ErrorSummary)
	if startedAt.Valid {
		snap.StartedAt = startedAt.Time
	}
	if endedAt.Valid {
		tm := endedAt.Time
		snap.EndedAt = &tm
	}
	return snap, nil
}

func MarkTerminal(ctx context.Context, db DBTX, runID, status, errorSummary string, endedAt time.Time, opts EnsureActiveOptions) (Snapshot, error) {
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
	if opts.HasCounterCols {
		if err := SyncCounts(ctx, db, runID); err != nil {
			return Snapshot{}, err
		}
	}
	if status == "completed" || status == "failed" {
		active, err := HasActiveDeliveries(ctx, db, runID)
		if err != nil {
			return Snapshot{}, err
		}
		if active {
			return Snapshot{}, fmt.Errorf("run %s still has active deliveries", runID)
		}
	}
	setClauses := []string{"status = $2"}
	args := []any{runID, status}
	if opts.HasTerminalCols {
		setClauses = append(setClauses,
			"error_summary = NULLIF($3, '')",
			"ended_at = COALESCE(ended_at, $4)",
		)
		args = append(args, errorSummary, endedAt.UTC())
	}
	result, err := db.ExecContext(ctx, `
		UPDATE runs
		SET `+strings.Join(setClauses, ", ")+`
		WHERE run_id = $1::uuid
		  AND (status IN ('running', 'paused') OR status = $2)
	`, args...)
	if err != nil {
		return Snapshot{}, fmt.Errorf("mark run terminal: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		current, loadErr := LoadSnapshot(ctx, db, runID, opts)
		if loadErr != nil {
			return Snapshot{}, loadErr
		}
		if current.Status != status {
			return Snapshot{}, fmt.Errorf("run %s already terminal with status %s", runID, current.Status)
		}
		return current, nil
	}
	return LoadSnapshot(ctx, db, runID, opts)
}
