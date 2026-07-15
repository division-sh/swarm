package runlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type Snapshot struct {
	RunID       string
	Status      string
	EventCount  int
	EntityCount int
	Failure     *runtimefailures.Envelope
	StartedAt   time.Time
	EndedAt     *time.Time
}

type EnsureActiveOptions struct {
	HasStartedAtCol         bool
	HasTriggerCols          bool
	HasCounterCols          bool
	HasEntityStateCountSrc  bool
	RequireEntityStateCount bool
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

var ErrPersistedBundleUnavailable = errors.New("persisted bundle source unavailable")

var ErrRunNotActive = errors.New("run is not active")

var ErrRunNotFound = errors.New("run not found")

type RunNotFoundError struct {
	RunID string
}

func (e *RunNotFoundError) Error() string {
	if e == nil {
		return ErrRunNotFound.Error()
	}
	return fmt.Sprintf("%s: run_id=%s", ErrRunNotFound, strings.TrimSpace(e.RunID))
}

func (e *RunNotFoundError) Unwrap() error {
	return ErrRunNotFound
}

type RunNotActiveError struct {
	RunID  string
	Status string
}

func (e *RunNotActiveError) Error() string {
	if e == nil {
		return ErrRunNotActive.Error()
	}
	return fmt.Sprintf("%s: run_id=%s status=%s", ErrRunNotActive, strings.TrimSpace(e.RunID), strings.TrimSpace(e.Status))
}

func (e *RunNotActiveError) Unwrap() error {
	return ErrRunNotActive
}

type PersistedBundleUnavailableError struct {
	BundleHash   string
	BundleSource string
	Cause        string
}

func (e *PersistedBundleUnavailableError) Error() string {
	if e == nil {
		return ErrPersistedBundleUnavailable.Error()
	}
	parts := []string{ErrPersistedBundleUnavailable.Error()}
	if e.BundleHash != "" {
		parts = append(parts, "bundle_hash="+strings.TrimSpace(e.BundleHash))
	}
	if e.BundleSource != "" {
		parts = append(parts, "bundle_source="+strings.TrimSpace(e.BundleSource))
	}
	if e.Cause != "" {
		parts = append(parts, "cause="+strings.TrimSpace(e.Cause))
	}
	return strings.Join(parts, " ")
}

func (e *PersistedBundleUnavailableError) Unwrap() error {
	return ErrPersistedBundleUnavailable
}

func CanonicalTerminalStatus(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "completed":
		return "completed", nil
	case "failed":
		return "failed", nil
	case "cancelled":
		return "cancelled", nil
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
	return ensureActive(ctx, db, runID, triggerEventID, triggerEventType, opts, true)
}

// RequirePresent locks an existing lifecycle row without requiring an active
// status. Typed runtime-log evidence uses this after terminalization; it may
// never create or reopen a run.
func RequirePresent(ctx context.Context, db DBTX, runID string) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	var status string
	err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid FOR UPDATE`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return &RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require run row: %w", err)
	}
	return nil
}

// RequireActive locks an existing lifecycle row and refuses every status
// except running or paused. It never creates or reopens a run.
func RequireActive(ctx context.Context, db DBTX, runID string, dialect Dialect) error {
	if db == nil {
		return fmt.Errorf("require active run: database is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("require active run: run_id is required")
	}
	var query string
	switch dialect {
	case DialectPostgres:
		query = `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid FOR UPDATE`
	case DialectSQLite:
		query = `SELECT COALESCE(status, '') FROM runs WHERE run_id = ?`
	default:
		return fmt.Errorf("require active run: unsupported dialect %q", dialect)
	}
	var status string
	err := db.QueryRowContext(ctx, query, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return &RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require active run: %w", err)
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if status != "running" && status != "paused" {
		return &RunNotActiveError{RunID: runID, Status: status}
	}
	return nil
}

func ensureActive(ctx context.Context, db DBTX, runID, triggerEventID, triggerEventType string, opts EnsureActiveOptions, allowTransactionWrap bool) error {
	if db == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return fmt.Errorf("ensure run row: %w", err)
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
	if bundleSource == BundleSourcePersisted {
		if allowTransactionWrap {
			if sqlDB, ok := db.(*sql.DB); ok {
				tx, err := sqlDB.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("ensure run row: begin persisted bundle source validation tx: %w", err)
				}
				committed := false
				defer func() {
					if !committed {
						_ = tx.Rollback()
					}
				}()
				if err := ensureActive(ctx, tx, runID, triggerEventID, triggerEventType, opts, false); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("ensure run row: commit persisted bundle source validation tx: %w", err)
				}
				committed = true
				return nil
			}
		}
		if err := lockRunCreation(ctx, db); err != nil {
			return err
		}
		exists, err := persistedBundleRowExists(ctx, db, bundleHash)
		if err != nil {
			return fmt.Errorf("ensure run row: validate persisted bundle source: %w", err)
		}
		if !exists {
			return &PersistedBundleUnavailableError{
				BundleHash:   bundleHash,
				BundleSource: bundleSource,
				Cause:        "persisted_missing_bundle_row",
			}
		}
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
			status = runs.status`
	if opts.HasTerminalCols {
		query += `,
			failure = runs.failure,
			ended_at = runs.ended_at`
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
	var existed bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid)`, runID).Scan(&existed); err != nil {
		return fmt.Errorf("ensure run row: inspect existing run: %w", err)
	}
	query += `
		WHERE runs.status IN ('running', 'paused')`
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("ensure run row: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ensure run row: read affected rows: %w", err)
	}
	if rows == 0 {
		var status string
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
			return fmt.Errorf("ensure run row: load inactive status: %w", err)
		}
		return &RunNotActiveError{RunID: runID, Status: status}
	}
	if !existed {
		occurredAt := time.Now().UTC()
		return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
			Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "started",
			SourceOwner: "runs", SourceIdentity: runID, DedupKey: "run-created:" + runID,
			OccurredAt: occurredAt, RunID: runID,
			Projection: runtimeauthoractivity.Projection{
				SubjectType: "run", SubjectID: runID, TriggerEventType: triggerEventType,
			},
		})
	}
	return nil
}

func lockRunCreation(ctx context.Context, db DBTX) error {
	if _, err := db.ExecContext(ctx, `LOCK TABLE runs IN ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("ensure run row: lock run creation: %w", err)
	}
	return nil
}

func persistedBundleRowExists(ctx context.Context, db DBTX, bundleHash string) (bool, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return false, nil
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM bundles
			WHERE bundle_hash = $1
		)
	`, bundleHash).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
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
			event_count = (
				SELECT COUNT(*)::integer
				FROM events
				WHERE run_id = $1::uuid
			),
			entity_count = (
				SELECT COUNT(DISTINCT entity_id)::integer
				FROM entity_state
				WHERE run_id = $1::uuid
			)
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
	if opts.RequireEntityStateCount && !opts.HasEntityStateCountSrc {
		return Snapshot{}, fmt.Errorf("run lifecycle entity count requires canonical run-scoped entity_state")
	}
	var (
		snap       Snapshot
		failureRaw []byte
		startedAt  sql.NullTime
		endedAt    sql.NullTime
	)
	query := `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			0,
			0,
			NULL::jsonb,
			NULL::timestamptz,
			NULL::timestamptz
		FROM runs
		WHERE run_id = $1::uuid
	`
	if opts.HasCounterCols || opts.HasEntityStateCountSrc || opts.HasTerminalCols || opts.HasStartedAtCol {
		query = `
		SELECT
			run_id::text,
			COALESCE(status, ''), `
		if opts.HasCounterCols {
			query += `
			COALESCE(event_count, 0),`
		} else {
			query += `
			0,`
		}
		if opts.HasEntityStateCountSrc {
			query += `
			COALESCE((
				SELECT COUNT(DISTINCT es.entity_id)::integer
				FROM entity_state es
				WHERE es.run_id = runs.run_id
			), 0),`
		} else {
			query += `
			0,`
		}
		if opts.HasTerminalCols {
			query += `
			failure,`
		} else {
			query += `
			NULL::jsonb,`
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
		&failureRaw,
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
	if len(failureRaw) > 0 {
		failure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return Snapshot{}, fmt.Errorf("load run snapshot failure: %w", err)
		}
		snap.Failure = &failure
	}
	if err := ValidateStatusFailure(snap.Status, snap.Failure); err != nil {
		return Snapshot{}, fmt.Errorf("load run snapshot: %w", err)
	}
	if startedAt.Valid {
		snap.StartedAt = startedAt.Time
	}
	if endedAt.Valid {
		tm := endedAt.Time
		snap.EndedAt = &tm
	}
	return snap, nil
}

func MarkTerminal(ctx context.Context, db DBTX, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time, opts EnsureActiveOptions) (Snapshot, error) {
	if db == nil {
		return Snapshot{}, fmt.Errorf("run lifecycle persistence is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return Snapshot{}, fmt.Errorf("run_id is required")
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return Snapshot{}, fmt.Errorf("mark run terminal: %w", err)
	}
	var err error
	status, err = CanonicalTerminalStatus(status)
	if err != nil {
		return Snapshot{}, err
	}
	if err := ValidateStatusFailure(status, failure); err != nil {
		return Snapshot{}, err
	}
	var failureJSON []byte
	if failure != nil {
		failureJSON, err = runtimefailures.MarshalEnvelope(*failure)
		if err != nil {
			return Snapshot{}, fmt.Errorf("mark run terminal failure: %w", err)
		}
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	if opts.RequireEntityStateCount && !opts.HasEntityStateCountSrc {
		return Snapshot{}, fmt.Errorf("run lifecycle entity count requires canonical run-scoped entity_state")
	}
	if opts.HasCounterCols && opts.HasEntityStateCountSrc {
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
			"failure = $3::jsonb",
			"ended_at = COALESCE(ended_at, $4)",
		)
		args = append(args, nullableFailureJSON(failureJSON), endedAt.UTC())
	}
	terminalPredicate := "status IN ('running', 'paused') OR status = $2"
	if opts.HasTerminalCols {
		terminalPredicate = "status IN ('running', 'paused') OR (status = $2 AND failure IS NOT DISTINCT FROM $3::jsonb)"
	}
	result, err := db.ExecContext(ctx, `
		UPDATE runs
		SET `+strings.Join(setClauses, ", ")+`
		WHERE run_id = $1::uuid
		  AND (`+terminalPredicate+`)
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
		if !sameFailure(current.Failure, failure) {
			return Snapshot{}, fmt.Errorf("run %s already terminal with conflicting failure", runID)
		}
		return current, nil
	}
	snapshot, err := LoadSnapshot(ctx, db, runID, opts)
	if err != nil {
		return Snapshot{}, err
	}
	occurredAt := endedAt.UTC()
	if snapshot.EndedAt != nil {
		occurredAt = snapshot.EndedAt.UTC()
	}
	if err := runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: status,
		SourceOwner: "runs", SourceIdentity: runID + ":" + status, DedupKey: "run-terminal:" + runID + ":" + status,
		OccurredAt: occurredAt, RunID: runID, Failure: failure,
		Projection: runtimeauthoractivity.Projection{SubjectType: "run", SubjectID: runID},
	}); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func ValidateStatusFailure(status string, failure *runtimefailures.Envelope) error {
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "failed" {
		if failure == nil {
			return fmt.Errorf("failed run requires canonical failure")
		}
		if err := runtimefailures.ValidateEnvelope(*failure); err != nil {
			return fmt.Errorf("failed run failure is invalid: %w", err)
		}
		return nil
	}
	if failure != nil {
		return fmt.Errorf("run status %s forbids failure", status)
	}
	return nil
}

func nullableFailureJSON(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func sameFailure(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}
