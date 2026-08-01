package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type postgresRunLifecycleMutation struct {
	store *PostgresStore
	tx    *sql.Tx
}

type sqliteRunLifecycleMutation struct {
	store *SQLiteRuntimeStore
	tx    *sql.Tx
}

func requirePostgresRunPresent(ctx context.Context, tx *sql.Tx, runID string) error {
	return (postgresRunLifecycleMutation{tx: tx}).RequirePresent(ctx, runID)
}

func requireSQLiteRunPresent(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqliteRunLifecycleMutation{tx: tx}).RequirePresent(ctx, runID)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return (postgresRunLifecycleMutation{tx: tx}).RequireActive(ctx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqliteRunLifecycleMutation{tx: tx}).RequireActive(ctx, runID)
}

func (s *PostgresStore) TransitionActiveRun(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	tx, ok := runtimepipelineSQLTx(ctx)
	if !ok {
		return "", errors.New("PostgreSQL active run lifecycle transition requires the current named mutation")
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).TransitionActive(ctx, request)
}

func (s *PostgresStore) RequirePresentRun(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL require run")
	if err != nil {
		return err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequirePresent(ctx, runID)
}

func (s *SQLiteRuntimeStore) RequirePresentRun(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite require run")
	if err != nil {
		return err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequirePresent(ctx, runID)
}

func (s *PostgresStore) RequireActiveRun(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL require active run")
	if err != nil {
		return err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActive(ctx, runID)
}

func (s *SQLiteRuntimeStore) RequireActiveRun(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite require active run")
	if err != nil {
		return err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActive(ctx, runID)
}

func (s *PostgresStore) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL require run source")
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func (s *SQLiteRuntimeStore) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite require run source")
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func (s *PostgresStore) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL require active run source")
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *SQLiteRuntimeStore) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite require active run source")
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *PostgresStore) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL create run")
	if err != nil {
		return "", err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).Create(ctx, request)
}

func (s *SQLiteRuntimeStore) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite create run")
	if err != nil {
		return "", err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).Create(ctx, request)
}

func (s *PostgresStore) ForkRunSource(ctx context.Context, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL fork run source")
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).ForkSource(ctx, request)
}

func (s *SQLiteRuntimeStore) ForkRunSource(ctx context.Context, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite fork run source")
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).ForkSource(ctx, request)
}

func (s *PostgresStore) ReviseRunSource(ctx context.Context, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL revise run source")
	if err != nil {
		return "", err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).ReviseSource(ctx, request)
}

func (s *SQLiteRuntimeStore) ReviseRunSource(ctx context.Context, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite revise run source")
	if err != nil {
		return "", err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).ReviseSource(ctx, request)
}

func (s *PostgresStore) SyncRunCounters(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "PostgreSQL sync run counters")
	if err != nil {
		return err
	}
	return (postgresRunLifecycleMutation{store: s, tx: tx}).SyncCounters(ctx, runID)
}

func (s *SQLiteRuntimeStore) SyncRunCounters(ctx context.Context, runID string) error {
	tx, err := requireRunLifecycleOperationTx(ctx, "SQLite sync run counters")
	if err != nil {
		return err
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).SyncCounters(ctx, runID)
}

func requireRunLifecycleOperationTx(ctx context.Context, operation string) (*sql.Tx, error) {
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return nil, fmt.Errorf("%s requires author activity ownership: %w", operation, err)
	}
	tx, ok := runtimepipelineSQLTx(ctx)
	if !ok {
		return nil, fmt.Errorf("%s requires the current named mutation", operation)
	}
	return tx, nil
}

func (s *SQLiteRuntimeStore) TransitionActiveRun(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	tx, ok := runtimepipelineSQLTx(ctx)
	if !ok {
		return "", errors.New("SQLite active run lifecycle transition requires the current named mutation")
	}
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).TransitionActive(ctx, request)
}

func requirePostgresRunActiveQuery(ctx context.Context, queryer rowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("PostgreSQL run lifecycle query authority is required")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := queryer.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("read PostgreSQL active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return err
	}
	if !parsed.Active() {
		return &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return fmt.Errorf("decode PostgreSQL active run lifecycle source: %w", err)
	}
	if fact.IsPersisted() {
		var exists bool
		if err := queryer.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)
		`, fact.BundleHash()).Scan(&exists); err != nil {
			return fmt.Errorf("validate PostgreSQL active run lifecycle source: %w", err)
		}
		if !exists {
			return &runtimerunlifecycle.PersistedBundleUnavailableError{
				BundleHash: fact.BundleHash(), BundleSource: runtimerunlifecycle.BundleSourcePersisted,
				Cause: "persisted_missing_bundle_row",
			}
		}
	}
	return nil
}

func requireSQLiteRunActiveQuery(ctx context.Context, queryer rowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("SQLite run lifecycle query authority is required")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := queryer.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("read SQLite active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return err
	}
	if !parsed.Active() {
		return &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return fmt.Errorf("decode SQLite active run lifecycle source: %w", err)
	}
	if fact.IsPersisted() {
		var exists bool
		if err := queryer.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)
		`, fact.BundleHash()).Scan(&exists); err != nil {
			return fmt.Errorf("validate SQLite active run lifecycle source: %w", err)
		}
		if !exists {
			return &runtimerunlifecycle.PersistedBundleUnavailableError{
				BundleHash: fact.BundleHash(), BundleSource: runtimerunlifecycle.BundleSourcePersisted,
				Cause: "persisted_missing_bundle_row",
			}
		}
	}
	return nil
}

func requirePostgresRunActiveSource(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	return (postgresRunLifecycleMutation{tx: tx}).RequireActiveSource(ctx, runID)
}

func requireSQLiteRunActiveSource(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	return (sqliteRunLifecycleMutation{tx: tx}).RequireActiveSource(ctx, runID)
}

func (m postgresRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	var state string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, strings.TrimSpace(runID)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require PostgreSQL run: %w", err)
	}
	_, err = runtimerunlifecycle.ParseState(state)
	return err
}

func (m sqliteRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("SQLite run lifecycle mutation requires transaction")
	}
	var state string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status
		FROM runs
		WHERE run_id = ?
	`, strings.TrimSpace(runID)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require SQLite run: %w", err)
	}
	_, err = runtimerunlifecycle.ParseState(state)
	return err
}

func (m postgresRunLifecycleMutation) RequireActive(ctx context.Context, runID string) error {
	_, err := m.RequireActiveSource(ctx, runID)
	return err
}

func (m sqliteRunLifecycleMutation) RequireActive(ctx context.Context, runID string) error {
	_, err := m.RequireActiveSource(ctx, runID)
	return err
}

func (m postgresRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m sqliteRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m postgresRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m sqliteRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m postgresRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.BundleSourceFact, error) {
	if m.tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("load PostgreSQL run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("decode PostgreSQL run lifecycle source: %w", err)
	}
	if err := m.requirePersistedSource(ctx, fact); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return fact, nil
}

func (m sqliteRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.BundleSourceFact, error) {
	if m.tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("SQLite run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("load SQLite run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("decode SQLite run lifecycle source: %w", err)
	}
	if err := m.requirePersistedSource(ctx, fact); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return fact, nil
}

func (m postgresRunLifecycleMutation) requirePersistedSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) error {
	if !fact.IsPersisted() {
		return nil
	}
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate PostgreSQL persisted run source: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.PersistedBundleUnavailableError{
			BundleHash: fact.BundleHash(), BundleSource: "persisted",
			Cause: "persisted_missing_bundle_row",
		}
	}
	return nil
}

func (m sqliteRunLifecycleMutation) requirePersistedSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) error {
	if !fact.IsPersisted() {
		return nil
	}
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate SQLite persisted run source: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.PersistedBundleUnavailableError{
			BundleHash: fact.BundleHash(), BundleSource: "persisted",
			Cause: "persisted_missing_bundle_row",
		}
	}
	return nil
}

func (m postgresRunLifecycleMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("PostgreSQL run lifecycle creation requires transaction")
	}
	request.StartedAt = runtimerunlifecycle.CanonicalTimestamp(request.StartedAt)
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := m.requirePersistedSourceForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	origin := request.Origin
	var inserted bool
	err := m.tx.QueryRowContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			$1::uuid, 'running', $2, $3, $4,
			NULLIF($5, '')::uuid, NULLIF($6, ''),
			NULLIF($7, '')::uuid, NULLIF($8, 0),
			NULLIF($9, '')::uuid, NULLIF($10, '')::uuid,
			$11
		)
		ON CONFLICT (run_id) DO NOTHING
		RETURNING TRUE
	`, request.RunID, bundleHash, bundleSource, origin.Kind(),
		origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC()).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		return m.classifyCreateExisting(ctx, request)
	}
	if err != nil {
		return "", fmt.Errorf("create PostgreSQL run lifecycle: %w", err)
	}
	if !inserted {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := recordRunStarted(ctx, request); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("SQLite run lifecycle creation requires transaction")
	}
	request.StartedAt = runtimerunlifecycle.CanonicalTimestamp(request.StartedAt)
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	origin := request.Origin
	result, err := m.tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			?, 'running', ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''),
			?
		)
		ON CONFLICT (run_id) DO NOTHING
	`, request.RunID, bundleHash, bundleSource, origin.Kind(),
		origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC())
	if err != nil {
		return "", fmt.Errorf("create SQLite run lifecycle: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("inspect SQLite run lifecycle creation result: %w", err)
	}
	if rows == 0 {
		return m.classifyCreateExisting(ctx, request)
	}
	if err := recordRunStarted(ctx, request); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func insertRunForkRun(
	ctx context.Context,
	tx *sql.Tx,
	forkRunID, sourceRunID, forkEventID string,
	entityCount int,
	startedAt time.Time,
	identity runForkBundleInsertIdentity,
) error {
	if tx == nil {
		return errors.New("fork run lifecycle creation requires transaction")
	}
	if err := identity.BundleSourceFact.Validate(); err != nil {
		return fmt.Errorf("fork run lifecycle creation requires canonical executable bundle identity: %w", err)
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return fmt.Errorf("fork run lifecycle creation requires author activity ownership: %w", err)
	}
	mutation := postgresRunLifecycleMutation{tx: tx}
	if err := mutation.requirePersistedSourceForWrite(ctx, identity.BundleSourceFact); err != nil {
		return err
	}
	bundleHash, bundleSource := identity.BundleSourceFact.StorageValues()
	origin, err := runtimerunlifecycle.ForkMaterializationRunOrigin(sourceRunID, forkEventID)
	if err != nil {
		return err
	}
	startedAt = runtimerunlifecycle.CanonicalTimestamp(startedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, origin_kind, forked_from_run_id, forked_from_event_id,
			entity_count, event_count, started_at, bundle_hash, bundle_source
		)
		VALUES ($1::uuid, $2, $3, $4::uuid, $5::uuid, $6, 0, $7, $8, $9)
	`, forkRunID, string(runtimerunlifecycle.StatePaused), origin.Kind(), origin.SourceRunID(), origin.SourceEventID(),
		entityCount, startedAt.UTC(), bundleHash, bundleSource); err != nil {
		return fmt.Errorf("insert fork run lifecycle: %w", err)
	}
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, bundleHash)
	if err != nil {
		return err
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "fork_prepared",
		SourceOwner: "runs", SourceIdentity: forkRunID, DedupKey: "run-created:" + forkRunID,
		OccurredAt: startedAt.UTC(), RunID: forkRunID, Scope: scope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: forkRunID, ParentRunID: sourceRunID, TriggerEventType: "run.fork",
		},
	})
}

func (m postgresRunLifecycleMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("PostgreSQL active run lifecycle transition requires transaction")
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	current, err := loadPostgresRunLifecycleSnapshot(ctx, m.tx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if current.State == request.State {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if current.State == runtimerunlifecycle.StateRunning && request.State != runtimerunlifecycle.StatePaused {
		return "", fmt.Errorf("invalid PostgreSQL run lifecycle transition %s -> %s", current.State, request.State)
	}
	if current.State == runtimerunlifecycle.StatePaused && request.State != runtimerunlifecycle.StateRunning {
		return "", fmt.Errorf("invalid PostgreSQL run lifecycle transition %s -> %s", current.State, request.State)
	}
	result, err := transitionPostgresActiveRunStateTx(
		ctx,
		m.tx,
		request.RunID,
		request.State,
		current.State,
	)
	if err != nil {
		return "", fmt.Errorf("transition PostgreSQL run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("transition PostgreSQL run lifecycle affected %d rows", rows), rowsErr)
	}
	if request.State == runtimerunlifecycle.StateRunning {
		if m.store == nil {
			return "", errors.New("PostgreSQL run lifecycle resume requires selected-store candidate owner")
		}
		if _, err := m.store.requestCompletionCandidateTx(ctx, m.tx, request.RunID, nil); err != nil {
			return "", err
		}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("SQLite active run lifecycle transition requires transaction")
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	current, err := loadSQLiteRunLifecycleSnapshot(ctx, m.tx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.State == request.State {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if current.State == runtimerunlifecycle.StateRunning && request.State != runtimerunlifecycle.StatePaused {
		return "", fmt.Errorf("invalid SQLite run lifecycle transition %s -> %s", current.State, request.State)
	}
	if current.State == runtimerunlifecycle.StatePaused && request.State != runtimerunlifecycle.StateRunning {
		return "", fmt.Errorf("invalid SQLite run lifecycle transition %s -> %s", current.State, request.State)
	}
	result, err := transitionSQLiteActiveRunStateTx(
		ctx,
		m.tx,
		request.RunID,
		request.State,
		current.State,
	)
	if err != nil {
		return "", fmt.Errorf("transition SQLite run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("transition SQLite run lifecycle affected %d rows", rows), rowsErr)
	}
	if request.State == runtimerunlifecycle.StateRunning {
		if m.store == nil {
			return "", errors.New("SQLite run lifecycle resume requires selected-store candidate owner")
		}
		if _, err := m.store.requestCompletionCandidateTx(ctx, m.tx, request.RunID, nil); err != nil {
			return "", err
		}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m postgresRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL terminal run lifecycle transition requires selected store")
	}
	return m.store.markRunTerminalTx(ctx, m.tx, request)
}

func (m sqliteRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite terminal run lifecycle transition requires selected store")
	}
	return m.store.markRunTerminalTx(ctx, m.tx, request)
}

func (m postgresRunLifecycleMutation) ForkSource(
	ctx context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL fork source lifecycle transition requires selected store")
	}
	return m.store.markForkSourceTx(ctx, m.tx, request)
}

func (m sqliteRunLifecycleMutation) ForkSource(
	_ context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
		"%w: backend=sqlite run_id=%s",
		runtimerunlifecycle.ErrForkSourceUnsupported,
		strings.TrimSpace(request.RunID),
	)
}

func (m postgresRunLifecycleMutation) classifyCreateExisting(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := loadPostgresRunLifecycleSnapshot(ctx, m.tx, request.RunID, true)
	if err != nil {
		return "", err
	}
	return classifyCreateExisting(request, current)
}

func (m sqliteRunLifecycleMutation) classifyCreateExisting(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := loadSQLiteRunLifecycleSnapshot(ctx, m.tx, request.RunID)
	if err != nil {
		return "", err
	}
	return classifyCreateExisting(request, current)
}

func classifyCreateExisting(
	request runtimerunlifecycle.CreateRequest,
	current runtimerunlifecycle.Snapshot,
) (runtimerunlifecycle.MutationDisposition, error) {
	wantHash, wantSource := request.Source.StorageValues()
	if strings.TrimSpace(current.BundleHash) != wantHash || strings.TrimSpace(current.BundleSource) != wantSource {
		return "", fmt.Errorf(
			"run lifecycle source conflict for run_id=%s: stored=%s/%s requested=%s/%s",
			request.RunID, current.BundleHash, current.BundleSource, wantHash, wantSource,
		)
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if !current.Origin.Equal(request.Origin) {
		return "", fmt.Errorf(
			"run lifecycle origin conflict for run_id=%s: stored=%s requested=%s",
			request.RunID, current.Origin.Kind(), request.Origin.Kind(),
		)
	}
	return runtimerunlifecycle.MutationExactNoop, nil
}

func (m postgresRunLifecycleMutation) requirePersistedSourceForWrite(
	ctx context.Context,
	fact runtimecorrelation.BundleSourceFact,
) error {
	if !fact.IsPersisted() {
		return nil
	}
	if _, err := m.tx.ExecContext(ctx, `LOCK TABLE runs IN ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("serialize persisted PostgreSQL run source admission: %w", err)
	}
	return m.requirePersistedSource(ctx, fact)
}

func recordRunStarted(ctx context.Context, request runtimerunlifecycle.CreateRequest) error {
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, request.Source.BundleHash())
	if err != nil {
		return fmt.Errorf("record run lifecycle creation: %w", err)
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "started",
		SourceOwner: "runs", SourceIdentity: request.RunID, DedupKey: "run-created:" + request.RunID,
		OccurredAt: request.StartedAt.UTC(), RunID: request.RunID, Scope: scope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: request.RunID,
			TriggerEventType: request.Origin.ActivityTriggerType(),
		},
	})
}

func (m postgresRunLifecycleMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := m.RequireActiveSource(ctx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.Matches(request.Source) {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := m.requirePersistedSourceForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2, bundle_source = $3
		WHERE run_id = $1::uuid
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, request.RunID, bundleHash, bundleSource)
	if err != nil {
		return "", fmt.Errorf("revise PostgreSQL run lifecycle source: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("revise PostgreSQL run lifecycle source affected %d rows", rows), rowsErr)
	}
	if _, err := m.store.requestCompletionCandidateTx(ctx, m.tx, request.RunID, nil); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := m.RequireActiveSource(ctx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.Matches(request.Source) {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = ?, bundle_source = ?
		WHERE run_id = ?
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, bundleHash, bundleSource, request.RunID)
	if err != nil {
		return "", fmt.Errorf("revise SQLite run lifecycle source: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("revise SQLite run lifecycle source affected %d rows", rows), rowsErr)
	}
	if _, err := m.store.requestCompletionCandidateTx(ctx, m.tx, request.RunID, nil); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m postgresRunLifecycleMutation) SyncCounters(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("PostgreSQL run lifecycle counter synchronization requires transaction")
	}
	if _, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (
				SELECT COUNT(*)::integer FROM events WHERE run_id = $1::uuid
			),
			entity_count = (
				SELECT COUNT(DISTINCT entity_id)::integer FROM entity_state WHERE run_id = $1::uuid
			)
		WHERE run_id = $1::uuid
	`, strings.TrimSpace(runID)); err != nil {
		return fmt.Errorf("synchronize PostgreSQL run lifecycle counters: %w", err)
	}
	return nil
}

func (m sqliteRunLifecycleMutation) SyncCounters(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("SQLite run lifecycle counter synchronization requires transaction")
	}
	if _, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (
				SELECT COUNT(*) FROM events WHERE run_id = ?
			),
			entity_count = (
				SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = ?
			)
		WHERE run_id = ?
	`, strings.TrimSpace(runID), strings.TrimSpace(runID), strings.TrimSpace(runID)); err != nil {
		return fmt.Errorf("synchronize SQLite run lifecycle counters: %w", err)
	}
	return nil
}

func markDeletedPersistedBundleRunsTx(
	ctx context.Context,
	tx *sql.Tx,
	bundleHash string,
) (int64, error) {
	if tx == nil || strings.TrimSpace(bundleHash) == "" {
		return 0, errors.New("bundle source lifecycle transition requires transaction and bundle_hash")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_source = $2
		WHERE bundle_hash = $1
		  AND bundle_source = $3
		  AND status IN ('completed', 'failed', 'cancelled', 'forked')
	`, strings.TrimSpace(bundleHash), runtimerunlifecycle.BundleSourceDeleted, runtimerunlifecycle.BundleSourcePersisted)
	if err != nil {
		return 0, fmt.Errorf("mark deleted persisted bundle run sources: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted persisted bundle run sources: %w", err)
	}
	return updated, nil
}

func deleteMaterializedForkRunTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil || strings.TrimSpace(runID) == "" {
		return errors.New("materialized fork lifecycle deletion requires transaction and run_id")
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM runs
		WHERE run_id = $1::uuid
		  AND status = $2
	`, strings.TrimSpace(runID), string(runtimerunlifecycle.StatePaused))
	if err != nil {
		return fmt.Errorf("delete materialized fork run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return errors.Join(fmt.Errorf("delete materialized fork run lifecycle affected %d rows", rows), rowsErr)
	}
	return nil
}

func normalizedRunLifecycleTime(value time.Time) time.Time {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return runtimerunlifecycle.CanonicalTimestamp(value)
}

func runtimepipelineSQLTx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	return tx, ok && tx != nil
}
