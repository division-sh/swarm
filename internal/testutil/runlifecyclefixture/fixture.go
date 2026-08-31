package runlifecyclefixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const defaultBundleHash = sourceartifactfixture.BundleHash
const defaultRuntimeInstanceID = "00000000-0000-4000-8000-000000000001"

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type Fixture struct {
	RunID      string
	Origin     runtimerunlifecycle.RunOrigin
	Source     runtimecorrelation.SourceArtifactFact
	Artifact   *sourceartifact.AdmittedSourceArtifact
	BundleHash string
	StartedAt  time.Time
}

func ScenarioSetupOrigin() runtimerunlifecycle.RunOrigin {
	return runtimerunlifecycle.ScenarioSetupRunOrigin()
}

func ScenarioSetupOriginKind() string {
	return string(runtimerunlifecycle.OriginScenarioSetup)
}

func EventOrigin(t testing.TB, eventID, eventType string) runtimerunlifecycle.RunOrigin {
	t.Helper()
	origin, err := runtimerunlifecycle.EventRunOrigin(eventID, eventType)
	if err != nil {
		t.Fatalf("construct semantic event run origin: %v", err)
	}
	return origin
}

func RequirePostgres(t testing.TB, ctx context.Context, db *sql.DB, fixture Fixture) {
	t.Helper()
	require(t, ctx, db, DialectPostgres, fixture)
}

func RequireSQLite(t testing.TB, ctx context.Context, db *sql.DB, fixture Fixture) {
	t.Helper()
	require(t, ctx, db, DialectSQLite, fixture)
}

func RunPostgresMutation(
	ctx context.Context,
	db *sql.DB,
	fn func(context.Context, *sql.Tx, ActiveRunSourceOwner) error,
) error {
	return runMutationWithOwner(ctx, db, DialectPostgres, fn)
}

func RunSQLiteMutation(
	ctx context.Context,
	db *sql.DB,
	fn func(context.Context, *sql.Tx, ActiveRunSourceOwner) error,
) error {
	return runMutationWithOwner(ctx, db, DialectSQLite, fn)
}

type ActiveRunSourceOwner interface {
	RequireActiveRunSource(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)
}

// PostgresCreateRunInMutation gives runtime-package tests a semantic lifecycle
// fixture without duplicating run-table SQL outside the #2151 fixture owner.
func PostgresCreateRunInMutation(
	ctx context.Context,
	tx *sql.Tx,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).Create(ctx, request)
}

func PostgresSyncCountersInMutation(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).SyncCounters(ctx, runID)
}

func PostgresRequireActiveRunInMutation(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).RequireActive(ctx, runID)
}

func PostgresRequireActiveRunSourceInMutation(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
) (runtimecorrelation.SourceArtifactFact, error) {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).RequireActiveSource(ctx, runID)
}

func PostgresRequestCompletionCandidateInMutation(
	ctx context.Context,
	tx *sql.Tx,
	request runtimerunlifecycle.CandidateRequest,
) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := (sqlMutation{tx: tx, dialect: DialectPostgres}).RequirePresent(ctx, request.RunID); err != nil {
		return "", err
	}
	return runtimerunlifecycle.CandidateRequested, nil
}

func ForcePostgresCompletionCandidateRevision(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET completion_due_at = NULL WHERE run_id = $1::uuid`, strings.TrimSpace(runID))
	return err
}

func ForceSQLiteCompletionCandidateRevision(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET completion_due_at = NULL WHERE run_id = ?`, strings.TrimSpace(runID))
	return err
}

func PostgresTransitionActiveRunInMutation(
	ctx context.Context,
	tx *sql.Tx,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).TransitionActive(ctx, request)
}

func PostgresMarkTerminalRunInMutation(
	ctx context.Context,
	tx *sql.Tx,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (sqlMutation{tx: tx, dialect: DialectPostgres}).MarkTerminal(ctx, request)
}

func runMutationWithOwner(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fn func(context.Context, *sql.Tx, ActiveRunSourceOwner) error,
) error {
	return runMutation(ctx, db, dialect, func(txctx context.Context, tx *sql.Tx) error {
		return fn(txctx, tx, sqlMutation{tx: tx, dialect: dialect})
	})
}

func RevisePostgresSource(
	ctx context.Context,
	db *sql.DB,
	runID string,
	source runtimecorrelation.SourceArtifactFact,
) error {
	return reviseSource(ctx, db, DialectPostgres, runID, source)
}

func ReviseSQLiteSource(
	ctx context.Context,
	db *sql.DB,
	runID string,
	source runtimecorrelation.SourceArtifactFact,
) error {
	return reviseSource(ctx, db, DialectSQLite, runID, source)
}

type CorruptSnapshot struct {
	RunID             string
	State             string
	BundleHash        string
	OriginKind        string
	TriggerEventID    string
	TriggerEventType  string
	OriginServiceID   string
	OriginGeneration  int64
	ForkedFromRunID   string
	ForkedFromEventID string
	ContinuedAsRunID  string
	EventCount        int
	EntityCount       int
	Failure           *runtimefailures.Envelope
	StartedAt         time.Time
	EndedAt           time.Time
}

// RequireCorruptPostgresSnapshot is reserved for hostile readback tests whose
// subject is persisted state that valid lifecycle construction forbids.
func RequireCorruptPostgresSnapshot(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	snapshot CorruptSnapshot,
) {
	t.Helper()
	if err := AttemptCorruptPostgresSnapshot(ctx, db, snapshot); err != nil {
		t.Fatalf("materialize corrupt PostgreSQL run snapshot %s: %v", snapshot.RunID, err)
	}
}

func AttemptCorruptPostgresSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshot CorruptSnapshot,
) error {
	snapshot, failure, endedAt, err := normalizeCorruptSnapshot(snapshot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id, continued_as_run_id,
			event_count, entity_count, failure, started_at, ended_at
		)
		VALUES (
			$1::uuid, $2, $3, $4,
			NULLIF($5, '')::uuid, NULLIF($6, ''), NULLIF($7, '')::uuid, NULLIF($8, 0),
			NULLIF($9, '')::uuid, NULLIF($10, '')::uuid, NULLIF($11, '')::uuid,
			$12, $13, NULLIF($14, '')::jsonb, $15, $16
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
		strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
		strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
		strings.TrimSpace(snapshot.OriginServiceID), snapshot.OriginGeneration,
		strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromEventID),
		strings.TrimSpace(snapshot.ContinuedAsRunID),
		snapshot.EventCount, snapshot.EntityCount, failure, snapshot.StartedAt.UTC(), endedAt)
	return err
}

// RequireCorruptSQLiteSnapshot is reserved for hostile readback tests whose
// subject is persisted state that valid lifecycle construction forbids.
func RequireCorruptSQLiteSnapshot(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	snapshot CorruptSnapshot,
) {
	t.Helper()
	if err := AttemptCorruptSQLiteSnapshot(ctx, db, snapshot); err != nil {
		t.Fatalf("materialize corrupt SQLite run snapshot %s: %v", snapshot.RunID, err)
	}
}

func AttemptCorruptSQLiteSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshot CorruptSnapshot,
) error {
	snapshot, failure, endedAt, err := normalizeCorruptSnapshot(snapshot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id, continued_as_run_id,
			event_count, entity_count, failure, started_at, ended_at
		)
		VALUES (
			?, ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''),
			?, ?, NULLIF(?, ''), ?, ?
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
		strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
		strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
		strings.TrimSpace(snapshot.OriginServiceID), snapshot.OriginGeneration,
		strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromEventID),
		strings.TrimSpace(snapshot.ContinuedAsRunID),
		snapshot.EventCount, snapshot.EntityCount, failure, snapshot.StartedAt.UTC(), endedAt)
	return err
}

func normalizeCorruptSnapshot(snapshot CorruptSnapshot) (CorruptSnapshot, string, any, error) {
	if snapshot.StartedAt.IsZero() {
		snapshot.StartedAt = time.Now().UTC()
	}
	if strings.TrimSpace(snapshot.OriginKind) == "" {
		return CorruptSnapshot{}, "", nil, fmt.Errorf(
			"corrupt run snapshot %s requires explicit origin_kind",
			snapshot.RunID,
		)
	}
	if state, err := runtimerunlifecycle.ParseState(snapshot.State); err == nil &&
		state.Terminal() &&
		snapshot.EndedAt.IsZero() {
		snapshot.EndedAt = snapshot.StartedAt
	}
	failure, err := marshalFixtureFailure(snapshot.Failure)
	if err != nil {
		return CorruptSnapshot{}, "", nil, fmt.Errorf(
			"marshal corrupt run failure %s: %w",
			snapshot.RunID, err,
		)
	}
	return snapshot, failure, nullableFixtureTime(snapshot.EndedAt), nil
}

func CorruptPostgresOrigin(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) {
	t.Helper()
	if err := corruptOrigin(ctx, db, DialectPostgres, runID, origin); err != nil {
		t.Fatalf("corrupt PostgreSQL run origin %s: %v", runID, err)
	}
}

func CorruptSQLiteOrigin(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) {
	t.Helper()
	if err := corruptOrigin(ctx, db, DialectSQLite, runID, origin); err != nil {
		t.Fatalf("corrupt SQLite run origin %s: %v", runID, err)
	}
}

func corruptOrigin(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) error {
	if err := origin.Validate(); err != nil {
		return err
	}
	query := `
		UPDATE runs
		SET origin_kind = ?,
		    trigger_event_id = NULLIF(?, ''),
		    trigger_event_type = NULLIF(?, ''),
		    origin_service_id = NULLIF(?, ''),
		    origin_generation = NULLIF(?, 0),
		    forked_from_run_id = NULLIF(?, ''),
		    forked_from_event_id = NULLIF(?, '')
		WHERE run_id = ?
	`
	args := []any{
		origin.Kind(), origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), strings.TrimSpace(runID),
	}
	if dialect == DialectPostgres {
		query = `
			UPDATE runs
			SET origin_kind = $2,
			    trigger_event_id = NULLIF($3, '')::uuid,
			    trigger_event_type = NULLIF($4, ''),
			    origin_service_id = NULLIF($5, '')::uuid,
			    origin_generation = NULLIF($6, 0),
			    forked_from_run_id = NULLIF($7, '')::uuid,
			    forked_from_event_id = NULLIF($8, '')::uuid
			WHERE run_id = $1::uuid
		`
		args = []any{
			strings.TrimSpace(runID), origin.Kind(), origin.EventID(), origin.EventType(), origin.ServiceID(),
			origin.Generation(), origin.SourceRunID(), origin.SourceEventID(),
		}
	}
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

func CorruptPostgresState(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	state string,
	endedAt time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET status = $2, ended_at = $3
		WHERE run_id = $1::uuid
	`, strings.TrimSpace(runID), strings.TrimSpace(state), nullableFixtureTime(endedAt)); err != nil {
		t.Fatalf("corrupt PostgreSQL run state %s: %v", runID, err)
	}
}

func CorruptSQLiteState(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	state string,
	endedAt time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET status = ?, ended_at = ?
		WHERE run_id = ?
	`, strings.TrimSpace(state), nullableFixtureTime(endedAt), strings.TrimSpace(runID)); err != nil {
		t.Fatalf("corrupt SQLite run state %s: %v", runID, err)
	}
}

func nullableFixtureTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func marshalFixtureFailure(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// CorruptPostgresSource is reserved for hostile readback tests that replace a
// valid run source with a storage value no constructor can produce.
func CorruptPostgresSource(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	bundleHash string,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2
		WHERE run_id = $1::uuid
	`, strings.TrimSpace(runID), strings.TrimSpace(bundleHash)); err != nil {
		t.Fatalf("corrupt PostgreSQL run source %s: %v", runID, err)
	}
}

func CorruptSQLiteSource(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	bundleHash string,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = ?
		WHERE run_id = ?
	`, strings.TrimSpace(bundleHash), strings.TrimSpace(runID)); err != nil {
		t.Fatalf("corrupt SQLite run source %s: %v", runID, err)
	}
}

func require(t testing.TB, ctx context.Context, db *sql.DB, dialect Dialect, fixture Fixture) {
	t.Helper()
	if err := Materialize(ctx, db, dialect, fixture); err != nil {
		t.Fatalf("materialize semantic run fixture %s: %v", strings.TrimSpace(fixture.RunID), err)
	}
}

func runMutation(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fn func(context.Context, *sql.Tx) error,
) error {
	if db == nil {
		return errors.New("semantic run fixture requires database")
	}
	if fn == nil {
		return errors.New("semantic run fixture mutation requires callback")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func reviseSource(
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	runID string,
	source runtimecorrelation.SourceArtifactFact,
) error {
	return runMutation(ctx, db, dialect, func(txctx context.Context, tx *sql.Tx) error {
		_, err := (sqlMutation{tx: tx, dialect: dialect}).ReviseSource(txctx, runtimerunlifecycle.SourceRevisionRequest{
			RunID:  strings.TrimSpace(runID),
			Source: source,
		})
		return err
	})
}

func Materialize(ctx context.Context, db *sql.DB, dialect Dialect, fixture Fixture) error {
	if db == nil {
		return errors.New("semantic run fixture requires database")
	}
	fixture.RunID = strings.TrimSpace(fixture.RunID)
	if fixture.RunID == "" {
		return errors.New("semantic run fixture requires run_id")
	}
	source := fixture.Source
	artifact := fixture.Artifact
	if artifact != nil {
		artifactHash := artifact.BundleHash()
		if bundleHash := strings.TrimSpace(fixture.BundleHash); bundleHash != "" && bundleHash != artifactHash {
			return fmt.Errorf("semantic run fixture bundle_hash %s contradicts artifact %s", bundleHash, artifactHash)
		}
		if err := source.Validate(); err == nil && source.BundleHash() != artifactHash {
			return fmt.Errorf("semantic run fixture source %s contradicts artifact %s", source.BundleHash(), artifactHash)
		}
		var sourceErr error
		source, sourceErr = runtimecorrelation.NewSourceArtifactFact(artifactHash)
		if sourceErr != nil {
			return sourceErr
		}
	}
	if err := source.Validate(); err != nil {
		bundleHash := strings.TrimSpace(fixture.BundleHash)
		if bundleHash == "" {
			bundleHash = defaultBundleHash
		}
		var sourceErr error
		source, sourceErr = runtimecorrelation.NewSourceArtifactFact(bundleHash)
		if sourceErr != nil {
			return sourceErr
		}
	}
	if fixture.StartedAt.IsZero() {
		fixture.StartedAt = time.Now().UTC()
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
	if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); !ok {
		ctx = runtimeauthoractivity.WithScope(
			ctx,
			runtimeauthoractivity.BundleScope(defaultRuntimeInstanceID, source.BundleHash()),
		)
	} else if scope.Kind == runtimeauthoractivity.ScopeRuntime {
		ctx = runtimeauthoractivity.WithScope(
			ctx,
			runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, source.BundleHash()),
		)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if artifact == nil && source.BundleHash() == defaultBundleHash {
		artifact = sourceartifactfixture.Artifact()
	}
	if err := ensureFixtureSourceArtifact(ctx, tx, dialect, source.BundleHash(), artifact); err != nil {
		return err
	}
	if _, err := (sqlMutation{tx: tx, dialect: dialect}).Create(ctx, runtimerunlifecycle.CreateRequest{
		RunID: fixture.RunID, Origin: fixture.Origin,
		Source: source, StartedAt: fixture.StartedAt.UTC(),
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func ensureFixtureSourceArtifact(ctx context.Context, tx *sql.Tx, dialect Dialect, bundleHash string, artifact *sourceartifact.AdmittedSourceArtifact) error {
	query := `SELECT source_blob, member_count, total_bytes FROM source_artifacts WHERE bundle_hash = ?`
	if dialect == DialectPostgres {
		query = `SELECT source_blob, member_count, total_bytes FROM source_artifacts WHERE bundle_hash = $1`
	}
	var persisted sourceartifact.Persisted
	persisted.BundleHash = bundleHash
	err := tx.QueryRowContext(ctx, query, bundleHash).Scan(
		&persisted.SourceBlob,
		&persisted.MemberCount,
		&persisted.TotalBytes,
	)
	if err == nil {
		persisted.CreatedAt = time.Now().UTC()
		if err := persisted.Validate(); err != nil {
			return fmt.Errorf("validate semantic fixture source artifact %s: %w", bundleHash, err)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read semantic fixture source artifact %s: %w", bundleHash, err)
	}
	if artifact == nil {
		return fmt.Errorf("semantic run fixture requires exact admitted source artifact %s", bundleHash)
	}
	persisted, err = sourceartifact.PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("project semantic fixture source artifact %s: %w", bundleHash, err)
	}
	if persisted.BundleHash != bundleHash {
		return fmt.Errorf("semantic run fixture source artifact %s contradicts requested %s", persisted.BundleHash, bundleHash)
	}
	insert := `
		INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes, created_at)
		VALUES (?, ?, ?, ?, ?)
	`
	if dialect == DialectPostgres {
		insert = `
			INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes, created_at)
			VALUES ($1, $2, $3, $4, $5)
		`
	}
	if _, err := tx.ExecContext(ctx, insert, persisted.BundleHash, persisted.SourceBlob, persisted.MemberCount, persisted.TotalBytes, persisted.CreatedAt); err != nil {
		return fmt.Errorf("persist semantic fixture source artifact %s: %w", bundleHash, err)
	}
	return nil
}

type sqlMutation struct {
	tx      *sql.Tx
	dialect Dialect
}

func (m sqlMutation) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.RequireActiveSource(ctx, runID)
}

func (m sqlMutation) RequirePresent(ctx context.Context, runID string) error {
	_, _, _, err := m.load(ctx, runID, false)
	return err
}

func (m sqlMutation) RequireActive(ctx context.Context, runID string) error {
	_, _, _, err := m.load(ctx, runID, true)
	return err
}

func (m sqlMutation) RequirePresentSource(
	ctx context.Context,
	runID string,
) (runtimecorrelation.SourceArtifactFact, error) {
	_, source, _, err := m.load(ctx, runID, false)
	return source, err
}

func (m sqlMutation) RequireActiveSource(
	ctx context.Context,
	runID string,
) (runtimecorrelation.SourceArtifactFact, error) {
	_, source, _, err := m.load(ctx, runID, true)
	return source, err
}

func (m sqlMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash := request.Source.BundleHash()
	origin := request.Origin
	var (
		result sql.Result
		err    error
	)
	switch m.dialect {
	case DialectPostgres:
		result, err = m.tx.ExecContext(ctx, `
			INSERT INTO runs (
				run_id, status, bundle_hash, origin_kind,
				trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
				forked_from_run_id, forked_from_event_id, started_at
			)
			VALUES (
				$1::uuid, 'running', $2, $3,
				NULLIF($4, '')::uuid, NULLIF($5, ''), NULLIF($6, '')::uuid, NULLIF($7, 0),
				NULLIF($8, '')::uuid, NULLIF($9, '')::uuid, $10
			)
			ON CONFLICT (run_id) DO NOTHING
		`, request.RunID, bundleHash, origin.Kind(),
			origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
			origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC())
	case DialectSQLite:
		result, err = m.tx.ExecContext(ctx, `
			INSERT INTO runs (
				run_id, status, bundle_hash, origin_kind,
				trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
				forked_from_run_id, forked_from_event_id, started_at
			)
			VALUES (
				?, 'running', ?, ?,
				NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0),
				NULLIF(?, ''), NULLIF(?, ''), ?
			)
			ON CONFLICT (run_id) DO NOTHING
		`, request.RunID, bundleHash, origin.Kind(),
			origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
			origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC())
	default:
		return "", fmt.Errorf("semantic run fixture has unsupported dialect %q", m.dialect)
	}
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows == 1 {
		return runtimerunlifecycle.MutationApplied, nil
	}
	state, source, currentOrigin, err := m.load(ctx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if !state.Active() || source != request.Source || !currentOrigin.Equal(request.Origin) {
		return "", errors.New("semantic run fixture conflicts with existing run")
	}
	return runtimerunlifecycle.MutationExactNoop, nil
}

func (m sqlMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	query := `UPDATE runs SET status = ? WHERE run_id = ?`
	args := []any{request.State, request.RunID}
	if m.dialect == DialectPostgres {
		query = `UPDATE runs SET status = $2 WHERE run_id = $1::uuid`
		args = []any{request.RunID, request.State}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", &runtimerunlifecycle.RunNotFoundError{RunID: request.RunID}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqlMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	failure, err := marshalFixtureFailure(request.Failure)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	query := `UPDATE runs SET status = ?, failure = NULLIF(?, ''), ended_at = ? WHERE run_id = ?`
	args := []any{request.State, failure, request.EndedAt.UTC(), request.RunID}
	if m.dialect == DialectPostgres {
		query = `UPDATE runs SET status = $2, failure = NULLIF($3, '')::jsonb, ended_at = $4 WHERE run_id = $1::uuid`
		args = []any{request.RunID, request.State, failure, request.EndedAt.UTC()}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if rows != 1 {
		return runtimerunlifecycle.Snapshot{}, "", &runtimerunlifecycle.RunNotFoundError{RunID: request.RunID}
	}
	endedAt := request.EndedAt.UTC()
	return runtimerunlifecycle.Snapshot{
		RunID:   request.RunID,
		State:   request.State,
		Failure: request.Failure,
		EndedAt: &endedAt,
	}, runtimerunlifecycle.MutationApplied, nil
}

func (sqlMutation) ForkSource(
	context.Context,
	runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", errors.New("semantic run fixture adapter forbids fork transitions")
}

func (m sqlMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	_, current, _, err := m.load(ctx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if current == request.Source {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	bundleHash := request.Source.BundleHash()
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	query := `
		UPDATE runs
		SET bundle_hash = ?
		WHERE run_id = ?
	`
	args := []any{bundleHash, request.RunID}
	if m.dialect == DialectPostgres {
		query = `
			UPDATE runs
			SET bundle_hash = $2
			WHERE run_id = $1::uuid
		`
		args = []any{request.RunID, bundleHash}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", &runtimerunlifecycle.RunNotFoundError{RunID: request.RunID}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqlMutation) SyncCounters(ctx context.Context, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("semantic run fixture counter synchronization requires run_id")
	}
	query := `
		UPDATE runs
		SET event_count = (SELECT COUNT(*) FROM events WHERE run_id = ?),
		    entity_count = (SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = ?)
		WHERE run_id = ?
	`
	args := []any{runID, runID, runID}
	if m.dialect == DialectPostgres {
		query = `
			UPDATE runs
			SET event_count = (
			        SELECT COUNT(*)::integer FROM events WHERE run_id = $1::uuid
			    ),
			    entity_count = (
			        SELECT COUNT(DISTINCT entity_id)::integer
			        FROM entity_state
			        WHERE run_id = $1::uuid
			    )
			WHERE run_id = $1::uuid
		`
		args = []any{runID}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	return nil
}

func (m sqlMutation) load(
	ctx context.Context,
	runID string,
	requireActive bool,
) (
	runtimerunlifecycle.State,
	runtimecorrelation.SourceArtifactFact,
	runtimerunlifecycle.RunOrigin,
	error,
) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, errors.New("semantic run fixture requires run_id")
	}
	query := `
		SELECT status, bundle_hash, origin_kind,
		       COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(origin_service_id, ''), COALESCE(origin_generation, 0),
		       COALESCE(forked_from_run_id, ''), COALESCE(forked_from_event_id, '')
		FROM runs
		WHERE run_id = ?
	`
	if m.dialect == DialectPostgres {
		query = `
			SELECT status, bundle_hash, origin_kind,
			       COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, ''),
			       COALESCE(origin_service_id::text, ''), COALESCE(origin_generation, 0),
			       COALESCE(forked_from_run_id::text, ''), COALESCE(forked_from_event_id::text, '')
			FROM runs
			WHERE run_id = $1::uuid
			FOR UPDATE
		`
	}
	var statusRaw, bundleHash, originKind string
	var eventID, eventType, serviceID, sourceRunID, sourceEventID string
	var generation int64
	if err := m.tx.QueryRowContext(ctx, query, runID).Scan(
		&statusRaw,
		&bundleHash,
		&originKind,
		&eventID,
		&eventType,
		&serviceID,
		&generation,
		&sourceRunID,
		&sourceEventID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, err
	}
	state, err := runtimerunlifecycle.ParseState(statusRaw)
	if err != nil {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, err
	}
	if requireActive && !state.Active() {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, &runtimerunlifecycle.RunNotActiveError{
			RunID: runID,
			State: state,
		}
	}
	source, err := sourceFact(bundleHash)
	if err != nil {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, err
	}
	if err := m.requirePersistedSource(ctx, source); err != nil {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, err
	}
	origin, err := runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRunID, sourceEventID,
	)
	if err != nil {
		return "", runtimecorrelation.SourceArtifactFact{}, runtimerunlifecycle.RunOrigin{}, err
	}
	return state, source, origin, nil
}

func (m sqlMutation) requirePersistedSource(
	ctx context.Context,
	source runtimecorrelation.SourceArtifactFact,
) error {
	query := `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)`
	if m.dialect == DialectPostgres {
		query = `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`
	}
	var exists bool
	if err := m.tx.QueryRowContext(ctx, query, source.BundleHash()).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return &runtimerunlifecycle.SourceArtifactUnavailableError{
			BundleHash: source.BundleHash(),
			Cause:      "missing_source_artifact",
		}
	}
	return nil
}

func sourceFact(bundleHash string) (runtimecorrelation.SourceArtifactFact, error) {
	return runtimecorrelation.NewSourceArtifactFact(strings.TrimSpace(bundleHash))
}
