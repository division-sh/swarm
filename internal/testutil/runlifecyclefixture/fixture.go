package runlifecyclefixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

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

func ForcePostgresCompletionCandidateRevision(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET completion_due_at = NULL WHERE run_id = $1::uuid`, strings.TrimSpace(runID))
	return err
}

func ForceSQLiteCompletionCandidateRevision(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET completion_due_at = NULL WHERE run_id = ?`, strings.TrimSpace(runID))
	return err
}

type CorruptSnapshot struct {
	RunID               string
	State               string
	BundleHash          string
	OriginKind          string
	TriggerEventID      string
	TriggerEventType    string
	OriginServiceID     string
	OriginGeneration    int64
	ForkedFromRunID     string
	ForkedFromPointKind string
	ForkedFromRevision  int64
	ForkedFromEventID   string
	ContinuedAsRunID    string
	EventCount          int
	Failure             *runtimefailures.Envelope
	StartedAt           time.Time
	EndedAt             time.Time
}

// NormalizeCorruptSnapshot preserves hostile lifecycle fields while preparing
// failure and timestamps for the private lifecycle snapshot fault writer.
func NormalizeCorruptSnapshot(snapshot CorruptSnapshot) (CorruptSnapshot, string, any, error) {
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
		    forked_from_point_kind = NULLIF(?, ''),
		    forked_from_revision = NULLIF(?, 0),
		    forked_from_event_id = NULLIF(?, '')
		WHERE run_id = ?
	`
	args := []any{
		origin.Kind(), origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.ForkPointKind(), origin.ForkRevision(), origin.SourceEventID(), strings.TrimSpace(runID),
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
			    forked_from_point_kind = NULLIF($8, ''),
			    forked_from_revision = NULLIF($9, 0),
			    forked_from_event_id = NULLIF($10, '')::uuid
			WHERE run_id = $1::uuid
		`
		args = []any{
			strings.TrimSpace(runID), origin.Kind(), origin.EventID(), origin.EventType(), origin.ServiceID(),
			origin.Generation(), origin.SourceRunID(), origin.ForkPointKind(), origin.ForkRevision(), origin.SourceEventID(),
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
