package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresCanonicalFailureMigrationNormalizesEveryDurableCarrier(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	createPostgresLegacyFailureTables(t, ctx, db)
	ids := seedPostgresLegacyFailures(t, ctx, db)

	if err := ensurePostgresCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensurePostgresCanonicalFailureSchema: %v", err)
	}

	assertPostgresLegacyColumnsRemoved(t, ctx, db)
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM event_deliveries WHERE delivery_id = $1::uuid`, ids.delivery), runtimefailures.ClassInternalFailure, "legacy_handler_failure")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM event_receipts WHERE receipt_id = $1::uuid`, ids.receipt), runtimefailures.ClassInternalFailure, "legacy_handler_failure")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM dead_letters WHERE dead_letter_id = $1::uuid`, ids.deadLetter), runtimefailures.ClassTargetUnreachable, "target_not_subscribed")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM activity_attempts WHERE request_event_id = $1::uuid`, ids.activity), runtimefailures.ClassConnectorFailure, "legacy_activity_failed")
	assertActivityFailurePayloadMigrated(t, queryFailure(t, db, `SELECT result_payload FROM activity_attempts WHERE request_event_id = $1::uuid`, ids.activity))
	assertActivityFailurePayloadMigrated(t, queryFailure(t, db, `SELECT payload FROM events WHERE event_id = $1::uuid`, ids.activityResult))

	var sideEffects string
	if err := db.QueryRowContext(ctx, `SELECT side_effects::text FROM event_receipts WHERE receipt_id = $1::uuid`, ids.receipt).Scan(&sideEffects); err != nil {
		t.Fatalf("read migrated receipt side effects: %v", err)
	}
	if strings.Contains(sideEffects, `"error"`) || strings.Contains(sideEffects, `"failure_type"`) {
		t.Fatalf("legacy receipt failure fields survived: %s", sideEffects)
	}
}

func TestPostgresCanonicalFailureMigrationRejectsUnknownRowsAtomically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	createPostgresLegacyFailureTables(t, ctx, db)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (delivery_id, event_id, subscriber_type, subscriber_id, status, reason_code, last_error)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-a', 'failed', 'unknown_legacy_failure', 'opaque')
	`, uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("seed unknown legacy delivery: %v", err)
	}

	err := ensurePostgresCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "unknown or ambiguous") {
		t.Fatalf("migration error = %v, want unknown-row blocker", err)
	}
	if postgresColumnExists(t, ctx, db, "event_deliveries", "failure") || !postgresColumnExists(t, ctx, db, "event_deliveries", "last_error") {
		t.Fatal("failed migration did not roll back Postgres schema changes")
	}
}

func TestSQLiteCanonicalFailureMigrationNormalizesEveryDurableCarrier(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "failure-migration.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	createSQLiteLegacyFailureTables(t, ctx, db)
	ids := seedSQLiteLegacyFailures(t, ctx, db)

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}

	assertSQLiteLegacyColumnsRemoved(t, ctx, db)
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM event_deliveries WHERE delivery_id = ?`, ids.delivery), runtimefailures.ClassInternalFailure, "legacy_handler_failure")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM event_receipts WHERE receipt_id = ?`, ids.receipt), runtimefailures.ClassInternalFailure, "legacy_handler_failure")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM dead_letters WHERE dead_letter_id = ?`, ids.deadLetter), runtimefailures.ClassTargetUnreachable, "target_not_subscribed")
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM activity_attempts WHERE request_event_id = ?`, ids.activity), runtimefailures.ClassConnectorFailure, "legacy_activity_failed")
	assertActivityFailurePayloadMigrated(t, queryFailure(t, db, `SELECT result_payload FROM activity_attempts WHERE request_event_id = ?`, ids.activity))
	assertActivityFailurePayloadMigrated(t, queryFailure(t, db, `SELECT payload FROM events WHERE event_id = ?`, ids.activityResult))
}

func TestSQLiteCanonicalFailureMigrationPreservesReplyContextAddedByNewerMaster(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "failure-migration-reply-context.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	createSQLiteLegacyFailureTables(t, ctx, db)
	mustExecTest(t, ctx, db, `
		CREATE TABLE reply_contexts (reply_context_id TEXT PRIMARY KEY);
		INSERT INTO reply_contexts (reply_context_id) VALUES ('reply-1');
		ALTER TABLE activity_attempts ADD COLUMN reply_context_id TEXT
	`)
	ids := seedSQLiteLegacyFailures(t, ctx, db)
	mustExecTest(t, ctx, db, `UPDATE activity_attempts SET reply_context_id = 'reply-1' WHERE request_event_id = ?`, ids.activity)

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}
	var replyContextID string
	if err := db.QueryRowContext(ctx, `SELECT reply_context_id FROM activity_attempts WHERE request_event_id = ?`, ids.activity).Scan(&replyContextID); err != nil {
		t.Fatalf("read migrated reply context: %v", err)
	}
	if replyContextID != "reply-1" {
		t.Fatalf("reply_context_id = %q, want reply-1", replyContextID)
	}
}

func TestSQLiteCanonicalFailureMigrationRejectsUnknownRowsAtomically(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "failure-migration-unknown.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	createSQLiteLegacyFailureTables(t, ctx, db)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (delivery_id, event_id, subscriber_type, subscriber_id, status, reason_code, last_error)
		VALUES (?, ?, 'agent', 'agent-a', 'failed', 'unknown_legacy_failure', 'opaque')
	`, uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("seed unknown legacy delivery: %v", err)
	}

	err = ensureSQLiteCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "unknown or ambiguous") {
		t.Fatalf("migration error = %v, want unknown-row blocker", err)
	}
	columns := sqliteColumnSet(t, ctx, db, "event_deliveries")
	if columns["failure"] || !columns["last_error"] {
		t.Fatal("failed migration did not roll back SQLite schema changes")
	}
}

func TestPostgresRunTerminalEvidenceMigrationSplitsFailureFromCancellationReason(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `
		DROP TABLE IF EXISTS run_control_state CASCADE;
		DROP TABLE IF EXISTS runs CASCADE;
		CREATE TABLE runs (
			run_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			error_summary TEXT,
			ended_at TIMESTAMPTZ
		);
		CREATE TABLE run_control_state (
			run_id UUID PRIMARY KEY REFERENCES runs(run_id),
			control_status TEXT NOT NULL,
			reason TEXT,
			controlled_by TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)
	`)
	failedRunID, cancelledRunID := uuid.NewString(), uuid.NewString()
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "migrated_run_failure", nil)
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES ($1::uuid, 'failed', $2, $3)`, failedRunID, mustMarshalTestFailure(t, failure), time.Now().UTC())
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES ($1::uuid, 'cancelled', 'bundle_legacy_orphaned', $2)`, cancelledRunID, time.Now().UTC())
	mustExecTest(t, ctx, db, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at) VALUES ($1::uuid, 'stopped', 'bundle_legacy_orphaned', 'migration-test', $2)`, cancelledRunID, time.Now().UTC())

	if err := ensurePostgresCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensurePostgresCanonicalFailureSchema: %v", err)
	}
	if postgresColumnExists(t, ctx, db, "runs", "error_summary") {
		t.Fatal("runs.error_summary survived")
	}
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM runs WHERE run_id = $1::uuid`, failedRunID), runtimefailures.ClassInternalFailure, "migrated_run_failure")
	var cancelledFailure []byte
	var reason string
	if err := db.QueryRowContext(ctx, `SELECT r.failure, rc.reason FROM runs r JOIN run_control_state rc ON rc.run_id = r.run_id WHERE r.run_id = $1::uuid`, cancelledRunID).Scan(&cancelledFailure, &reason); err != nil {
		t.Fatalf("load cancelled run evidence: %v", err)
	}
	if len(cancelledFailure) != 0 || reason != "bundle_legacy_orphaned" {
		t.Fatalf("cancelled evidence = failure:%s reason:%q", cancelledFailure, reason)
	}
}

func TestPostgresRunTerminalEvidenceMigrationRejectsAmbiguousFailedProseAtomically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS runs CASCADE; CREATE TABLE runs (run_id UUID PRIMARY KEY, status TEXT NOT NULL, error_summary TEXT, ended_at TIMESTAMPTZ)`)
	runID := uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES ($1::uuid, 'failed', 'opaque prose', now())`, runID)
	err := ensurePostgresCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), runID) || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("migration error = %v, want actionable failed-prose blocker", err)
	}
	if postgresColumnExists(t, ctx, db, "runs", "failure") || !postgresColumnExists(t, ctx, db, "runs", "error_summary") {
		t.Fatal("failed Postgres run migration did not roll back")
	}
}

func TestSQLiteRunTerminalEvidenceMigrationSplitsFailureFromCancellationReason(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "run-terminal-migration.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	mustExecTest(t, ctx, db, `
		CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, error_summary TEXT, ended_at TIMESTAMP);
		CREATE TABLE run_control_state (run_id TEXT PRIMARY KEY, control_status TEXT NOT NULL, reason TEXT, controlled_by TEXT NOT NULL, updated_at TIMESTAMP NOT NULL)
	`)
	failedRunID, cancelledRunID := uuid.NewString(), uuid.NewString()
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "migrated_run_failure", nil)
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES (?, 'failed', ?, ?)`, failedRunID, mustMarshalTestFailure(t, failure), time.Now().UTC())
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES (?, 'cancelled', 'bundle_legacy_orphaned', ?)`, cancelledRunID, time.Now().UTC())
	mustExecTest(t, ctx, db, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at) VALUES (?, 'stopped', 'bundle_legacy_orphaned', 'migration-test', ?)`, cancelledRunID, time.Now().UTC())

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}
	if sqliteColumnSet(t, ctx, db, "runs")["error_summary"] {
		t.Fatal("sqlite runs.error_summary survived")
	}
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM runs WHERE run_id = ?`, failedRunID), runtimefailures.ClassInternalFailure, "migrated_run_failure")
	var cancelledFailure sql.NullString
	var reason string
	if err := db.QueryRowContext(ctx, `SELECT r.failure, rc.reason FROM runs r JOIN run_control_state rc ON rc.run_id = r.run_id WHERE r.run_id = ?`, cancelledRunID).Scan(&cancelledFailure, &reason); err != nil {
		t.Fatalf("load sqlite cancelled run evidence: %v", err)
	}
	if cancelledFailure.Valid || reason != "bundle_legacy_orphaned" {
		t.Fatalf("sqlite cancelled evidence = failure:%#v reason:%q", cancelledFailure, reason)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runs SET status = 'failed', failure = NULL WHERE run_id = ?`, cancelledRunID); err == nil {
		t.Fatal("sqlite terminal evidence trigger accepted failed status without failure")
	}
}

func TestSQLiteRunTerminalEvidenceMigrationRejectsAmbiguousFailedProseAtomically(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "run-terminal-migration-reject.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	mustExecTest(t, ctx, db, `CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, error_summary TEXT, ended_at TIMESTAMP)`)
	runID := uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id, status, error_summary, ended_at) VALUES (?, 'failed', 'opaque prose', ?)`, runID, time.Now().UTC())
	err = ensureSQLiteCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), runID) || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("migration error = %v, want actionable failed-prose blocker", err)
	}
	columns := sqliteColumnSet(t, ctx, db, "runs")
	if columns["failure"] || !columns["error_summary"] {
		t.Fatal("failed SQLite run migration did not roll back")
	}
}

func TestPostgresCanonicalFailureMigrationReplacesEmptyAgentTurnError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS agent_turns CASCADE`)
	mustExecTest(t, ctx, db, `CREATE TABLE agent_turns (turn_id UUID PRIMARY KEY, error TEXT)`)
	mustExecTest(t, ctx, db, `INSERT INTO agent_turns (turn_id, error) VALUES ($1::uuid, '')`, uuid.NewString())

	if err := ensurePostgresCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensurePostgresCanonicalFailureSchema: %v", err)
	}
	if postgresColumnExists(t, ctx, db, "agent_turns", "error") || !postgresColumnExists(t, ctx, db, "agent_turns", "failure") {
		t.Fatal("Postgres agent_turns did not migrate exclusively to failure")
	}
}

func TestPostgresCanonicalFailureMigrationRejectsAmbiguousAgentTurnErrorAtomically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS agent_turns CASCADE`)
	mustExecTest(t, ctx, db, `CREATE TABLE agent_turns (turn_id UUID PRIMARY KEY, error TEXT)`)
	mustExecTest(t, ctx, db, `INSERT INTO agent_turns (turn_id, error) VALUES ($1::uuid, 'opaque provider failure')`, uuid.NewString())

	err := ensurePostgresCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "ambiguous legacy error rows") {
		t.Fatalf("migration error = %v, want ambiguous agent-turn blocker", err)
	}
	if !postgresColumnExists(t, ctx, db, "agent_turns", "error") || postgresColumnExists(t, ctx, db, "agent_turns", "failure") {
		t.Fatal("failed Postgres agent-turn migration did not roll back atomically")
	}
}

func TestSQLiteCanonicalFailureMigrationReplacesEmptyAgentTurnError(t *testing.T) {
	db := openSQLiteFailureMigrationTestDB(t, "agent-turn-empty.db")
	ctx := context.Background()
	mustExecTest(t, ctx, db, `
		CREATE TABLE agent_turns (turn_id TEXT PRIMARY KEY, error TEXT);
		INSERT INTO agent_turns (turn_id, error) VALUES ('turn-1', '')
	`)

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}
	columns := sqliteColumnSet(t, ctx, db, "agent_turns")
	if columns["error"] || !columns["failure"] {
		t.Fatal("SQLite agent_turns did not migrate exclusively to failure")
	}
}

func TestSQLiteCanonicalFailureMigrationRejectsAmbiguousAgentTurnErrorAtomically(t *testing.T) {
	db := openSQLiteFailureMigrationTestDB(t, "agent-turn-ambiguous.db")
	ctx := context.Background()
	mustExecTest(t, ctx, db, `
		CREATE TABLE agent_turns (turn_id TEXT PRIMARY KEY, error TEXT);
		INSERT INTO agent_turns (turn_id, error) VALUES ('turn-1', 'opaque provider failure')
	`)

	err := ensureSQLiteCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "ambiguous legacy error rows") {
		t.Fatalf("migration error = %v, want ambiguous agent-turn blocker", err)
	}
	columns := sqliteColumnSet(t, ctx, db, "agent_turns")
	if !columns["error"] || columns["failure"] {
		t.Fatal("failed SQLite agent-turn migration did not roll back atomically")
	}
}

func TestPostgresCanonicalFailureMigrationNormalizesFailureBearingEvents(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS events CASCADE`)
	mustExecTest(t, ctx, db, `CREATE TABLE events (event_id UUID PRIMARY KEY, event_name TEXT NOT NULL, payload JSONB NOT NULL)`)
	fixtures := seedPostgresLegacyFailureEvents(t, ctx, db)

	if err := ensurePostgresCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensurePostgresCanonicalFailureSchema: %v", err)
	}
	assertMigratedFailureEvents(t, fixtures, func(id string) []byte {
		return queryFailure(t, db, `SELECT payload FROM events WHERE event_id = $1::uuid`, id)
	})
}

func TestSQLiteCanonicalFailureMigrationNormalizesFailureBearingEvents(t *testing.T) {
	db := openSQLiteFailureMigrationTestDB(t, "failure-events.db")
	ctx := context.Background()
	mustExecTest(t, ctx, db, `CREATE TABLE events (event_id TEXT PRIMARY KEY, event_name TEXT NOT NULL, payload TEXT NOT NULL)`)
	fixtures := seedSQLiteLegacyFailureEvents(t, ctx, db)

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}
	assertMigratedFailureEvents(t, fixtures, func(id string) []byte {
		return queryFailure(t, db, `SELECT payload FROM events WHERE event_id = ?`, id)
	})
}

func TestPostgresCanonicalFailureMigrationRejectsUnknownFailureEventAtomically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS events CASCADE`)
	mustExecTest(t, ctx, db, `CREATE TABLE events (event_id UUID PRIMARY KEY, event_name TEXT NOT NULL, payload JSONB NOT NULL)`)
	id := uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES ($1::uuid, 'platform.auth_required', '{"reason":"unknown"}'::jsonb)`, id)

	err := ensurePostgresCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "unknown legacy auth reason") {
		t.Fatalf("migration error = %v, want actionable unknown-event blocker", err)
	}
	raw := queryFailure(t, db, `SELECT payload FROM events WHERE event_id = $1::uuid`, id)
	if strings.Contains(string(raw), `"failure"`) {
		t.Fatalf("failed Postgres event migration did not roll back: %s", raw)
	}
}

func TestSQLiteCanonicalFailureMigrationRejectsUnknownFailureEventAtomically(t *testing.T) {
	db := openSQLiteFailureMigrationTestDB(t, "failure-events-unknown.db")
	ctx := context.Background()
	mustExecTest(t, ctx, db, `CREATE TABLE events (event_id TEXT PRIMARY KEY, event_name TEXT NOT NULL, payload TEXT NOT NULL)`)
	id := uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES (?, 'platform.auth_required', '{"reason":"unknown"}')`, id)

	err := ensureSQLiteCanonicalFailureSchema(ctx, db)
	if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "unknown legacy auth reason") {
		t.Fatalf("migration error = %v, want actionable unknown-event blocker", err)
	}
	raw := queryFailure(t, db, `SELECT payload FROM events WHERE event_id = ?`, id)
	if strings.Contains(string(raw), `"failure"`) {
		t.Fatalf("failed SQLite event migration did not roll back: %s", raw)
	}
}

type legacyFailureEventFixture struct {
	id          string
	eventType   string
	payload     map[string]any
	failurePath []string
	class       runtimefailures.Class
	detail      string
	removed     []string
}

func legacyFailureEventFixtures() []legacyFailureEventFixture {
	return []legacyFailureEventFixture{
		{eventType: "platform.agent_panic", payload: map[string]any{"error": "panic prose"}, failurePath: []string{"failure"}, class: runtimefailures.ClassInternalFailure, detail: "legacy_agent_panic", removed: []string{"error"}},
		{eventType: "platform.agent_failed", payload: map[string]any{"error": "panic prose"}, failurePath: []string{"failure"}, class: runtimefailures.ClassInternalFailure, detail: "legacy_agent_failed", removed: []string{"error"}},
		{eventType: "platform.auth_required", payload: map[string]any{"reason": "claude_auth_required"}, failurePath: []string{"failure"}, class: runtimefailures.ClassAuthenticationNeeded, detail: "legacy_authentication_required", removed: []string{"reason"}},
		{eventType: "platform.recovery_failed", payload: map[string]any{"error": "recovery prose"}, failurePath: []string{"failure"}, class: runtimefailures.ClassDependencyUnavailable, detail: "legacy_startup_recovery_failed", removed: []string{"error"}},
		{eventType: "platform.event_quarantined", payload: map[string]any{"quarantine_reason": "repeated panic prose", "sample_error": "panic prose"}, failurePath: []string{"last_failure"}, class: runtimefailures.ClassInternalFailure, detail: "legacy_agent_panic", removed: []string{"quarantine_reason", "sample_error"}},
		{eventType: "platform.dead_letter", payload: map[string]any{"failure_type": "retry_exhausted", "error_message": "handler prose"}, failurePath: []string{"failure"}, class: runtimefailures.ClassRetryExhausted, detail: "legacy_retry_exhausted", removed: []string{"failure_type", "error_message"}},
		{eventType: "platform.paused", payload: map[string]any{"reason": "claude_auth_required"}, failurePath: []string{"last_failure"}, class: runtimefailures.ClassAuthenticationNeeded, detail: "legacy_authentication_required"},
		{eventType: "platform.paused", payload: map[string]any{"reason": "claude_credit_exhausted"}, failurePath: []string{"last_failure"}, class: runtimefailures.ClassConnectorFailure, detail: "provider_credit_exhausted"},
		{eventType: "platform.boot", payload: map[string]any{"recovery_decision": map[string]any{"reason_code": "startup_recovery_failed", "error_text": "recovery prose"}}, failurePath: []string{"recovery_decision", "failure"}, class: runtimefailures.ClassDependencyUnavailable, detail: "startup_manager_recovery_failed", removed: []string{"recovery_decision.error_text"}},
		{eventType: "platform.runtime_log", payload: map[string]any{"details": map[string]any{"component": "legacy", "action": "failed", "error": "diagnostic prose"}}, removed: []string{"details.error"}},
	}
}

func seedPostgresLegacyFailureEvents(t testing.TB, ctx context.Context, db *sql.DB) []legacyFailureEventFixture {
	t.Helper()
	fixtures := legacyFailureEventFixtures()
	for i := range fixtures {
		fixtures[i].id = uuid.NewString()
		raw, _ := json.Marshal(fixtures[i].payload)
		mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES ($1::uuid, $2, $3::jsonb)`, fixtures[i].id, fixtures[i].eventType, string(raw))
	}
	return fixtures
}

func seedSQLiteLegacyFailureEvents(t testing.TB, ctx context.Context, db *sql.DB) []legacyFailureEventFixture {
	t.Helper()
	fixtures := legacyFailureEventFixtures()
	for i := range fixtures {
		fixtures[i].id = uuid.NewString()
		raw, _ := json.Marshal(fixtures[i].payload)
		mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES (?, ?, ?)`, fixtures[i].id, fixtures[i].eventType, string(raw))
	}
	return fixtures
}

func assertMigratedFailureEvents(t testing.TB, fixtures []legacyFailureEventFixture, load func(string) []byte) {
	t.Helper()
	for _, fixture := range fixtures {
		var payload map[string]any
		if err := json.Unmarshal(load(fixture.id), &payload); err != nil {
			t.Fatalf("decode migrated %s payload: %v", fixture.eventType, err)
		}
		if len(fixture.failurePath) > 0 {
			value := any(payload)
			for _, segment := range fixture.failurePath {
				object, ok := value.(map[string]any)
				if !ok {
					t.Fatalf("%s failure path %v is not an object: %#v", fixture.eventType, fixture.failurePath, value)
				}
				value = object[segment]
			}
			raw, _ := json.Marshal(value)
			failure, err := runtimefailures.UnmarshalEnvelope(raw)
			if err != nil {
				t.Fatalf("decode migrated %s failure: %v", fixture.eventType, err)
			}
			if failure.Class != fixture.class || failure.Detail.Code != fixture.detail {
				t.Fatalf("%s failure = %s/%s, want %s/%s", fixture.eventType, failure.Class, failure.Detail.Code, fixture.class, fixture.detail)
			}
		}
		for _, path := range fixture.removed {
			segments := strings.Split(path, ".")
			object := payload
			for _, segment := range segments[:len(segments)-1] {
				next, _ := object[segment].(map[string]any)
				object = next
			}
			if object != nil {
				if _, exists := object[segments[len(segments)-1]]; exists {
					t.Fatalf("%s legacy field %s survived: %#v", fixture.eventType, path, payload)
				}
			}
		}
	}
}

func openSQLiteFailureMigrationTestDB(t testing.TB, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type legacyFailureIDs struct {
	delivery       string
	receipt        string
	deadLetter     string
	activity       string
	activityResult string
}

func createPostgresLegacyFailureTables(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS event_receipts, event_deliveries, dead_letters, activity_attempts CASCADE`)
	mustExecTest(t, ctx, db, `
		CREATE TABLE event_deliveries (
			delivery_id UUID PRIMARY KEY, event_id UUID NOT NULL, subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL, status TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
			reason_code TEXT, last_error TEXT
		);
		CREATE TABLE event_receipts (
			receipt_id UUID PRIMARY KEY, event_id UUID NOT NULL, subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL, outcome TEXT NOT NULL, reason_code TEXT,
			side_effects JSONB NOT NULL DEFAULT '{}'::jsonb
		);
		CREATE TABLE dead_letters (
			dead_letter_id UUID PRIMARY KEY, failure_type TEXT NOT NULL, target_failure_reason TEXT,
			target_context JSONB, error_message TEXT
		);
		CREATE TABLE activity_attempts (
			request_event_id UUID PRIMARY KEY, status TEXT NOT NULL, result_event_id UUID,
			result_event_type TEXT, result_payload JSONB, error TEXT, completed_at TIMESTAMPTZ
		)
	`)
}

func seedPostgresLegacyFailures(t testing.TB, ctx context.Context, db *sql.DB) legacyFailureIDs {
	t.Helper()
	ids := legacyFailureIDs{delivery: uuid.NewString(), receipt: uuid.NewString(), deadLetter: uuid.NewString(), activity: uuid.NewString(), activityResult: uuid.NewString()}
	eventID := uuid.NewString()
	mustExecTest(t, ctx, db, `
		INSERT INTO events (event_id, event_name, payload, produced_by_type)
		VALUES ($1::uuid, 'activity.failed', '{"activity_id":"activity-a","error":"provider failed"}'::jsonb, 'platform')
	`, ids.activityResult)
	mustExecTest(t, ctx, db, `
		INSERT INTO event_deliveries (delivery_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, last_error)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-a', 'failed', 1, 'handler_error', 'boom')
	`, ids.delivery, eventID)
	mustExecTest(t, ctx, db, `
		INSERT INTO event_receipts (receipt_id, event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-a', 'dead_letter', 'handler_error', '{"error":"boom","failure_type":"handler_error"}'::jsonb)
	`, ids.receipt, eventID)
	mustExecTest(t, ctx, db, `
		INSERT INTO dead_letters (dead_letter_id, failure_type, target_failure_reason, target_context, error_message)
		VALUES ($1::uuid, 'target_resolution_failed', 'target_not_subscribed', '{"target":"agent-a"}'::jsonb, 'missing')
	`, ids.deadLetter)
	mustExecTest(t, ctx, db, `
		INSERT INTO activity_attempts (request_event_id, status, result_event_id, result_event_type, result_payload, error, completed_at)
		VALUES ($1::uuid, 'failed', $2::uuid, 'activity.failed', '{"activity_id":"activity-a","error":"provider failed"}'::jsonb, 'provider failed', now())
	`, ids.activity, ids.activityResult)
	return ids
}

func createSQLiteLegacyFailureTables(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	mustExecTest(t, ctx, db, `
		CREATE TABLE runs (run_id TEXT PRIMARY KEY);
		CREATE TABLE events (event_id TEXT PRIMARY KEY, event_name TEXT, payload TEXT);
		CREATE TABLE event_deliveries (
			delivery_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL, status TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
			reason_code TEXT, last_error TEXT
		);
		CREATE TABLE event_receipts (
			receipt_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL, outcome TEXT NOT NULL, reason_code TEXT, side_effects TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE dead_letters (
			dead_letter_id TEXT PRIMARY KEY, original_event_id TEXT, original_event TEXT NOT NULL,
			original_payload TEXT NOT NULL DEFAULT '{}', entity_id TEXT, flow_instance TEXT NOT NULL,
			failure_type TEXT NOT NULL, target_failure_reason TEXT, target_context TEXT, error_message TEXT,
			retry_count INTEGER NOT NULL DEFAULT 0, chain_depth INTEGER NOT NULL DEFAULT 0,
			handler_node TEXT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE activity_attempts (
			request_event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source_event_id TEXT, parent_event_id TEXT,
			entity_id TEXT, flow_instance TEXT, node_id TEXT NOT NULL, handler_event_key TEXT NOT NULL,
			activity_id TEXT NOT NULL, tool TEXT NOT NULL, effect_class TEXT NOT NULL, attempt INTEGER NOT NULL,
			status TEXT NOT NULL, success_event TEXT NOT NULL, failure_event TEXT NOT NULL, result_event_id TEXT,
			result_event_type TEXT, result_payload TEXT, error TEXT, input_hash TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, updated_at TIMESTAMP NOT NULL
		)
	`)
}

func seedSQLiteLegacyFailures(t testing.TB, ctx context.Context, db *sql.DB) legacyFailureIDs {
	t.Helper()
	ids := legacyFailureIDs{delivery: uuid.NewString(), receipt: uuid.NewString(), deadLetter: uuid.NewString(), activity: uuid.NewString(), activityResult: uuid.NewString()}
	eventID, runID := uuid.NewString(), uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO runs (run_id) VALUES (?)`, runID)
	mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES (?, 'task.requested', '{}')`, eventID)
	mustExecTest(t, ctx, db, `INSERT INTO events (event_id, event_name, payload) VALUES (?, 'activity.failed', '{"activity_id":"activity-a","error":"provider failed"}')`, ids.activityResult)
	mustExecTest(t, ctx, db, `
		INSERT INTO event_deliveries (delivery_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, last_error)
		VALUES (?, ?, 'agent', 'agent-a', 'failed', 1, 'handler_error', 'boom')
	`, ids.delivery, eventID)
	mustExecTest(t, ctx, db, `
		INSERT INTO event_receipts (receipt_id, event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects)
		VALUES (?, ?, 'agent', 'agent-a', 'dead_letter', 'handler_error', '{"error":"boom","failure_type":"handler_error"}')
	`, ids.receipt, eventID)
	mustExecTest(t, ctx, db, `
		INSERT INTO dead_letters (dead_letter_id, original_event_id, original_event, flow_instance, failure_type, target_failure_reason, target_context, error_message)
		VALUES (?, ?, 'task.failed', 'flow-a', 'target_resolution_failed', 'target_not_subscribed', '{"target":"agent-a"}', 'missing')
	`, ids.deadLetter, eventID)
	mustExecTest(t, ctx, db, `
		INSERT INTO activity_attempts (
			request_event_id, run_id, node_id, handler_event_key, activity_id, tool, effect_class, attempt,
			status, success_event, failure_event, result_event_id, result_event_type, result_payload, error,
			input_hash, started_at, completed_at, updated_at
		) VALUES (?, ?, 'node-a', 'task.ready', 'activity-a', 'connector-a', 'non_idempotent_write', 1,
			'failed', 'activity.succeeded', 'activity.failed', ?, 'activity.failed', '{"activity_id":"activity-a","error":"provider failed"}', 'provider failed',
			'input-hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, ids.activity, runID, ids.activityResult)
	return ids
}

func TestPostgresDirectiveOperationFailureMigrationMapsOnlyClosedRecoveryCodes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	createPostgresLegacyDirectiveOperationTable(t, ctx, db)
	leaseID, notAdmittedID := uuid.NewString(), uuid.NewString()
	mustExecTest(t, ctx, db, `INSERT INTO agent_directive_operations (operation_id, state, error_code, error_message) VALUES ($1::uuid, 'indeterminate', 'execution_lease_expired', 'directive execution lease expired before a durable outcome')`, leaseID)
	mustExecTest(t, ctx, db, `INSERT INTO agent_directive_operations (operation_id, state, error_code, error_message) VALUES ($1::uuid, 'failed', 'execution_not_admitted', 'keyless directive operation was abandoned before execution admission')`, notAdmittedID)

	if err := ensurePostgresCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensurePostgresCanonicalFailureSchema: %v", err)
	}
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM agent_directive_operations WHERE operation_id = $1::uuid`, leaseID), runtimefailures.ClassOutcomeUncertain, runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail)
	notAdmitted := queryFailure(t, db, `SELECT failure FROM agent_directive_operations WHERE operation_id = $1::uuid`, notAdmittedID)
	assertPersistedFailure(t, notAdmitted, runtimefailures.ClassInternalFailure, runtimeagentcontrol.DirectiveExecutionNotAdmittedDetail)
	decoded, err := runtimefailures.UnmarshalEnvelope(notAdmitted)
	if err != nil || !decoded.Deterministic {
		t.Fatalf("not-admitted failure = %#v err=%v, want deterministic", decoded, err)
	}
	for _, column := range []string{"error_code", "error_message", "error_details"} {
		if postgresColumnExists(t, ctx, db, "agent_directive_operations", column) {
			t.Fatalf("legacy column %s survived", column)
		}
	}
}

func TestSQLiteDirectiveOperationFailureMigrationMapsOnlyClosedRecoveryCodes(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "directive-failure-migration.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	createSQLiteLegacyDirectiveOperationTable(t, ctx, db)
	leaseID, notAdmittedID := uuid.NewString(), uuid.NewString()
	insertSQLiteLegacyDirectiveOperation(t, ctx, db, leaseID, "indeterminate", "execution_lease_expired", "directive execution lease expired before a durable outcome", "")
	insertSQLiteLegacyDirectiveOperation(t, ctx, db, notAdmittedID, "failed", "execution_not_admitted", "keyless directive operation was abandoned before execution admission", "")

	if err := ensureSQLiteCanonicalFailureSchema(ctx, db); err != nil {
		t.Fatalf("ensureSQLiteCanonicalFailureSchema: %v", err)
	}
	assertPersistedFailure(t, queryFailure(t, db, `SELECT failure FROM agent_directive_operations WHERE operation_id = ?`, leaseID), runtimefailures.ClassOutcomeUncertain, runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail)
	notAdmitted := queryFailure(t, db, `SELECT failure FROM agent_directive_operations WHERE operation_id = ?`, notAdmittedID)
	assertPersistedFailure(t, notAdmitted, runtimefailures.ClassInternalFailure, runtimeagentcontrol.DirectiveExecutionNotAdmittedDetail)
	decoded, err := runtimefailures.UnmarshalEnvelope(notAdmitted)
	if err != nil || !decoded.Deterministic {
		t.Fatalf("not-admitted failure = %#v err=%v, want deterministic", decoded, err)
	}
	columns := sqliteColumnSet(t, ctx, db, "agent_directive_operations")
	for _, column := range []string{"error_code", "error_message", "error_details"} {
		if columns[column] {
			t.Fatalf("legacy column %s survived", column)
		}
	}
}

func TestPostgresDirectiveOperationFailureMigrationRejectsAmbiguousRowsAtomically(t *testing.T) {
	tests := directiveOperationBlockedMigrationRows()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := context.Background()
			createPostgresLegacyDirectiveOperationTable(t, ctx, db)
			id := uuid.NewString()
			mustExecTest(t, ctx, db, `INSERT INTO agent_directive_operations (operation_id, state, error_code, error_message, error_details) VALUES ($1::uuid, $2, $3, $4, $5::jsonb)`, id, test.state, test.code, test.message, nullableMigrationTestValue(test.details))
			err := ensurePostgresCanonicalFailureSchema(ctx, db)
			if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("migration error = %v, want operation id and %q", err, test.want)
			}
			if postgresColumnExists(t, ctx, db, "agent_directive_operations", "failure") || !postgresColumnExists(t, ctx, db, "agent_directive_operations", "error_code") {
				t.Fatal("blocked Postgres migration did not roll back atomically")
			}
		})
	}
}

func TestSQLiteDirectiveOperationFailureMigrationRejectsAmbiguousRowsAtomically(t *testing.T) {
	for _, test := range directiveOperationBlockedMigrationRows() {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "directive-failure-blocked.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			ctx := context.Background()
			createSQLiteLegacyDirectiveOperationTable(t, ctx, db)
			id := uuid.NewString()
			insertSQLiteLegacyDirectiveOperation(t, ctx, db, id, test.state, test.code, test.message, test.details)
			err = ensureSQLiteCanonicalFailureSchema(ctx, db)
			if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("migration error = %v, want operation id and %q", err, test.want)
			}
			columns := sqliteColumnSet(t, ctx, db, "agent_directive_operations")
			if columns["failure"] || !columns["error_code"] {
				t.Fatal("blocked SQLite migration did not roll back atomically")
			}
		})
	}
}

type directiveOperationBlockedMigrationRow struct {
	name, state, code, message, details, want string
}

func directiveOperationBlockedMigrationRows() []directiveOperationBlockedMigrationRow {
	return []directiveOperationBlockedMigrationRow{
		{name: "board step prose", state: "failed", code: "board_step_failed", message: "provider failed", want: "cannot recover"},
		{name: "unknown code", state: "failed", code: "other_failure", message: "opaque", want: "unknown failed"},
		{name: "legacy details", state: "failed", code: "execution_not_admitted", message: "keyless directive operation was abandoned before execution admission", details: `{"cause":"raw"}`, want: "error_details"},
		{name: "forbidden state", state: "prepared", code: "execution_not_admitted", message: "keyless directive operation was abandoned before execution admission", want: "forbids legacy"},
	}
}

func createPostgresLegacyDirectiveOperationTable(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	mustExecTest(t, ctx, db, `DROP TABLE IF EXISTS agent_directive_operations; CREATE TABLE agent_directive_operations (operation_id UUID PRIMARY KEY, state TEXT NOT NULL, response JSONB, error_code TEXT, error_message TEXT, error_details JSONB)`)
}

func createSQLiteLegacyDirectiveOperationTable(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	mustExecTest(t, ctx, db, `
		CREATE TABLE agent_directive_operations (
			operation_id TEXT PRIMARY KEY, method TEXT NOT NULL, actor_token_id TEXT NOT NULL,
			idempotency_key TEXT, request_hash TEXT NOT NULL, agent_id TEXT NOT NULL,
			directive_text TEXT NOT NULL, requested_run_id TEXT, resolved_run_id TEXT NOT NULL,
			run_id_resolution TEXT NOT NULL, source TEXT NOT NULL, operator_id TEXT,
			directive_event_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL, execution_owner_id TEXT,
			execution_lease_expires_at TIMESTAMP, response TEXT, error_code TEXT, error_message TEXT,
			error_details TEXT, execution_admitted_at TIMESTAMP, executed_at TIMESTAMP,
			completed_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
			expires_at TIMESTAMP
		)
	`)
}

func insertSQLiteLegacyDirectiveOperation(t testing.TB, ctx context.Context, db *sql.DB, id, state, code, message, details string) {
	t.Helper()
	mustExecTest(t, ctx, db, `
		INSERT INTO agent_directive_operations (
			operation_id, method, actor_token_id, request_hash, agent_id, directive_text,
			resolved_run_id, run_id_resolution, source, directive_event_id, state,
			error_code, error_message, error_details, created_at, updated_at
		) VALUES (?, 'agent.send_directive', 'actor', 'hash', 'agent', 'do work', ?, 'specified', 'v1_rpc', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, id, uuid.NewString(), uuid.NewString(), state, code, message, nullableMigrationTestValue(details))
}

func nullableMigrationTestValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func mustExecTest(t testing.TB, ctx context.Context, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec test SQL: %v\n%s", err, query)
	}
}

func queryFailure(t testing.TB, db *sql.DB, query string, args ...any) []byte {
	t.Helper()
	var raw []byte
	if err := db.QueryRow(query, args...).Scan(&raw); err != nil {
		t.Fatalf("query failure: %v", err)
	}
	return raw
}

func assertPersistedFailure(t testing.TB, raw []byte, class runtimefailures.Class, detail string) {
	t.Helper()
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode persisted failure: %v", err)
	}
	if failure.Class != class || failure.Detail.Code != detail {
		t.Fatalf("persisted failure = %s/%s, want %s/%s", failure.Class, failure.Detail.Code, class, detail)
	}
}

func assertActivityFailurePayloadMigrated(t testing.TB, raw []byte) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode migrated activity payload: %v", err)
	}
	if _, exists := payload["error"]; exists {
		t.Fatalf("legacy activity payload error survived: %s", raw)
	}
	failureRaw, _ := json.Marshal(payload["failure"])
	failure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
	if err != nil {
		t.Fatalf("decode migrated activity payload failure: %v", err)
	}
	if failure.Class != runtimefailures.ClassConnectorFailure || failure.Detail.Code != "legacy_activity_failed" {
		t.Fatalf("activity payload failure = %s/%s", failure.Class, failure.Detail.Code)
	}
}

func assertPostgresLegacyColumnsRemoved(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	for table, columns := range map[string][]string{
		"event_deliveries":  {"last_error"},
		"dead_letters":      {"failure_type", "target_failure_reason", "target_context", "error_message"},
		"activity_attempts": {"error"},
	} {
		for _, column := range columns {
			if postgresColumnExists(t, ctx, db, table, column) {
				t.Fatalf("legacy Postgres column survived: %s.%s", table, column)
			}
		}
	}
}

func postgresColumnExists(t testing.TB, ctx context.Context, db *sql.DB, table, column string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
		)
	`, table, column).Scan(&exists); err != nil {
		t.Fatalf("inspect Postgres column %s.%s: %v", table, column, err)
	}
	return exists
}

func assertSQLiteLegacyColumnsRemoved(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	for table, columns := range map[string][]string{
		"event_deliveries":  {"last_error"},
		"dead_letters":      {"failure_type", "target_failure_reason", "target_context", "error_message"},
		"activity_attempts": {"error"},
	} {
		got := sqliteColumnSet(t, ctx, db, table)
		for _, column := range columns {
			if got[column] {
				t.Fatalf("legacy SQLite column survived: %s.%s", table, column)
			}
		}
	}
}

func sqliteColumnSet(t testing.TB, ctx context.Context, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		t.Fatalf("inspect SQLite table %s: %v", table, err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan SQLite table %s: %v", table, err)
		}
		out[name] = true
	}
	return out
}
