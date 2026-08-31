package testsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

const (
	flowMaterializationFailureFunction = "test_fail_event_delivery_after_flow_materialization"
	flowMaterializationFailureTrigger  = "test_event_delivery_after_flow_materialization"
	replayScopeFailureFunction         = "test_fail_replay_scope_after_delivery"
	replayScopeFailureTrigger          = "test_replay_scope_after_delivery"
	apiCompletionFailureFunction       = "test_fail_api_completion_after_publication"
	apiCompletionFailureTrigger        = "test_api_completion_after_publication"
)

type EventCorruptionClaim struct {
	Invariant string
	Reason    string
}

// CorruptEventStore is the sole test-only escape hatch for proving behavior
// against durable states that semantic constructors and named operations forbid.
func CorruptEventStore(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	if _, err := db.ExecContext(ctx, statement, args...); err != nil {
		t.Fatalf("corrupt event store for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
}

func RejectEventStoreCorruption(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	if _, err := db.ExecContext(ctx, statement, args...); err == nil {
		t.Fatalf("event store accepted corruption for %s (%s)", claim.Invariant, claim.Reason)
	}
}

func RejectEventStoreCorruptionCategory(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	category string,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	_, err := db.ExecContext(ctx, statement, args...)
	if err == nil {
		t.Fatalf("event store accepted corruption for %s (%s)", claim.Invariant, claim.Reason)
	}
	message := strings.ToLower(err.Error())
	switch category {
	case "not_null":
		if !strings.Contains(message, "not null") && !strings.Contains(message, "not-null") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	case "check":
		if !strings.Contains(message, "check constraint") && !strings.Contains(message, "constraint failed") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	default:
		t.Fatalf("unknown event corruption error category %q", category)
	}
}

func RequireEventRowCount(t testing.TB, ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, eventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_id = ?`
	if dialect == authoractivityfixture.DialectPostgres {
		query = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`
	}
	var got int
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&got); err != nil {
		t.Fatalf("count event fixture %s: %v", eventID, err)
	}
	if got != want {
		t.Fatalf("event fixture rows for %s = %d, want %d", eventID, got, want)
	}
}

// InstallPostgresEventDeliveryFailureAfterFlowMaterialization proves that a
// named publish operation rolls back event, lifecycle, route, and delivery
// writes when its final delivery boundary fails. The trigger refuses to inject
// the requested failure unless all required earlier lifecycle facts are visible
// in the same transaction.
func InstallPostgresEventDeliveryFailureAfterFlowMaterialization(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	claim EventCorruptionClaim,
	flowTemplate string,
) {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("event delivery failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		t.Fatal("event delivery failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		DECLARE lifecycle_run UUID;
		DECLARE lifecycle_instance TEXT;
		BEGIN
			SELECT run_id, instance_path INTO lifecycle_run, lifecycle_instance
			FROM flow_instances
			WHERE run_id = NEW.run_id AND flow_template = '%s'
			ORDER BY created_at DESC, run_id DESC, instance_path DESC
			LIMIT 1;
			IF lifecycle_instance IS NULL THEN
				RAISE EXCEPTION 'event delivery failure injection reached before flow instance materialization';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM entity_state WHERE run_id = lifecycle_run AND flow_instance = lifecycle_instance) THEN
				RAISE EXCEPTION 'event delivery failure injection reached before entity materialization';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM routing_rules WHERE run_id = lifecycle_run AND flow_instance = lifecycle_instance) THEN
				RAISE EXCEPTION 'event delivery failure injection reached before route materialization';
			END IF;
			RAISE EXCEPTION 'injected delivery route persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, flowMaterializationFailureFunction, quotedTemplate)
	if _, err := db.ExecContext(ctx, functionSQL); err != nil {
		t.Fatalf("install event delivery failure function for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON event_deliveries
		FOR EACH ROW EXECUTE FUNCTION %s()
	`, flowMaterializationFailureTrigger, flowMaterializationFailureFunction)
	if _, err := db.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install event delivery failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+flowMaterializationFailureTrigger+" ON event_deliveries")
		_, _ = db.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+flowMaterializationFailureFunction+"()")
	})
}

// InstallSQLiteEventDeliveryFailureAfterFlowMaterialization is the SQLite
// counterpart to InstallPostgresEventDeliveryFailureAfterFlowMaterialization.
func InstallSQLiteEventDeliveryFailureAfterFlowMaterialization(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	claim EventCorruptionClaim,
	flowTemplate string,
) {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("event delivery failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		t.Fatal("event delivery failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON event_deliveries
		BEGIN
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM flow_instances WHERE run_id = NEW.run_id AND flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before flow instance materialization') END;
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = NEW.run_id AND fi.flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before entity materialization') END;
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM routing_rules rr JOIN flow_instances fi ON fi.run_id = rr.run_id AND fi.instance_path = rr.flow_instance WHERE fi.run_id = NEW.run_id AND fi.flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before route materialization') END;
			SELECT RAISE(ABORT, 'injected delivery route persistence failure');
		END
	`, flowMaterializationFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := db.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install SQLite event delivery failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+flowMaterializationFailureTrigger)
	})
}

func InstallPostgresReplayScopeFailureAfterDelivery(t testing.TB, ctx context.Context, db *sql.DB, claim EventCorruptionClaim, flowTemplate string) {
	t.Helper()
	quotedTemplate := requireEventFailureClaim(t, claim, flowTemplate)
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.event_id) THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before event persistence';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.event_id) THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before delivery persistence';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM routing_rules rr JOIN flow_instances fi ON fi.run_id = rr.run_id AND fi.instance_path = rr.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s') THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before lifecycle persistence';
			END IF;
			RAISE EXCEPTION 'injected committed replay-scope persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, replayScopeFailureFunction, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := db.ExecContext(ctx, functionSQL); err != nil {
		t.Fatalf("install replay-scope failure function for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes FOR EACH ROW EXECUTE FUNCTION %s()`, replayScopeFailureTrigger, replayScopeFailureFunction)); err != nil {
		t.Fatalf("install replay-scope failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+replayScopeFailureTrigger+" ON committed_replay_scopes")
		_, _ = db.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+replayScopeFailureFunction+"()")
	})
}

func InstallSQLiteReplayScopeFailureAfterDelivery(t testing.TB, ctx context.Context, db *sql.DB, claim EventCorruptionClaim, flowTemplate string) {
	t.Helper()
	quotedTemplate := requireEventFailureClaim(t, claim, flowTemplate)
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes BEGIN
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.event_id)
				THEN RAISE(ABORT, 'replay-scope failure injection reached before event persistence') END;
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.event_id)
				THEN RAISE(ABORT, 'replay-scope failure injection reached before delivery persistence') END;
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM routing_rules rr JOIN flow_instances fi ON fi.run_id = rr.run_id AND fi.instance_path = rr.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s')
				THEN RAISE(ABORT, 'replay-scope failure injection reached before lifecycle persistence') END;
			SELECT RAISE(ABORT, 'injected committed replay-scope persistence failure');
		END
	`, replayScopeFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := db.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install SQLite replay-scope failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+replayScopeFailureTrigger)
	})
}

func InstallPostgresAPICompletionFailureAfterPublication(t testing.TB, ctx context.Context, db *sql.DB, claim EventCorruptionClaim, flowTemplate string) {
	t.Helper()
	quotedTemplate := requireEventFailureClaim(t, claim, flowTemplate)
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		DECLARE publication_event UUID;
		BEGIN
			publication_event := NEW.resource_id::uuid;
			IF NOT EXISTS (SELECT 1 FROM events WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND fi.flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM routing_rules rr JOIN flow_instances fi ON fi.run_id = rr.run_id AND fi.instance_path = rr.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND fi.flow_template = '%s') THEN
				RAISE EXCEPTION 'API completion failure injection reached before complete publication persistence';
			END IF;
			RAISE EXCEPTION 'injected API idempotency completion persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, apiCompletionFailureFunction, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := db.ExecContext(ctx, functionSQL); err != nil {
		t.Fatalf("install API completion failure function for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON api_idempotency FOR EACH ROW WHEN (NEW.method = 'event.publish') EXECUTE FUNCTION %s()`, apiCompletionFailureTrigger, apiCompletionFailureFunction)); err != nil {
		t.Fatalf("install API completion failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+apiCompletionFailureTrigger+" ON api_idempotency")
		_, _ = db.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+apiCompletionFailureFunction+"()")
	})
}

func InstallSQLiteAPICompletionFailureAfterPublication(t testing.TB, ctx context.Context, db *sql.DB, claim EventCorruptionClaim, flowTemplate string) {
	t.Helper()
	quotedTemplate := requireEventFailureClaim(t, claim, flowTemplate)
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON api_idempotency WHEN NEW.method = 'event.publish' BEGIN
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND fi.flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM routing_rules rr JOIN flow_instances fi ON fi.run_id = rr.run_id AND fi.instance_path = rr.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND fi.flow_template = '%s')
				THEN RAISE(ABORT, 'API completion failure injection reached before complete publication persistence') END;
			SELECT RAISE(ABORT, 'injected API idempotency completion persistence failure');
		END
	`, apiCompletionFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := db.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install SQLite API completion failure trigger for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+apiCompletionFailureTrigger)
	})
}

func requireEventFailureClaim(t testing.TB, claim EventCorruptionClaim, flowTemplate string) string {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("event failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		t.Fatal("event failure injection requires a flow template")
	}
	return strings.ReplaceAll(flowTemplate, "'", "''")
}

func eventCorruptionStatement(t testing.TB, dialect authoractivityfixture.Dialect, claim EventCorruptionClaim, sqliteStatement, postgresStatement string) string {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("unsafe event corruption requires an invariant and reason")
	}
	statement := sqliteStatement
	if dialect == authoractivityfixture.DialectPostgres {
		statement = postgresStatement
	}
	if strings.TrimSpace(statement) == "" {
		t.Fatal("unsafe event corruption statement is required")
	}
	return statement
}
