package testsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

const (
	flowMaterializationFailureFunction = "test_fail_event_delivery_after_flow_materialization"
	flowMaterializationFailureTrigger  = "test_event_delivery_after_flow_materialization"
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
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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

func RequireEventRowCount(t testing.TB, ctx context.Context, db *sql.DB, dialect runtimeauthoractivity.Dialect, eventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_id = ?`
	if dialect == runtimeauthoractivity.DialectPostgres {
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
		DECLARE lifecycle_instance TEXT;
		BEGIN
			SELECT instance_id INTO lifecycle_instance
			FROM flow_instances
			WHERE flow_template = '%s'
			ORDER BY created_at DESC, instance_id DESC
			LIMIT 1;
			IF lifecycle_instance IS NULL THEN
				RAISE EXCEPTION 'event delivery failure injection reached before flow instance materialization';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM entity_state WHERE flow_instance = lifecycle_instance) THEN
				RAISE EXCEPTION 'event delivery failure injection reached before entity materialization';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM routing_rules WHERE flow_instance = lifecycle_instance) THEN
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

func eventCorruptionStatement(t testing.TB, dialect runtimeauthoractivity.Dialect, claim EventCorruptionClaim, sqliteStatement, postgresStatement string) string {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("unsafe event corruption requires an invariant and reason")
	}
	statement := sqliteStatement
	if dialect == runtimeauthoractivity.DialectPostgres {
		statement = postgresStatement
	}
	if strings.TrimSpace(statement) == "" {
		t.Fatal("unsafe event corruption statement is required")
	}
	return statement
}
