package testsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

const (
	replayScopeFailureFunction   = "test_fail_replay_scope_after_delivery"
	replayScopeFailureTrigger    = "test_replay_scope_after_delivery"
	apiCompletionFailureFunction = "test_fail_api_completion_after_publication"
	apiCompletionFailureTrigger  = "test_api_completion_after_publication"
)

type EventCorruptionClaim struct {
	Invariant string
	Reason    string
}

func RequireEventCorruptionClaim(t testing.TB, claim EventCorruptionClaim) {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("unsafe event corruption requires an invariant and reason")
	}
}

func RequireEventCorruptionResult(t testing.TB, claim EventCorruptionClaim, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("corrupt event store for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
}

// Inspect the real native insertion error; this assertion executes no SQL.
func RequireEventStoreCorruptionCategory(
	t testing.TB,
	claim EventCorruptionClaim,
	category string,
	err error,
) {
	t.Helper()
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
			   NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s'
			   ) THEN
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
				NOT EXISTS (
					SELECT 1 FROM flow_instances fi
					JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
					JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
					WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s'
				)
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
			   NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND fi.flow_template = '%s'
			   ) THEN
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
				NOT EXISTS (
					SELECT 1 FROM flow_instances fi
					JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
					JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
					WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND fi.flow_template = '%s'
				)
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
