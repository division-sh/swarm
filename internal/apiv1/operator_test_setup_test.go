package apiv1

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	handler := testSetupHandler(t, pg, source)
	runID := uuid.NewString()
	entityID := uuid.NewString()

	resp := rpcCall(t, handler, testSetupBody(runID, entityID, "waiting", "seeded", true, "idem-setup"))
	if resp.Error != nil {
		t.Fatalf("test.setup_entities error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["run_id"] != runID {
		t.Fatalf("test.setup_entities run_id = %#v, want %s", result["run_id"], runID)
	}
	entities := result["entities"].([]any)
	if len(entities) != 1 {
		t.Fatalf("result entities len = %d, want 1", len(entities))
	}
	entityResult := asMap(t, entities[0])
	if entityResult["alias"] != "subject" || entityResult["entity_id"] != entityID || entityResult["flow_instance"] != "operating" || entityResult["entity_type"] != "product" || entityResult["current_state"] != "waiting" {
		t.Fatalf("setup entity result = %#v", entityResult)
	}
	assertTestSetupPersistence(t, db, runID, entityID, "waiting", "seeded", true)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}

	replay := rpcCall(t, handler, testSetupBody(runID, entityID, "waiting", "seeded", true, "idem-setup"))
	if replay.Error != nil {
		t.Fatalf("test.setup_entities replay error = %#v", replay.Error)
	}
	assertTestSetupPersistence(t, db, runID, entityID, "waiting", "seeded", true)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, testSetupBody(runID, entityID, "ready", "changed", false, "idem-setup"))
	if conflict.Error == nil {
		t.Fatal("test.setup_entities idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("test.setup_entities conflict data = %#v", data)
	}
	assertTestSetupPersistence(t, db, runID, entityID, "waiting", "seeded", true)

	if _, err := db.Exec(`DELETE FROM api_idempotency`); err != nil {
		t.Fatalf("delete setup api idempotency rows: %v", err)
	}
	expiredReplay := rpcCall(t, handler, testSetupBody(runID, entityID, "waiting", "seeded", true, "idem-setup-after-expiry"))
	if expiredReplay.Error != nil {
		t.Fatalf("test.setup_entities replay after idempotency expiry error = %#v", expiredReplay.Error)
	}
	assertTestSetupPersistence(t, db, runID, entityID, "waiting", "seeded", true)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after expired replay = %d, want 1", count)
	}
}

func testSetupHandler(t *testing.T, pg *store.PostgresStore, source semanticview.Source) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             pg,
			Entities:         pg,
			TestSetup:        pg,
			Idempotency:      pg,
			Events:           failingRunStartPublisher{err: errors.New("unexpected test setup event publish")},
			Source:           source,
			RunBundleContext: pg,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				Fingerprint:     runStartTestFingerprint,
			},
		}),
	})
}

func testSetupBody(runID, entityID, currentState, note string, reviewReady bool, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"setup","method":"test.setup_entities","params":{"bundle_hash":%q,"run_id":%q,"idempotency_key":%q,"entities":[{"alias":"subject","entity_id":%q,"flow_instance":"operating","entity_type":"product","current_state":%q,"fields":{"note":%q},"gates":{"review_ready":%t}}]}}`,
		runStartTestBundleHash,
		runID,
		idempotencyKey,
		entityID,
		currentState,
		note,
		reviewReady,
	)
}

func assertTestSetupPersistence(t *testing.T, db *sql.DB, runID, entityID, currentState, note string, reviewReady bool) {
	t.Helper()
	var runStatus, triggerType, bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRow(`
		SELECT status, trigger_event_type, COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load setup run row: %v", err)
	}
	if runStatus != "running" || triggerType != "test.setup_entities" {
		t.Fatalf("setup run row status=%q trigger=%q, want running/test.setup_entities", runStatus, triggerType)
	}
	if bundleHash != runStartTestBundleHash || bundleSource != storerunlifecycle.BundleSourceEphemeral || legacyFingerprint != runStartTestFingerprint {
		t.Fatalf("setup run row bundle identity = hash:%q source:%q fingerprint:%q", bundleHash, bundleSource, legacyFingerprint)
	}

	var flowInstance, entityType, gotState, gotNote, gateJSON string
	if err := db.QueryRow(`
		SELECT flow_instance, entity_type, current_state, fields->>'note', gates->>'review_ready'
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entityID).Scan(&flowInstance, &entityType, &gotState, &gotNote, &gateJSON); err != nil {
		t.Fatalf("load setup entity row: %v", err)
	}
	if flowInstance != "operating" || entityType != "product" || gotState != currentState || gotNote != note || gateJSON != fmt.Sprintf("%t", reviewReady) {
		t.Fatalf("setup entity row = flow:%q type:%q state:%q note:%q gate:%q", flowInstance, entityType, gotState, gotNote, gateJSON)
	}

	var mutations int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		  AND writer_type = 'platform'
		  AND writer_id = 'test.setup_entities'
	`, runID, entityID).Scan(&mutations); err != nil {
		t.Fatalf("count setup mutations: %v", err)
	}
	if mutations != 3 {
		t.Fatalf("setup mutation rows = %d, want 3", mutations)
	}

	var payload json.RawMessage
	if err := db.QueryRow(`
		SELECT fields
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entityID).Scan(&payload); err != nil {
		t.Fatalf("reload setup fields: %v", err)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode setup fields: %v", err)
	}
	if decoded["note"] != note {
		t.Fatalf("decoded setup fields = %#v, want note %q", decoded, note)
	}
}
