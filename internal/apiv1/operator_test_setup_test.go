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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(testSetupValidationBundle(t))
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

func TestOperatorTestSetupValidationFixtureDeclaresNoInputPins(t *testing.T) {
	bundle := testSetupValidationBundle(t)
	if pins := bundle.FlowInputEventPins(""); len(pins) != 0 {
		t.Fatalf("root input pins = %#v, want none", pins)
	}
	for flowID := range bundle.FlowSchemas {
		if pins := bundle.FlowInputEventPins(flowID); len(pins) != 0 {
			t.Fatalf("flow %s input pins = %#v, want none", flowID, pins)
		}
	}
}

func TestOperatorTestSetupRejectsContractInvalidEntities(t *testing.T) {
	cases := []struct {
		name      string
		edit      func(map[string]any)
		wantField string
	}{
		{
			name: "undeclared entity type",
			edit: func(entity map[string]any) {
				entity["entity_type"] = "unknown"
			},
			wantField: "entities[0].entity_type",
		},
		{
			name: "undeclared current state",
			edit: func(entity map[string]any) {
				entity["current_state"] = "missing"
			},
			wantField: "entities[0].current_state",
		},
		{
			name: "undeclared field",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{"missing": "value"}
			},
			wantField: "entities[0].fields.missing",
		},
		{
			name: "field type mismatch",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{"review_score": "not-an-integer"}
			},
			wantField: "entities[0].fields.review_score",
		},
		{
			name: "named type undeclared nested key",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{
					"business_brief": map[string]any{
						"summary": "ok",
						"extra":   "not-declared",
					},
				}
			},
			wantField: "entities[0].fields.business_brief",
		},
		{
			name: "named type nested type mismatch",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{
					"business_brief": map[string]any{
						"summary": 42,
					},
				}
			},
			wantField: "entities[0].fields.business_brief",
		},
		{
			name: "list field invalid item type",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{
					"feature_list": []any{map[string]any{"name": 42}},
				}
			},
			wantField: "entities[0].fields.feature_list",
		},
		{
			name: "map field invalid value type",
			edit: func(entity map[string]any) {
				entity["fields"] = map[string]any{
					"review_scores": map[string]any{"quality": "not-an-integer"},
				}
			},
			wantField: "entities[0].fields.review_scores",
		},
		{
			name: "undeclared gate",
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{"missing_gate": true}
			},
			wantField: "entities[0].gates.missing_gate",
		},
		{
			name: "non boolean gate",
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{"review_ready": "yes"}
			},
			wantField: "entities[0].gates.review_ready",
		},
		{
			name: "flow entity mismatch",
			edit: func(entity map[string]any) {
				entity["flow_instance"] = "secondary"
			},
			wantField: "entities[0].entity_type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := &store.PostgresStore{DB: db}
			handler := testSetupHandler(t, pg, semanticview.Wrap(testSetupValidationBundle(t)))
			entity := validTestSetupEntity(uuid.NewString(), "waiting", "seeded", true)
			tc.edit(entity)
			resp := rpcCall(t, handler, testSetupBodyWithEntity(uuid.NewString(), "idem-invalid-"+tc.name, entity))
			if resp.Error == nil {
				t.Fatal("test.setup_entities invalid setup error = nil")
			}
			if resp.Error.Code != codeInvalidParams {
				t.Fatalf("test.setup_entities invalid setup code = %d, want %d data=%#v", resp.Error.Code, codeInvalidParams, resp.Error.Data)
			}
			data := asMap(t, resp.Error.Data)
			details := asMap(t, data["details"])
			if details["field"] != tc.wantField {
				t.Fatalf("test.setup_entities invalid field = %#v, want %s; details=%#v", details["field"], tc.wantField, details)
			}
			if count := countAPIIdempotencyRows(t, db); count != 0 {
				t.Fatalf("api_idempotency rows after invalid setup = %d, want 0", count)
			}
			if count := countTestSetupEntityRows(t, db); count != 0 {
				t.Fatalf("entity_state rows after invalid setup = %d, want 0", count)
			}
		})
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
	return testSetupBodyWithEntity(runID, idempotencyKey, validTestSetupEntity(entityID, currentState, note, reviewReady))
}

func testSetupBodyWithEntity(runID, idempotencyKey string, entity map[string]any) string {
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      "setup",
		"method":  testSetupEntitiesMethod,
		"params": map[string]any{
			"bundle_hash":     runStartTestBundleHash,
			"run_id":          runID,
			"idempotency_key": idempotencyKey,
			"entities":        []any{entity},
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func validTestSetupEntity(entityID, currentState, note string, reviewReady bool) map[string]any {
	return map[string]any{
		"alias":         "subject",
		"entity_id":     entityID,
		"flow_instance": "operating",
		"entity_type":   "product",
		"current_state": currentState,
		"fields": map[string]any{
			"note": note,
		},
		"gates": map[string]any{
			"review_ready": reviewReady,
		},
	}
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

func countTestSetupEntityRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state`).Scan(&count); err != nil {
		t.Fatalf("count entity_state rows: %v", err)
	}
	return count
}

func testSetupValidationBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.CopyTestSetupValidation(t),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}
