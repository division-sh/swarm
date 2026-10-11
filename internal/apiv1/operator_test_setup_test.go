package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	bundle := testSetupValidationBundle(t)
	source := semanticview.Wrap(bundle)
	bundleHash := bundle.SourceArtifact.BundleHash()
	handler := testSetupHandler(t, pg, source)
	runID := uuid.NewString()
	entityID := uuid.NewString()

	resp := rpcCall(t, handler, testSetupBody(bundleHash, runID, entityID, "seeded", "idem-setup"))
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
	if state, present := entityResult["current_state"]; !present || state != nil || entityResult["alias"] != "subject" || entityResult["entity_id"] != entityID || entityResult["flow_instance"] != "operating" || entityResult["entity_type"] != "product" {
		t.Fatalf("setup entity result = %#v", entityResult)
	}
	assertTestSetupPersistence(t, pg, bundleHash, runID, entityID, "seeded")
	if count := countAPIIdempotencyRows(t, pg); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}

	replay := rpcCall(t, handler, testSetupBody(bundleHash, runID, entityID, "seeded", "idem-setup"))
	if replay.Error != nil {
		t.Fatalf("test.setup_entities replay error = %#v", replay.Error)
	}
	assertTestSetupPersistence(t, pg, bundleHash, runID, entityID, "seeded")
	if count := countAPIIdempotencyRows(t, pg); count != 1 {
		t.Fatalf("api_idempotency rows after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, testSetupBody(bundleHash, runID, entityID, "changed", "idem-setup"))
	if conflict.Error == nil {
		t.Fatal("test.setup_entities idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("test.setup_entities conflict data = %#v", data)
	}
	assertTestSetupPersistence(t, pg, bundleHash, runID, entityID, "seeded")

	if _, err := db.Exec(`DELETE FROM api_idempotency`); err != nil {
		t.Fatalf("delete setup api idempotency rows: %v", err)
	}
	expiredReplay := rpcCall(t, handler, testSetupBody(bundleHash, runID, entityID, "seeded", "idem-setup-after-expiry"))
	if expiredReplay.Error != nil {
		t.Fatalf("test.setup_entities replay after idempotency expiry error = %#v", expiredReplay.Error)
	}
	assertTestSetupPersistence(t, pg, bundleHash, runID, entityID, "seeded")
	if count := countAPIIdempotencyRows(t, pg); count != 1 {
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
		name            string
		constructedRoot bool
		edit            func(map[string]any)
		wantField       string
		wantReason      string
	}{
		{
			name: "undeclared entity type",
			edit: func(entity map[string]any) {
				entity["entity_type"] = "unknown"
			},
			wantField: "entities[0].entity_type",
		},
		{
			name:            "undeclared current state",
			constructedRoot: true,
			edit: func(entity map[string]any) {
				entity["current_state"] = "missing"
			},
			wantField:  "entities[0].current_state",
			wantReason: "must be a declared state for the selected flow",
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
			name:            "undeclared gate",
			constructedRoot: true,
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{"missing_gate": true}
			},
			wantField:  "entities[0].gates.missing_gate",
			wantReason: "is not declared for the selected flow",
		},
		{
			name:            "non boolean gate",
			constructedRoot: true,
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{"review_ready": "yes"}
			},
			wantField:  "entities[0].gates.review_ready",
			wantReason: "must be boolean",
		},
		{
			name: "flow entity mismatch",
			edit: func(entity map[string]any) {
				entity["flow_instance"] = "secondary"
			},
			wantField: "entities[0].entity_type",
		},
		{
			name: "import declared current state",
			edit: func(entity map[string]any) {
				entity["current_state"] = "waiting"
			},
			wantField:  "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import null current state",
			edit: func(entity map[string]any) {
				entity["current_state"] = nil
			},
			wantField:  "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import empty current state",
			edit: func(entity map[string]any) {
				entity["current_state"] = ""
			},
			wantField:  "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import declared gate",
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{"review_ready": true}
			},
			wantField:  "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import null gates",
			edit: func(entity map[string]any) {
				entity["gates"] = nil
			},
			wantField:  "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import empty gates",
			edit: func(entity map[string]any) {
				entity["gates"] = map[string]any{}
			},
			wantField:  "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			bundle := testSetupValidationBundle(t)
			handler := testSetupHandler(t, pg, semanticview.Wrap(bundle))
			runID := uuid.NewString()
			entity := validTestSetupEntity(uuid.NewString(), "seeded")
			if tc.constructedRoot {
				entity["entity_id"], entity["flow_instance"], entity["entity_type"] = runID, "", "item"
				entity["current_state"], entity["fields"] = "new", map[string]any{}
			}
			baseline, err := testSetupEntityRequestFromParam(entity, 0, runID)
			if err != nil {
				t.Fatalf("invalid baseline input: %v", err)
			}
			if err := validateTestSetupEntityAgainstBundle(bundle, baseline, 0); err != nil {
				t.Fatalf("invalid baseline contract: %v", err)
			}
			tc.edit(entity)
			resp := rpcCall(t, handler, testSetupBodyWithEntity(bundle.SourceArtifact.BundleHash(), runID, "idem-invalid-"+tc.name, entity))
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
			if tc.wantReason != "" && details["reason"] != tc.wantReason {
				t.Fatalf("test.setup_entities invalid reason = %#v, want %q; details=%#v", details["reason"], tc.wantReason, details)
			}
			if count := countAPIIdempotencyRows(t, pg); count != 0 {
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
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("test setup source has no admitted source artifact")
	}
	fact := sourceartifactfixture.RequireArtifact(t, context.Background(), pg, bundle.SourceArtifact)
	handlers := testOperatorHandlers(testOperatorCapabilities{
		Now:         func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
		Ready:       func() bool { return true },
		Database:    fakePinger{},
		Runs:        pg,
		Entities:    pg,
		Idempotency: pg,
		Events: scenarioSetupImportPublisher{
			bundleScopedFailingEventPublisher: bundleScopedFailingEventPublisher{
				failingRunStartPublisher: failingRunStartPublisher{err: errors.New("unexpected test setup event publish")},
				fact:                     fact,
			},
			TestSetupStore: pg,
		},
		Source:           source,
		RunBundleContext: pg,
		Bundle: runtimecontracts.BundleIdentity{
			WorkflowName:    "review",
			WorkflowVersion: "1.0.0",
			BundleHash:      fact.BundleHash(),
		},
	})
	if handlers[testSetupEntitiesMethod] == nil {
		t.Fatal("test.setup_entities handler is not configured")
	}
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers:   handlers,
	})
}

func testSetupBody(bundleHash, runID, entityID, note, idempotencyKey string) string {
	return testSetupBodyWithEntity(bundleHash, runID, idempotencyKey, validTestSetupEntity(entityID, note))
}

func testSetupBodyWithEntity(bundleHash, runID, idempotencyKey string, entity map[string]any) string {
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      "setup",
		"method":  testSetupEntitiesMethod,
		"params": map[string]any{
			"bundle_hash":     bundleHash,
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

func validTestSetupEntity(entityID, note string) map[string]any {
	return map[string]any{
		"alias":         "subject",
		"entity_id":     entityID,
		"flow_instance": "operating",
		"entity_type":   "product",
		"fields": map[string]any{
			"note": note,
		},
	}
}

func assertTestSetupPersistence(t *testing.T, selected interface {
	operatorread.RunReader
	EntityReadStore
	runtimepipeline.WorkflowInstancePersistenceReader
}, expectedBundleHash, runID, entityID, note string) {
	t.Helper()
	run, err := selected.LoadRunHeader(context.Background(), runID)
	if err != nil {
		t.Fatalf("load setup run row: %v", err)
	}
	if run.Status != "running" || run.Origin.Kind() != storerunlifecycle.OriginScenarioSetup {
		t.Fatalf("setup run row status=%q origin=%q, want running/scenario_setup", run.Status, run.Origin.Kind())
	}
	if run.BundleHash != expectedBundleHash {
		t.Fatalf("setup run row source artifact hash = %q, want %q", run.BundleHash, expectedBundleHash)
	}

	assertTestSetupImportedEntity(t, selected, runID, entityID, note)

	projection, err := storetest.ReadTrackedEntityMutationProjectionStorage(context.Background(), selected, runID, entityID)
	if err != nil {
		t.Fatalf("read setup mutation and field storage: %v", err)
	}
	mutations := 0
	for _, mutation := range projection.Mutations {
		if mutation.WriterType != "platform" || mutation.WriterID != "test.setup_entities" {
			continue
		}
		if mutation.Domain != "authored_field" || mutation.Path != "note" {
			t.Fatalf("setup wrote another mutation domain/path: %+v", mutation)
		}
		mutations++
	}
	if mutations != 1 {
		t.Fatalf("setup authored note mutation rows = %d, want one and no other mutation domain/path", mutations)
	}

	decoded := map[string]any{}
	if err := json.Unmarshal(projection.Fields, &decoded); err != nil {
		t.Fatalf("decode setup fields: %v", err)
	}
	if decoded["note"] != note {
		t.Fatalf("decoded setup fields = %#v, want note %q", decoded, note)
	}
}

func assertTestSetupImportedEntity(t *testing.T, selected interface {
	EntityReadStore
	runtimepipeline.WorkflowInstancePersistenceReader
}, runID, entityID, note string) {
	t.Helper()
	full, err := selected.LoadOperatorEntity(context.Background(), entityID, runID)
	if err != nil {
		t.Fatalf("load imported setup entity: %v", err)
	}
	entity := full.Entity
	if entity.RunID != runID || entity.EntityID != entityID || entity.FlowInstance != "operating" || entity.EntityType != "product" || len(full.Fields) != 1 || full.Fields["note"] != note {
		t.Fatalf("imported setup identity/fields = %+v", full)
	}
	if entity.StageDefined != nil || entity.CurrentState != nil || entity.Revision != nil || entity.EnteredStateAt != nil || entity.LifecycleCreatedAt != nil || entity.LifecycleUpdatedAt != nil || full.Gates != nil || full.Bookkeeping != nil || full.Accumulated != nil || full.Loops != nil {
		t.Fatalf("imported setup entity acquired lifecycle: %+v", full)
	}
	if entity.CreatedAt.IsZero() || entity.UpdatedAt.IsZero() || !entity.CreatedAt.Equal(entity.UpdatedAt) {
		t.Fatalf("imported setup data clocks = %+v, want unchanged birth clocks", entity)
	}
	instances, err := selected.ListWorkflowInstances(context.Background(), runID)
	if err != nil || len(instances) != 0 {
		t.Fatalf("data-only setup constructed instances = %+v, err=%v", instances, err)
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
