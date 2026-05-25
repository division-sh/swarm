package apiv1

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

const runStartTestFingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const runStartTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()
	body := runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "idem-start")

	started := rpcCall(t, handler, body)
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	result := asMap(t, started.Result)
	if result["run_id"] != runID || result["status"] != "running" {
		t.Fatalf("run.start result = %#v", result)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", count)
	}
	assertRunStartPersistence(t, db, runID, "scan.requested", runStartTestFingerprint)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("run.start replay error = %#v", replay.Error)
	}
	if replayResult := asMap(t, replay.Result); replayResult["run_id"] != runID || replayResult["status"] != "running" {
		t.Fatalf("run.start replay result = %#v", replayResult)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"changed"}`, "idem-start"))
	if conflict.Error == nil {
		t.Fatal("run.start idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("run.start conflict data = %#v", data)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after conflict = %d, want 1", count)
	}

	conflictBundle := rpcCall(t, handler, runStartBody(runID, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "idem-start"))
	if conflictBundle.Error == nil {
		t.Fatal("run.start bundle-change idempotency conflict error = nil")
	}
	if data := asMap(t, conflictBundle.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("run.start bundle-change conflict data = %#v", data)
	}

	conflictEvent := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.missing", `{"topic":"medicine"}`, "idem-start"))
	if conflictEvent.Error == nil {
		t.Fatal("run.start event-change idempotency conflict error = nil")
	}
	if data := asMap(t, conflictEvent.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("run.start event-change conflict data = %#v", data)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after body-domain conflicts = %d, want 1", count)
	}
}

func TestOperatorRunStartHandlersPersistBootFingerprintWhenBundleRefOmitted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()

	started := rpcCall(t, handler, runStartBodyWithoutBundle(runID, "scan.requested", `{"topic":"medicine"}`, "idem-start-no-bundle"))
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	assertRunStartPersistence(t, db, runID, "scan.requested", runStartTestFingerprint)
}

func TestOperatorRunStartHandlersRejectCanonicalBundleHashUntilSourceOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()

	resp := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-start-hash"))
	if resp.Error == nil {
		t.Fatal("run.start canonical bundle_hash error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
		t.Fatalf("unsupported bundle hash data = %#v", data)
	}
	assertNoRunStartPersistence(t, db, runID)
}

func TestOperatorRunStartHandlersFailClosedBeforePersistence(t *testing.T) {
	t.Run("bundle mismatch", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "idem-mismatch"))
		if resp.Error == nil {
			t.Fatal("run.start bundle mismatch error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleMismatchCode {
			t.Fatalf("bundle mismatch data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("invalid bundle fingerprint", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, "sha256:not-lower-64-hex", "scan.requested", `{"topic":"medicine"}`, "idem-invalid-bundle"))
		if resp.Error == nil {
			t.Fatal("run.start invalid bundle fingerprint error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleRefCode {
			t.Fatalf("unsupported bundle ref data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("invalid canonical bundle hash", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBodyWithBundleHash(runID, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "scan.requested", `{"topic":"medicine"}`, "idem-invalid-bundle-hash"))
		if resp.Error == nil {
			t.Fatal("run.start invalid bundle_hash error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
			t.Fatalf("unsupported bundle hash data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("canonical and legacy bundle params conflict", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBodyWithBothBundleInputs(runID, runStartTestBundleHash, runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "idem-bundle-conflict"))
		if resp.Error == nil {
			t.Fatal("run.start bundle input conflict error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
			t.Fatalf("bundle input conflict data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("undeclared event", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.missing", `{"topic":"medicine"}`, "idem-missing-event"))
		if resp.Error == nil {
			t.Fatal("run.start undeclared event error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventNotDeclaredCode {
			t.Fatalf("undeclared event data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("payload validation", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
			ContractBundle: source,
			PayloadValidator: func(eventType string, payload []byte) error {
				if eventType != "scan.requested" {
					return fmt.Errorf("unexpected event type %q", eventType)
				}
				return errors.New("schema violation")
			},
		})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "idem-invalid-payload"))
		if resp.Error == nil {
			t.Fatal("run.start payload validation error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != PayloadValidationFailedCode {
			t.Fatalf("payload validation data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("invalid caller run id", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, runStartBody("abc", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "idem-invalid-run-id"))
		if resp.Error == nil {
			t.Fatal("run.start invalid run_id error = nil")
		}
		if resp.Error.Code != codeInvalidParams {
			t.Fatalf("invalid run_id error code = %d, want invalid params", resp.Error.Code)
		}
		if details := asMap(t, resp.Error.Data)["details"]; asMap(t, details)["field"] != "run_id" {
			t.Fatalf("invalid run_id details = %#v", details)
		}
		if count := countAllRunRows(t, db); count != 0 {
			t.Fatalf("run rows after invalid run_id = %d, want 0", count)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 0 {
			t.Fatalf("event rows after invalid run_id = %d, want 0", count)
		}
		if count := countAPIIdempotencyRows(t, db); count != 0 {
			t.Fatalf("api_idempotency rows after invalid run_id = %d, want 0", count)
		}
	})

	t.Run("invalid payload entity id", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"entity_id":"not-a-uuid","topic":"medicine"}`, "idem-invalid-entity-id"))
		assertInvalidRunStartParam(t, resp, "payload.entity_id")
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("non-string payload entity id", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"entity_id":123,"topic":"medicine"}`, "idem-non-string-entity-id"))
		assertInvalidRunStartParam(t, resp, "payload.entity_id")
		assertNoRunStartPersistence(t, db, runID)
	})
}

func TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()
	cases := []struct {
		method string
		params string
	}{
		{method: "run.stop", params: fmt.Sprintf(`{"run_id":%q,"idempotency_key":"idem-stop"}`, runID)},
		{method: "run.pause", params: fmt.Sprintf(`{"run_id":%q,"idempotency_key":"idem-pause"}`, runID)},
		{method: "run.continue", params: fmt.Sprintf(`{"run_id":%q,"idempotency_key":"idem-continue"}`, runID)},
		{method: "runtime.pause", params: `{"idempotency_key":"idem-runtime-pause"}`},
		{method: "runtime.resume", params: `{"idempotency_key":"idem-runtime-resume"}`},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"control","method":%q,"params":%s}`, tc.method, tc.params))
			if resp.Error == nil {
				t.Fatalf("%s error = nil, want METHOD_UNAVAILABLE", tc.method)
			}
			if data := asMap(t, resp.Error.Data); data["code"] != MethodUnavailableCode {
				t.Fatalf("%s data = %#v, want METHOD_UNAVAILABLE", tc.method, data)
			}
		})
	}
}

func runStartTestHandler(t *testing.T, pg *store.PostgresStore, bus *runtimebus.EventBus, source semanticview.Source) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:         func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:       func() bool { return true },
			Database:    fakePinger{},
			Runs:        pg,
			Idempotency: pg,
			Events:      bus,
			Source:      source,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				Fingerprint:     runStartTestFingerprint,
			},
		}),
	})
}

func runStartBody(runID, fingerprint, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"bundle_ref":{"fingerprint":%q},"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		fingerprint,
		eventName,
		payload,
		runID,
		idempotencyKey,
	)
}

func runStartBodyWithBundleHash(runID, bundleHash, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"bundle_hash":%q,"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		bundleHash,
		eventName,
		payload,
		runID,
		idempotencyKey,
	)
}

func runStartBodyWithBothBundleInputs(runID, bundleHash, fingerprint, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"bundle_hash":%q,"bundle_ref":{"fingerprint":%q},"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		bundleHash,
		fingerprint,
		eventName,
		payload,
		runID,
		idempotencyKey,
	)
}

func runStartBodyWithoutBundle(runID, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		eventName,
		payload,
		runID,
		idempotencyKey,
	)
}

func assertInvalidRunStartParam(t *testing.T, resp rpcResponse, field string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("run.start invalid %s error = nil", field)
	}
	if resp.Error.Code != codeInvalidParams {
		t.Fatalf("invalid %s error code = %d, want invalid params", field, resp.Error.Code)
	}
	if details := asMap(t, resp.Error.Data)["details"]; asMap(t, details)["field"] != field {
		t.Fatalf("invalid %s details = %#v", field, details)
	}
}

func assertRunStartPersistence(t *testing.T, db *sql.DB, runID, eventName, bundleFingerprint string) {
	t.Helper()
	var runStatus, triggerType, bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRow(`
		SELECT status, trigger_event_type, COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if runStatus != "running" || triggerType != eventName {
		t.Fatalf("run row status=%q trigger=%q, want running/%s", runStatus, triggerType, eventName)
	}
	if bundleHash != "" || bundleSource != "legacy" || legacyFingerprint != bundleFingerprint {
		t.Fatalf("run row bundle identity = hash:%q source:%q fingerprint:%q, want legacy with compatibility fingerprint %q", bundleHash, bundleSource, legacyFingerprint, bundleFingerprint)
	}
	var entityID, producedBy string
	var payload json.RawMessage
	if err := db.QueryRow(`
		SELECT entity_id::text, produced_by, payload
		FROM events
		WHERE run_id = $1::uuid AND event_name = $2
	`, runID, eventName).Scan(&entityID, &producedBy, &payload); err != nil {
		t.Fatalf("load run.start event row: %v", err)
	}
	if entityID != runID {
		t.Fatalf("event entity_id = %q, want run_id %q", entityID, runID)
	}
	if producedBy != "api.v1" {
		t.Fatalf("event produced_by = %q, want api.v1", producedBy)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	if decoded["entity_id"] != runID || decoded["topic"] != "medicine" {
		t.Fatalf("persisted payload = %#v", decoded)
	}
}

func assertNoRunStartPersistence(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	if count := countRunRowsByID(t, db, runID); count != 0 {
		t.Fatalf("run rows for %s = %d, want 0", runID, count)
	}
	if count := countEventRowsByRunID(t, db, runID); count != 0 {
		t.Fatalf("event rows for %s = %d, want 0", runID, count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("api_idempotency rows = %d, want 0", count)
	}
}

func countRunRowsByID(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count run rows: %v", err)
	}
	return count
}

func countEventRowsByRunID(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count event rows: %v", err)
	}
	return count
}

func countAllRunRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&count); err != nil {
		t.Fatalf("count all run rows: %v", err)
	}
	return count
}

func runStartTestBundle(eventName string) *runtimecontracts.WorkflowContractBundle {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Path:  "discovery",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{eventName},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": flow.Nodes["scan-orchestrator"],
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": &root.Children[0],
			},
		},
	}
}
