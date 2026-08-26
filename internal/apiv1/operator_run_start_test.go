package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimerunstart "github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const runStartTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func runStartTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	return mustAPITestBundleSourceFact(runStartTestBundleHash)
}

func runStartTestEventBusOptions(source semanticview.Source) runtimebus.EventBusOptions {
	return runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runStartTestBundleSourceFact(),
	}
}

func TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()
	body := runStartBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-start")

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
	assertRunStartPersistence(t, db, runID, "scan.requested")
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

	conflict := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"changed"}`, "idem-start"))
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

	conflictEvent := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.missing", `{"topic":"medicine"}`, "idem-start"))
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

func TestOperatorRunStartHandlersUseActiveEphemeralBundleScopeForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()

	started := rpcCall(t, handler, runStartBodyWithoutBundle(runID, "scan.requested", `{"topic":"medicine"}`, "idem-start-no-bundle"))
	if started.Error != nil {
		t.Fatalf("run.start active ephemeral bundle scope error = %#v", started.Error)
	}
	result := asMap(t, started.Result)
	if result["run_id"] != runID || result["status"] != "running" {
		t.Fatalf("run.start result = %#v", result)
	}
	assertRunStartPersistence(t, db, runID, "scan.requested")
}

func TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	handler := runStartTestHandler(t, pg, missingRunStartBundleScopePublisher{}, source)
	runID := uuid.NewString()

	started := rpcCall(t, handler, runStartBodyWithoutBundle(runID, "scan.requested", `{"topic":"medicine"}`, "idem-start-no-bundle"))
	if started.Error == nil {
		t.Fatal("run.start missing bundle scope error = nil")
	}
	if data := asMap(t, started.Error.Data); data["code"] != BundleScopeRequiredCode {
		t.Fatalf("missing bundle scope data = %#v", data)
	}
	assertNoRunStartPersistence(t, db, runID)

	retiredRunID := uuid.NewString()
	retired := rpcCall(t, handler, runStartBodyWithRetiredBundleInput(retiredRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-start-retired-input"))
	assertInvalidRunStartParam(t, retired, retiredBundleInputName())
	assertNoRunStartPersistence(t, db, retiredRunID)
}

func TestOperatorRunStartHandlersAcceptCanonicalBundleHashForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()

	resp := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-start-hash"))
	if resp.Error != nil {
		t.Fatalf("run.start canonical bundle_hash error = %#v", resp.Error)
	}
	assertRunStartPersistence(t, db, runID, "scan.requested")
}

func TestOperatorRunStartRejectsFlowScopedEventName(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(flowScopedEventPublishTestBundle())
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := runStartTestHandler(t, pg, bus, source)
	runID := uuid.NewString()

	resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "repo-scaffold/repo_scaffold.repo_commit_succeeded", `{"topic":"medicine"}`, "idem-start-flow-scoped"))
	if resp.Error == nil {
		t.Fatal("run.start flow-scoped event error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != EventNotDeclaredCode {
		t.Fatalf("run.start flow-scoped data = %#v, want %s", data, EventNotDeclaredCode)
	}
	details := asMap(t, data["details"])
	if details["event_name"] != "repo-scaffold/repo_scaffold.repo_commit_succeeded" || details["reason"] != "not_declared_root_input" {
		t.Fatalf("run.start flow-scoped details = %#v", details)
	}
	assertNoRunStartPersistence(t, db, runID)
}

func TestOperatorRunStartHandlersFailClosedBeforePersistence(t *testing.T) {
	t.Run("non-routable bundle hash", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "idem-mismatch"))
		if resp.Error == nil {
			t.Fatal("run.start non-routable bundle error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleUnavailableCode {
			t.Fatalf("bundle unavailable data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("existing run bundle mismatch", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()
		runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID:      runID,
			BundleHash: runStartTestBundleHash,
		})

		resp := rpcCall(t, handler, runStartBody(runID, "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "idem-existing-mismatch"))
		if resp.Error == nil {
			t.Fatal("run.start existing-run bundle mismatch error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleMismatchCode {
			t.Fatalf("bundle mismatch data = %#v", data)
		}
		if count := countEventRowsByRunID(t, db, runID); count != 0 {
			t.Fatalf("event rows for mismatched run = %d, want 0", count)
		}
		if count := countAPIIdempotencyRows(t, db); count != 0 {
			t.Fatalf("api_idempotency rows after mismatch = %d, want 0", count)
		}
	})

	t.Run("invalid canonical bundle hash", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, "sha256:not-lower-64-hex", "scan.requested", `{"topic":"medicine"}`, "idem-invalid-bundle"))
		if resp.Error == nil {
			t.Fatal("run.start invalid bundle_hash error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
			t.Fatalf("unsupported bundle hash data = %#v", data)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("invalid canonical bundle hash", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
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

	t.Run("retired bundle input is rejected", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBodyWithCanonicalAndRetiredInput(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-retired-bundle-input"))
		assertInvalidRunStartParam(t, resp, retiredBundleInputName())
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("undeclared event", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.missing", `{"topic":"medicine"}`, "idem-missing-event"))
		if resp.Error == nil {
			t.Fatal("run.start undeclared event error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventNotDeclaredCode {
			t.Fatalf("undeclared event data = %#v", data)
		} else {
			details := asMap(t, data["details"])
			if details["event_name"] != "scan.missing" || details["reason"] != "not_declared_root_input" {
				t.Fatalf("undeclared event details = %#v", details)
			}
			if got := stringSliceFromAny(t, details["declared_events"]); len(got) != 1 || got[0] != "scan.requested" {
				t.Fatalf("undeclared declared_events = %#v", got)
			}
			if got := stringSliceFromAny(t, details["routable_events"]); len(got) != 1 || got[0] != "scan.requested" {
				t.Fatalf("undeclared routable_events = %#v", got)
			}
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("declared but unroutable root input", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		const eventName = "scan.unroutable_requested"
		bundle := runStartTestBundle(eventName)
		bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"] = runtimecontracts.SystemNodeContract{
			ID:           "scan-orchestrator",
			SubscribesTo: []string{"scan.other_requested"},
		}
		bundle.Nodes["scan-orchestrator"] = bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"]
		source := semanticview.Wrap(bundle)
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, eventName, `{"topic":"medicine"}`, "idem-unroutable-event"))
		if resp.Error == nil {
			t.Fatal("run.start declared unroutable event error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventNotDeclaredCode {
			t.Fatalf("declared unroutable event data = %#v", data)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != eventName || details["reason"] != "declared_root_input_not_routable" {
			t.Fatalf("declared unroutable event details = %#v", details)
		}
		if got := stringSliceFromAny(t, details["declared_events"]); len(got) != 1 || got[0] != eventName {
			t.Fatalf("declared unroutable declared_events = %#v", got)
		}
		if got := stringSliceFromAny(t, details["routable_events"]); len(got) != 0 {
			t.Fatalf("declared unroutable routable_events = %#v, want empty", got)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("payload validation", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
			ContractBundle:   source,
			BundleSourceFact: runStartTestBundleSourceFact(),
			PayloadValidator: func(_ context.Context, eventType string, payload []byte) error {
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

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-invalid-payload"))
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
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, runStartBody("abc", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-invalid-run-id"))
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

	t.Run("payload entity id is not envelope authority", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := runStartTestHandler(t, pg, bus, source)
		runID := uuid.NewString()
		payloadEntityID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.requested", fmt.Sprintf(`{"entity_id":%q,"topic":"medicine"}`, payloadEntityID), "idem-payload-entity-id-not-authority"))
		if resp.Error != nil {
			t.Fatalf("run.start payload entity_id error = %#v", resp.Error)
		}
		var eventID, eventEntityID, eventFlowInstance, targetRoute, targetSet string
		var payload json.RawMessage
		if err := db.QueryRow(`
				SELECT event_id::text, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''),
				       COALESCE(target_route::text, '{}'), COALESCE(target_set::text, '[]'), payload
				FROM events
				WHERE run_id = $1::uuid AND event_name = 'scan.requested'
			`, runID).Scan(&eventID, &eventEntityID, &eventFlowInstance, &targetRoute, &targetSet, &payload); err != nil {
			t.Fatalf("load run.start event row: %v", err)
		}
		assertStoredEventProjectionMatchesDeliveries(t, eventEntityID, eventFlowInstance, targetRoute, targetSet, runID, postgresEventDeliveryTargetRoutes(t, db, eventID))
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("decode persisted payload: %v", err)
		}
		if decoded["entity_id"] != payloadEntityID || decoded["topic"] != "medicine" {
			t.Fatalf("persisted payload = %#v, want business entity_id %s and topic medicine", decoded, payloadEntityID)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		handler := runStartTestHandler(t, pg, failingRunStartPublisher{err: errors.New("simulated run.start publish failure")}, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-publish-failure"))
		if resp.Error == nil {
			t.Fatal("run.start publish failure error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventPublishFailedCode {
			t.Fatalf("publish failure data = %#v", data)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "scan.requested" || details["run_id"] != runID || details["phase"] != "publish" || !strings.Contains(fmt.Sprint(details["reason"]), "simulated run.start publish failure") {
			t.Fatalf("publish failure details = %#v", details)
		}
		assertNoRunStartPersistence(t, db, runID)
	})

	t.Run("post-validation invalid event type is a publish contradiction", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		handler := runStartTestHandler(t, pg, failingRunStartPublisher{err: runtimebus.ErrInvalidEventType}, source)
		runID := uuid.NewString()

		resp := rpcCall(t, handler, runStartBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "idem-invalid-event-after-validation"))
		if resp.Error == nil {
			t.Fatal("run.start post-validation invalid event error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventPublishFailedCode {
			t.Fatalf("post-validation data = %#v, want %s", data, EventPublishFailedCode)
		}
		if data["retryable"] != false {
			t.Fatalf("post-validation data = %#v, want non-retryable contradiction", data)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "scan.requested" || details["run_id"] != runID || details["phase"] != "publish" {
			t.Fatalf("post-validation details = %#v", details)
		}
		if details["reason"] != runtimebus.ErrInvalidEventType.Error() {
			t.Fatalf("post-validation reason = %#v", details["reason"])
		}
		assertNoRunStartPersistence(t, db, runID)
	})
}

func TestInvalidEventTypeMappersSeparateRootInputFromCatalogFailures(t *testing.T) {
	params := eventPublicationParams{EventName: "scan.requested", EventID: "event-1", RunID: "run-1"}

	runStartErr := runStartEventPublishError(params, runtimebus.ErrInvalidEventType)
	var runStartAppErr *ApplicationError
	if !errors.As(runStartErr, &runStartAppErr) || runStartAppErr.Code != EventPublishFailedCode {
		t.Fatalf("run.start mapping = %#v, want %s", runStartErr, EventPublishFailedCode)
	}
	if runStartAppErr.Retryable {
		t.Fatalf("run.start invalid-event contradiction = %#v, want non-retryable", runStartAppErr)
	}

	for name, mapped := range map[string]error{
		"event publish": eventPublishPublishError(params, runtimebus.ErrInvalidEventType),
		"event replay":  eventReplayPublishError(params.EventName, runtimebus.ErrInvalidEventType),
	} {
		t.Run(name, func(t *testing.T) {
			var appErr *ApplicationError
			if !errors.As(mapped, &appErr) || appErr.Code != EventNotDeclaredCode {
				t.Fatalf("mapping = %#v, want %s", mapped, EventNotDeclaredCode)
			}
			details, ok := appErr.Details.(map[string]any)
			if !ok || details["reason"] != "event_not_admitted_by_publisher" {
				t.Fatalf("details = %#v", appErr.Details)
			}
			if details["reason"] == string(runtimerunstart.RootInputNotDeclared) || details["reason"] == string(runtimerunstart.RootInputNotRoutable) {
				t.Fatalf("catalog failure entered root-input reason: %#v", details)
			}
		})
	}
}

func TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
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

func runStartTestHandler(t *testing.T, pg *store.PostgresStore, bus EventPublisher, source semanticview.Source) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             pg,
			Idempotency:      pg,
			Events:           bus,
			Source:           source,
			RunBundleContext: pg,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				BundleHash:      runStartTestBundleHash,
			},
		}),
	})
}

type failingRunStartPublisher struct {
	err error
}

func (p failingRunStartPublisher) Publish(context.Context, events.Event) error {
	return p.err
}

func (p failingRunStartPublisher) PublishAcknowledged(context.Context, events.Event) error {
	return p.err
}

func (p failingRunStartPublisher) LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, nil
}

func (p failingRunStartPublisher) PublishAPIEventAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, p.err
}

func (p failingRunStartPublisher) PublishAPIEventWithRunCreationAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion, *durabledata.RunCreationCommand) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, p.err
}

func (p failingRunStartPublisher) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
	return runtimecorrelation.WithBundleSourceFact(ctx, runStartTestBundleSourceFact()), nil
}

type missingRunStartBundleScopePublisher struct{}

func (missingRunStartBundleScopePublisher) Publish(context.Context, events.Event) error {
	return errors.New("unexpected publish without active runtime bundle scope")
}

func (missingRunStartBundleScopePublisher) PublishAcknowledged(context.Context, events.Event) error {
	return errors.New("unexpected publish without active runtime bundle scope")
}

func (missingRunStartBundleScopePublisher) LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, nil
}

func (missingRunStartBundleScopePublisher) PublishAPIEventAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, errors.New("unexpected publish without active runtime bundle scope")
}

func (missingRunStartBundleScopePublisher) PublishAPIEventWithRunCreationAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion, *durabledata.RunCreationCommand) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, errors.New("unexpected publish without active runtime bundle scope")
}

func runStartBody(runID, bundleHash, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"bundle_hash":%q,"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		bundleHash,
		eventName,
		payload,
		runID,
		idempotencyKey,
	)
}

func retiredBundleInputName() string {
	return "bundle_" + "ref"
}

func retiredBundleInputFieldName() string {
	return "finger" + "print"
}

func runStartBodyWithRetiredBundleInput(runID, bundleHash, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{%q:{%q:%q},"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		retiredBundleInputName(),
		retiredBundleInputFieldName(),
		bundleHash,
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

func runStartBodyWithCanonicalAndRetiredInput(runID, bundleHash, eventName, payload, idempotencyKey string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"start","method":"run.start","params":{"bundle_hash":%q,%q:{%q:%q},"event_name":%q,"payload":%s,"run_id":%q,"idempotency_key":%q}}`,
		bundleHash,
		retiredBundleInputName(),
		retiredBundleInputFieldName(),
		bundleHash,
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

func stringSliceFromAny(t *testing.T, value any) []string {
	t.Helper()
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("slice item = %#v, want string", item)
			}
			out = append(out, text)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		t.Fatalf("value = %#v, want string slice", value)
		return nil
	}
}

func assertRunStartPersistence(t *testing.T, db *sql.DB, runID, eventName string) {
	t.Helper()
	var runStatus, triggerType, bundleHash, bundleSource string
	if err := db.QueryRow(`
		SELECT status, trigger_event_type, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &bundleHash, &bundleSource); err != nil {
		t.Fatalf("load run row: %v", err)
	}
	if runStatus != "running" || triggerType != eventName {
		t.Fatalf("run row status=%q trigger=%q, want running/%s", runStatus, triggerType, eventName)
	}
	if bundleHash != runStartTestBundleHash || bundleSource != storerunlifecycle.BundleSourceEphemeral {
		t.Fatalf("run row bundle identity = hash:%q source:%q, want %s/%s", bundleHash, bundleSource, runStartTestBundleHash, storerunlifecycle.BundleSourceEphemeral)
	}
	var entityID, flowInstance, producedBy, targetRoute, targetSet string
	var payload json.RawMessage
	if err := db.QueryRow(`
		SELECT COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), produced_by, COALESCE(target_route::text, '{}'), COALESCE(target_set::text, '[]'), payload
		FROM events
		WHERE run_id = $1::uuid AND event_name = $2
	`, runID, eventName).Scan(&entityID, &flowInstance, &producedBy, &targetRoute, &targetSet, &payload); err != nil {
		t.Fatalf("load run.start event row: %v", err)
	}
	var eventID string
	if err := db.QueryRow(`SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`, runID, eventName).Scan(&eventID); err != nil {
		t.Fatalf("load run.start event identity: %v", err)
	}
	assertStoredEventProjectionMatchesDeliveries(t, entityID, flowInstance, targetRoute, targetSet, runID, postgresEventDeliveryTargetRoutes(t, db, eventID))
	if producedBy != "api.v1" {
		t.Fatalf("event produced_by = %q, want api.v1", producedBy)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	if _, ok := decoded["entity_id"]; ok {
		t.Fatalf("persisted payload must not carry envelope entity_id: %#v", decoded)
	}
	if decoded["topic"] != "medicine" {
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
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					eventName: {},
				},
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
