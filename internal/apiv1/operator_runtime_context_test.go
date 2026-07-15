package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const runtimeContextTestBundleHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const runtimeContextTestBundleHashC = "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func TestOperatorRuntimeContextManagerRoutesCreateNewWorkToSelectedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	chPrimary := fixture.busA.Subscribe("scan-orchestrator", events.EventType("triage.requested"))
	defer fixture.busA.Unsubscribe("scan-orchestrator")
	chSelected := fixture.busB.Subscribe("scan-orchestrator", events.EventType("triage.requested"))
	defer fixture.busB.Unsubscribe("scan-orchestrator")

	published := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashB, "triage.requested", `{"topic":"context-b"}`, "", "idem-context-publish"))
	if published.Error != nil {
		t.Fatalf("event.publish error = %#v", published.Error)
	}
	publishedResult := asMap(t, published.Result)
	publishedRunID := stringValue(t, publishedResult["run_id"], "run_id")
	publishedEventID := stringValue(t, publishedResult["event_id"], "event_id")
	assertRunBundleIdentity(t, fixture.db, publishedRunID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")
	if got := countEventsByName(t, fixture.db, "triage.requested"); got != 1 {
		t.Fatalf("triage.requested count after event.publish = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEvent(t, chSelected, "selected context delivery")
	if got.ID() != publishedEventID {
		t.Fatalf("selected context delivered event = %s, want %s", got.ID(), publishedEventID)
	}
	requireNoAPIV1RuntimeBusEvent(t, chPrimary, "primary context selected bundle route")

	runID := uuid.NewString()
	started := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runtimeContextTestBundleHashB, "triage.requested", `{"topic":"context-b-start"}`, "idem-context-start"))
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	assertRunBundleIdentity(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")
	if got := countEventsByName(t, fixture.db, "triage.requested"); got != 2 {
		t.Fatalf("triage.requested count after run.start = %d, want 2", got)
	}
}

func TestOperatorRuntimeContextManagerRoutesExistingRunByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	chSelected := fixture.busB.Subscribe("scan-orchestrator", events.EventType("triage.requested"))
	defer fixture.busB.Unsubscribe("scan-orchestrator")
	runID := uuid.NewString()
	started := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runtimeContextTestBundleHashB, "triage.requested", `{"topic":"seed-existing"}`, "idem-existing-seed"))
	if started.Error != nil {
		t.Fatalf("seed run.start error = %#v", started.Error)
	}
	got := requireAPIV1RuntimeBusEvent(t, chSelected, "seed existing-run delivery")
	if got.RunID() != runID {
		t.Fatalf("seed existing-run delivery run = %s, want %s", got.RunID(), runID)
	}

	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"publish-existing","method":"event.publish","params":{"run_id":%q,"event_name":"triage.requested","payload":{"topic":"existing-run"},"idempotency_key":"idem-existing-context"}}`,
		runID,
	)
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("event.publish existing run error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("event.publish existing run deliveries = %#v, want typed agent and node rows", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
	if got := countEventRowsByRunID(t, fixture.db, runID); got != 2 {
		t.Fatalf("event rows for existing run = %d, want 2", got)
	}
	assertRunBundleIdentity(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")
	got = requireAPIV1RuntimeBusEvent(t, chSelected, "existing-run selected-context delivery")
	if got.ID() != eventID || got.RunID() != runID {
		t.Fatalf("existing-run delivery id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), eventID, runID)
	}
}

func TestOperatorRuntimeContextManagerRejectsExistingRunUnavailableSourceStates(t *testing.T) {
	tests := []struct {
		name          string
		bundleHash    string
		bundleSource  string
		fingerprint   string
		seedBundleRow bool
		wantCode      string
		wantCause     string
	}{
		{
			name:         "deleted",
			bundleHash:   runtimeContextTestBundleHashB,
			bundleSource: storerunlifecycle.BundleSourceDeleted,
			fingerprint:  runStartTestFingerprint,
			wantCode:     BundleUnavailableCode,
			wantCause:    storerunlifecycle.BundleSourceDeleted,
		},
		{
			name:         "legacy",
			bundleHash:   "",
			bundleSource: storerunlifecycle.BundleSourceLegacy,
			fingerprint:  runStartTestFingerprint,
			wantCode:     BundleUnavailableCode,
			wantCause:    storerunlifecycle.BundleSourceLegacy,
		},
		{
			name:         "persisted missing bundle row",
			bundleHash:   runtimeContextTestBundleHashC,
			bundleSource: storerunlifecycle.BundleSourcePersisted,
			wantCode:     BundleDataIntegrityErrorCode,
			wantCause:    "persisted_missing_bundle_row",
		},
		{
			name:          "persisted unloaded context",
			bundleHash:    runtimeContextTestBundleHashC,
			bundleSource:  storerunlifecycle.BundleSourcePersisted,
			seedBundleRow: true,
			wantCode:      BundleUnavailableCode,
			wantCause:     "runtime_context_not_loaded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newOperatorRuntimeContextFixture(t)
			if tt.seedBundleRow {
				seedOperatorBundleDeleteBundle(t, context.Background(), fixture.db, tt.bundleHash)
			}
			handler := fixture.handler(t)
			runID := uuid.NewString()
			seedRuntimeContextRunBundle(t, fixture.db, runID, tt.bundleHash, tt.bundleSource, tt.fingerprint)
			keyName := strings.ReplaceAll(tt.name, " ", "-")

			calls := []struct {
				method string
				body   string
			}{
				{
					method: "event.publish",
					body:   eventPublishExistingRunBody(runID, "", "idem-context-publish-"+keyName),
				},
				{
					method: "run.start",
					body:   runStartBodyWithoutBundle(runID, "triage.requested", `{"topic":"blocked"}`, "idem-context-start-"+keyName),
				},
			}
			for _, call := range calls {
				resp := rpcCall(t, handler, call.body)
				assertRuntimeContextBundleError(t, resp, call.method, tt.wantCode, tt.wantCause)
				if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
					t.Fatalf("%s event rows for unavailable run = %d, want 0", call.method, got)
				}
			}
		})
	}
}

func TestOperatorRuntimeContextManagerRejectsExistingRunRequestedHashMismatch(t *testing.T) {
	tests := []struct {
		method string
		body   func(string) string
	}{
		{
			method: "event.publish",
			body: func(runID string) string {
				return eventPublishExistingRunBody(runID, runStartTestBundleHash, "idem-context-publish-mismatch")
			},
		},
		{
			method: "run.start",
			body: func(runID string) string {
				return runStartBodyWithBundleHash(runID, runStartTestBundleHash, "triage.requested", `{"topic":"mismatch"}`, "idem-context-start-mismatch")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			fixture := newOperatorRuntimeContextFixture(t)
			handler := fixture.handler(t)
			runID := uuid.NewString()
			seedRuntimeContextRunBundle(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")

			resp := rpcCall(t, handler, tt.body(runID))
			assertRuntimeContextBundleError(t, resp, tt.method, BundleMismatchCode, "")
			if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
				t.Fatalf("%s event rows for mismatched run = %d, want 0", tt.method, got)
			}
		})
	}
}

func TestOperatorRuntimeContextManagerRoutesEventReplayByOriginalRunBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	ctx := testAuthorActivityContext(context.Background())
	seedActiveAPIV1RuntimeBusAgent(t, ctx, fixture.pg, "agent-a")
	chPrimary := fixture.busA.Subscribe("agent-a")
	defer fixture.busA.Unsubscribe("agent-a")
	chSelected := fixture.busB.Subscribe("agent-a")
	defer fixture.busB.Unsubscribe("agent-a")
	original := seedReplayableOperatorEvent(t, ctx, fixture.pg, "triage.requested", []string{"agent-a"}, eventReplayStatusDelivered)
	seedRuntimeContextRunBundle(t, fixture.db, original.RunID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")

	resp := rpcCall(t, handler, eventReplayBody(original.EventID, []string{"agent-a"}, "idem-context-event-replay"))
	if resp.Error != nil {
		t.Fatalf("event.replay error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	replayEventID := stringValue(t, result["replay_event_id"], "replay_event_id")
	auditEventID := stringValue(t, result["audit_event_id"], "audit_event_id")
	assertReplayEventDelivered(t, chSelected, replayEventID, original.EventID)
	assertNoReplayEvent(t, chPrimary)
	assertReplayPersistence(t, fixture.db, original.EventID, replayEventID, auditEventID, 1)
}

func TestOperatorRuntimeContextManagerRoutesRunControlByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	baseStore := &recordingRuntimeContextRunControlStore{}
	selectedStore := &recordingRuntimeContextRunControlStore{}
	baseControl := runtimeruncontrol.NewController(baseStore, nil, runtimeruncontrol.Options{})
	selectedControl := runtimeruncontrol.NewController(selectedStore, nil, runtimeruncontrol.Options{})
	manager := runtimeContextManagerWithRuntimes(t, fixture,
		&swruntime.Runtime{Bus: fixture.busA, RunControl: baseControl},
		&swruntime.Runtime{Bus: fixture.busB, RunControl: selectedControl},
	)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Idempotency:      fixture.pg,
			RunBundleContext: fixture.pg,
			RunControl:       baseControl,
			RuntimeContexts:  manager,
		}),
	})

	for _, method := range []string{"run.pause", "run.continue", "run.stop"} {
		runID := uuid.NewString()
		seedRuntimeContextRunBundle(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")
		resp := rpcCall(t, handler, runControlBody(method, runID, "idem-context-"+method))
		if resp.Error != nil {
			t.Fatalf("%s error = %#v", method, resp.Error)
		}
	}
	if baseStore.totalCalls() != 0 {
		t.Fatalf("base run control calls = %d, want 0", baseStore.totalCalls())
	}
	if selectedStore.pauseCalls != 1 || selectedStore.continueCalls != 1 || selectedStore.stopCalls != 1 {
		t.Fatalf("selected run control calls pause/continue/stop = %d/%d/%d, want 1/1/1", selectedStore.pauseCalls, selectedStore.continueCalls, selectedStore.stopCalls)
	}
}

func TestOperatorRuntimeContextManagerRoutesAgentDirectiveByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	baseAgent := &directiveIntegrationAgent{id: "agent-1"}
	selectedAgent := &directiveIntegrationAgent{id: "agent-1"}
	baseManager := runtimeContextTestAgentManager(t, fixture.pg, fixture.busA, baseAgent, runtimeContextTestSourceFact(runStartTestBundleHash))
	selectedManager := runtimeContextTestAgentManager(t, fixture.pg, fixture.busB, selectedAgent, runtimeContextTestSourceFact(runtimeContextTestBundleHashB))
	manager := runtimeContextManagerWithRuntimes(t, fixture,
		&swruntime.Runtime{Bus: fixture.busA, Manager: baseManager},
		&swruntime.Runtime{Bus: fixture.busB, Manager: selectedManager},
	)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Idempotency:      fixture.pg,
			RunBundleContext: fixture.pg,
			AgentControl:     baseManager,
			RuntimeContexts:  manager,
		}),
	})
	runID := uuid.NewString()
	seedRuntimeContextRunBundle(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")

	resp := rpcCall(t, handler, agentDirectiveBodyWithRun("agent-1", runID, "inspect context", "idem-context-agent-directive"))
	if resp.Error != nil {
		t.Fatalf("agent.send_directive error = %#v", resp.Error)
	}
	if selectedAgent.calls != 1 {
		t.Fatalf("selected agent calls = %d, want 1", selectedAgent.calls)
	}
	if baseAgent.calls != 0 {
		t.Fatalf("base agent calls = %d, want 0", baseAgent.calls)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	resp := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashC, "triage.requested", `{"topic":"missing"}`, "", "idem-unloaded-context"))
	if resp.Error == nil {
		t.Fatal("event.publish unloaded bundle error = nil")
	}
	data := asMap(t, resp.Error.Data)
	details := asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != "runtime_context_not_loaded" {
		t.Fatalf("event.publish unloaded bundle error data = %#v", data)
	}
	if got := countAllRunRows(t, fixture.db); got != 0 {
		t.Fatalf("run rows after unloaded bundle = %d, want 0", got)
	}

	executor := &recordingRunForkExecutor{}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-unloaded-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashC,
	))
	if forkResp.Error == nil {
		t.Fatal("run.fork unloaded target error = nil")
	}
	forkData := asMap(t, forkResp.Error.Data)
	forkDetails := asMap(t, forkData["details"])
	if forkData["code"] != BundleUnavailableCode || forkDetails["cause"] != "runtime_context_not_loaded" {
		t.Fatalf("run.fork unloaded target error data = %#v", forkData)
	}
	if executor.calls != 0 {
		t.Fatalf("run.fork executor calls = %d, want 0 for unloaded target", executor.calls)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	result := fixture.manager.DeactivateBundleHash(runtimeContextTestBundleHashB, swruntime.RuntimeContextCauseUnloaded)
	if !result.Found || !result.Changed {
		t.Fatalf("DeactivateBundleHash result = %#v, want changed", result)
	}
	handler := fixture.handler(t)

	explicit := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashB, "triage.requested", `{"topic":"deactivated"}`, "", "idem-deactivated-context"))
	if explicit.Error == nil {
		t.Fatal("event.publish deactivated explicit hash error = nil")
	}
	data := asMap(t, explicit.Error.Data)
	details := asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("event.publish deactivated explicit hash error data = %#v", data)
	}
	if got := countAllRunRows(t, fixture.db); got != 0 {
		t.Fatalf("run rows after explicit deactivated bundle = %d, want 0", got)
	}

	runID := uuid.NewString()
	seedRuntimeContextRunBundle(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted, "")
	existing := rpcCall(t, handler, eventPublishBodyWithoutBundle(runID, "triage.requested", `{"topic":"existing-deactivated"}`, "", "idem-existing-deactivated-context"))
	if existing.Error == nil {
		t.Fatal("event.publish deactivated run context error = nil")
	}
	data = asMap(t, existing.Error.Data)
	details = asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("event.publish deactivated run context error data = %#v", data)
	}
	if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
		t.Fatalf("event rows for deactivated existing run = %d, want 0", got)
	}

	executor := &recordingRunForkExecutor{}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-deactivated-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashB,
	))
	if forkResp.Error == nil {
		t.Fatal("run.fork deactivated target error = nil")
	}
	forkData := asMap(t, forkResp.Error.Data)
	forkDetails := asMap(t, forkData["details"])
	if forkData["code"] != BundleUnavailableCode || forkDetails["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("run.fork deactivated target error data = %#v", forkData)
	}
	if executor.calls != 0 {
		t.Fatalf("run.fork executor calls = %d, want 0 for deactivated target", executor.calls)
	}
}

func TestRunForkExecutorForBundleContextRebindsSelectedContractSelection(t *testing.T) {
	primarySource := semanticview.Wrap(runStartTestBundle("scan.requested"))
	targetBundle := runStartTestBundle("triage.requested")
	targetBundle.Semantics.Name = "target-review"
	targetBundle.Semantics.Version = "2.0.0"
	targetSource := semanticview.Wrap(targetBundle)
	executor := SelectedContractRunForkExecutor{
		ContractSelection: store.RunForkContractSelection{
			Mode:            store.RunForkContractSelectionModeSelectedContracts,
			ContractsRoot:   "/tmp/primary-contracts",
			WorkflowName:    primarySource.WorkflowName(),
			WorkflowVersion: primarySource.WorkflowVersion(),
		},
	}

	rebound := runForkExecutorForBundleContext(executor, &swruntime.BundleContext{
		Source:        targetSource,
		ContractsRoot: "/tmp/target-contracts",
		Runtime:       &swruntime.Runtime{},
	})

	selected, ok := rebound.(SelectedContractRunForkExecutor)
	if !ok {
		t.Fatalf("rebound executor type = %T, want SelectedContractRunForkExecutor", rebound)
	}
	if selected.ContractSelection.Mode != store.RunForkContractSelectionModeSelectedContracts ||
		selected.ContractSelection.ContractsRoot != "/tmp/target-contracts" ||
		selected.ContractSelection.WorkflowName != "target-review" ||
		selected.ContractSelection.WorkflowVersion != "2.0.0" {
		t.Fatalf("rebound contract selection = %#v", selected.ContractSelection)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForAmbiguousRuntimeConsumers(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	ingress := &recordingRuntimeIngress{}
	runtimeHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:             func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RuntimeIngress:  ingress,
			Idempotency:     newMutatingProbeIdempotencyStore(),
			RuntimeContexts: fixture.manager,
		}),
	})
	for _, method := range []string{"runtime.pause", "runtime.resume"} {
		runtimeResp := rpcCall(t, runtimeHandler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"runtime-control","method":%q,"params":{"idempotency_key":%q}}`, method, "idem-"+method))
		if runtimeResp.Error == nil {
			t.Fatalf("%s error = nil", method)
		}
		if data := asMap(t, runtimeResp.Error.Data); data["code"] != BundleScopeRequiredCode {
			t.Fatalf("%s error data = %#v, want %s", method, data, BundleScopeRequiredCode)
		}
	}
	if ingress.called {
		t.Fatal("runtime control called singleton ingress in multi-context mode")
	}

	executor := &recordingRunForkExecutor{
		result: RunForkExecutionResult{
			Owner:              "runtime.run_fork.selected_contract_execution",
			SourceRunID:        runForkTestSourceRunID,
			SourceRunStatus:    store.RunForkSourceFrozenStatus,
			SourceFrozen:       true,
			ForkRunID:          runForkTestForkRunID,
			ForkEventID:        runForkTestEventID,
			ForkRunStatus:      "running",
			ExecutedEventCount: 1,
		},
	}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashB,
	))
	if forkResp.Error != nil {
		t.Fatalf("run.fork error = %#v", forkResp.Error)
	}
	if executor.calls != 1 {
		t.Fatalf("run.fork executor calls = %d, want 1", executor.calls)
	}
	if executor.last.BundleHash != runtimeContextTestBundleHashB ||
		!executor.last.ConfirmSourceFreeze ||
		executor.last.ContractSelection.Mode != store.RunForkContractSelectionModeBundleHash ||
		executor.last.ContractSelection.BundleHash != runtimeContextTestBundleHashB {
		t.Fatalf("run.fork executor request = %#v", executor.last)
	}

	agentControl := &fakeAgentControlController{}
	agentHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:             func() time.Time { return time.Unix(1700000000, 0).UTC() },
			AgentControl:    agentControl,
			Idempotency:     newMutatingProbeIdempotencyStore(),
			RuntimeContexts: fixture.manager,
		}),
	})
	for _, method := range []string{"agent.restart", "agent.replay_backlog"} {
		resp := rpcCall(t, agentHandler, agentControlBody(method, "agent-1", "idem-"+method))
		if resp.Error == nil {
			t.Fatalf("%s error = nil", method)
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleScopeRequiredCode {
			t.Fatalf("%s error data = %#v, want %s", method, data, BundleScopeRequiredCode)
		}
	}
	if agentControl.restartCalls != 0 || agentControl.replayCalls != 0 {
		t.Fatalf("agent singleton calls restart/replay = %d/%d, want 0/0", agentControl.restartCalls, agentControl.replayCalls)
	}

}

type operatorRuntimeContextFixture struct {
	db      *sql.DB
	pg      *store.PostgresStore
	sourceA semanticview.Source
	sourceB semanticview.Source
	busA    *runtimebus.EventBus
	busB    *runtimebus.EventBus
	manager *swruntime.RuntimeContextManager
}

func newOperatorRuntimeContextFixture(t *testing.T) operatorRuntimeContextFixture {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := testAuthorActivityContext(context.Background())
	seedOperatorBundleDeleteBundle(t, ctx, db, runStartTestBundleHash)
	seedOperatorBundleDeleteBundle(t, ctx, db, runtimeContextTestBundleHashB)
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")

	sourceA := semanticview.Wrap(runStartTestBundle("scan.requested"))
	sourceB := semanticview.Wrap(runStartTestBundle("triage.requested"))
	busA := newRuntimeContextTestBus(t, pg, sourceA, runStartTestBundleHash)
	busB := newRuntimeContextTestBus(t, pg, sourceB, runtimeContextTestBundleHashB)
	manager, err := swruntime.NewRuntimeContextManager(pg,
		swruntime.BundleContext{
			BundleHash:       runStartTestBundleHash,
			BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           sourceA,
			Runtime:          runtimeContextTestRuntime(&swruntime.Runtime{Bus: busA}, runStartTestBundleHash),
		},
		swruntime.BundleContext{
			BundleHash:       runtimeContextTestBundleHashB,
			BundleSourceFact: runtimeContextTestSourceFact(runtimeContextTestBundleHashB),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           sourceB,
			Runtime:          runtimeContextTestRuntime(&swruntime.Runtime{Bus: busB}, runtimeContextTestBundleHashB),
		},
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	return operatorRuntimeContextFixture{
		db:      db,
		pg:      pg,
		sourceA: sourceA,
		sourceB: sourceB,
		busA:    busA,
		busB:    busB,
		manager: manager,
	}
}

func (f operatorRuntimeContextFixture) handler(t *testing.T) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             f.pg,
			Observability:    f.pg,
			Idempotency:      f.pg,
			Events:           f.busA,
			Source:           f.sourceA,
			RunBundleContext: f.pg,
			RuntimeContexts:  f.manager,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				Fingerprint:     runStartTestFingerprint,
			},
		}),
	})
}

func seedRuntimeContextRunBundle(t *testing.T, db *sql.DB, runID, bundleHash, bundleSource, fingerprint string) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
		VALUES ($1::uuid, 'running', NULLIF($2, ''), $3, NULLIF($4, ''))
		ON CONFLICT (run_id) DO UPDATE SET
			bundle_hash = EXCLUDED.bundle_hash,
			bundle_source = EXCLUDED.bundle_source,
			bundle_fingerprint = EXCLUDED.bundle_fingerprint
	`, runID, strings.TrimSpace(bundleHash), strings.TrimSpace(bundleSource), strings.TrimSpace(fingerprint)); err != nil {
		t.Fatalf("seed runtime context run bundle: %v", err)
	}
}

func runtimeContextManagerWithRuntimes(t *testing.T, fixture operatorRuntimeContextFixture, runtimeA, runtimeB *swruntime.Runtime) *swruntime.RuntimeContextManager {
	t.Helper()
	runtimeA = runtimeContextTestRuntime(runtimeA, runStartTestBundleHash)
	runtimeB = runtimeContextTestRuntime(runtimeB, runtimeContextTestBundleHashB)
	manager, err := swruntime.NewRuntimeContextManager(fixture.pg,
		swruntime.BundleContext{
			BundleHash:       runStartTestBundleHash,
			BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           fixture.sourceA,
			Runtime:          runtimeA,
		},
		swruntime.BundleContext{
			BundleHash:       runtimeContextTestBundleHashB,
			BundleSourceFact: runtimeContextTestSourceFact(runtimeContextTestBundleHashB),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           fixture.sourceB,
			Runtime:          runtimeB,
		},
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager with runtimes: %v", err)
	}
	return manager
}

func runtimeContextTestAgentManager(t *testing.T, pg *store.PostgresStore, bus *runtimebus.EventBus, agent *directiveIntegrationAgent, fact runtimecorrelation.BundleSourceFact) *runtimemanager.AgentManager {
	t.Helper()
	manager := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{BaseContext: testAuthorActivityContextForSource(context.Background(), fact)}, pg)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id, Model: "regular"}); err != nil {
		t.Fatalf("SpawnAgent(%s): %v", agent.id, err)
	}
	return manager
}

func runtimeContextTestRuntime(rt *swruntime.Runtime, bundleHash string) *swruntime.Runtime {
	if rt == nil {
		return nil
	}
	rt.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	rt.Options.BundleSourceFact = runtimeContextTestSourceFact(bundleHash)
	return rt
}

func eventPublishExistingRunBody(runID, bundleHash, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"run_id":%q`, runID),
		`"event_name":"triage.requested"`,
		`"payload":{"topic":"blocked"}`,
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(bundleHash) != "" {
		parts = append(parts, fmt.Sprintf(`"bundle_hash":%q`, bundleHash))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func assertRuntimeContextBundleError(t *testing.T, resp rpcResponse, method, wantCode, wantCause string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want %s", method, wantCode)
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != wantCode {
		t.Fatalf("%s error data = %#v, want code %s", method, data, wantCode)
	}
	if strings.TrimSpace(wantCause) == "" {
		return
	}
	details := asMap(t, data["details"])
	if details["cause"] != wantCause {
		t.Fatalf("%s error details = %#v, want cause %s", method, details, wantCause)
	}
}

type recordingRuntimeContextRunControlStore struct {
	stopCalls     int
	pauseCalls    int
	continueCalls int
}

func (s *recordingRuntimeContextRunControlStore) StopRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.stopCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusCancelled, ControlStatus: runtimeruncontrol.StatusStopped}, nil
}

func (s *recordingRuntimeContextRunControlStore) PauseRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.pauseCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusPaused, ControlStatus: runtimeruncontrol.StatusPaused}, nil
}

func (s *recordingRuntimeContextRunControlStore) ContinueRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.continueCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusRunning, ControlStatus: runtimeruncontrol.StatusRunning}, nil
}

func (*recordingRuntimeContextRunControlStore) RunDispatchBlocked(context.Context, string) (bool, error) {
	return false, nil
}

func (s *recordingRuntimeContextRunControlStore) totalCalls() int {
	return s.stopCalls + s.pauseCalls + s.continueCalls
}

func newRuntimeContextTestBus(t *testing.T, pg *store.PostgresStore, source semanticview.Source, bundleHash string) *runtimebus.EventBus {
	t.Helper()
	bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runtimeContextTestSourceFact(bundleHash),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return bus
}

func runtimeContextTestSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	return runtimecorrelation.BundleSourceFact{
		BundleHash:   strings.TrimSpace(bundleHash),
		BundleSource: storerunlifecycle.BundleSourcePersisted,
	}
}

type recordingRuntimeIngress struct {
	called bool
}

func (r *recordingRuntimeIngress) Pause(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	r.called = true
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusPaused}, nil
}

func (r *recordingRuntimeIngress) Resume(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	r.called = true
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusRunning}, nil
}
