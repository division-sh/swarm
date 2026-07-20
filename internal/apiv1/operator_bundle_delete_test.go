package apiv1

import (
	"context"
	"database/sql"
	"testing"
	"time"

	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorBundleDeleteForceUsesOwnerChainAndIdempotency(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	idempotency := newRecordingAPIIdempotencyStore()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:          func() time.Time { return now },
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  idempotency,
			BundleDelete: executor,
		}),
	})

	body := `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `","force":true,"idempotency_key":"force-1"}}`
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("bundle.delete force error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != true || result["status"] != "completed" || result["bundle_hash"] != runStartTestBundleHash || result["force"] != true || result["deleted"] != true {
		t.Fatalf("bundle.delete force result = %#v", result)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete executor calls = %d, want 1", len(executor.calls))
	}
	if executor.calls[0].BundleHash != runStartTestBundleHash || !executor.calls[0].Force || executor.calls[0].DryRun {
		t.Fatalf("bundle.delete request = %#v", executor.calls[0])
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("bundle.delete replay error = %#v", replay.Error)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete executor calls after replay = %d, want unchanged 1", len(executor.calls))
	}

	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"dry_run":true,"idempotency_key":"force-1"}}`)
	if conflict.Error == nil {
		t.Fatal("bundle.delete idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("bundle.delete conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestOperatorBundleDeleteForceErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "missing bundle", err: store.ErrBundleNotFound, code: BundleNotFoundCode},
		{name: "busy", err: bundledelete.ErrOperationInProgress, code: BundleDeleteInProgressCode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash, err: tt.err}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Ready:        func() bool { return true },
					Database:     fakePinger{},
					Idempotency:  newRecordingAPIIdempotencyStore(),
					BundleDelete: executor,
				}),
			})

			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"idempotency_key":"force-error"}}`)
			if resp.Error == nil {
				t.Fatalf("bundle.delete %s error = nil", tt.name)
			}
			if data := asMap(t, resp.Error.Data); data["code"] != tt.code {
				t.Fatalf("bundle.delete %s data = %#v, want %s", tt.name, data, tt.code)
			}
			if len(executor.calls) != 1 {
				t.Fatalf("bundle.delete executor calls = %d, want 1", len(executor.calls))
			}
		})
	}
}

func TestOperatorBundleDeleteNonForceMissingBundleError(t *testing.T) {
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash, err: store.ErrBundleNotFound}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  newRecordingAPIIdempotencyStore(),
			BundleDelete: executor,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","idempotency_key":"non-force-missing"}}`)
	if resp.Error == nil {
		t.Fatal("bundle.delete missing bundle error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != BundleNotFoundCode {
		t.Fatalf("bundle.delete missing bundle data = %#v, want %s", data, BundleNotFoundCode)
	}
	if len(executor.calls) != 1 || executor.calls[0].Force {
		t.Fatalf("bundle.delete missing bundle request = %#v", executor.calls)
	}
}

func TestOperatorBundleDeleteDeactivatesLoadedRuntimeContext(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	manager := newBundleDeleteRuntimeContextManager(t)
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:             func() time.Time { return now },
			Ready:           func() bool { return true },
			Database:        fakePinger{},
			Idempotency:     newRecordingAPIIdempotencyStore(),
			RuntimeContexts: manager,
			BundleDelete:    executor,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","idempotency_key":"runtime-context-delete"}}`)
	if resp.Error != nil {
		t.Fatalf("bundle.delete error = %#v", resp.Error)
	}
	if result := asMap(t, resp.Result); result["deleted"] != true {
		t.Fatalf("bundle.delete result = %#v, want deleted", result)
	}
	lookup := manager.LookupBundleHashStatus(runStartTestBundleHash)
	if lookup.Loaded() || lookup.Cause != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("runtime context lookup after bundle.delete = %#v, want unloaded", lookup)
	}
}

func TestOperatorBundleDeleteForceDoesNotInferRuntimeDeactivationFromResult(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		result bundledelete.Result
	}{
		{
			name: "pre_cleanup_inventory_failure_stays_loaded",
			result: bundledelete.Result{
				OK:             false,
				Status:         "partial_failure",
				OperationName:  bundledelete.DefaultOperationName,
				BundleHash:     runStartTestBundleHash,
				Force:          true,
				PartialFailure: true,
				Errors:         []bundledelete.PartialError{{Scope: "managed_containers", Message: "inventory failed"}},
			},
		},
		{
			name: "cleanup_started_partial_failure_deactivates",
			result: bundledelete.Result{
				OK:             false,
				Status:         "partial_failure",
				OperationName:  bundledelete.DefaultOperationName,
				BundleHash:     runStartTestBundleHash,
				Force:          true,
				PartialFailure: true,
				Cleanup: preservationcleanup.Result{
					OperationName: preservationcleanup.BundleForceDeleteOperationName,
					AppliedAt:     now,
					ControlledBy:  preservationcleanup.BundleForceDeleteControlledBy,
				},
				Errors: []bundledelete.PartialError{{Scope: "managed_containers", Message: "stop failed"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newBundleDeleteRuntimeContextManager(t)
			executor := &staticBundleDeleteResultExecutor{result: tt.result}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now:             func() time.Time { return now },
					Ready:           func() bool { return true },
					Database:        fakePinger{},
					Idempotency:     newRecordingAPIIdempotencyStore(),
					RuntimeContexts: manager,
					BundleDelete:    executor,
				}),
			})

			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"idempotency_key":"runtime-context-partial-`+tt.name+`"}}`)
			if resp.Error != nil {
				t.Fatalf("bundle.delete partial error = %#v", resp.Error)
			}
			lookup := manager.LookupBundleHashStatus(runStartTestBundleHash)
			if !lookup.Loaded() {
				t.Fatalf("runtime context lookup after force result = %#v, want coordinator-owned quiescence only", lookup)
			}
		})
	}
}

func TestOperatorBundleDeleteNonForceUsesOwnerChainAndIdempotency(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	idempotency := newRecordingAPIIdempotencyStore()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:          func() time.Time { return now },
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  idempotency,
			BundleDelete: executor,
		}),
	})

	body := `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `","dry_run":true,"idempotency_key":"non-force-1"}}`
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("bundle.delete non-force error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != true || result["status"] != "dry_run" || result["bundle_hash"] != runStartTestBundleHash || result["force"] != false || result["dry_run"] != true {
		t.Fatalf("bundle.delete non-force result = %#v", result)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete owner calls = %d, want 1", len(executor.calls))
	}
	if executor.calls[0].BundleHash != runStartTestBundleHash || executor.calls[0].Force || !executor.calls[0].DryRun {
		t.Fatalf("bundle.delete non-force request = %#v", executor.calls[0])
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("bundle.delete non-force replay error = %#v", replay.Error)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("bundle.delete owner calls after replay = %d, want unchanged 1", len(executor.calls))
	}

	explicitFalse := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete-false","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":false,"dry_run":true,"idempotency_key":"non-force-false"}}`)
	if explicitFalse.Error != nil {
		t.Fatalf("bundle.delete explicit force false error = %#v", explicitFalse.Error)
	}
	if len(executor.calls) != 2 {
		t.Fatalf("bundle.delete owner calls after explicit force false = %d, want 2", len(executor.calls))
	}
	if executor.calls[1].BundleHash != runStartTestBundleHash || executor.calls[1].Force || !executor.calls[1].DryRun {
		t.Fatalf("bundle.delete explicit force false request = %#v", executor.calls[1])
	}
}

func TestOperatorBundleDeleteNonForceActiveRunsError(t *testing.T) {
	activeRunID := "00000000-0000-0000-0000-000000000181"
	executor := &recordingBundleDeleteExecutor{
		bundleHash: runStartTestBundleHash,
		err: &bundledelete.ActiveRunsRemainError{
			BundleHash: runStartTestBundleHash,
			ActiveRuns: []bundledelete.RunRef{{
				RunID:        activeRunID,
				Status:       "running",
				BundleHash:   runStartTestBundleHash,
				BundleSource: "persisted",
			}},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  newRecordingAPIIdempotencyStore(),
			BundleDelete: executor,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","idempotency_key":"non-force-active"}}`)
	if resp.Error == nil {
		t.Fatal("bundle.delete active-runs error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != BundleHasActiveRunsCode {
		t.Fatalf("bundle.delete active-runs data = %#v, want %s", data, BundleHasActiveRunsCode)
	}
	details := asMap(t, data["details"])
	activeIDs := asSlice(t, details["active_run_ids"])
	if len(activeIDs) != 1 || activeIDs[0] != activeRunID {
		t.Fatalf("bundle.delete active run ids = %#v, want %s", activeIDs, activeRunID)
	}
	if len(executor.calls) != 1 || executor.calls[0].Force {
		t.Fatalf("bundle.delete active owner calls = %#v", executor.calls)
	}
}

func TestOperatorBundleDeleteActiveRunsSentinelErrorOmitsRunRefs(t *testing.T) {
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash, err: bundledelete.ErrActiveRunsRemain}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  newRecordingAPIIdempotencyStore(),
			BundleDelete: executor,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","idempotency_key":"active-sentinel"}}`)
	if resp.Error == nil {
		t.Fatal("bundle.delete active-runs sentinel error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != BundleHasActiveRunsCode {
		t.Fatalf("bundle.delete active-runs sentinel data = %#v, want %s", data, BundleHasActiveRunsCode)
	}
	details := asMap(t, data["details"])
	activeIDs := asSlice(t, details["active_run_ids"])
	if len(activeIDs) != 0 {
		t.Fatalf("bundle.delete active run ids = %#v, want empty", activeIDs)
	}
	if _, ok := details["active_runs"]; ok {
		t.Fatalf("bundle.delete active_runs details = %#v, want omitted without run refs", details["active_runs"])
	}
}

func TestOperatorBundleDeleteBlocksPostDeleteNewWorkFromPersistedRuntimeSource(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deleteParams string
	}{
		{name: "non_force", deleteParams: `"bundle_hash":"` + runStartTestBundleHash + `","idempotency_key":"non-force-delete-current-runtime"`},
		{name: "force", deleteParams: `"bundle_hash":"` + runStartTestBundleHash + `","force":true,"idempotency_key":"force-delete-current-runtime"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			ctx := context.Background()
			seedOperatorBundleDeleteBundle(t, ctx, db, runStartTestBundleHash)
			source := semanticview.Wrap(runStartTestBundle("scan.requested"))
			sourceFact := runtimecorrelation.BundleSourceFact{
				BundleHash:        runStartTestBundleHash,
				BundleSource:      storerunlifecycle.BundleSourcePersisted,
				BundleFingerprint: runStartTestFingerprint,
			}
			bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
				ContractBundle:   source,
				BundleSourceFact: sourceFact,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			runtimeContexts := newBundleDeleteRuntimeContextManager(t)
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Now:              func() time.Time { return time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC) },
					Ready:            func() bool { return true },
					Database:         fakePinger{},
					Runs:             pg,
					Observability:    pg,
					Idempotency:      pg,
					Events:           bus,
					Source:           source,
					RunBundleContext: pg,
					RuntimeContexts:  runtimeContexts,
					Bundle: runtimecontracts.BundleIdentity{
						WorkflowName:    "review",
						WorkflowVersion: "1.0.0",
						Fingerprint:     runStartTestFingerprint,
					},
					BundleDelete: &bundledelete.Coordinator{
						Planner:            pg,
						Cleaner:            pg,
						Finalizer:          pg,
						Locks:              pg,
						ContainerInventory: emptyBundleDeleteContainerInventory{},
						Containers:         noopBundleDeleteContainers{},
						RuntimeQuiescer:    runtimeContexts,
						Now:                func() time.Time { return time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC) },
					},
				}),
			})

			deleted := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{`+tc.deleteParams+`}}`)
			if deleted.Error != nil {
				t.Fatalf("bundle.delete error = %#v", deleted.Error)
			}
			if result := asMap(t, deleted.Result); result["deleted"] != true {
				t.Fatalf("bundle.delete result = %#v, want deleted", result)
			}

			published := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "publish-after-delete"))
			assertBundleUnavailableNewWork(t, published, "event.publish")
			if count := countEventsByName(t, db, "scan.requested"); count != 0 {
				t.Fatalf("scan.requested events after event.publish = %d, want 0", count)
			}
			if count := countAllRunRows(t, db); count != 0 {
				t.Fatalf("run rows after event.publish = %d, want 0", count)
			}

			runID := uuid.NewString()
			started := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "start-after-delete"))
			assertBundleUnavailableNewWork(t, started, "run.start")
			if count := countRunRowsByID(t, db, runID); count != 0 {
				t.Fatalf("run rows after run.start = %d, want 0", count)
			}
			if count := countEventRowsByRunID(t, db, runID); count != 0 {
				t.Fatalf("event rows after run.start = %d, want 0", count)
			}
			if count := countAPIIdempotencyRows(t, db); count != 1 {
				t.Fatalf("api_idempotency rows after rejected new work = %d, want only bundle.delete row", count)
			}
		})
	}
}

func seedOperatorBundleDeleteBundle(t *testing.T, ctx context.Context, db *sql.DB, bundleHash string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, metadata, ingested_at)
		VALUES ($1, 'version: 1', '{}'::jsonb, '{}'::jsonb, now())
	`, bundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
}

func assertBundleUnavailableNewWork(t *testing.T, resp rpcResponse, method string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want BUNDLE_UNAVAILABLE", method)
	}
	if data := asMap(t, resp.Error.Data); data["code"] != BundleUnavailableCode {
		t.Fatalf("%s error data = %#v, want %s", method, data, BundleUnavailableCode)
	}
}

func newBundleDeleteRuntimeContextManager(t *testing.T) *swruntime.RuntimeContextManager {
	t.Helper()
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	workOwner := newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, runStartTestBundleHash)
	bus, err := newScopedAPITestEventBus(t, nil, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
		WorkOwner:        workOwner,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager, err := swruntime.NewRuntimeContextManager(nil, swruntime.BundleContext{
		BundleHash:       runStartTestBundleHash,
		BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
		BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
		Source:           source,
		Runtime:          &swruntime.Runtime{Bus: bus},
		WorkOwner:        workOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	return manager
}

type staticBundleDeleteResultExecutor struct {
	result bundledelete.Result
	calls  []bundledelete.Request
}

func (e *staticBundleDeleteResultExecutor) Execute(_ context.Context, req bundledelete.Request) (bundledelete.Result, error) {
	e.calls = append(e.calls, req)
	result := e.result
	if result.BundleHash == "" {
		result.BundleHash = req.BundleHash
	}
	result.Force = req.Force
	result.DryRun = req.DryRun
	return result, nil
}

type emptyBundleDeleteContainerInventory struct{}

func (emptyBundleDeleteContainerInventory) ManagedResetContainerInventory(context.Context) ([]destructivereset.ContainerRef, error) {
	return nil, nil
}

type noopBundleDeleteContainers struct{}

func (noopBundleDeleteContainers) Apply(_ context.Context, req destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error) {
	return destructivereset.ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
		Selected:      req.Result.Plan.EntityContainers,
	}, nil
}
