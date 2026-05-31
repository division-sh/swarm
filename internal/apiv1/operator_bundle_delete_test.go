package apiv1

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/bundledelete"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/destructivereset"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
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

func TestOperatorBundleDeleteNonForceFailsClosedBeforeOwner(t *testing.T) {
	executor := &recordingBundleDeleteExecutor{bundleHash: runStartTestBundleHash}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:        func() bool { return true },
			Database:     fakePinger{},
			Idempotency:  newRecordingAPIIdempotencyStore(),
			BundleDelete: executor,
		}),
	})

	for _, body := range []string{
		`{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `"}}`,
		`{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"` + runStartTestBundleHash + `","force":false}}`,
	} {
		resp := rpcCall(t, handler, body)
		if resp.Error == nil {
			t.Fatal("bundle.delete non-force error = nil")
		}
		if resp.Error.Code != codeInvalidParams {
			t.Fatalf("bundle.delete non-force code = %d, want %d", resp.Error.Code, codeInvalidParams)
		}
	}
	if len(executor.calls) != 0 {
		t.Fatalf("bundle.delete owner calls after non-force = %d, want 0", len(executor.calls))
	}
}

func TestOperatorBundleDeleteForceBlocksPostDeleteNewWorkFromPersistedRuntimeSource(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	seedOperatorBundleDeleteBundle(t, ctx, db, runStartTestBundleHash)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        runStartTestBundleHash,
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: runStartTestFingerprint,
	}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
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
				Now:                func() time.Time { return time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC) },
			},
		}),
	})

	deleted := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"delete","method":"bundle.delete","params":{"bundle_hash":"`+runStartTestBundleHash+`","force":true,"idempotency_key":"force-delete-current-runtime"}}`)
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
