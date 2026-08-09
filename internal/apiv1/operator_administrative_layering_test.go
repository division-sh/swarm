package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestAdministrativeOperationsLayerIdempotencyLeaseAndExternalWorkWithOneConnection(t *testing.T) {
	t.Run("bundle delete", testBundleDeleteLayeredPostgresCapacity)
	t.Run("runtime nuke", testRuntimeNukeLayeredPostgresCapacity)
}

func testBundleDeleteLayeredPostgresCapacity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seedOperatorBundleDeleteBundle(t, ctx, db, runStartTestBundleHash)

	external := newBlockingAdministrativeExternalWork()
	coordinator := &bundledelete.Coordinator{
		Planner: selected, Cleaner: selected, Finalizer: selected, Locks: selected,
		ContainerInventory: external,
		Containers:         noopBundleDeleteContainers{},
		RuntimeQuiescer:    noopBundleRuntimeQuiescer{},
	}
	req := Request{
		Method: "bundle.delete", ActorTokenID: "operator-layered", RequestHash: "bundle-delete-hash",
		Params: map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "idempotency_key": "bundle-delete-layered"},
	}
	result := make(chan error, 1)
	go func() {
		_, err := executeBundleDelete(ctx, req, BundleDeleteHandlerOptions{Executor: coordinator, Idempotency: selected}, time.Now().UTC())
		result <- err
	}()
	external.waitUntilBlocked(t, ctx)
	assertAdministrativeUnrelatedQueryProgresses(t, ctx, db)
	external.release()
	if err := <-result; err != nil {
		t.Fatalf("bundle.delete layered execution: %v", err)
	}
	assertAdministrativeCapacityReleased(t, db)

	if _, err := executeBundleDelete(ctx, req, BundleDeleteHandlerOptions{Executor: coordinator, Idempotency: selected}, time.Now().UTC()); err != nil {
		t.Fatalf("bundle.delete replay: %v", err)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("bundle.delete idempotency rows = %d, want 1", count)
	}
}

func testRuntimeNukeLayeredPostgresCapacity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	external := newBlockingAdministrativeExternalWork()
	coordinator := &destructivereset.Coordinator{
		Planner:    destructivereset.InventoryPlanner{Reader: selected},
		Locks:      selected,
		Quiescer:   destructivereset.Quiescer{Store: selected},
		Cleaner:    destructivereset.Cleaner{Store: selected},
		Containers: external,
	}
	req := Request{
		Method: "runtime.nuke", ActorTokenID: "operator-layered", RequestHash: "runtime-nuke-hash",
		Params: map[string]any{"include_bundles": false, "idempotency_key": "runtime-nuke-layered"},
	}
	result := make(chan error, 1)
	go func() {
		_, err := executeRuntimeNuke(ctx, req, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC())
		result <- err
	}()
	external.waitUntilBlocked(t, ctx)
	assertAdministrativeUnrelatedQueryProgresses(t, ctx, db)
	external.release()
	if err := <-result; err != nil {
		t.Fatalf("runtime.nuke layered execution: %v", err)
	}
	assertAdministrativeCapacityReleased(t, db)

	if _, err := executeRuntimeNuke(ctx, req, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC()); err != nil {
		t.Fatalf("runtime.nuke replay: %v", err)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("runtime.nuke idempotency rows = %d, want 1", count)
	}
}

type blockingAdministrativeExternalWork struct {
	started chan struct{}
	resume  chan struct{}
}

func newBlockingAdministrativeExternalWork() *blockingAdministrativeExternalWork {
	return &blockingAdministrativeExternalWork{started: make(chan struct{}), resume: make(chan struct{})}
}

func (w *blockingAdministrativeExternalWork) ManagedResetContainerInventory(ctx context.Context) ([]destructivereset.ContainerRef, error) {
	return nil, w.block(ctx)
}

func (w *blockingAdministrativeExternalWork) Apply(ctx context.Context, req destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error) {
	if err := w.block(ctx); err != nil {
		return destructivereset.ContainerResetResult{}, err
	}
	return destructivereset.ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
		Selected:      append([]destructivereset.ContainerRef(nil), req.Result.Plan.EntityContainers...),
	}, nil
}

func (w *blockingAdministrativeExternalWork) block(ctx context.Context) error {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.resume:
		return nil
	}
}

func (w *blockingAdministrativeExternalWork) waitUntilBlocked(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatalf("administrative external work was not reached: %v", ctx.Err())
	case <-w.started:
	}
}

func (w *blockingAdministrativeExternalWork) release() {
	select {
	case <-w.resume:
	default:
		close(w.resume)
	}
}

type noopBundleRuntimeQuiescer struct{}

func (noopBundleRuntimeQuiescer) QuiesceBundleRuntime(context.Context, string) error { return nil }

func assertAdministrativeUnrelatedQueryProgresses(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var value int
	if err := db.QueryRowContext(queryCtx, `SELECT 1`).Scan(&value); err != nil {
		t.Fatalf("unrelated database work while external work is blocked: %v", err)
	}
	if value != 1 {
		t.Fatalf("unrelated database result = %d, want 1", value)
	}
}

func assertAdministrativeCapacityReleased(t *testing.T, db *sql.DB) {
	t.Helper()
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("administrative retained capacity after completion = %d, want 1", got)
	}
}

func TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin surrounding transaction: %v", err)
	}
	defer tx.Rollback()
	lease, acquired, err := selected.AcquireDestructiveReset(ctx)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire operation lease with surrounding transaction: lease=%v acquired=%v err=%v", lease, acquired, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release exact operation lease session: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatalf("surrounding transaction after lease release: %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("capacity after exact lease release = %d, want 1", got)
	}
}

func TestAdministrativeOperationCancellationReleasesLeaseAndCapacity(t *testing.T) {
	t.Run("bundle delete", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		db.SetMaxOpenConns(1)
		seedOperatorBundleDeleteBundle(t, context.Background(), db, runStartTestBundleHash)

		external := newBlockingAdministrativeExternalWork()
		coordinator := &bundledelete.Coordinator{
			Planner: selected, Cleaner: selected, Finalizer: selected, Locks: selected,
			ContainerInventory: external,
			Containers:         noopBundleDeleteContainers{},
			RuntimeQuiescer:    noopBundleRuntimeQuiescer{},
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := executeBundleDelete(ctx, Request{
				Method: "bundle.delete", ActorTokenID: "operator-cancel", RequestHash: "bundle-delete-cancel",
				Params: map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "idempotency_key": "bundle-delete-cancel"},
			}, BundleDeleteHandlerOptions{Executor: coordinator, Idempotency: selected}, time.Now().UTC())
			result <- err
		}()
		blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer blockedCancel()
		external.waitUntilBlocked(t, blockedCtx)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("bundle.delete cancellation error = %v, want context canceled", err)
		}
		if count := countAPIIdempotencyRows(t, db); count != 0 {
			t.Fatalf("bundle.delete cancellation idempotency rows = %d, want 0", count)
		}
		assertAdministrativeCapacityReleased(t, db)
		leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer leaseCancel()
		lease, acquired, err := selected.AcquireBundleDelete(leaseCtx)
		if err != nil || !acquired || lease == nil {
			t.Fatalf("reacquire bundle.delete lease after cancellation: lease=%v acquired=%v err=%v", lease, acquired, err)
		}
		if err := lease.Release(leaseCtx); err != nil {
			t.Fatalf("release reacquired bundle.delete lease: %v", err)
		}
	})

	t.Run("runtime nuke", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		db.SetMaxOpenConns(1)

		external := newBlockingAdministrativeExternalWork()
		coordinator := &destructivereset.Coordinator{
			Planner:    destructivereset.InventoryPlanner{Reader: selected},
			Locks:      selected,
			Quiescer:   destructivereset.Quiescer{Store: selected},
			Cleaner:    destructivereset.Cleaner{Store: selected},
			Containers: external,
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := executeRuntimeNuke(ctx, Request{
				Method: "runtime.nuke", ActorTokenID: "operator-cancel", RequestHash: "runtime-nuke-cancel",
				Params: map[string]any{"include_bundles": false, "idempotency_key": "runtime-nuke-cancel"},
			}, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC())
			result <- err
		}()
		blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer blockedCancel()
		external.waitUntilBlocked(t, blockedCtx)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime.nuke cancellation error = %v, want context canceled", err)
		}
		if count := countAPIIdempotencyRows(t, db); count != 0 {
			t.Fatalf("runtime.nuke cancellation idempotency rows = %d, want 0", count)
		}
		assertAdministrativeCapacityReleased(t, db)
		leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer leaseCancel()
		lease, acquired, err := selected.AcquireDestructiveReset(leaseCtx)
		if err != nil || !acquired || lease == nil {
			t.Fatalf("reacquire runtime.nuke lease after cancellation: lease=%v acquired=%v err=%v", lease, acquired, err)
		}
		if err := lease.Release(leaseCtx); err != nil {
			t.Fatalf("release reacquired runtime.nuke lease: %v", err)
		}
	})
}
