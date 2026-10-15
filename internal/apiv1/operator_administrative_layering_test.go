package apiv1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestAdministrativeOperationsLayerIdempotencyLeaseAndExternalWorkWithOneConnection(t *testing.T) {
	t.Run("runtime nuke", testRuntimeNukeLayeredPostgresCapacity)
}

func testRuntimeNukeLayeredPostgresCapacity(t *testing.T) {
	selected := storetest.StartPostgresRuntimeStore(t)
	if err := storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capability := acquireAdministrativeResetCapability(t, selected)

	external := newBlockingAdministrativeExternalWork()
	coordinator := &destructivereset.Coordinator{
		Operations: capability, RuntimeContexts: emptyAdministrativeResetLifecycle{},
		Planner:    destructivereset.InventoryPlanner{Reader: selected},
		Locks:      selected,
		Quiescer:   destructivereset.Quiescer{Store: selected},
		Cleaner:    destructivereset.Cleaner{Store: administrativeResetCleanup{capability}},
		Containers: external,
	}
	req := Request{
		Method: "runtime.nuke", ActorTokenID: "operator-layered", RequestHash: "runtime-nuke-hash",
		Params: map[string]any{"include_source_artifacts": false, "idempotency_key": "runtime-nuke-layered"},
	}
	result := make(chan error, 1)
	go func() {
		_, err := executeRuntimeNuke(ctx, req, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC())
		result <- err
	}()
	joined := false
	defer func() {
		cancel()
		external.release()
		if !joined {
			<-result
		}
	}()
	external.waitUntilBlocked(t, ctx)
	assertAdministrativeUnrelatedQueryProgresses(t, ctx, selected)
	external.release()
	err := <-result
	joined = true
	if err != nil {
		t.Fatalf("runtime.nuke layered execution: %v", err)
	}
	if _, err := executeRuntimeNuke(ctx, req, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC()); err != nil {
		t.Fatalf("runtime.nuke replay: %v", err)
	}
	if count := countAPIIdempotencyRows(t, selected); count != 1 {
		t.Fatalf("runtime.nuke idempotency rows = %d, want 1", count)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertAdministrativeCapacityReleased(t, selected)
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
		Selected:      append([]destructivereset.ContainerRef(nil), req.Result.Plan.ManagedContainers...),
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

func assertAdministrativeUnrelatedQueryProgresses(t *testing.T, ctx context.Context, selected any) {
	t.Helper()
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	value, err := storetest.ReadPostgresAdministrativeUnrelatedProgress(queryCtx, selected)
	if err != nil {
		t.Fatalf("unrelated database work while external work is blocked: %v", err)
	}
	if value != 1 {
		t.Fatalf("unrelated database result = %d, want 1", value)
	}
}

func assertAdministrativeCapacityReleased(t *testing.T, selected any) {
	t.Helper()
	if got := storetest.ObserveSelectedPool(t, selected).MaxOpenConnections; got != 1 {
		t.Fatalf("administrative retained capacity after completion = %d, want 1", got)
	}
}

func TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction(t *testing.T) {
	selected := storetest.StartPostgresRuntimeStore(t)
	if err := storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	surrounding, err := storetest.HoldPostgresAdministrativeSurroundingRead(ctx, selected)
	if err != nil {
		t.Fatalf("begin surrounding transaction: %v", err)
	}
	defer func() {
		if err := surrounding.Close(); err != nil {
			t.Errorf("join surrounding transaction rollback: %v", err)
		}
	}()
	lease, acquired, err := selected.AcquireDestructiveReset(ctx)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire operation lease with surrounding transaction: lease=%v acquired=%v err=%v", lease, acquired, err)
	}
	release := sync.OnceValue(func() error { return lease.Release(ctx) })
	defer func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	}()
	if err := release(); err != nil {
		t.Fatalf("release exact operation lease session: %v", err)
	}
	if _, err := surrounding.ReadAfterLeaseRelease(ctx); err != nil {
		t.Fatalf("surrounding transaction after lease release: %v", err)
	}
	if got := storetest.ObserveSelectedPool(t, selected).MaxOpenConnections; got != 1 {
		t.Fatalf("capacity after exact lease release = %d, want 1", got)
	}
}

func TestAdministrativeOperationCancellationReleasesLeaseAndCapacity(t *testing.T) {
	t.Run("runtime nuke", func(t *testing.T) {
		selected := storetest.StartPostgresRuntimeStore(t)
		if err := storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected); err != nil {
			t.Fatal(err)
		}
		capability := acquireAdministrativeResetCapability(t, selected)

		external := newBlockingAdministrativeExternalWork()
		coordinator := &destructivereset.Coordinator{
			Operations: capability, RuntimeContexts: emptyAdministrativeResetLifecycle{},
			Planner:    destructivereset.InventoryPlanner{Reader: selected},
			Locks:      selected,
			Quiescer:   destructivereset.Quiescer{Store: selected},
			Cleaner:    destructivereset.Cleaner{Store: administrativeResetCleanup{capability}},
			Containers: external,
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := executeRuntimeNuke(ctx, Request{
				Method: "runtime.nuke", ActorTokenID: "operator-cancel", RequestHash: "runtime-nuke-cancel",
				Params: map[string]any{"include_source_artifacts": false, "idempotency_key": "runtime-nuke-cancel"},
			}, RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}, time.Now().UTC())
			result <- err
		}()
		joined := false
		defer func() {
			cancel()
			external.release()
			if !joined {
				<-result
			}
		}()
		blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer blockedCancel()
		external.waitUntilBlocked(t, blockedCtx)
		cancel()
		err := <-result
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime.nuke cancellation error = %v, want context canceled", err)
		}
		if count := countAPIIdempotencyRows(t, selected); count != 0 {
			t.Fatalf("runtime.nuke cancellation idempotency rows = %d, want 0", count)
		}
		if err := capability.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertAdministrativeCapacityReleased(t, selected)
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
