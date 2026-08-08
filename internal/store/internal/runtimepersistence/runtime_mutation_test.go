package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestSQLiteRuntimeStore_PrivateMutationRetriesBusy(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, time.Millisecond)
	ctx, cancel := context.WithTimeout(storeTestWorkContext(t, testAuthorActivityContext()), 2*time.Second)
	defer cancel()

	lockTx, err := lockStore.backend.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	lockCommitted := false
	t.Cleanup(func() {
		if !lockCommitted {
			_ = lockTx.Rollback()
		}
	})
	if err := writeRuntimeMutationTestMarker(ctx, lockTx); err != nil {
		t.Fatalf("acquire sqlite write lock: %v", err)
	}

	busySeen := make(chan struct{})
	var closeBusy sync.Once
	var attempts int32
	done := make(chan error, 1)
	go func() {
		done <- store.runRuntimeMutation(ctx, "sqlite runtime mutation proof", func(txctx context.Context, tx *sql.Tx) error {
			if tx == nil {
				return errors.New("runtime mutation transaction is required")
			}
			atomic.AddInt32(&attempts, 1)
			err := writeRuntimeMutationTestMarker(txctx, tx)
			if sqliteRuntimeMutationBusyError(err) {
				closeBusy.Do(func() { close(busySeen) })
			}
			return err
		})
	}()

	select {
	case <-busySeen:
	case <-ctx.Done():
		t.Fatalf("wait for deterministic sqlite busy attempt: %v", ctx.Err())
	}
	if err := lockTx.Commit(); err != nil {
		t.Fatalf("release sqlite write lock: %v", err)
	}
	lockCommitted = true

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("private mutation after lock release: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for retried runtime mutation: %v", ctx.Err())
	}
	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Fatalf("attempts = %d, want retry after busy", got)
	}
}

func TestSQLiteRuntimeStore_PrivateMutationStopsRetryOnContextDeadline(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, time.Millisecond)
	baseCtx := testAuthorActivityContext()

	lockTx, err := lockStore.backend.BeginTx(baseCtx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })
	if err := writeRuntimeMutationTestMarker(baseCtx, lockTx); err != nil {
		t.Fatalf("acquire sqlite write lock: %v", err)
	}

	// The deadline is derived from the retry budget rather than an arbitrary
	// wall-time margin: the production retry deadline clamps to the context
	// deadline (so the deadline always wins structurally), and a budget-derived
	// value far below the 5s budget but far above the first attempt's busy
	// cost leaves no starvation window before the first retry attempt (#1658).
	ctx, cancel := context.WithTimeout(baseCtx, sqliteRuntimeMutationRetryBudget/20)
	defer cancel()
	var attempts int32
	err = store.runRuntimeMutation(ctx, "sqlite runtime mutation deadline proof", func(txctx context.Context, tx *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		return writeRuntimeMutationTestMarker(txctx, tx)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunRuntimeMutation error = %v, want context deadline", err)
	}
	if got := atomic.LoadInt32(&attempts); got == 0 {
		t.Fatal("expected at least one busy attempt before context deadline")
	}
}

func TestSQLiteRuntimeStore_PrivateMutationContextDeadlineCapsDriverBusyTimeout(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, 50*time.Millisecond)
	baseCtx := testAuthorActivityContext()

	lockTx, err := lockStore.backend.BeginTx(baseCtx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })
	if err := writeRuntimeMutationTestMarker(baseCtx, lockTx); err != nil {
		t.Fatalf("acquire sqlite write lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = store.runRuntimeMutation(ctx, "sqlite runtime mutation deadline proof", func(txctx context.Context, tx *sql.Tx) error {
		return writeRuntimeMutationTestMarker(txctx, tx)
	})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunRuntimeMutation error = %v, want context deadline", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("elapsed = %s, want context deadline to cap sqlite busy_timeout", elapsed)
	}
}

func TestPostgresStore_RunEventTransactionSerializesStoryCommitOrder(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := admitTestPostgresStore(t, db)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
	defer cancel()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- store.runEventTransaction(ctx, func(context.Context, *sql.Tx) error {
			close(firstStarted)
			select {
			case <-firstRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatalf("first postgres story transaction did not start: %v", ctx.Err())
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- store.runEventTransaction(ctx, func(context.Context, *sql.Tx) error {
			close(secondStarted)
			return nil
		})
	}()
	select {
	case <-secondStarted:
		t.Fatal("second postgres story callback entered before the first committed")
	case <-time.After(100 * time.Millisecond):
	}

	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first RunEventTransaction: %v", err)
	}
	select {
	case <-secondStarted:
	case <-ctx.Done():
		t.Fatalf("second postgres story callback did not start after first commit: %v", ctx.Err())
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second RunEventTransaction: %v", err)
	}
}

func newSQLiteRuntimeMutationBusyStores(t *testing.T, busyTimeout time.Duration) (*SQLiteRuntimeStore, *SQLiteRuntimeStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.db")
	store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	lockStore := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	for _, db := range []*sql.DB{store.backend.ConstructionHandle(), lockStore.backend.ConstructionHandle()} {
		if _, err := db.ExecContext(testAuthorActivityContext(), fmt.Sprintf("PRAGMA busy_timeout = %d", int(busyTimeout/time.Millisecond))); err != nil {
			t.Fatalf("set sqlite busy_timeout: %v", err)
		}
	}
	return store, lockStore
}

func writeRuntimeMutationTestMarker(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE runtime_store_metadata
		SET created_at = created_at
		WHERE id = 1
	`)
	return err
}
