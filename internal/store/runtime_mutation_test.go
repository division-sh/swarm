package store

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

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStore_RunRuntimeMutationRetriesBusyAndFlushesPostCommitOnce(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, time.Millisecond)
	ctx, cancel := context.WithTimeout(storeTestWorkContext(t, testAuthorActivityContext()), 2*time.Second)
	defer cancel()

	lockTx, err := lockStore.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	lockCommitted := false
	t.Cleanup(func() {
		if !lockCommitted {
			_ = lockTx.Rollback()
		}
	})
	if _, err := lockTx.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatalf("hold sqlite write lock: %v", err)
	}

	busySeen := make(chan struct{})
	var closeBusy sync.Once
	var attempts int32
	var postCommitActions int32
	done := make(chan error, 1)
	go func() {
		done <- store.RunRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
			if tx == nil {
				return errors.New("runtime mutation transaction is required")
			}
			atomic.AddInt32(&attempts, 1)
			if !runtimepipeline.QueuePipelinePostCommitAction(txctx, func(context.Context) {
				atomic.AddInt32(&postCommitActions, 1)
			}) {
				return errors.New("queue pipeline post-commit action")
			}
			_, err := tx.ExecContext(txctx, `
				INSERT INTO runs (run_id, status, started_at)
				VALUES (?, 'running', ?)
			`, uuid.NewString(), time.Now().UTC())
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
			t.Fatalf("RunRuntimeMutation after lock release: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for retried runtime mutation: %v", ctx.Err())
	}
	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Fatalf("attempts = %d, want retry after busy", got)
	}
	if got := atomic.LoadInt32(&postCommitActions); got != 1 {
		t.Fatalf("post-commit actions = %d, want only successful attempt flushed", got)
	}
}

func TestSQLiteRuntimeStore_RunRuntimeMutationStopsRetryOnContextDeadline(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, time.Millisecond)
	baseCtx := testAuthorActivityContext()

	lockTx, err := lockStore.DB.BeginTx(baseCtx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })
	if _, err := lockTx.ExecContext(baseCtx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatalf("hold sqlite write lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 35*time.Millisecond)
	defer cancel()
	var attempts int32
	err = store.RunRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		_, err := tx.ExecContext(txctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, uuid.NewString(), time.Now().UTC())
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunRuntimeMutation error = %v, want context deadline", err)
	}
	if got := atomic.LoadInt32(&attempts); got == 0 {
		t.Fatal("expected at least one busy attempt before context deadline")
	}
}

func TestSQLiteRuntimeStore_RunRuntimeMutationContextDeadlineCapsDriverBusyTimeout(t *testing.T) {
	store, lockStore := newSQLiteRuntimeMutationBusyStores(t, 50*time.Millisecond)
	baseCtx := testAuthorActivityContext()

	lockTx, err := lockStore.DB.BeginTx(baseCtx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })
	if _, err := lockTx.ExecContext(baseCtx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatalf("hold sqlite write lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = store.RunRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, uuid.NewString(), time.Now().UTC())
		return err
	})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunRuntimeMutation error = %v, want context deadline", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("elapsed = %s, want context deadline to cap sqlite busy_timeout", elapsed)
	}
}

func TestSQLiteRuntimeStore_RunRuntimeMutationDoesNotRetryActiveTransaction(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := testAuthorActivityContext()
	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32
	err = store.RunRuntimeMutation(runtimepipeline.WithPipelineSQLTxContext(ctx, tx), func(_ context.Context, gotTx *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		if gotTx != tx {
			t.Fatalf("transaction = %#v, want active transaction", gotTx)
		}
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("RunRuntimeMutation error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry inside active transaction", got)
	}
}

func TestSQLiteRuntimeStore_RunRuntimeMutationPostCommitCanReenterRuntimeMutation(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx, cancel := context.WithTimeout(storeTestWorkContext(t, testAuthorActivityContext()), 2*time.Second)
	defer cancel()

	innerDone := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- store.RunRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO runs (run_id, status, started_at)
				VALUES (?, 'running', ?)
			`, uuid.NewString(), time.Now().UTC()); err != nil {
				return err
			}
			if !runtimepipeline.QueuePipelinePostCommitAction(txctx, func(context.Context) {
				innerCtx := runtimepipeline.WithoutPipelineSQLTxContext(ctx)
				innerDone <- store.RunRuntimeMutation(innerCtx, func(innerTxCtx context.Context, innerTx *sql.Tx) error {
					_, err := innerTx.ExecContext(innerTxCtx, `
						INSERT INTO runs (run_id, status, started_at)
						VALUES (?, 'running', ?)
					`, uuid.NewString(), time.Now().UTC())
					return err
				})
			}) {
				return errors.New("queue pipeline post-commit action")
			}
			return nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunRuntimeMutation with re-entrant post-commit action: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("post-commit runtime mutation re-entry did not complete before context deadline: %v", ctx.Err())
	}

	select {
	case err := <-innerDone:
		if err != nil {
			t.Fatalf("re-entrant RunRuntimeMutation from post-commit action: %v", err)
		}
	default:
		t.Fatal("post-commit action did not run")
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
	for _, db := range []*sql.DB{store.DB, lockStore.DB} {
		if _, err := db.ExecContext(testAuthorActivityContext(), fmt.Sprintf("PRAGMA busy_timeout = %d", int(busyTimeout/time.Millisecond))); err != nil {
			t.Fatalf("set sqlite busy_timeout: %v", err)
		}
	}
	return store, lockStore
}
