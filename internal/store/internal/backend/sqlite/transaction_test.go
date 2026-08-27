package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestRunTransactionPreBusyPhasesUseCallerContext(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "prebusy.db"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- backend.RunTransaction(ctx, "slow admitted mutation", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			select {
			case <-holderRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	<-holderStarted

	queuedStarted := make(chan struct{})
	queuedDone := make(chan error, 1)
	go func() {
		queuedDone <- backend.RunTransaction(ctx, "queued mutation", func(context.Context, *sql.Tx) error {
			close(queuedStarted)
			return nil
		})
	}()
	waitForQueuedMutations(t, backend, 1)

	timer := time.NewTimer(mutationRetryBudget + 100*time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-holderDone:
		t.Fatalf("admitted non-busy mutation returned before release: %v", err)
	case err := <-queuedDone:
		t.Fatalf("queued pre-busy mutation returned before admission: %v", err)
	case <-timer.C:
	}
	select {
	case <-queuedStarted:
		t.Fatal("queued callback entered while the first mutation retained admission")
	default:
	}

	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatalf("admitted non-busy mutation: %v", err)
	}
	select {
	case <-queuedStarted:
	case <-ctx.Done():
		t.Fatalf("queued callback did not enter after admission release: %v", ctx.Err())
	}
	if err := <-queuedDone; err != nil {
		t.Fatalf("queued pre-busy mutation: %v", err)
	}
}

func TestRunTransactionQueuedCancellationPreventsCallbackEntry(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "queued-cancel.db"))
	holderCtx, holderCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer holderCancel()

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- backend.RunTransaction(holderCtx, "cancellation holder", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	<-holderStarted

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	var callbackEntries atomic.Int32
	queuedDone := make(chan error, 1)
	go func() {
		queuedDone <- backend.RunTransaction(queuedCtx, "cancelled queued mutation", func(context.Context, *sql.Tx) error {
			callbackEntries.Add(1)
			return nil
		})
	}()
	waitForQueuedMutations(t, backend, 1)
	cancelQueued()

	select {
	case err := <-queuedDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("queued cancellation did not return while holder retained admission")
	}
	if got := callbackEntries.Load(); got != 0 {
		t.Fatalf("cancelled queued callback entries = %d, want 0", got)
	}

	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatalf("release cancellation holder: %v", err)
	}
}

func TestAcquireMutationCancellationWinsTokenRace(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "token-race.db"))
	ctx := &cancelOnSecondErrContext{Context: context.Background(), done: make(chan struct{})}

	err := backend.acquireMutation(ctx, mutationOperation{label: "token race", phase: mutationPhaseFirstAttempt})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("token race error = %v, want context.Canceled", err)
	}
	if err := backend.acquireMutation(context.Background(), mutationOperation{label: "token retained", phase: mutationPhaseFirstAttempt}); err != nil {
		t.Fatalf("admission token was not returned after cancellation: %v", err)
	}
	backend.releaseMutation()
}

func TestRunTransactionRealBusyExhaustionRetainsBusyCause(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy-exhaustion.db")
	target := newTransactionTestBackend(t, path)
	holder := newTransactionTestBackend(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lockTx, err := holder.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback() }()
	if _, err := lockTx.ExecContext(ctx, `UPDATE mutation_probe SET value = value + 1 WHERE id = 1`); err != nil {
		t.Fatalf("acquire external SQLite write lock: %v", err)
	}

	var attempts atomic.Int32
	started := time.Now()
	err = target.RunTransaction(ctx, "real busy exhaustion", func(txCtx context.Context, tx *sql.Tx) error {
		attempts.Add(1)
		_, err := tx.ExecContext(txCtx, `UPDATE mutation_probe SET value = value + 1 WHERE id = 1`)
		return err
	})
	elapsed := time.Since(started)
	if err == nil || !mutationBusyError(err) {
		t.Fatalf("busy exhaustion error = %v, want retained SQLite busy cause", err)
	}
	if !strings.Contains(err.Error(), "retry budget 5s exceeded") || !strings.Contains(strings.ToLower(err.Error()), "locked") {
		t.Fatalf("busy exhaustion error = %v, want budget and actual lock cause", err)
	}
	if strings.Contains(err.Error(), "missing busy/locked cause") {
		t.Fatalf("busy exhaustion synthesized a cause: %v", err)
	}
	if elapsed < mutationRetryBudget || elapsed > mutationRetryBudget+2*time.Second {
		t.Fatalf("busy exhaustion elapsed = %s, want recovery budget near %s", elapsed, mutationRetryBudget)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("busy attempts = %d, want recovery retries", got)
	}
}

func TestRunTransactionPostBusyReadmissionUsesRecoveryBudget(t *testing.T) {
	t.Run("budget exhaustion retains first busy cause", func(t *testing.T) {
		backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "readmission-budget.db"))
		proof := startPostBusyReadmissionProof(t, backend)
		defer proof.releaseHolder()

		select {
		case err := <-proof.targetDone:
			if err == nil || !errors.Is(err, proof.busyCause) {
				t.Fatalf("post-busy re-admission error = %v, want retained cause %v", err, proof.busyCause)
			}
			if !strings.Contains(err.Error(), "retry budget 5s exceeded") {
				t.Fatalf("post-busy re-admission error = %v, want recovery budget exhaustion", err)
			}
		case <-time.After(mutationRetryBudget + 2*time.Second):
			t.Fatal("post-busy re-admission did not consume the active recovery budget")
		}
		if got := proof.attempts.Load(); got != 1 {
			t.Fatalf("target callback entries = %d, want no retry callback while admission is held", got)
		}
	})

	t.Run("caller cancellation wins re-admission", func(t *testing.T) {
		backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "readmission-cancel.db"))
		ctx, cancel := context.WithCancel(context.Background())
		proof := startPostBusyReadmissionProofWithContext(t, backend, ctx)
		defer proof.releaseHolder()
		cancel()

		select {
		case err := <-proof.targetDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("post-busy re-admission cancellation = %v, want context.Canceled", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("caller cancellation did not interrupt post-busy re-admission")
		}
		if got := proof.attempts.Load(); got != 1 {
			t.Fatalf("target callback entries = %d, want no callback after cancellation", got)
		}
	})
}

func TestMutationBackoffReturnsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- waitMutationBackoff(ctx, time.Now().Add(mutationRetryBudget), time.Second)
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("backoff cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("caller cancellation did not interrupt mutation backoff")
	}
}

func TestRunReadTransactionBypassesMutationAdmission(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "read-bypass.db"))
	backend.db.SetMaxOpenConns(2)

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- backend.RunTransaction(context.Background(), "blocked mutation", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	<-holderStarted

	readDone := make(chan error, 1)
	go func() {
		readDone <- backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			var value int
			return tx.QueryRowContext(ctx, `SELECT value FROM mutation_probe WHERE id = 1`).Scan(&value)
		})
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read while mutation admission held: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read consumed process-local mutation admission")
	}

	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatalf("release blocked mutation: %v", err)
	}
}

func TestRunReadTransactionPreservesSnapshotAndCallerCancellation(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "read-snapshot.db"))
	backend.db.SetMaxOpenConns(4)
	if _, err := backend.db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}

	err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var first int
		if err := tx.QueryRowContext(ctx, `SELECT value FROM mutation_probe WHERE id = 1`).Scan(&first); err != nil {
			return err
		}
		if err := backend.RunTransaction(ctx, "concurrent snapshot writer", func(writeCtx context.Context, writeTx *sql.Tx) error {
			_, err := writeTx.ExecContext(writeCtx, `UPDATE mutation_probe SET value = value + 1 WHERE id = 1`)
			return err
		}); err != nil {
			return err
		}
		var second int
		if err := tx.QueryRowContext(ctx, `SELECT value FROM mutation_probe WHERE id = 1`).Scan(&second); err != nil {
			return err
		}
		if second != first {
			return fmt.Errorf("read snapshot changed from %d to %d", first, second)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("consistent read transaction: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var entered atomic.Bool
	err = backend.RunReadTransaction(cancelled, func(context.Context, *sql.Tx) error {
		entered.Store(true)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read error = %v, want context.Canceled", err)
	}
	if entered.Load() {
		t.Fatal("cancelled read entered callback")
	}
}

type postBusyReadmissionProof struct {
	busyCause     error
	targetDone    <-chan error
	attempts      *atomic.Int32
	releaseOnce   sync.Once
	holderRelease chan struct{}
	holderDone    <-chan error
}

func startPostBusyReadmissionProof(t *testing.T, backend *Backend) *postBusyReadmissionProof {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return startPostBusyReadmissionProofWithContext(t, backend, ctx)
}

func startPostBusyReadmissionProofWithContext(t *testing.T, backend *Backend, targetCtx context.Context) *postBusyReadmissionProof {
	t.Helper()
	busyCause := errors.New("database is locked: deterministic post-busy proof")
	firstAttempt := make(chan struct{})
	returnBusy := make(chan struct{})
	var attempts atomic.Int32
	targetDone := make(chan error, 1)
	go func() {
		targetDone <- backend.RunTransaction(targetCtx, "post-busy target", func(context.Context, *sql.Tx) error {
			if attempts.Add(1) == 1 {
				close(firstAttempt)
				<-returnBusy
				return busyCause
			}
			return nil
		})
	}()
	<-firstAttempt

	holderCtx, holderCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(holderCancel)
	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- backend.RunTransaction(holderCtx, "post-busy holder", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	waitForQueuedMutations(t, backend, 1)
	close(returnBusy)
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("queued holder did not acquire admission after first busy result")
	}
	waitForQueuedMutations(t, backend, 1)

	return &postBusyReadmissionProof{
		busyCause: busyCause, targetDone: targetDone, attempts: &attempts,
		holderRelease: holderRelease, holderDone: holderDone,
	}
}

func (p *postBusyReadmissionProof) releaseHolder() {
	p.releaseOnce.Do(func() {
		close(p.holderRelease)
		<-p.holderDone
	})
}

func newTransactionTestBackend(t *testing.T, path string) *Backend {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(1)")
	if err != nil {
		t.Fatalf("open SQLite test database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS mutation_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL); INSERT OR IGNORE INTO mutation_probe (id, value) VALUES (1, 0)`); err != nil {
		t.Fatalf("prepare SQLite mutation probe: %v", err)
	}
	backend, err := New(db)
	if err != nil {
		t.Fatalf("construct SQLite backend: %v", err)
	}
	return backend
}

func waitForQueuedMutations(t *testing.T, backend *Backend, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if queuedMutationCount(backend) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued mutation count = %d, want %d", queuedMutationCount(backend), want)
}

func queuedMutationCount(backend *Backend) int {
	backend.mutationState.Lock()
	defer backend.mutationState.Unlock()
	return backend.mutationState.waiting
}

type cancelOnSecondErrContext struct {
	context.Context
	done  chan struct{}
	calls atomic.Int32
	once  sync.Once
}

func (c *cancelOnSecondErrContext) Done() <-chan struct{} {
	return c.done
}

func (c *cancelOnSecondErrContext) Err() error {
	if c.calls.Add(1) == 1 {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.Canceled
}
