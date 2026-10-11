package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestTransactionProbeIncludesConnectionPoolWait(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	db.SetMaxOpenConns(1)
	backend, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	collector, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var one int
			return tx.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
		})
	}()
	<-started
	time.Sleep(25 * time.Millisecond)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	counts := collector.Snapshot().Total
	if counts.WriteCommits != 1 || counts.PermitWait != 0 || counts.PoolWait < 20*time.Millisecond ||
		counts.BeginDuration <= 0 || counts.CleanupDuration <= 0 {
		t.Fatalf("transaction phase receipt = %+v", counts)
	}
}

func TestOrderingRetainedRollbackMeasuresPhysicalCut(t *testing.T) {
	backend, exits := newExitProbe(t)
	exits.rollbackEntered, exits.rollbackRelease = make(chan struct{}), make(chan struct{})
	collector, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := backend.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.NewSessionAuthority(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	primary := errors.New("owned rollback")
	done := make(chan error, 1)
	joined := false
	var once sync.Once
	release := func() { once.Do(func() { close(exits.rollbackRelease) }) }
	defer func() {
		release()
		if !joined {
			<-done
		}
	}()
	go func() {
		_, err := RunAuthorityTransactionOutcome(ctx, session, func(ctx context.Context, tx *sql.Tx) error {
			if err := authoractivity.FenceMutationOrder(ctx, tx, authoractivity.DialectPostgres); err != nil {
				return err
			}
			return primary
		})
		done <- err
	}()
	select {
	case <-exits.rollbackEntered:
	case err := <-done:
		joined = true
		t.Fatalf("native rollback never entered: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Ordering-context retirement has already happened. Keep the real native
	// rollback outstanding so a context-cleanup cut cannot satisfy this receipt.
	const hold = 30 * time.Millisecond
	time.Sleep(hold)
	release()
	err = <-done
	joined = true
	if err != primary {
		t.Fatalf("rollback changed original error identity: %v", err)
	}
	counts := collector.Snapshot().Retained.Ordering
	if counts.AcquisitionAttempts != 1 || counts.Acquired != 1 || counts.RolledBackHolders != 1 ||
		counts.UnresolvedHolders != 0 || counts.RollbackHoldDuration < hold {
		t.Fatalf("holder ended before physical rollback: %+v", counts)
	}
}
