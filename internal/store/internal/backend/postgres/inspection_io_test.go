package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestInspectionSemanticRefusalDoesNotFabricateTransportFailure(t *testing.T) {
	dsn, _, _ := testutil.StartEmptyPostgres(t)
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := OpenForInspection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	refusal := &runlifecycle.RunNotFoundError{RunID: "00000000-0000-0000-0000-000000000001"}
	readErr := b.InspectSnapshot(ctx, func(snapshot context.Context) error {
		var value int
		if err := b.QueryRowContext(snapshot, `SELECT 1`).Scan(&value); err != nil {
			return err
		}
		return refusal
	})
	closeErr := b.Close()
	if !errors.Is(readErr, refusal) || closeErr != nil {
		t.Fatalf("healthy semantic refusal fabricated a transport error: read=%v close=%v", readErr, closeErr)
	}
}

func TestInspectionNativeReadCancellationJoinsTransactionAndDisposes(t *testing.T) {
	dsn, db, _ := testutil.StartEmptyPostgres(t)
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := OpenForInspection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := b.InspectSnapshot(ctx, func(snapshot context.Context) error {
		var readOnly bool
		var isolation string
		if err := b.QueryRowContext(snapshot, `SELECT pg_backend_pid(), current_setting('transaction_read_only')::boolean, current_setting('transaction_isolation')`).Scan(&pid, &readOnly, &isolation); err != nil {
			return err
		}
		if !readOnly || isolation != "repeatable read" {
			t.Fatalf("inspection lost its read purpose: %t %s", readOnly, isolation)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	bounded, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	started := time.Now()
	err = b.InspectSnapshot(bounded, func(snapshot context.Context) error {
		return b.QueryRowContext(snapshot, `SELECT pg_sleep(0.3)`).Scan(new(any))
	})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) >= time.Second {
		t.Fatalf("read deadline was only accounting: %v (%s)", err, time.Since(started))
	}
	if stats := b.db.Stats(); stats.OpenConnections != 0 || stats.InUse != 0 || stats.Idle != 0 {
		t.Fatalf("failed transport returned to the pool: %+v", stats)
	}
	for _, socket := range b.inspectionDialer.sockets {
		socket.mu.Lock()
		closed := socket.closed
		socket.mu.Unlock()
		if !closed {
			t.Fatal("read returned without joined native disposal")
		}
	}
	// PostgreSQL detects a client close after its current statement reaches an
	// I/O boundary, not necessarily while pg_sleep is executing.
	deadline := time.Now().Add(time.Second)
	for {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)`, pid).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled read retained a native session")
		}
		time.Sleep(time.Millisecond)
	}
}
