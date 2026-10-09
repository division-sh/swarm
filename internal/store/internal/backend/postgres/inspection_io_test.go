package postgres

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestInspectionSemanticRefusalDoesNotFabricateTransportFailure(t *testing.T) {
	for _, refusal := range []error{
		&runlifecycle.RunNotFoundError{RunID: "00000000-0000-0000-0000-000000000001"},
		errors.New("semantic contract refusal"),
	} {
		t.Run(refusal.Error(), func(t *testing.T) {
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
			var firstPID, secondPID int
			readErr := b.InspectSnapshot(ctx, func(snapshot context.Context) error {
				var value int
				if err := b.QueryRowContext(snapshot, `SELECT 1, pg_backend_pid()`).Scan(&value, &firstPID); err != nil {
					return err
				}
				return refusal
			})
			nextErr := b.InspectSnapshot(ctx, func(snapshot context.Context) error {
				var value int
				if err := b.QueryRowContext(snapshot, `SELECT 2, pg_backend_pid()`).Scan(&value, &secondPID); err != nil {
					return err
				}
				if value != 2 || firstPID != secondPID {
					return errors.New("healthy refusal replaced its native session")
				}
				return nil
			})
			closeErr := b.Close()
			if !errors.Is(readErr, refusal) || !failures.OnlyBranches(readErr, func(branch error) bool { return errors.Is(refusal, branch) }) || nextErr != nil || closeErr != nil {
				t.Fatalf("healthy semantic refusal fabricated a transport error: read=%v next=%v close=%v", readErr, nextErr, closeErr)
			}
		})
	}
}

func TestInspectionNativeFailureDisposesReadPool(t *testing.T) {
	for _, boundary := range []string{"query_read", "query_write", "rollback_read", "rollback_write", "cleanup_bind", "close"} {
		t.Run(boundary, func(t *testing.T) {
			dsn, _, _ := testutil.StartEmptyPostgres(t)
			cfg, err := pq.NewConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			b, err := OpenForInspection(cfg)
			if err != nil {
				t.Fatal(err)
			}
			witness := errors.New("native inspection " + boundary)
			b.inspectionDialer.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &possessionFailureSocket{Conn: conn, boundary: boundary, witness: witness}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			semantic := errors.New("semantic observation refused")
			err = b.InspectSnapshot(ctx, func(snapshot context.Context) error {
				var value int
				if err := b.QueryRowContext(snapshot, `SELECT 1`).Scan(&value); err != nil {
					return errors.Join(semantic, err)
				}
				if boundary == "rollback_read" || boundary == "rollback_write" {
					return semantic
				}
				return nil
			})
			if boundary == "close" {
				err = errors.Join(err, b.Close())
			}
			if !errors.Is(err, witness) {
				t.Fatalf("native cause lost: %v", err)
			}
			if (boundary == "query_read" || boundary == "query_write" || boundary == "rollback_read" || boundary == "rollback_write") && !errors.Is(err, semantic) {
				t.Fatalf("mixed semantic cause lost: %v", err)
			}
			if stats := b.db.Stats(); stats.OpenConnections != 0 || stats.InUse != 0 || stats.Idle != 0 {
				t.Fatalf("unhealthy read pool remained usable: %+v", stats)
			}
			for _, socket := range b.inspectionDialer.sockets {
				socket.mu.Lock()
				closed := socket.closed
				socket.mu.Unlock()
				if !closed {
					t.Fatal("native disposal was not joined")
				}
			}
			closeErr := b.Close()
			if boundary == "close" {
				if !errors.Is(closeErr, witness) {
					t.Fatalf("real close failure was hidden: %v", closeErr)
				}
			} else if closeErr != nil {
				t.Fatalf("disposed pool close fabricated another failure: %v", closeErr)
			}
		})
	}
}

func TestInspectionRejectsOverlapAndPreservesCleanupDeadline(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(strconv.FormatBool(expiry), func(t *testing.T) {
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
			if err := b.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			semantic := errors.New("semantic refusal")
			err = b.withInspectionIO(ctx, func(_ context.Context, settle func() error) error {
				if err := b.Ping(ctx); err == nil {
					t.Fatal("overlapping inspection entered its native phase")
				}
				if err := settle(); err != nil {
					return err
				}
				if expiry {
					b.inspectionDialer.mu.Lock()
					cleanup := b.inspectionDialer.ctx
					b.inspectionDialer.mu.Unlock()
					<-cleanup.Done()
				}
				return semantic
			})
			if !errors.Is(err, semantic) || errors.Is(err, context.DeadlineExceeded) != expiry {
				t.Fatalf("cleanup disposition lost: %v", err)
			}
			if expiry && b.db.Stats().OpenConnections != 0 {
				t.Fatal("cleanup deadline left an idle native connection")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		})
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
