package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	modernc "modernc.org/sqlite"
)

func TestOwnershipProofRetainsIndependentPoolCapacity(t *testing.T) {
	for _, limit := range []int{1, 4, 0} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			backend, ctx := newOwnershipProofTestBackend(t, limit)
			collector, uninstall, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer uninstall()
			proof, err := backend.RetainOwnershipProof(ctx)
			if err != nil {
				t.Fatalf("retain ownership proof: %v", err)
			}
			t.Cleanup(func() { _ = proof.Close() })
			assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 1), 1)

			ordinaryCount := limit
			if ordinaryCount == 0 {
				ordinaryCount = 4
			}
			ordinary := holdOwnershipProofOrdinaryConnections(t, backend, ctx, ordinaryCount)
			assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 1), ordinaryCount+1)
			readOwnershipProofValue(t, proof, ctx, 0)
			if _, err := ordinary[0].ExecContext(ctx, "UPDATE mutation_probe SET value = ? WHERE id = ?", 17, 1); err != nil {
				t.Fatalf("change data through an ordinary connection: %v", err)
			}
			readOwnershipProofValue(t, proof, ctx, 17)
			canceledRead, cancelRead := context.WithCancel(ctx)
			cancelRead()
			var canceledValue int
			if err := proof.QueryRowContext(canceledRead, "SELECT value FROM mutation_probe WHERE id = ?", 1).Scan(&canceledValue); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled proof read = %v, want context.Canceled", err)
			}
			readOwnershipProofValue(t, proof, ctx, 17)
			assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 1), ordinaryCount+1)
			if got := collector.Snapshot().Total.BeginAttempts; got != 0 {
				t.Fatalf("proof reads began %d transactions, want no read transaction", got)
			}

			if err := proof.Close(); err != nil {
				t.Fatalf("close ownership proof: %v", err)
			}
			assertOwnershipProofCapacity(t, backend, limit, ordinaryCount)
			var value int
			if err := proof.QueryRowContext(ctx, "SELECT value FROM mutation_probe WHERE id = ?", 1).Scan(&value); err == nil {
				t.Fatal("closed ownership proof still reads")
			}
			if _, err := ordinary[0].ExecContext(ctx, "UPDATE mutation_probe SET value = ? WHERE id = ?", 23, 1); err != nil {
				t.Fatalf("ordinary connection stopped working after proof close: %v", err)
			}
		})
	}
}

func TestOwnershipProofReservationsRestoreCapacity(t *testing.T) {
	for _, limit := range []int{1, 4, 0} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			backend, ctx := newOwnershipProofTestBackend(t, limit)
			for round := 0; round < 3; round++ {
				first, err := backend.RetainOwnershipProof(ctx)
				if err != nil {
					t.Fatalf("round %d first reservation: %v", round, err)
				}
				t.Cleanup(func() { _ = first.Close() })
				second, err := backend.RetainOwnershipProof(ctx)
				if err != nil {
					t.Fatalf("round %d second reservation: %v", round, err)
				}
				t.Cleanup(func() { _ = second.Close() })
				assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 2), 2)
				if err := first.Close(); err != nil {
					t.Fatalf("round %d close first reservation: %v", round, err)
				}
				assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 1), 1)
				readOwnershipProofValue(t, second, ctx, 0)
				third, err := backend.RetainOwnershipProof(ctx)
				if err != nil {
					t.Fatalf("round %d replacement reservation: %v", round, err)
				}
				t.Cleanup(func() { _ = third.Close() })
				assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 2), 2)
				if err := second.Close(); err != nil {
					t.Fatalf("round %d close second reservation: %v", round, err)
				}
				_ = first.Close()
				assertOwnershipProofCapacity(t, backend, ownershipProofLimit(limit, 1), 1)
				readOwnershipProofValue(t, third, ctx, 0)
				if err := third.Close(); err != nil {
					t.Fatalf("round %d close replacement reservation: %v", round, err)
				}
				assertOwnershipProofCapacity(t, backend, limit, 0)
			}
		})
	}
}

func TestOwnershipProofAcquisitionFailureRestoresCapacity(t *testing.T) {
	for _, failure := range []string{"caller-canceled", "database-closed", "backend-closed"} {
		t.Run(failure, func(t *testing.T) {
			backend, ctx := newOwnershipProofTestBackend(t, 4)
			wantCanceled := false
			switch failure {
			case "caller-canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantCanceled = true
			case "database-closed":
				if err := backend.db.Close(); err != nil {
					t.Fatal(err)
				}
			case "backend-closed":
				if err := backend.Close(); err != nil {
					t.Fatal(err)
				}
			}
			proof, err := backend.RetainOwnershipProof(ctx)
			if proof != nil {
				_ = proof.Close()
				t.Fatal("failed acquisition returned an ownership proof")
			}
			if err == nil || (wantCanceled && !errors.Is(err, context.Canceled)) {
				t.Fatalf("failed acquisition error = %v, want failure preserving caller cancellation when present", err)
			}
			assertOwnershipProofCapacity(t, backend, 4, 0)
		})
	}
}

func TestOwnershipProofDeadConnectionDoesNotFallback(t *testing.T) {
	backend, ctx := newOwnershipProofTestBackend(t, 1)
	proof, err := backend.RetainOwnershipProof(ctx)
	if err != nil {
		t.Fatalf("retain ownership proof: %v", err)
	}
	t.Cleanup(func() { _ = proof.Close() })
	if err := proof.conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("retire physical proof connection: %v", err)
	}
	if err := backend.Ping(ctx); err != nil {
		t.Fatalf("ordinary pool is not available to distinguish fallback: %v", err)
	}
	var value int
	if err := proof.QueryRowContext(ctx, "SELECT value FROM mutation_probe WHERE id = ?", 1).Scan(&value); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("dead proof read = %v, want sql.ErrConnDone without fallback", err)
	}
	if err := backend.QueryRowContext(ctx, "SELECT value FROM mutation_probe WHERE id = ?", 1).Scan(&value); err != nil || value != 0 {
		t.Fatalf("ordinary pool read = (%d, %v), want (0, nil)", value, err)
	}
	if err := proof.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("close retired proof: %v", err)
	}
	assertOwnershipProofCapacity(t, backend, 1, 0)
}

func TestOwnershipProofBackendCloseDisposesReservations(t *testing.T) {
	for _, limit := range []int{1, 4, 0} {
		for _, concurrent := range []bool{false, true} {
			t.Run(fmt.Sprintf("limit=%d/concurrent=%t", limit, concurrent), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				connector := &ownershipProofBarrierConnector{
					path:       "file:" + filepath.Join(t.TempDir(), "close-order.db"),
					poolClosed: make(chan struct{}),
				}
				db := sql.OpenDB(connector)
				db.SetMaxOpenConns(limit)
				t.Cleanup(func() { _ = db.Close() })
				backend, err := New(db)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
				proofs := make([]*OwnershipProof, 0, 2)
				for i := 0; i < 2; i++ {
					proof, err := backend.RetainOwnershipProof(ctx)
					if err != nil {
						t.Fatalf("retain ownership proof %d: %v", i, err)
					}
					t.Cleanup(func() { _ = proof.Close() })
					proofs = append(proofs, proof)
				}
				proofErrorsAtPoolClose := make(chan []error, 1)
				connector.atPoolClose = func() {
					readCtx, cancelRead := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancelRead()
					readErrors := make([]error, 0, len(proofs))
					for _, proof := range proofs {
						var value int
						readErrors = append(readErrors, proof.QueryRowContext(readCtx, "SELECT 1").Scan(&value))
					}
					proofErrorsAtPoolClose <- readErrors
				}
				if concurrent {
					start := make(chan struct{})
					done := make(chan error, len(proofs)+1)
					go func() { <-start; done <- backend.Close() }()
					for _, proof := range proofs {
						go func() { <-start; done <- proof.Close() }()
					}
					close(start)
					for i := 0; i < len(proofs)+1; i++ {
						select {
						case err := <-done:
							if err != nil {
								t.Fatalf("concurrent close: %v", err)
							}
						case <-ctx.Done():
							t.Fatalf("concurrent close did not finish: %v", ctx.Err())
						}
					}
				} else if err := backend.Close(); err != nil {
					t.Fatalf("close backend with retained proofs: %v", err)
				}
				select {
				case readErrors := <-proofErrorsAtPoolClose:
					for i, err := range readErrors {
						if !errors.Is(err, sql.ErrConnDone) {
							t.Fatalf("proof %d at pool close = %v, want its connection already closed", i, err)
						}
					}
				case <-ctx.Done():
					t.Fatalf("pool-close observation did not finish: %v", ctx.Err())
				}
				assertOwnershipProofCapacity(t, backend, limit, 0)
				if got := backend.db.Stats().OpenConnections; got != 0 {
					t.Fatalf("closed backend has %d open connections", got)
				}
				for i, proof := range proofs {
					var value int
					if err := proof.QueryRowContext(ctx, "SELECT 1").Scan(&value); err == nil {
						t.Fatalf("proof %d still reads after backend close", i)
					}
					_ = proof.Close()
				}
				assertOwnershipProofCapacity(t, backend, limit, 0)
				if proof, err := backend.RetainOwnershipProof(ctx); proof != nil || err == nil {
					if proof != nil {
						_ = proof.Close()
					}
					t.Fatalf("closed backend acquisition = (%v, %v), want failure without proof", proof, err)
				}
			})
		}
	}
}

func TestOwnershipProofPendingAcquisitionCancellation(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("backend_close=%t", shutdown), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connector := &ownershipProofBarrierConnector{
				path:    "file:" + filepath.Join(t.TempDir(), "pending.db"),
				entered: make(chan struct{}), canceled: make(chan struct{}),
				allowReturn: make(chan struct{}), poolClosed: make(chan struct{}),
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(connector.allowReturn) }) }
			db := sql.OpenDB(connector)
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(0)
			backend, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = backend.Close() }()
			defer release()
			ordinary, err := backend.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer ordinary.Close()
			connector.block.Store(true)
			acquireCtx, cancelAcquire := context.WithCancel(ctx)
			defer cancelAcquire()
			type result struct {
				proof *OwnershipProof
				err   error
			}
			acquired := make(chan result, 1)
			go func() {
				proof, err := backend.RetainOwnershipProof(acquireCtx)
				acquired <- result{proof: proof, err: err}
			}()
			waitOwnershipProofSignal(t, ctx, connector.entered, "reservation entered the connector")
			// database/sql counts the pending physical opening as in use.
			assertOwnershipProofCapacity(t, backend, 2, 2)
			closed := make(chan error, 1)
			if shutdown {
				go func() { closed <- backend.Close() }()
			} else {
				cancelAcquire()
			}
			waitOwnershipProofSignal(t, ctx, connector.canceled, "pending connector observed cancellation")
			select {
			case got := <-acquired:
				if got.proof != nil {
					_ = got.proof.Close()
				}
				t.Fatal("acquisition returned before the connector returned")
			default:
			}
			select {
			case <-connector.poolClosed:
				t.Fatal("pool closed before pending acquisition returned")
			default:
			}
			if shutdown {
				select {
				case err := <-closed:
					t.Fatalf("backend close returned before pending acquisition joined: %v", err)
				default:
				}
			}
			release()
			select {
			case got := <-acquired:
				if got.proof != nil {
					_ = got.proof.Close()
					t.Fatal("canceled pending acquisition returned a proof")
				}
				if got.err == nil || (!shutdown && !errors.Is(got.err, context.Canceled)) {
					t.Fatalf("pending acquisition error = %v, want failure preserving caller cancellation when present", got.err)
				}
			case <-ctx.Done():
				t.Fatalf("pending acquisition did not return: %v", ctx.Err())
			}
			if err := ordinary.Close(); err != nil {
				t.Fatal(err)
			}
			if shutdown {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("backend close: %v", err)
					}
				case <-ctx.Done():
					t.Fatalf("backend close did not finish: %v", ctx.Err())
				}
				waitOwnershipProofSignal(t, ctx, connector.poolClosed, "pool closed after acquisition joined")
			} else {
				proof, err := backend.RetainOwnershipProof(ctx)
				if err != nil {
					t.Fatalf("reservation after caller cancellation: %v", err)
				}
				t.Cleanup(func() { _ = proof.Close() })
				var value int
				if err := proof.QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil || value != 1 {
					t.Fatalf("replacement proof read = (%d, %v), want (1, nil)", value, err)
				}
				if err := proof.Close(); err != nil {
					t.Fatal(err)
				}
			}
			assertOwnershipProofCapacity(t, backend, 1, 0)
		})
	}
}

func newOwnershipProofTestBackend(t *testing.T, limit int) (*Backend, context.Context) {
	t.Helper()
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "ownership-proof.db"))
	backend.db.SetMaxOpenConns(limit)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close ownership-proof test backend: %v", err)
		}
	})
	return backend, ctx
}

func ownershipProofLimit(base, retained int) int {
	if base == 0 {
		return 0
	}
	return base + retained
}

func assertOwnershipProofCapacity(t *testing.T, backend *Backend, limit, inUse int) {
	t.Helper()
	stats := backend.db.Stats()
	if stats.MaxOpenConnections != limit || stats.InUse != inUse {
		t.Fatalf("pool capacity = max %d, in use %d; want max %d, in use %d", stats.MaxOpenConnections, stats.InUse, limit, inUse)
	}
}

func holdOwnershipProofOrdinaryConnections(t *testing.T, backend *Backend, ctx context.Context, count int) []*sql.Conn {
	t.Helper()
	connections := make([]*sql.Conn, 0, count)
	for i := 0; i < count; i++ {
		conn, err := backend.Conn(ctx)
		if err != nil {
			t.Fatalf("hold ordinary connection %d: %v", i, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		connections = append(connections, conn)
	}
	return connections
}

func readOwnershipProofValue(t *testing.T, proof *OwnershipProof, ctx context.Context, want int) {
	t.Helper()
	var value int
	if err := proof.QueryRowContext(ctx, "SELECT value FROM mutation_probe WHERE id = ?", 1).Scan(&value); err != nil {
		t.Fatalf("ownership proof read: %v", err)
	}
	if value != want {
		t.Fatalf("ownership proof value = %d, want current committed value %d", value, want)
	}
}

func waitOwnershipProofSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("%s: %v", label, ctx.Err())
	}
}

// The connector exposes cancellation separately from return so shutdown must
// join the real acquisition, not merely signal its context and close the pool.
type ownershipProofBarrierConnector struct {
	path                                       string
	block                                      atomic.Bool
	entered, canceled, allowReturn, poolClosed chan struct{}
	closeOnce                                  sync.Once
	atPoolClose                                func()
}

func (c *ownershipProofBarrierConnector) Driver() driver.Driver { return &modernc.Driver{} }

func (c *ownershipProofBarrierConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.block.CompareAndSwap(true, false) {
		close(c.entered)
		<-ctx.Done()
		close(c.canceled)
		<-c.allowReturn
		return nil, ctx.Err()
	}
	return c.Driver().Open(c.path)
}

func (c *ownershipProofBarrierConnector) Close() error {
	c.closeOnce.Do(func() {
		if c.atPoolClose != nil {
			c.atPoolClose()
		}
		close(c.poolClosed)
	})
	return nil
}
