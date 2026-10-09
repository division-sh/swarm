package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPostgresCanceledReadJoinsClaimedRollbackDisposition(t *testing.T) {
	for _, rollbackFinished := range []bool{false, true} {
		for _, callbackFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("rollback_finished=%t/callback_failure=%t", rollbackFinished, callbackFailure), func(t *testing.T) {
				b, probe := newExitProbe(t)
				probe.rollbackEntered, probe.rollbackRelease = make(chan struct{}), make(chan struct{})
				rollbackFailure := errors.New("independent cancellation rollback failure")
				probe.rollbackFailure = rollbackFailure
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(probe.rollbackRelease) }) }
				defer release()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				callbackReturned := make(chan struct{})
				done := make(chan error, 1)
				callbackErr := errors.New("independent read failure")
				go func() {
					done <- b.RunReadTransaction(ctx, func(readCtx context.Context, tx *sql.Tx) error {
						var value int
						if err := tx.QueryRowContext(readCtx, "SELECT 1").Scan(&value); err != nil {
							close(callbackReturned)
							return err
						}
						cancel()
						select {
						case <-probe.rollbackEntered:
						case <-time.After(3 * time.Second):
							close(callbackReturned)
							return errors.New("cancellation rollback did not reach its barrier")
						}
						if rollbackFinished {
							release()
							<-probe.rollbackDone
						}
						close(callbackReturned)
						if callbackFailure {
							return callbackErr
						}
						return nil
					})
				}()
				select {
				case <-callbackReturned:
				case <-time.After(3 * time.Second):
					t.Fatal("read callback did not return")
				}
				release()
				var err error
				select {
				case err = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("rollback/discard cleanup did not join")
				}
				if !errors.Is(err, context.Canceled) || !errors.Is(err, rollbackFailure) {
					t.Fatalf("cancellation lost claimed rollback outcome: %v", err)
				}
				if callbackFailure && !errors.Is(err, callbackErr) {
					t.Fatalf("cancellation lost independent callback failure: %v", err)
				}
				select {
				case <-probe.rollbackDone:
				default:
					t.Fatal("read returned before the claimed rollback finished")
				}
				if probe.closed.Load() != 1 {
					t.Fatalf("failed rollback disposition closed connections=%d, want exactly one", probe.closed.Load())
				}
				next, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := b.db.PingContext(next); err != nil {
					t.Fatalf("discarded read prevents new connection admission: %v", err)
				}
			})
		}
	}
}
