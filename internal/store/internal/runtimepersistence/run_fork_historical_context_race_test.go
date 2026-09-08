package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestRunForkHistoricalIdentityQueuedCancellationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newSnapshotOwnershipFixture(t, backend, false, false)
			staged, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			for _, operation := range []string{"plan", "activate"} {
				t.Run(operation, func(t *testing.T) {
					// Occupy the only connection. WaitCount confirms the operation is
					// queued rather than assuming goroutine scheduling from a sleep.
					previous := f.db.Stats().MaxOpenConnections
					f.db.SetMaxOpenConns(1)
					t.Cleanup(func() { f.db.SetMaxOpenConns(previous) })
					connection, err := f.db.Conn(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer connection.Close()
					waits := f.db.Stats().WaitCount
					ctx, cancel := context.WithCancel(f.ctx)
					defer cancel()
					done := make(chan error, 1)
					go func() {
						if operation == "plan" {
							_, err := f.store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID})
							done <- err
						} else {
							_, err := f.store.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, ConfirmSourceFreeze: true})
							done <- err
						}
					}()
					deadline := time.NewTimer(10 * time.Second)
					defer deadline.Stop()
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					for f.db.Stats().WaitCount == waits {
						select {
						case err := <-done:
							t.Fatalf("operation finished before connection barrier: %v", err)
						case <-deadline.C:
							t.Fatal("operation did not reach connection barrier")
						case <-tick.C:
						}
					}
					cancel()
					select {
					case err := <-done:
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("queued cancellation=%v", err)
						}
					case <-deadline.C:
						t.Fatal("cancelled operation did not finish")
					}
					if err := connection.Close(); err != nil {
						t.Fatal(err)
					}
					f.db.SetMaxOpenConns(previous)
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
						t.Fatal("cancelled operation mutated staged child or source")
					}
				})
			}
			// The source may lawfully advance after staging. The unchanged
			// activation head/safety owner must still refuse, never freeze it.
			f.advance(t)
			advanced := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			activation, err := f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, ConfirmSourceFreeze: true})
			if _, fact, ok := runForkReplayResumeBlockerFromError(err); !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced || activation.Activated {
				t.Fatalf("stale activation=%#v err=%v", activation, err)
			}
			if !reflect.DeepEqual(advanced, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
				t.Fatal("source-advanced refusal mutated staged child or source")
			}
		})
	}
}
