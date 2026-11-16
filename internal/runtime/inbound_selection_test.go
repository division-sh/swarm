package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type inboundSelectionResult struct {
	runID string
	done  func() error
	err   error
}

func acquireInboundSelection(ctx context.Context, manager *RuntimeContextManager, target StandingTarget, ordinary bool) inboundSelectionResult {
	if ordinary {
		use, _, err := manager.AcquireIngress(ctx, target.Alias, target.Provider)
		if use == nil {
			return inboundSelectionResult{err: err}
		}
		return inboundSelectionResult{runID: use.Context.StandingTargets[0].RunID, done: use.Done, err: err}
	}
	use, _, err := manager.AcquireInboundPublication(ctx, target.Alias, target.Provider)
	if use == nil {
		return inboundSelectionResult{err: err}
	}
	return inboundSelectionResult{runID: use.context.StandingTargets[0].RunID, done: use.Done, err: err}
}

type countedInboundLandingRuns struct {
	runlifecycle.LandingRunReader
	reads atomic.Int32
}

func (r *countedInboundLandingRuns) LoadLandingRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LandingRun, bool, error) {
	r.reads.Add(1)
	return r.LandingRunReader.LoadLandingRun(ctx, scope)
}

func TestInboundSelectionInvalidatesDetachedReadsOnFailedAndABAWriters(t *testing.T) {
	for _, path := range []string{"ordinary", "stored"} {
		for _, change := range []string{"failed", "aba", "changed"} {
			t.Run(path+"/"+change, func(t *testing.T) {
				catalog, source := runtimeAdmissionTestCatalog(t, "selection", "inbound")
				definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
				manager, err := newTestRuntimeContextManager(t, nil, definition)
				if err != nil {
					t.Fatal(err)
				}
				if path == "stored" {
					if err := manager.SealInboundExecution(t.Context()); err != nil {
						t.Fatal(err)
					}
					definition.Runtime.CloseAdmission()
					if err := manager.OpenStoredInbound(); err != nil {
						t.Fatal(err)
					}
				}
				target := definition.StandingTargets[0]
				rows := manager.landingRuns.(runtimeTestLandingRuns)
				counted := &countedInboundLandingRuns{LandingRunReader: rows}
				reader := &heldInboundLandingRuns{LandingRunReader: counted, entered: make(chan struct{}), release: make(chan struct{})}
				manager.landingRuns = reader
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				got := make(chan inboundSelectionResult, 1)
				go func() { got <- acquireInboundSelection(ctx, manager, target, path == "ordinary") }()
				selected := make(chan error, 1)
				joined := false
				wantRun := target.RunID
				defer func() {
					if t.Failed() {
						cancel()
					}
					close(reader.release)
					result := <-got
					if result.err != nil || result.done == nil || result.runID != wantRun {
						t.Errorf("detached admission: run=%s want=%s admitted=%t err=%v", result.runID, wantRun, result.done != nil, result.err)
					}
					if counted.reads.Load() < 2 {
						t.Error("selection did not invalidate the detached native observation")
					}
					if result.done != nil {
						if manager.liveSelectionMu.TryLock() {
							manager.liveSelectionMu.Unlock()
							t.Error("admitted use did not exclude selection")
						}
						if err := result.done(); err != nil {
							t.Error(err)
						}
					}
					if !joined {
						<-selected
					}
					if definition.WorkOwner.ActiveCount() != 0 {
						t.Error("admission leaked an execution lease")
					}
				}()
				<-reader.entered
				failed := errors.New("selected body failed without changing the landing value")
				go func() {
					selected <- func() error {
						release := manager.BeginLiveRunSelection()
						defer release()
						if change == "failed" {
							return failed
						}
						original := rows[target.FlowPath]
						next := original
						next.RunID = eventtest.UUID(t.Name() + "-next")
						rows[target.FlowPath] = next
						if change == "aba" {
							rows[target.FlowPath] = original
						}
						return nil
					}()
				}()
				select {
				case err := <-selected:
					joined = true
					if change == "failed" && !errors.Is(err, failed) || change != "failed" && err != nil {
						t.Fatalf("selected body result: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("selection waited for the unadmitted native read")
				}
				wantRun = rows[target.FlowPath].RunID
			})
		}
	}
}

func TestInboundSelectionCancellationDoesNotAdmitAfterExclusiveRelease(t *testing.T) {
	for _, path := range []string{"ordinary", "publication", "preparing"} {
		t.Run(path, func(t *testing.T) {
			catalog, source := runtimeAdmissionTestCatalog(t, "selection-cancel", "inbound")
			definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
			manager, err := newTestRuntimeContextManager(t, nil, definition)
			if err != nil {
				t.Fatal(err)
			}
			if path == "preparing" {
				manager.PrepareInboundStop()
			}
			release := sync.OnceFunc(manager.BeginLiveRunSelection())
			defer release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			barrier := &startupClaimBarrierContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			unblock := sync.OnceFunc(func() { close(barrier.release) })
			defer unblock()
			got := make(chan inboundSelectionResult, 1)
			go func() {
				got <- acquireInboundSelection(barrier, manager, definition.StandingTargets[0], path == "ordinary")
			}()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("request did not reach its cancellable wait")
			}
			cancel()
			unblock()
			select {
			case result := <-got:
				if result.done != nil {
					_ = result.done()
					t.Fatal("canceled waiter acquired custody")
				}
				if !errors.Is(result.err, context.Canceled) {
					t.Fatalf("canceled wait returned %v", result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation waited for exclusive selection to release")
			}
			release()
			if !manager.liveSelectionMu.TryLock() {
				t.Fatal("canceled waiter retained selection exclusion")
			}
			manager.liveSelectionMu.Unlock()
			if definition.WorkOwner.ActiveCount() != 0 {
				t.Fatal("canceled waiter admitted execution after selection released")
			}
		})
	}
}
