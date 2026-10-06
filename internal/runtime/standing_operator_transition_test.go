package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type standingTransitionSignalProbe struct {
	runtimebus.DeliveryContinuationOwner
	signals atomic.Int32
}

func (p *standingTransitionSignalProbe) Signal() { p.signals.Add(1) }

func TestStandingRecoverySuppressionRequiresExactGenerationAndSignalsRestore(t *testing.T) {
	manager, expected := standingOperatorProcessFixture(t)
	entry := manager.contexts[runtimeContextTestHashA]
	probe := &standingTransitionSignalProbe{}
	if err := entry.runtime.Bus.SetDeliveryContinuationOwner(probe); err != nil {
		t.Fatal(err)
	}
	origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(expected.ServiceID, expected.Generation)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := manager.BeginStandingServiceOperation(context.Background(), expected, true)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.BeginStandingRunRecovery(context.Background(), expected.RunID, origin)
	if lease != nil || !errors.Is(err, runtimebus.ErrStandingRestartParked) {
		t.Fatalf("known transition was not deferred: lease=%v err=%v", lease, err)
	}
	foreign, err := runtimerunlifecycle.StandingGenerationRunOrigin(expected.ServiceID, expected.Generation+1)
	if err != nil {
		t.Fatal(err)
	}
	for _, coordinate := range []struct {
		run    string
		origin runtimerunlifecycle.RunOrigin
	}{{uuid.NewString(), origin}, {expected.RunID, foreign}} {
		lease, err := manager.BeginStandingRunRecovery(context.Background(), coordinate.run, coordinate.origin)
		if lease != nil || err == nil || errors.Is(err, runtimebus.ErrStandingRestartParked) {
			t.Fatalf("foreign coordinate borrowed deferral: lease=%v err=%v", lease, err)
		}
	}
	before := probe.signals.Load()
	if err := transition.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.signals.Load() != before+1 {
		t.Fatalf("restored owner did not signal exactly once: before=%d after=%d", before, probe.signals.Load())
	}
	if err := transition.Restore(context.Background()); err != nil || probe.signals.Load() != before+1 {
		t.Fatalf("settled transition signalled fictitious progress: err=%v signals=%d", err, probe.signals.Load())
	}
	lease, err = manager.BeginStandingRunRecovery(context.Background(), expected.RunID, origin)
	if err != nil || lease == nil {
		t.Fatalf("exact restored generation remained unavailable: lease=%v err=%v", lease, err)
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	child := entry.standing[expected.ServiceID]
	if err := child.Fence(); err != nil {
		t.Fatal(err)
	}
	lease, err = manager.BeginStandingRunRecovery(context.Background(), expected.RunID, origin)
	if lease != nil || err == nil || errors.Is(err, runtimebus.ErrStandingRestartParked) {
		t.Fatalf("untracked fence masqueraded as an owned transition: lease=%v err=%v", lease, err)
	}
	if err := child.Reopen(); err != nil {
		t.Fatal(err)
	}
}

func standingOperatorProcessFixture(t *testing.T) (*RuntimeContextManager, runtimepipeline.StandingServiceReconciliation) {
	t.Helper()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	target := &contextDef.StandingTargets[0]
	target.ServiceID, target.RunID, target.EntityID, target.InstanceID = uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	contextDef.Runtime.Scheduler = runtimeContextTestScheduler(t, contextDef.WorkOwner, nil)
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	disposition, err := runtimerunlifecycle.ClassifyStandingRestart(runtimerunlifecycle.StandingRestartFact{BindingEnabled: true,
		ExactCurrent: true, ServiceID: target.ServiceID, RunID: target.RunID, Generation: target.Generation,
		DeclarationPresent: true, EffectiveState: "active", OperatorOverride: "none", RunState: "running",
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager, runtimepipeline.StandingServiceReconciliation{
		ServiceID: target.ServiceID, RunID: target.RunID, Generation: target.Generation, BundleHash: contextDef.BundleHash(),
		FlowPath: target.FlowPath, InstanceID: target.InstanceID, EntityID: target.EntityID, RestartDisposition: disposition,
	}
}

func TestStandingOperatorActiveNoopPreservesExactChildAndSchedule(t *testing.T) {
	manager, expected := standingOperatorProcessFixture(t)
	entry := manager.contexts[runtimeContextTestHashA]
	child := entry.standing[expected.ServiceID]
	lease, err := child.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.runtime.Scheduler.RegisterGenericScheduleWakeup(lease.Context(), runtimeContextTestWakeup(t, "noop-future", time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	_ = lease.Done()
	transition, err := manager.BeginStandingServiceOperation(context.Background(), expected, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if entry.standing[expected.ServiceID] != child || manager.standingServiceSuppressedLocked(expected.ServiceID) {
		t.Fatal("fresh active no-op replaced or suppressed the child")
	}
	parked, err := entry.runtime.Scheduler.ParkOccurrence(context.Background(), child)
	if err != nil || parked.Count() != 1 {
		t.Fatalf("no-op lost original schedule: parked=%+v err=%v", parked, err)
	}
	if err := parked.RestoreOriginal(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStandingOperatorNoChildRestoresPriorSuppression(t *testing.T) {
	for _, state := range []string{"suspended", "terminal", "invalid"} {
		t.Run(state, func(t *testing.T) {
			manager, expected := standingOperatorProcessFixture(t)
			transition, err := manager.BeginStandingServiceOperation(context.Background(), expected, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := transition.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}
			fact := runtimerunlifecycle.StandingRestartFact{BindingEnabled: true,
				ExactCurrent: true, ServiceID: expected.ServiceID, RunID: expected.RunID, Generation: expected.Generation,
				DeclarationPresent: true, EffectiveState: "suspended", OperatorOverride: "suspended", RunState: "paused",
			}
			if state == "terminal" {
				fact.EffectiveState, fact.OperatorOverride, fact.RunState = "active", "none", "completed"
			} else if state == "invalid" {
				fact.RunState = "running"
			}
			expected.RestartDisposition, err = runtimerunlifecycle.ClassifyStandingRestart(fact)
			if err != nil {
				t.Fatal(err)
			}
			transition, err = manager.BeginStandingServiceOperation(context.Background(), expected, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(transition.occurrences) != 0 || !transition.previouslySuppressed {
				t.Fatal("non-executable command invented a child or lost prior suppression")
			}
			if err := transition.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !manager.standingServiceSuppressedLocked(expected.ServiceID) || manager.contexts[runtimeContextTestHashA].standing[expected.ServiceID] != nil {
				t.Fatal("no-child rollback published an executable target")
			}
			if _, err := manager.BeginStandingServiceTransition(context.Background(), expected.ServiceID); err == nil || !strings.Contains(err.Error(), "no process occurrence") {
				t.Fatalf("low-level missing-child guard changed: %v", err)
			}
		})
	}
}

func TestStandingOperatorRejectsInvalidChildComposition(t *testing.T) {
	for _, mismatch := range []string{"missing", "stale-run", "stale-generation", "foreign-source", "stale-target", "fenced", "suppressed", "non-executable-live"} {
		t.Run(mismatch, func(t *testing.T) {
			manager, expected := standingOperatorProcessFixture(t)
			entry := manager.contexts[runtimeContextTestHashA]
			child := entry.standing[expected.ServiceID]
			switch mismatch {
			case "missing":
				delete(entry.standing, expected.ServiceID)
				if err := child.RetireAndWait(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "stale-run":
				expected.RunID, expected.RestartDisposition.RunID = uuid.NewString(), uuid.NewString()
				expected.RestartDisposition.RunID = expected.RunID
			case "stale-generation":
				expected.Generation++
				expected.RestartDisposition.Generation = expected.Generation
			case "foreign-source":
				expected.BundleHash = runtimeContextTestHashB
			case "stale-target":
				entry.context.StandingTargets[0].PublicationSequence++
			case "fenced":
				if err := child.Fence(); err != nil {
					t.Fatal(err)
				}
			case "suppressed":
				if err := manager.SuppressStandingServiceTargets(expected.ServiceID); err != nil {
					t.Fatal(err)
				}
			case "non-executable-live":
				expected.RestartDisposition.Kind = runtimerunlifecycle.StandingRestartSuspended
				expected.RestartDisposition.RunState = "paused"
				expected.RestartDisposition.EffectiveState, expected.RestartDisposition.OperatorOverride = "suspended", "suspended"
			}
			before := entry.standing[expected.ServiceID]
			if _, err := manager.BeginStandingServiceOperation(context.Background(), expected, true); err == nil {
				t.Fatal("invalid child composition was admitted")
			}
			if entry.standing[expected.ServiceID] != before {
				t.Fatal("refusal changed the process child")
			}
		})
	}
}

func TestStandingOperatorCancellationJoinsHeldChildBeforeRestore(t *testing.T) {
	manager, expected := standingOperatorProcessFixture(t)
	child := manager.contexts[runtimeContextTestHashA].standing[expected.ServiceID]
	work, err := child.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = work.Done() })
	transition, err := manager.BeginStandingServiceOperation(context.Background(), expected, true)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transition.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled drain = %v", err)
	}
	if err := transition.Restore(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore bypassed held child: %v", err)
	}
	if _, err := child.Begin(context.Background()); err == nil {
		t.Fatal("failed drain reopened child before held work settled")
	}
	if err := work.Done(); err != nil {
		t.Fatal(err)
	}
	if err := transition.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manager.contexts[runtimeContextTestHashA].standing[expected.ServiceID] != child {
		t.Fatal("compensation replaced the captured child")
	}
	lease, err := child.Begin(context.Background())
	if err != nil {
		t.Fatalf("captured child did not reopen after join: %v", err)
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
}

func TestStandingOperatorCancelledSchedulerParkRetainsExactOwnerUntilJoin(t *testing.T) {
	manager, expected := standingOperatorProcessFixture(t)
	entry := manager.contexts[runtimeContextTestHashA]
	child := entry.standing[expected.ServiceID]
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	entry.runtime.Scheduler = runtimeContextTestScheduler(t, entry.workOwner, func(context.Context, genericschedule.Wakeup) {
		close(started)
		<-release
	})
	lease, err := child.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.runtime.Scheduler.RegisterGenericScheduleWakeup(lease.Context(), runtimeContextTestWakeup(t, "cancelled-park", time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("exact standing callback did not start")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	transition, err := manager.BeginStandingServiceOperation(cancelled, expected, true)
	if !errors.Is(err, context.Canceled) || transition == nil {
		t.Fatalf("failed parking lost retained transition: transition=%+v err=%v", transition, err)
	}
	if err := transition.Restore(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled parking reopened a live callback: %v", err)
	}
	if _, err := child.Begin(context.Background()); err == nil {
		t.Fatal("failed parking released execution before the callback joined")
	}
	releaseOnce.Do(func() { close(release) })
	if err := transition.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if entry.standing[expected.ServiceID] != child || manager.standingServiceSuppressedLocked(expected.ServiceID) {
		t.Fatal("parking compensation changed exact child or prior suppression")
	}
	restored, err := child.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Done(); err != nil {
		t.Fatal(err)
	}
}
