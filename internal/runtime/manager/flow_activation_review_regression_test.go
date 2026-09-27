package manager

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestFlowActivationPostMarkFailureReturnsToPendingRetry(t *testing.T) {
	for _, outcome := range []string{"error", "cancellation", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			bundle := testFlowBundle(t, "")
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			activationCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var canceledAttemptDone <-chan struct{}
			instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
				switch outcome {
				case "error":
					instances.readinessLoadErr = errors.New("transient readback failure after acknowledged completion")
				case "cancellation":
					key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
					am.dynamicFlowReadinessMu.Lock()
					canceledAttemptDone = am.dynamicFlowReadinessAttempts[key].done
					am.dynamicFlowReadinessMu.Unlock()
					cancel()
					instances.readinessLoadErr = activationCtx.Err()
				case "panic":
					panic("injected panic after acknowledged topology completion")
				}
			}
			var activationErr error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				activationErr = activateFlowInstanceForTest(am, activationCtx, req)
			}()
			if outcome == "panic" {
				if recovered != nil || activationErr == nil || !strings.Contains(activationErr.Error(), "panic") {
					t.Fatalf("panic was not reported by the activation owner: err=%v panic=%v", activationErr, recovered)
				}
			} else if activationErr == nil || recovered != nil {
				t.Fatalf("expected injected %s failure: err=%v panic=%v", outcome, activationErr, recovered)
			}
			if outcome == "cancellation" {
				select {
				case <-canceledAttemptDone:
				case <-time.After(5 * time.Second):
					t.Fatal("canceled caller returned before the accepted activation settled")
				}
			}
			instances.readinessLoadErr = nil
			if bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatal("failed attempt retained a route")
			}
			if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); err != nil {
				t.Fatal(err)
			}
			if !bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatal("retry omitted the abandoned topology")
			}
		})
	}
}

func TestFlowActivationShutdownPreservesFailedAttemptDisposition(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readinessLoadErr = errors.New("readback failed after topology mark")
	}
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		if calls == 1 {
			return errors.New("failed abandonment retained for retry")
		}
		return nil
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected error")
	}
	instances.readinessLoadErr = nil
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	active := am.dynamicFlowActiveAttempts[key]
	failed := active != nil && active.retirementKind == flowActivationFailedRetirement && active.locallyRetired
	am.dynamicFlowReadinessMu.Unlock()
	if !failed {
		t.Fatal("control: expected retained failed attempt after local join")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	if !row.Pending() {
		t.Fatal("shutdown changed failed abandonment into orderly retirement")
	}
}

func TestFlowActivationShutdownRetriesAcknowledgedFailedAbandonment(t *testing.T) {
	for _, autoEmit := range []string{"", "task.started"} {
		name := "no_creation"
		if autoEmit != "" {
			name = "creation_emitted"
		}
		t.Run(name, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{settleAttemptPostCommitErr: errors.New("acknowledged abandonment response lost")}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			bundle := testFlowBundle(t, autoEmit)
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
				instances.readinessLoadErr = errors.New("readback failed after topology mark")
			}
			ctx := testAuthorActivityContext(context.Background())
			if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
				t.Fatal("missing injected failure")
			}
			instances.readinessLoadErr = nil
			key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
			am.dynamicFlowReadinessMu.Lock()
			active := am.dynamicFlowActiveAttempts[key]
			failed := active != nil && active.retirementKind == flowActivationFailedRetirement && active.locallyRetired
			am.dynamicFlowReadinessMu.Unlock()
			if !failed {
				t.Fatal("acknowledged abandonment lost retained failure disposition")
			}
			if err := am.Shutdown(); err != nil {
				t.Fatal(err)
			}
			row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
			if err != nil || !found || !row.Pending() {
				t.Fatalf("failed abandonment did not return readiness to pending: found=%v pending=%v err=%v", found, row.Pending(), err)
			}
		})
	}
}

func TestFlowActivationFailedAttemptShutdownRetainsPersistentSettlement(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readinessLoadErr = errors.New("readback failed after topology mark")
	}
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		if calls <= 2 {
			return errors.New("durable abandonment unavailable")
		}
		return nil
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected activation failure")
	}
	instances.readinessLoadErr = nil
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	if err := am.Shutdown(); err == nil {
		t.Fatal("shutdown released a persistently failed abandonment")
	}
	am.dynamicFlowReadinessMu.Lock()
	retained := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if retained == nil || retained.retirementKind != flowActivationFailedRetirement {
		t.Fatal("failed shutdown lost the exact abandonment owner")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("retry joined shutdown settlement: %v", err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found || !row.Pending() {
		t.Fatalf("retry did not abandon failed attempt: found=%v pending=%v err=%v", found, row.Pending(), err)
	}
}

func TestFlowActivationFailedAttemptShutdownRetriesSettlementPanic(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readinessLoadErr = errors.New("readback failed after topology mark")
	}
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		switch calls {
		case 1:
			return errors.New("durable abandonment unavailable")
		case 2:
			panic("durable abandonment panicked")
		default:
			return nil
		}
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected activation failure")
	}
	instances.readinessLoadErr = nil
	if err := am.Shutdown(); err == nil || !strings.Contains(err.Error(), "durable abandonment panicked") {
		t.Fatalf("shutdown did not report retained settlement panic: %v", err)
	}
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	retained := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if retained == nil || retained.retirementKind != flowActivationFailedRetirement {
		t.Fatal("settlement panic lost failed disposition")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("retry after settlement panic: %v", err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found || !row.Pending() {
		t.Fatalf("retry did not abandon failed attempt: found=%v pending=%v err=%v", found, row.Pending(), err)
	}
}

func TestCompletedStandingPreRunHandoffRetainsReconstructionAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "replay_off"
		if replay {
			name = "replay_on"
		}
		t.Run(name, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			bundle := testFlowBundleWithTwoAgents(t, "")
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
			if err := activateFlowInstanceForTest(initial, ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := initial.Shutdown(); err != nil {
				t.Fatal(err)
			}
			restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
			setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
			if _, _, err := restarted.PrepareStandingFlowInstance(ctx, req); err != nil {
				t.Fatal(err)
			}
			startup, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, replay)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.PrepareAdmittedDynamicFlowAgentsForStart(ctx); err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
				cancel()
				t.Fatal(err)
			}
			t.Cleanup(func() { cancel(); _ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}) })
			if err := restarted.CompleteDynamicFlowRuntimeStartupTopology(ctx, startup); err != nil {
				t.Fatalf("completed standing reconstruction rejected: %v", err)
			}
		})
	}
}

func TestCompletedStandingPreRunFailureRetainsNextRestartAuthority(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bundle := testFlowBundleWithTwoAgents(t, "")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	if err := activateFlowInstanceForTest(initial, ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := initial.Shutdown(); err != nil {
		t.Fatal(err)
	}
	interrupted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(interrupted, semanticview.Wrap(bundle))
	if _, _, err := interrupted.PrepareStandingFlowInstance(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, false); err != nil {
		t.Fatal(err)
	}
	if err := interrupted.PrepareAdmittedDynamicFlowAgentsForStart(ctx); err != nil {
		t.Fatal(err)
	}
	if err := interrupted.Shutdown(); err != nil {
		t.Fatal(err)
	}
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	if _, _, err := restarted.PrepareStandingFlowInstance(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, false); err != nil {
		t.Fatalf("completed row lost reconstruction authority after pre-run failure: %v", err)
	}
}
