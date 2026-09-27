package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestFlowActivationPostMarkFailureReturnsToPendingRetry(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readinessLoadErr = errors.New("transient readback failure after acknowledged completion")
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("expected injected readback failure")
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
