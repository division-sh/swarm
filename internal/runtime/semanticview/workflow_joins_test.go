package semanticview_test

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestFanInBarrierContractDerivesEffectiveJoinPlan(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	raw := requireSingleJoinPlan(t, bundle.WorkflowJoins())
	if raw.Spec.Members.By != "" || raw.Spec.Window == nil || raw.Spec.Window.By != "" {
		t.Fatalf("authored join unexpectedly contains derived fields: %#v", raw.Spec)
	}

	effective := requireSingleJoinPlan(t, semanticview.Wrap(bundle).WorkflowJoins())
	if effective.Spec.Members.By != "payload.operating_id" || effective.Derivation.MembersByFrom != "resolution.dedup_by" {
		t.Fatalf("effective member derivation = %#v", effective)
	}
	if effective.Spec.Window == nil || effective.Spec.Window.By != "payload.period_id" || effective.Derivation.WindowByFrom != "resolution.window" {
		t.Fatalf("effective window derivation = %#v", effective)
	}
	if effective.Derivation.FanInPin != "operating_reported" {
		t.Fatalf("effective fan-in pin = %q", effective.Derivation.FanInPin)
	}

	rawAfter := requireSingleJoinPlan(t, bundle.WorkflowJoins())
	if rawAfter.Spec.Members.By != "" || rawAfter.Spec.Window == nil || rawAfter.Spec.Window.By != "" {
		t.Fatalf("effective lowering mutated authored join: %#v", rawAfter.Spec)
	}
}

func requireSingleJoinPlan(t *testing.T, plans []runtimecontracts.WorkflowJoinPlan) runtimecontracts.WorkflowJoinPlan {
	t.Helper()
	if len(plans) != 1 {
		t.Fatalf("join plans = %#v, want exactly one", plans)
	}
	return plans[0]
}
