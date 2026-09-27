package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestJoinLifecycleWiringRequiresExactCompiledOwner(t *testing.T) {
	root := canonicalrouting.CopyServedJoinProof(t)
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	if findings := (&checkerContext{source: source}).eventRuntimeWiring(); len(findings) != 0 {
		t.Fatalf("compiled join lifecycle handlers must be recognized: %#v", findings)
	}
	if len(source.WorkflowJoins()) != 1 {
		t.Fatalf("expected one compiled join, got %#v", source.WorkflowJoins())
	}
	// Drop the compiled owner while retaining its generated subscriptions. The
	// intrinsic event names must not independently authorize runtime handling.
	withoutJoin := sourceWithoutCompiledJoins{Source: source}
	findings := (&checkerContext{source: withoutJoin}).eventRuntimeWiring()
	for _, event := range []string{"platform.join_complete", "platform.join_timeout"} {
		if !reportContains(findings, "event_runtime_wiring_validation", "event "+event+" on node flow . node join-node has no matching executable handler") {
			t.Fatalf("missing exact-owner failure for %s: %#v", event, findings)
		}
	}
}

type sourceWithoutCompiledJoins struct{ semanticview.Source }

func (sourceWithoutCompiledJoins) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan { return nil }
