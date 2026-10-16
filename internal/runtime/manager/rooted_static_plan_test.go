package manager

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1StaticTopologyRequiresExactSourceWithoutAgents(t *testing.T) {
	first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	coordinate, err := agenttopology.NewSourceCoordinate(first.Admission())
	if err != nil {
		t.Fatal(err)
	}
	other, err := agenttopology.NewSourceCoordinate(second.Admission())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifySourceSetDesiredAgents(plan, coordinate, nil); err != nil {
		t.Fatalf("exact empty-agent source was refused: %v", err)
	}
	empty, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct {
		name       string
		plan       agenttopology.SourceSetPlan
		coordinate agenttopology.SourceCoordinate
	}{
		{"same-hash-other-root", plan, other},
		{"missing-root", plan, agenttopology.SourceCoordinate{BundleHash: coordinate.BundleHash}},
		{"absent-source", empty, coordinate},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if err := verifySourceSetDesiredAgents(cell.plan, cell.coordinate, nil); err == nil {
				t.Fatal("zero-agent topology accepted a source absent from the exact rooted plan")
			}
		})
	}
}

func TestS1ZeroAgentSourceSetRebindRequiresExactRoot(t *testing.T) {
	first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	options := runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory}
	compiled, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(runtimepipeline.WorkflowRepoRoot(), first, "", options)
	if err != nil {
		t.Fatal(err)
	}
	other, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(runtimepipeline.WorkflowRepoRoot(), second, "", options)
	if err != nil {
		t.Fatal(err)
	}
	coordinate, err := agenttopology.NewSourceCoordinate(first.Admission())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	if err := manager.InstallStartupTopology(newLifecyclePersistenceProbe(), admission, plan); err != nil {
		t.Fatal(err)
	}
	manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{}
	binding := lifecycleProbeProcessBinding()
	binding.BundleHash = coordinate.BundleHash
	gate := newSourceSetTransitionAdmissionProbe(plan.Revision)
	t.Cleanup(gate.release)
	prepared, err := manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), admission, plan, semanticview.Wrap(other), binding, binding, false, gate, false,
	)
	if prepared != nil {
		prepared.Abort()
	}
	if err == nil {
		t.Fatal("zero-agent rebind accepted another root under the first source-set plan")
	}
	manager.lifecycle.mu.Lock()
	installed := manager.lifecycle.sourceSetTransition != nil
	manager.lifecycle.mu.Unlock()
	if installed {
		t.Fatal("foreign-root rebind installed transition authority before refusing")
	}
	after, err := manager.completeStaticSourceSet()
	if err != nil || !reflect.DeepEqual(plan, after) {
		t.Fatalf("foreign-root rebind changed the installed plan: %+v, %v", after, err)
	}
	prepared, err = manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), admission, plan, semanticview.Wrap(compiled), binding, binding, false, gate, false,
	)
	if err != nil {
		t.Fatalf("exact-root zero-agent rebind refused: %v", err)
	}
	prepared.Abort()
}
