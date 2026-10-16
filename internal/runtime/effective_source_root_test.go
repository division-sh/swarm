package runtime

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1EffectiveIdentitySeparatesRootWithoutChangingContentIdentity(t *testing.T) {
	first, sourceA := loadRootedBudgetSource(t, "first", "stages: []\n")
	bundleA, _ := semanticview.Bundle(sourceA)
	admitRuntimeTestBundle(t, bundleA)
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	bundleB, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), second, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundleB)
	fact := sourceartifactfixture.FactFor(first.Artifact())
	a, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: sourceA, SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	b, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundleB), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	if a.Identity().Equal(b.Identity()) {
		t.Fatal("same-content different-root compilation reused an effective identity")
	}
	again, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: sourceA, SourceArtifactFact: fact})
	if err != nil || !again.Identity().Equal(a.Identity()) {
		t.Fatalf("same exact rooted source changed identity: %v", err)
	}
	for _, source := range []semanticview.Source{nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{SourceArtifact: first.Artifact()})} {
		if _, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: source, SourceArtifactFact: fact}); err == nil {
			t.Fatal("uncompiled public fields minted a rooted effective identity")
		}
	}
	for _, bundle := range []*runtimecontracts.WorkflowContractBundle{bundleB} {
		identity, err := runtimecontracts.BootBundleIdentity(bundle)
		if err != nil || identity.WorkflowName != "." || identity.WorkflowVersion != first.Artifact().BundleHash() || identity.BundleHash != fact.BundleHash() {
			t.Fatalf("root changed the machine content identity: %+v, %v", identity, err)
		}
	}
}
