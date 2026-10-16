package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ExecutableCompilerRejectsContentOnlyDecode(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := canonicalrouting.CopyConnectedRootInput(t)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	rooted, err := artifact.RootedSource()
	if err != nil {
		t.Fatalf("filesystem control lacks genuine root authority: %v", err)
	}
	if bundle, err := LoadWorkflowContractBundleFromRootedSource(repo, rooted, "", WorkflowContractLoadOptions{}); err != nil || bundle == nil {
		t.Fatalf("rooted filesystem control is not otherwise compilable: %v", err)
	}
	content, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	if _, present := content.RootAdmission(); present {
		t.Fatal("content decoder fabricated root authority")
	}
	contentRoot, err := content.RootedSource()
	if err == nil {
		t.Fatal("content-only decode returned opaque root authority")
	}
	if bundle, err := LoadWorkflowContractBundleFromRootedSource(repo, contentRoot, "", WorkflowContractLoadOptions{}); err == nil || bundle != nil {
		t.Fatal("content-only decode gained executable compiler admission without a retained rooted responsibility")
	}
}

func TestS1CompilerFreezesExactRootBesideContentIdentity(t *testing.T) {
	repo := repoRootForContractsTest(t)
	input, err := sourceartifact.AdmitDirectory(canonicalrouting.CopyConnectedRootInput(t))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := sourceartifactfixture.Rooted(t, "first", input)
	b, _ := sourceartifactfixture.Rooted(t, "second", input)
	if a.Artifact().BundleHash() != b.Artifact().BundleHash() || a.Admission().Equal(b.Admission()) {
		t.Fatal("compiler control does not separate root from byte identity")
	}
	for _, rooted := range []sourceartifact.RootedSource{a, b} {
		bundle, err := LoadWorkflowContractBundleFromRootedSource(repo, rooted, "", WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		frozen, err := bundle.RootedSource()
		if err != nil || !frozen.Admission().Equal(rooted.Admission()) || frozen.Artifact() != rooted.Artifact() {
			t.Fatalf("compiler lost exact owned root/content: %+v, %v", frozen.Admission().Reference(), err)
		}
		identity, err := BootBundleIdentity(bundle)
		if err != nil || identity.WorkflowName != "." || identity.WorkflowVersion != input.BundleHash() || identity.BundleHash != input.BundleHash() {
			t.Fatalf("root changed public machine identity: %+v, %v", identity, err)
		}
		original := bundle.SourceArtifact
		bundle.SourceArtifact = b.Artifact()
		if original == b.Artifact() {
			bundle.SourceArtifact = a.Artifact()
		}
		if frozen, err := bundle.RootedSource(); err == nil || frozen.Artifact() != nil {
			t.Fatal("same-content artifact replacement silently retargeted compiler root")
		}
		bundle.SourceArtifact = original
		if frozen, err := bundle.RootedSource(); err != nil || !frozen.Admission().Equal(rooted.Admission()) {
			t.Fatalf("restoring exposed bytes changed the frozen root: %v", err)
		}
	}
	for _, bundle := range []*WorkflowContractBundle{nil, {}, {SourceArtifact: a.Artifact()}} {
		if frozen, err := bundle.RootedSource(); err == nil || frozen.Artifact() != nil {
			t.Fatal("uncompiled public fields fabricated rooted compiler evidence")
		}
	}
}
