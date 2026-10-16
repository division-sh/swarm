package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestS1ExecutableCompilerRejectsContentOnlyDecode(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := canonicalrouting.CopyConnectedRootInput(t)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := artifact.RootedSource(); err != nil {
		t.Fatalf("filesystem control lacks genuine root authority: %v", err)
	}
	if bundle, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, "", WorkflowContractLoadOptions{}); err != nil || bundle == nil {
		t.Fatalf("rooted filesystem control is not otherwise compilable: %v", err)
	}
	content, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	if _, present := content.RootAdmission(); present {
		t.Fatal("content decoder fabricated root authority")
	}
	if bundle, err := LoadWorkflowContractBundleFromArtifact(repo, content, "", WorkflowContractLoadOptions{}); err == nil || bundle != nil {
		t.Fatal("content-only decode gained executable compiler admission without a retained rooted responsibility")
	}
}
