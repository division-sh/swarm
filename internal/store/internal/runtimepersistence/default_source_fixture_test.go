package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestStoreDefaultSourceFixtureCompilesItsExactArtifact(t *testing.T) {
	repo := pipeline.WorkflowRepoRoot()
	artifact := sourceartifactfixture.Artifact()
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", artifact)
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.SourceArtifact.BundleHash() != sourceartifactfixture.Fact().BundleHash() {
		t.Fatal("default compiled source differs from the artifact and fact")
	}
	scopes := semanticview.FlowScopes(semanticview.Wrap(bundle))
	if len(scopes) != 2 {
		t.Fatalf("default fixture declarations = %+v", scopes)
	}
	for _, scope := range scopes {
		if (scope.ID != "." && scope.ID != "global") || scope.Mode != "static" || len(scope.Agents) != 0 || len(scope.Nodes) != 0 {
			t.Fatalf("default fixture invented consumers: %+v", scopes)
		}
	}
}
