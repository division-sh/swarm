package canonicalrouting

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectedFutureProviderFixtureUsesCompiledProducers(t *testing.T) {
	root, repo := CopySelectedFutureProviderInput(t), RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	if len(semanticview.AgentDeclarations(semanticview.Wrap(bundle))) != 4 {
		t.Fatal("future provider fixture lost its four exact descendant declarations")
	}
	if findings := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("future-provider fixture must pass complete source boot verification: %#v", findings)
	}
}
