package canonicalrouting

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectedFutureKeyedProviderFixturePassesCompleteBoot(t *testing.T) {
	root, repo := CopySelectedFutureKeyedProviderInput(t), RepoRoot(t)
	requireSelectedFutureProviderCompleteBoot(t, root, repo)
}

func TestSelectedFutureMintedProviderFixturePassesCompleteBoot(t *testing.T) {
	root, repo := CopySelectedFutureMintedProviderInput(t), RepoRoot(t)
	requireSelectedFutureProviderCompleteBoot(t, root, repo)
}

func TestSelectedFutureReusedProviderFixturePassesCompleteBoot(t *testing.T) {
	root, repo := CopySelectedFutureReusedProviderInput(t), RepoRoot(t)
	requireSelectedFutureProviderCompleteBoot(t, root, repo)
}

func requireSelectedFutureProviderCompleteBoot(t *testing.T, root, repo string) {
	t.Helper()
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	if len(semanticview.AgentDeclarations(source)) != 1 {
		t.Fatal("keyed provider fixture must retain its one real worker declaration")
	}
	if findings := bootverify.Run(context.Background(), source, bootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("keyed provider fixture must pass complete boot verification: %#v", findings)
	}
}
