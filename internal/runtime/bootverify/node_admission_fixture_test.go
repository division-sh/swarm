package bootverify

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func admitBootHandlerFixture(t *testing.T, raw string) (runtimecontracts.SystemNodeEventHandler, error) {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: node-admission-fixture\nstages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n      "+strings.ReplaceAll(strings.TrimSpace(raw), "\n", "\n      ")+"\n")
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	return bundle.Nodes["worker"].EventHandlers["proof.requested"], nil
}

func mustBootHandlerFixture(t *testing.T, raw string) runtimecontracts.SystemNodeEventHandler {
	t.Helper()
	handler, err := admitBootHandlerFixture(t, raw)
	if err != nil {
		t.Fatalf("admit handler source: %v", err)
	}
	return handler
}
