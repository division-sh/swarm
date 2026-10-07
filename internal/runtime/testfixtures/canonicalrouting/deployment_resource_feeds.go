package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const DeploymentFixtureCloseDeclaration = "fixture.close.requested:\n  account_id: text\n"

// CopyTwoDeploymentFeeds owns the two independent template-feed route variant.
func CopyTwoDeploymentFeeds(t testing.TB) string {
	t.Helper()
	root := CopyNotifyAllChildren(t, NotifyAllChildrenOptions{FiniteLifecycle: true})
	if err := os.Remove(filepath.Join(root, NotifyAllChildrenChildFlowID, "agents.yaml")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "schema.yaml")
	replaceDeploymentFixtureExactlyOnce(t, path, "    resolution: select\n", "    resolution: select-or-create\n")
	return root
}

// CopySelectedDeploymentResource owns the supported root, singleton, and
// dynamic receiver variants used by selected deployment-feed fork proofs.
func CopySelectedDeploymentResource(t testing.TB, route string, keyed bool) string {
	t.Helper()
	root := CopyRootOutputSingletonConnect(t)
	eventsYAML := "root.ready:\n  account_id: text\n  document: json?\n"
	if keyed {
		eventsYAML = "root.ready:\n  key: account_id\n  account_id: text\n  document: json?\n"
	}
	writeClosedVariantFile(t, root, "events.yaml", eventsYAML+DeploymentFixtureCloseDeclaration)
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-output-singleton-connect\nstages: {active: {}, done: {final: true}}\npins:\n  inputs: [fixture.close.requested]\n  outputs:\n    - root.ready\n    - fixture.close.requested\nconnect:\n  - event: root.ready\n    from: .\n    to: consumer\n  - event: fixture.close.requested\n    from: .\n    to: consumer\n")
	writeClosedVariantFile(t, root, "entities.yaml", "fixture_state: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "close-fixture:\n  execution_type: system_node\n  event_handlers:\n    fixture.close.requested: {advances_to: done}\n")
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "name: consumer\nstages: {active: {}, done: {final: true}}\npins:\n  inputs: [root.ready, fixture.close.requested]\n")
	writeClosedVariantFile(t, root, "consumer/entities.yaml", "consumer_state: {}\n")
	writeClosedVariantFile(t, root, "consumer/nodes.yaml", "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    root.ready: {}\n    fixture.close.requested: {advances_to: done}\n")
	switch route {
	case "singleton":
	case "dynamic":
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "consumer/schema.yaml"), "name: consumer\n", "name: consumer\ninstance: account_id\n")
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "schema.yaml"), "  - event: root.ready\n    from: .\n    to: consumer\n", "  - event: root.ready\n    from: .\n    to: consumer\n    resolution: select-or-create\n")
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "schema.yaml"), "  - event: fixture.close.requested\n    from: .\n    to: consumer\n", "  - event: fixture.close.requested\n    from: .\n    to: consumer\n    resolution: select\n")
		writeClosedVariantFile(t, root, "consumer/entities.yaml", "consumer_state:\n  account_id: text\n")
	case "root":
		if err := os.RemoveAll(filepath.Join(root, "consumer")); err != nil {
			t.Fatal(err)
		}
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "schema.yaml"), "connect:\n  - event: root.ready\n    from: .\n    to: consumer\n  - event: fixture.close.requested\n    from: .\n    to: consumer\n", "")
		writeClosedVariantFile(t, root, "nodes.yaml", "root-collector:\n  execution_type: system_node\n  subscribes_to: [root.ready, fixture.close.requested]\n  event_handlers:\n    root.ready: {}\n    fixture.close.requested: {advances_to: done}\n")
	default:
		t.Fatalf("unknown selected deployment route %q", route)
	}
	return root
}

func replaceDeploymentFixtureExactlyOnce(t testing.TB, path, old, replacement string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), old) != 1 {
		t.Fatalf("fixture %s no longer has exact source declaration %q", path, old)
	}
	applyClosedReplacement(t, path, old, replacement)
}
