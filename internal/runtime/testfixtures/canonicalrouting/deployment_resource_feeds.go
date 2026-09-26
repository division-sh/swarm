package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CopyTwoDeploymentFeeds owns the two independent template-feed route variant.
func CopyTwoDeploymentFeeds(t testing.TB) string {
	t.Helper()
	root := CopyNotifyAllChildren(t, NotifyAllChildrenOptions{})
	if err := os.Remove(filepath.Join(root, NotifyAllChildrenChildFlowID, "agents.yaml")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, NotifyAllChildrenChildFlowID, "schema.yaml")
	replaceDeploymentFixtureExactlyOnce(t, path, `      - event: account.notify.requested
        resolution:
          mode: select
`, `      - event: account.notify.requested
        resolution:
          mode: select-or-create
`)
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
	writeClosedVariantFile(t, root, "events.yaml", eventsYAML)
	switch route {
	case "singleton":
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "consumer/nodes.yaml"), "    root.ready:\n      create_entity: true\n", "    root.ready: {}\n")
		if err := os.Remove(filepath.Join(root, "consumer", "entities.yaml")); err != nil {
			t.Fatal(err)
		}
	case "dynamic":
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "consumer/schema.yaml"), "mode: singleton\n", "mode: template\ninstance: account_id\n")
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "consumer/schema.yaml"), "    events: [root.ready]\n", "    events:\n      - event: root.ready\n        resolution:\n          mode: select-or-create\n")
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "consumer/entities.yaml"), "consumer_state:\n  entity_id: text\n", "consumer_state:\n  entity_id: text\n  account_id: text\n")
	case "root":
		if err := os.RemoveAll(filepath.Join(root, "consumer")); err != nil {
			t.Fatal(err)
		}
		replaceDeploymentFixtureExactlyOnce(t, filepath.Join(root, "schema.yaml"), "connect:\n  - event: root.ready\n    from: .\n    to: consumer\n", "")
		writeClosedVariantFile(t, root, "nodes.yaml", "root-collector:\n  execution_type: system_node\n  subscribes_to: [root.ready]\n  event_handlers:\n    root.ready: {}\n")
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
