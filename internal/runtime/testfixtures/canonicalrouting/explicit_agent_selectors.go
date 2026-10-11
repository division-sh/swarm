package canonicalrouting

import "testing"

func ExplicitAgentSelectorDocuments() map[string]string {
	return map[string]string{
		"schema.yaml":        "name: explicit-static-receivers\npins:\n  inputs: [work.requested]\nconnect:\n  - {event: work.requested, from: ., to: review}\n  - {event: work.requested, from: ., to: other}\n",
		"events.yaml":        "work.requested:\n",
		"agents.yaml":        "root-worker:\n  model: regular\n  intent: {inline: Review root-local work.}\n  subscriptions: [work.requested]\n",
		"review/schema.yaml": "name: review\nstages:\n  pending: {}\npins:\n  inputs: [work.requested]\n",
		"review/agents.yaml": "worker:\n  model: regular\n  intent: {inline: Review the selected work.}\n  subscriptions: [work.requested]\n",
		"review/nodes.yaml":  "handler:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
		"other/schema.yaml":  "name: other\nstages:\n  pending: {}\npins:\n  inputs: [work.requested]\n",
		"other/agents.yaml":  "worker:\n  model: regular\n  intent: {inline: Review only work selected for other.}\n  subscriptions: [work.requested]\n",
	}
}

func CopyExplicitAgentSelectors(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range ExplicitAgentSelectorDocuments() {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
