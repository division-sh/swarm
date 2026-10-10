package canonicalrouting

import "testing"

func CopyCompleteEventCarrierElection(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: complete-event-carrier\n")
	writeClosedVariantFile(t, root, "events.yaml", "custom.replay.checked:\n  task_id: string\n  text: string\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "declarative-node:\n  execution_type: system_node\n  event_handlers:\n    custom.replay.checked: {}\n")
	writeClosedVariantFile(t, root, "agents.yaml", "complete-event-agent:\n  model: regular\n  intent: {inline: Prove complete event dispatch.}\n  subscriptions: [custom.replay.checked]\n")
	return root
}
