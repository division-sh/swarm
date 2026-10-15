package canonicalrouting

import "testing"

// CopyOutboxMaterializedManifest isolates materializer-before-guard ordering
// from the native workflow mutation and exact constructed receiver admission.
func CopyOutboxMaterializedManifest(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, source := range map[string]string{
		"schema.yaml":          "name: native-outbox-manifest\nstages:\n  active: {}\n",
		"entities.yaml":        "outbox_state: {}\n",
		"review/schema.yaml":   "name: review\ninstance: instance_key\nstages:\n  active: {}\npins:\n  inputs: [task.started]\n",
		"review/entities.yaml": "review_item:\n  instance_key: text\n",
		"review/events.yaml":   "task.started:\n",
		"review/nodes.yaml":    "target-node:\n  execution_type: system_node\n  subscribes_to: [task.started]\n  event_handlers:\n    task.started: {}\n",
	} {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}
