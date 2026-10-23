package canonicalrouting

import "testing"

// WriteManagerSubscriptionAdmission owns the root and keyed review scopes used
// by the manager's registration and reconfiguration refusal proofs.
func WriteManagerSubscriptionAdmission(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for relative, source := range map[string]string{
		"schema.yaml": "pins:\n  inputs: [root.ready]\n",
		"events.yaml": "root.ready:\n",
		"nodes.yaml": `root-observer:
  execution_type: system_node
  subscribes_to: [root.ready]
  event_handlers:
    root.ready: {}
`,
		"review/schema.yaml":   "instance: review_id\npins:\n  inputs: [task.ready]\n",
		"review/entities.yaml": "review:\n  review_id: {type: text, immutable: true}\n",
		"review/events.yaml":   "task.ready:\n",
		"review/agents.yaml": `reviewer:
  model: regular
  intent: {inline: Observe review-local work.}
  subscriptions: [task.ready]
`,
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}
