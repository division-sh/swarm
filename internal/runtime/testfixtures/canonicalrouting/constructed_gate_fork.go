package canonicalrouting

import "testing"

func CopyConstructedGateMutationControl(t testing.TB, fields bool) string {
	root := CopyConstructedGateForkControl(t, fields)
	if fields {
		writeClosedVariantFile(t, root, "entities.yaml", "default:\n  name:\n    type: text\n    initial: At R\n")
	}
	return root
}

// CopyConstructedGateForkControl binds the native fork frontier controls to
// their actual authored root input and an unchanged pending gate.
func CopyConstructedGateForkControl(t testing.TB, fields bool) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": "name: contention-gate\nstages:\n  pending:\n    initial: true\n    gate:\n      decision: review\n      outcomes:\n        approve: {advances_to: done}\n  done: {terminal: true}\npins:\n  inputs:\n    - item.received\n",
		"events.yaml": "item.received:\n",
		"nodes.yaml":  "frontier:\n  execution_type: system_node\n  subscribes_to: [item.received]\n  event_handlers:\n    item.received:\n      guard: {id: retained_frontier, check: 'true'}\n",
	}
	if fields {
		files["entities.yaml"] = "default:\n  name: text\n"
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
