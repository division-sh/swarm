package canonicalrouting

import "testing"

func CopyConstructedGateMutationControl(t testing.TB, fields bool) string {
	root := CopyConstructedGateForkControl(t, fields)
	if fields {
		writeClosedVariantFile(t, root, "state.yaml", "name: \"At R\"\n")
	}
	return root
}

// CopyConstructedGateForkControl binds the native fork frontier controls to
// their actual authored root input and an unchanged pending gate.
func CopyConstructedGateForkControl(t testing.TB, fields bool) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": "name: contention-gate\nstages:\n  pending:\n    gate:\n      decision: review\n      outcomes:\n        approve: {advances_to: done}\n  done: {final: true}\ninputs:\n  - item.received\n",
		"events.yaml": "item.received:\n",
		"nodes.yaml":  "frontier:\n  item.received:\n    guard: true\n",
	}
	if fields {
		files["state.yaml"] = "default:\n  name: text\n"
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
