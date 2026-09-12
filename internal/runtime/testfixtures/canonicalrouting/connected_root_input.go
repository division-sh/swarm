package canonicalrouting

import "testing"

// CopyConnectedRootInput gives the root an input owned by a child producer.
func CopyConnectedRootInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-input\npins:\n  inputs:\n    events: [work.ready]\nconnect:\n  - {event: work.ready, from: producer, to: .}\n")
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\nmode: static\npins:\n  outputs:\n    events: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n  value: text\n")
	return root
}
