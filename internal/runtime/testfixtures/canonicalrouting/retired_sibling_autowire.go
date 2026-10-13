package canonicalrouting

import "testing"

// The unconnected private input is intentional negative component evidence.
// The explicit root export makes the producer output independently consumable.
func CopyRetiredSiblingAutowire(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: retired-sibling-autowire
pins:
  outputs: [scan.requested]
connect:
  - {event: scan.requested, from: producer, to: '.'}
`)
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\npins:\n  outputs: [scan.requested]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "scan.requested:\n")
	writeClosedVariantFile(t, root, "discovery/schema.yaml", "name: discovery\npins:\n  inputs: [scan.requested]\n")
	writeClosedVariantFile(t, root, "discovery/events.yaml", "scan.requested:\n")
	writeClosedVariantFile(t, root, "discovery/nodes.yaml", "scan-orchestrator:\n  execution_type: system_node\n  event_handlers:\n    scan.requested: {}\n")
	return root
}
