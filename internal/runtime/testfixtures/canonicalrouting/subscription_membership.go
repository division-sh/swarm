package canonicalrouting

import "testing"

// CopyAncestorEventWithoutReceiverMembership returns the exact invalid
// root-input/child-subscriber shape used to prove receiver-local admission.
func CopyAncestorEventWithoutReceiverMembership(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	removeClosedVariantFiles(t, root, "entities.yaml", "events.yaml", "nodes.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", `name: root
pins:
  inputs:
    events:
      - root.started
`)
	writeClosedVariantFile(t, root, "events.yaml", "root.started:\n")
	writeClosedVariantFile(t, root, "child/schema.yaml", `name: child
stages:
  idle: {initial: true}
  done: {terminal: true}
`)
	writeClosedVariantFile(t, root, "child/nodes.yaml", `listener:
  execution_type: system_node
  event_handlers:
    root.started:
      advances_to: done
`)
	return root
}
