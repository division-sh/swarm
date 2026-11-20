package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyNestedSelectorFork isolates the selected C53 route from the separate
// keyless audit history in the #3109 reproduction.
func CopyNestedSelectorFork(t testing.TB) string {
	t.Helper()
	root := CopyNestedEventSchema(t)
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "  - {event: ticket.done, from: ticket, to: audit}\n", "")
	applyClosedReplacement(t, filepath.Join(root, "account/ticket/schema.yaml"), "outputs: [ticket.done, ticket.cross]", "outputs: [ticket.cross]")
	applyClosedReplacement(t, filepath.Join(root, "account/ticket/events.yaml"), "ticket.done:\n", "")
	applyClosedReplacement(t, filepath.Join(root, "account/ticket/nodes.yaml"), "  opened:\n    emit: {event: ticket.done}\n", "  opened: {}\n")
	applyClosedReplacement(t, filepath.Join(root, "account/audit/schema.yaml"), "inputs: [ticket.done, root.audit]", "inputs: [root.audit]")
	applyClosedReplacement(t, filepath.Join(root, "account/audit/nodes.yaml"), "  ticket.done: {}\n", "")
	return root
}
