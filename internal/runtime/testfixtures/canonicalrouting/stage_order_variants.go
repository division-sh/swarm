package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// RenameStoppedRunReadinessSource changes the source coordinate, not its entry.
func RenameStoppedRunReadinessSource(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"name: routing-root-ingress\n", "name: audit2277-new-source\n")
}

func CopyForkReceiverMailboxGate(t testing.TB) string {
	t.Helper()
	root := CopyForkReceiverBusinessMutationOwnership(t, false)
	applyClosedReplacement(t, filepath.Join(root, "consumer/schema.yaml"), "  active: {}\n", `  active:
    gate:
      decision: review_receiver
      outcomes:
        approve: {advances_to: done}
`)
	return root
}
