package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// Both versions are input capabilities of the selected root. This does not
// feed an internal child declaration through an unaddressed publication.
func CopyTwoRootDeploymentFeeds(t testing.TB) string {
	t.Helper()
	root := CopySelectedDeploymentResource(t, "root", true)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [fixture.close.requested]", "inputs: [root.ready, root.other.ready, fixture.close.requested]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - root.ready\n", "    - root.ready\n    - root.other.ready\n")
	ApplyOverlay(t, root, "events.yaml", "root.other.ready:\n  key: account_id\n  account_id: text\n  document: json?\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "subscribes_to: [root.ready, fixture.close.requested]", "subscribes_to: [root.ready, root.other.ready, fixture.close.requested]")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    root.ready: {}\n", "    root.ready: {}\n    root.other.ready: {}\n")
	return root
}
