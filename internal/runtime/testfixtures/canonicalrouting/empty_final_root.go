package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// A genuinely terminal root needs no invented ingress to finish an empty feed.
func CopySelectedDeploymentEmptyFinalRoot(t testing.TB) string {
	t.Helper()
	root := CopySelectedDeploymentResource(t, "root", true)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "stages: {active: {}, done: {final: true}}", "stages: {done: {final: true}}")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [fixture.close.requested]", "inputs: [root.ready, fixture.close.requested]")
	return root
}
