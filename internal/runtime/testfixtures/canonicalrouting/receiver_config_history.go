package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyReceiverConfigHistory declares the recorded business values used by the
// fixed-revision and fork-companion proofs, including runtime-control collisions.
func CopyReceiverConfigHistory(t testing.TB) string {
	t.Helper()
	root := CopyTemplateInstanceRoute(t, TemplateInstanceRouteOptions{Consumer: TemplateInstanceAgentConsumer})
	writeClosedVariantFile(t, root, "consumer/schema.yaml", `name: consumer
instance: vertical_id
pins:
  inputs:
    - deploy.done
`)
	applyClosedReplacement(t, filepath.Join(root, "consumer/entities.yaml"), "  vertical_id:", "  nested: json?\n  status: boolean?\n  flow_path: json?\n  vertical_id:")
	return root
}
