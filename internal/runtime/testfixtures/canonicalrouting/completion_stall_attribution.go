package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyRootIngressServedCompletionEmit consumes the output in a separate scope
// so its node settlement cannot replace the emitting agent's last turn.
func CopyRootIngressServedCompletionEmit(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedLiveAgent(t)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "item.processed:\n  item_id: text?\n", "item.processed:\n  item_id: text?\nexternal.observed:\n")
	applyClosedReplacement(t, filepath.Join(root, "agents.yaml"), "  model: regular\n", "  model: regular\n  emit_events: [external.observed]\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - item.processed\n", "    - item.processed\n  outputs: [external.observed]\nconnect:\n  - {event: external.observed, from: ., to: observation}\n")
	writeClosedVariantFile(t, root, "observation/schema.yaml", "name: observation\npins:\n  inputs: [external.observed]\n")
	writeClosedVariantFile(t, root, "observation/nodes.yaml", "observer:\n  execution_type: system_node\n  event_handlers:\n    external.observed:\n      guard: {id: record_observation, check: true}\n")
	return root
}
