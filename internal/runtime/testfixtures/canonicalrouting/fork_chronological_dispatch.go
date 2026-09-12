package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkChronologicalDispatch keeps the receiving entity available for both
// frontier events. Terminal-owner rejection is a separate negative proof.
func CopyForkChronologicalDispatch(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"schema.yaml": `name: fork-chronological-dispatch
initial_state: pending
states: [pending, processed, done]
terminal_states: [done]
pins:
  inputs:
    events: [item.received]
  outputs:
    events:
      - {event: item.processed, sink: harness}
`,
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "item.received:\n  entity_id: uuid\nitem.processed: {}\n",
		"nodes.yaml": `test-node:
  execution_type: system_node
  subscribes_to: [item.received]
  produces: [item.processed]
  event_handlers:
    item.received:
      advances_to: processed
      emit: {event: item.processed}
`,
	} {
		writeClosedVariantFile(t, root, filepath.Clean(path), body)
	}
	return root
}
