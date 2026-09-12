package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyMaterializingSenderExistingReceiver keeps first-delivery state acquisition
// and its same-flow emission in one ordinary authored handler transaction.
func CopyMaterializingSenderExistingReceiver(t testing.TB, requiresExisting bool) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: materializing-sender\npins:\n  inputs:\n    events: [{event: start, source: external}]\n")
	writeClosedVariantFile(t, root, "entities.yaml", "work:\n  case_id: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "start:\n  case_id: text\nwork.ready:\n  case_id: text\n")
	nodes := `intake:
  execution_type: system_node
  subscribes_to: [start]
  event_handlers:
    start:
      data_accumulation:
        writes: [{target_field: case_id, expression: payload.case_id}]
      emit: {event: work.ready, fields: {case_id: payload.case_id}}
receiver:
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready:
      guard: {id: exact_owner, check: "entity.case_id == payload.case_id"}
`
	if !requiresExisting {
		nodes = strings.Replace(nodes, "entity.case_id == payload.case_id", "payload.case_id == 'exact'", 1)
	}
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	return root
}
