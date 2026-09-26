package canonicalrouting

import (
	"os"
	"path/filepath"
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
      guard: {id: exact_owner, check: "has(entity.case_id) && entity.case_id == payload.case_id"}
`
	if !requiresExisting {
		nodes = strings.Replace(nodes, "has(entity.case_id) && entity.case_id == payload.case_id", "payload.case_id == 'exact'", 1)
	}
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	return root
}

func CopyProspectiveTerminalSender(t testing.TB) string {
	t.Helper()
	root := CopyMaterializingSenderExistingReceiver(t, true)
	writeClosedVariantFile(t, root, "schema.yaml", "name: materializing-sender\nstages:\n  waiting: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [{event: start, source: external}]\n")
	path := filepath.Join(root, "nodes.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	nodes := strings.Replace(string(raw), "    start:\n", "    start:\n      advances_to: done\n", 1)
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	return root
}
