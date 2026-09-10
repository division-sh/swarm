package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopySelectedStoreExecutionTarget(t testing.TB) string {
	t.Helper()
	root := CopySelectedStoreExecution(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "name: selected-store-execution\n", "name: selected-store-execution-target\n")
	return root
}

// CopySelectedStoreExecution declares the fixed source used by the selected
// materialization store tests. Historical conversation/timer facts remain
// store fixtures; they do not invent selected executable agents or handlers.
func CopySelectedStoreExecution(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: selected-store-execution
pins:
  inputs:
    events:
      - {event: item.received, source: external}
      - {event: review.ready, source: external}
`)
	writeClosedVariantFile(t, root, "events.yaml", "item.received: {}\nreview.ready: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `test-node:
  execution_type: system_node
  subscribes_to: [item.received, review.ready]
  event_handlers:
    item.received: {}
    review.ready: {}
`)
	writeClosedVariantFile(t, root, "flow-a/schema.yaml", "name: flow-a\n")
	writeClosedVariantFile(t, root, "selected-state-flow/schema.yaml", "name: selected-state-flow\n")
	for _, owner := range []struct{ path, entity string }{{"flow-a/1", "default"}, {"selected-state-flow/at-t", "selected_case"}} {
		writeClosedVariantFile(t, root, owner.path+"/schema.yaml", `name: selected-store-state
stages:
  pending: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: state.seeded, source: external}
      - {event: state.closed, source: external}
`)
		writeClosedVariantFile(t, root, owner.path+"/entities.yaml", owner.entity+":\n  name: text\n")
		writeClosedVariantFile(t, root, owner.path+"/events.yaml", "state.seeded:\n  name: text\nstate.closed: {}\n")
		writeClosedVariantFile(t, root, owner.path+"/nodes.yaml", `state-owner:
  execution_type: system_node
  subscribes_to: [state.seeded, state.closed]
  event_handlers:
    state.seeded:
      advances_to: pending
      data_accumulation:
        writes:
          - {target_field: name, expression: payload.name}
    state.closed:
      advances_to: done
`)
	}
	return root
}
