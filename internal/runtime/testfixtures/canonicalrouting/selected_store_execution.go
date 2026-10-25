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
    - item.received
    - review.ready
    - state.seeded
    - state.closed
  outputs:
    - state.seeded
    - state.closed
connect:
  - {from: ., event: state.seeded, to: flow-a/1}
  - {from: ., event: state.closed, to: flow-a/1}
  - {from: ., event: state.seeded, to: selected-state-flow/at-t}
  - {from: ., event: state.closed, to: selected-state-flow/at-t}
`)
	writeClosedVariantFile(t, root, "events.yaml", "item.received:\nreview.ready:\nstate.seeded:\n  name: text\nstate.closed:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `test-node:
  execution_type: system_node
  subscribes_to: [item.received, review.ready]
  event_handlers:
    item.received: {}
    review.ready: {}
`)
	writeClosedVariantFile(t, root, "flow-a/schema.yaml", "name: flow-a\n")
	writeClosedVariantFile(t, root, "selected-state-flow/schema.yaml", "name: selected-state-flow\n")
	for _, owner := range []string{"flow-a/1", "selected-state-flow/at-t"} {
		writeClosedVariantFile(t, root, owner+"/schema.yaml", `name: selected-store-state
stages:
  pending: {}
  done: {final: true}
pins:
  inputs:
    - state.seeded
    - state.closed
`)
		writeClosedVariantFile(t, root, owner+"/state.yaml", "name: text\n")
		writeClosedVariantFile(t, root, owner+"/nodes.yaml", `state-owner:
  execution_type: system_node
  subscribes_to: [state.seeded, state.closed]
  event_handlers:
    state.seeded:
      advances_to: pending
      data_accumulation:
        writes:
          - {target_field: name, value: payload.name}
    state.closed:
      advances_to: done
`)
	}
	return root
}
