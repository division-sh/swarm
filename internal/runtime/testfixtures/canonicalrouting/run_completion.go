package canonicalrouting

import "testing"

func CopyRunCompletionSystemNode(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "entities.yaml", `
run:
  topic: string
`)
	writeClosedVariantFile(t, root, "schema.yaml", `
initial_state: active
terminal_states:
  - done
states:
  - active
  - done
pins:
  inputs:
    events:
      - flow.started
  outputs:
    events: [flow.started]
connect:
  - event: flow.started
    from: .
    to: discovery
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `
root-completion:
  event_handlers:
    flow.started:
      advances_to: done
`)
	writeClosedVariantFile(t, root, "discovery/schema.yaml", `
name: discovery
initial_state: active
terminal_states:
  - done
states:
  - active
  - done
pins:
  inputs:
    events:
      - flow.started
`)
	writeClosedVariantFile(t, root, "discovery/entities.yaml", `
discovery: {}
`)
	writeClosedVariantFile(t, root, "events.yaml", `
flow.started:
  entity_id:
    type: string?
  topic:
    type: string?
`)
	writeClosedVariantFile(t, root, "discovery/nodes.yaml", `
pipeline:
  execution_type: system_node
  subscribes_to:
    - flow.started
  event_handlers:
    flow.started:
      advances_to: done
`)
	return root
}
