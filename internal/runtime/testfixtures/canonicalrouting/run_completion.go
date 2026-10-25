package canonicalrouting

import "testing"

func CopyRunCompletionSystemNode(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "state.yaml", `topic: string
`)
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  active: {}
  done: {final: true}
inputs:
  - flow.started
outputs:
  - flow.started
connect:
  - event: flow.started
    from: .
    to: discovery
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `
root-completion:
  flow.started:
    advances_to: done
`)
	writeClosedVariantFile(t, root, "discovery/schema.yaml", `name: discovery
stages:
  active: {}
  done: {final: true}
inputs:
  - flow.started
`)
	removeClosedVariantFiles(t, root, "discovery/state.yaml")
	writeClosedVariantFile(t, root, "events.yaml", `
flow.started:
  entity_id:
    type: string?
  topic:
    type: string?
`)
	writeClosedVariantFile(t, root, "discovery/nodes.yaml", `
pipeline:
  flow.started:
    advances_to: done
`)
	return root
}
