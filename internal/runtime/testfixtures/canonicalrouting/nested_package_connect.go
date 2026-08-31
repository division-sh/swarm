package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyNestedFlowConnect(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{

		"schema.yaml": "name: nested-flow-connect\n",

		"child/schema.yaml": `name: child
mode: static
pins:
  inputs:
    events: [micro.done]
connect:
  - event: micro.done
    from: grandchild
    to: .
`,
		"child/nodes.yaml": `child-aggregator:
  id: child-aggregator
  execution_type: system_node
  subscribes_to: [micro.done]
  event_handlers:
    micro.done: {}
`,

		"child/grandchild/schema.yaml": `name: grandchild
mode: static
pins:
  outputs:
    events: [micro.done]
`,
		"child/grandchild/events.yaml": "micro.done: {}\n",
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, filepath.ToSlash(name), body)
	}
	return root
}
