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
pins:
  inputs:
    - micro.done
connect:
  - event: micro.done
    from: grandchild
    to: .
`,
		"child/nodes.yaml": `child-aggregator:
  micro.done: {}
`,

		"child/grandchild/schema.yaml": `name: grandchild
pins:
  outputs:
    - micro.done
`,
		"child/grandchild/events.yaml": "micro.done:\n",
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, filepath.ToSlash(name), body)
	}
	return root
}
