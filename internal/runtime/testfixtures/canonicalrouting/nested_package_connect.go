package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyNestedFlowConnect(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"child/grandchild/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"child/manifest.yaml":            "version: 1.0.0\nplatform_version: '*'\n",
		"manifest.yaml":                  "version: 1.0.0\nplatform_version: '*'\n",

		"schema.yaml": "stages: []\n# fixture: nested-flow-connect\n",

		"child/schema.yaml": `# fixture: child
pins:
  inputs:
    - micro.done
connect:
  - event: micro.done
    from: grandchild
    to: .
`,
		"child/nodes.yaml": `child-aggregator:
  execution_type: system_node
  subscribes_to: [micro.done]
  event_handlers:
    micro.done: {}
`,

		"child/grandchild/schema.yaml": `# fixture: grandchild
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
