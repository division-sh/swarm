package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyNestedPackageConnect(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `name: nested-package-connect
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
    mode: static
`,
		"schema.yaml": "name: nested-package-connect\n",
		"flows/child/package.yaml": `name: child
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: grandchild
    flow: grandchild
    mode: static
connect:
  - event: micro.done
    from: grandchild
    to: .
`,
		"flows/child/schema.yaml": `name: child
mode: static
pins:
  inputs:
    events: [micro.done]
`,
		"flows/child/nodes.yaml": `child-aggregator:
  id: child-aggregator
  execution_type: system_node
  subscribes_to: [micro.done]
  event_handlers:
    micro.done: {}
`,
		"flows/child/flows/grandchild/package.yaml": `name: grandchild
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`,
		"flows/child/flows/grandchild/schema.yaml": `name: grandchild
mode: static
pins:
  outputs:
    events: [micro.done]
`,
		"flows/child/flows/grandchild/events.yaml": "micro.done: {}\n",
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, filepath.ToSlash(name), body)
	}
	return root
}
