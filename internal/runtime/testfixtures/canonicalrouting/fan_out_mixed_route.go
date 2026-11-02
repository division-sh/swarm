package canonicalrouting

import "testing"

// CopyFanOutMixedRoute owns the positive mixed-cardinality route source used
// by selected-store fan-out atomicity proofs.
func CopyFanOutMixedRoute(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"child/manifest.yaml":    "version: 1.0.0\nplatform_version: '*'\n",
		"manifest.yaml":          "version: 1.0.0\nplatform_version: '*'\n",
		"multi-a/manifest.yaml":  "version: 1.0.0\nplatform_version: '*'\n",
		"multi-b/manifest.yaml":  "version: 1.0.0\nplatform_version: '*'\n",
		"one/manifest.yaml":      "version: 1.0.0\nplatform_version: '*'\n",
		"producer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `# fixture: mixed-route
connect:
  - {event: mixed.one, from: producer, to: one}
  - {event: mixed.multi, from: producer, to: multi-a}
  - {event: mixed.multi, from: producer, to: multi-b}
  - {event: mixed.multi, from: producer, to: child}
`,
		"producer/schema.yaml": `# fixture: producer
outputs:
  - mixed.none
  - mixed.one
  - mixed.multi
`,
		"producer/events.yaml": "mixed.none:\nmixed.one:\nmixed.multi:\n",
		"one/schema.yaml": `# fixture: one
stages:
  active: {}
inputs:
  - mixed.one
`,
		"one/nodes.yaml": `one-node:
  mixed.one: {}
`,
		"multi-a/schema.yaml": `# fixture: multi-a
stages:
  active: {}
inputs:
  - mixed.multi
`,
		"multi-a/nodes.yaml": `multi-a-node:
  mixed.multi: {}
`,
		"multi-b/schema.yaml": `# fixture: multi-b
stages:
  active: {}
inputs:
  - mixed.multi
`,
		"multi-b/nodes.yaml": `multi-b-node:
  mixed.multi: {}
`,
		"child/schema.yaml": `# fixture: child
stages:
  active: {}
inputs:
  - mixed.multi
`,
		"child/nodes.yaml": `child-node:
  mixed.multi: {}
`,
	}
	for label, body := range files {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}
