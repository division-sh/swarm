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
pins:
  outputs:
    - mixed.none
    - mixed.one
    - mixed.multi
`,
		"producer/events.yaml": "mixed.none:\nmixed.one:\nmixed.multi:\n",
		"one/schema.yaml": `# fixture: one
stages:
  active: {}
pins:
  inputs:
    - mixed.one
`,
		"one/entities.yaml": "test_entity: {}\n",
		"one/nodes.yaml": `one-node:
  execution_type: system_node
  subscribes_to: [mixed.one]
  event_handlers:
    mixed.one: {}
`,
		"multi-a/schema.yaml": `# fixture: multi-a
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"multi-a/entities.yaml": "test_entity: {}\n",
		"multi-a/nodes.yaml": `multi-a-node:
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi: {}
`,
		"multi-b/schema.yaml": `# fixture: multi-b
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"multi-b/entities.yaml": "test_entity: {}\n",
		"multi-b/nodes.yaml": `multi-b-node:
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi: {}
`,
		"child/schema.yaml": `# fixture: child
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"child/entities.yaml": "test_entity: {}\n",
		"child/nodes.yaml": `child-node:
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi: {}
`,
	}
	for label, body := range files {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}
