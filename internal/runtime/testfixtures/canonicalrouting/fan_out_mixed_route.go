package canonicalrouting

import "testing"

// CopyFanOutMixedRoute owns the positive mixed-cardinality route source used
// by selected-store fan-out atomicity proofs.
func CopyFanOutMixedRoute(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": `name: mixed-route
connect:
  - {event: mixed.one, from: producer, to: one}
  - {event: mixed.multi, from: producer, to: multi-a}
  - {event: mixed.multi, from: producer, to: multi-b}
  - {event: mixed.multi, from: producer, to: child}
`,
		"producer/schema.yaml": `name: producer
pins:
  outputs:
    - mixed.none
    - mixed.one
    - mixed.multi
`,
		"producer/events.yaml": "mixed.none:\nmixed.one:\nmixed.multi:\n",
		"one/schema.yaml": `name: one
stages:
  active: {}
pins:
  inputs:
    - mixed.one
`,
		"one/entities.yaml": "test_entity: {}\n",
		"one/nodes.yaml": `one-node:
  mixed.one: {}
`,
		"multi-a/schema.yaml": `name: multi-a
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"multi-a/entities.yaml": "test_entity: {}\n",
		"multi-a/nodes.yaml": `multi-a-node:
  mixed.multi: {}
`,
		"multi-b/schema.yaml": `name: multi-b
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"multi-b/entities.yaml": "test_entity: {}\n",
		"multi-b/nodes.yaml": `multi-b-node:
  mixed.multi: {}
`,
		"child/schema.yaml": `name: child
stages:
  active: {}
pins:
  inputs:
    - mixed.multi
`,
		"child/entities.yaml": "test_entity: {}\n",
		"child/nodes.yaml": `child-node:
  mixed.multi: {}
`,
	}
	for label, body := range files {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}
