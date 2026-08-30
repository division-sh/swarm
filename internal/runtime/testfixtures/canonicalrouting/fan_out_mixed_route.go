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
mode: static
pins:
  outputs:
    events: [mixed.none, mixed.one, mixed.multi]
`,
		"producer/events.yaml": "mixed.none: {}\nmixed.one: {}\nmixed.multi: {}\n",
		"one/schema.yaml": `name: one
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [mixed.one]
`,
		"one/entities.yaml": "test_entity: {}\n",
		"one/nodes.yaml": `one-node:
  id: one-node
  execution_type: system_node
  subscribes_to: [mixed.one]
  event_handlers:
    mixed.one: {}
`,
		"multi-a/schema.yaml": `name: multi-a
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [mixed.multi]
`,
		"multi-a/entities.yaml": "test_entity: {}\n",
		"multi-a/nodes.yaml": `multi-a-node:
  id: multi-a-node
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi: {}
`,
		"multi-b/schema.yaml": `name: multi-b
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [mixed.multi]
`,
		"multi-b/entities.yaml": "test_entity: {}\n",
		"multi-b/nodes.yaml": `multi-b-node:
  id: multi-b-node
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi: {}
`,
		"child/schema.yaml": `name: child
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events: [mixed.multi]
`,
		"child/entities.yaml": "test_entity: {}\n",
		"child/nodes.yaml": `child-node:
  id: child-node
  execution_type: system_node
  subscribes_to: [mixed.multi]
  event_handlers:
    mixed.multi:
      create_entity: true
`,
	}
	for label, body := range files {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}
