package canonicalrouting

import "testing"

// CopyReceiverInitializationGeometry gives both receiver levels the same
// variable names, but different values and distinct canonical instance keys.
func CopyReceiverInitializationGeometry(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"manifest.yaml": "name: receiver-initialization-geometry\nversion: 1.0.0\n",
		"schema.yaml": `name: receiver-initialization-geometry
initial_state: active
states: [active]
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [worker.requested]
connect:
  - {event: worker.requested, from: ., to: worker}
`,
		"entities.yaml": "root: {}\n",
		"events.yaml": `work.requested:
  worker_id: text
  label: text
  count: integer
worker.requested:
  worker_id: text
  label: text
  count: integer
`,
		"nodes.yaml": `root:
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [worker.requested]
  event_handlers:
    work.requested:
      emit:
        event: worker.requested
        fields:
          worker_id: payload.worker_id
          label: payload.label
          count: payload.count
`,
		"worker/schema.yaml": `name: worker
mode: template
instance: worker_id
initial_state: active
states: [active]
instance_variables:
  variables:
    label: text
    count: integer
auto_emit_on_create:
  event: worker.ready
pins:
  inputs:
    events:
      - event: worker.requested
        resolution: {mode: select-or-create}
        initialize: {label: payload.label, count: payload.count}
  outputs:
    events: [leaf.requested]
connect:
  - {event: leaf.requested, from: ., to: leaf}
`,
		"worker/events.yaml": `worker.ready:
  worker_id: text
  label: text
  count: integer
leaf.requested:
  worker_id: text
  label: text
  count: integer
`,
		"worker/entities.yaml": `worker:
  worker_id: {type: text, _unused_reason: canonical receiver key}
`,
		"worker/nodes.yaml": `receiver:
  execution_type: system_node
  subscribes_to: [worker.requested, worker.ready]
  produces: [leaf.requested]
  event_handlers:
    worker.requested:
      guard: {check: "payload.worker_id != ''"}
    worker.ready:
      emit:
        event: leaf.requested
        fields:
          worker_id: "payload.worker_id + '-leaf'"
          label: "'leaf-' + payload.label"
          count: payload.count + 1
`,
		"worker/leaf/schema.yaml": `name: leaf
mode: template
instance: worker_id
initial_state: active
states: [active]
instance_variables:
  variables:
    label: text
    count: integer
auto_emit_on_create:
  event: leaf.ready
pins:
  inputs:
    events:
      - event: leaf.requested
        resolution: {mode: select-or-create}
        initialize: {label: payload.label, count: payload.count}
`,
		"worker/leaf/events.yaml": `leaf.ready:
  worker_id: text
  label: text
  count: integer
`,
		"worker/leaf/entities.yaml": `leaf:
  worker_id: {type: text, _unused_reason: canonical receiver key}
  label: text
  count: integer
`,
		"worker/leaf/nodes.yaml": `receiver:
  execution_type: system_node
  subscribes_to: [leaf.requested, leaf.ready]
  event_handlers:
    leaf.requested:
      guard: {check: "payload.worker_id != ''"}
    leaf.ready:
      data_accumulation:
        source_event: leaf.ready
        writes: [label, count]
`,
	}
	for relative, body := range files {
		writeClosedVariantFile(t, root, relative, body)
	}
	return root
}
