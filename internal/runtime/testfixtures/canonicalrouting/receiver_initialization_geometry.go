package canonicalrouting

import "testing"

// CopyReceiverInitializationAgentGeometry puts a real managed agent between
// the two creation levels so a restart can interrupt, rather than skip, work.
func CopyReceiverInitializationAgentGeometry(t testing.TB) string {
	t.Helper()
	root := CopyReceiverInitializationGeometry(t)
	writeClosedVariantFile(t, root, "worker/nodes.yaml", `receiver:
  execution_type: system_node
  subscribes_to: [worker.requested]
  event_handlers:
    worker.requested:
      guard: {check: payload.worker_id != ''}
`)
	writeClosedVariantFile(t, root, "worker/agents.yaml", `bridge:
  model: regular
  intent:
    inline: Create the next receiver using worker_id, label and count from the admitted configuration.
  subscriptions: [worker.ready]
  emit_events: [leaf.requested]
`)
	return root
}

// CopyReceiverInitializationGeometry gives both receiver levels the same
// variable names, but different values and distinct canonical instance keys.
func CopyReceiverInitializationGeometry(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"manifest.yaml": "name: receiver-initialization-geometry\nversion: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `name: receiver-initialization-geometry
pins:
  inputs:
    - work.requested
  outputs:
    - worker.requested
connect:
  - {event: worker.requested, from: ., to: worker, resolution: select-or-create}
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
instance: worker_id
auto_emit_on_create:
  event: worker.ready
pins:
  inputs:
    - worker.requested
  outputs:
    - leaf.requested
connect:
  - {event: leaf.requested, from: ., to: leaf, resolution: select-or-create}
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
  label: text
  count: integer
`,
		"worker/nodes.yaml": `receiver:
  execution_type: system_node
  subscribes_to: [worker.requested, worker.ready]
  produces: [leaf.requested]
  event_handlers:
    worker.requested:
      guard: {check: payload.worker_id != ''}
    worker.ready:
      emit:
        event: leaf.requested
        fields:
          worker_id: payload.worker_id + '-leaf'
          label: |-
                   'leaf-' + payload.label
          count: payload.count + 1
`,
		"worker/leaf/schema.yaml": `name: leaf
instance: worker_id
auto_emit_on_create:
  event: leaf.ready
pins:
  inputs:
    - leaf.requested
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
      guard: {check: payload.worker_id != ''}
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

// CopyReceiverStateMutationGeometry retains the ordinary route while giving
// its creating-input handler an explicit lawful state write.
func CopyReceiverStateMutationGeometry(t testing.TB) string {
	t.Helper()
	root := CopyReceiverInitializationGeometry(t)
	writeClosedVariantFile(t, root, "worker/nodes.yaml", `receiver:
  execution_type: system_node
  subscribes_to: [worker.requested, worker.ready]
  produces: [leaf.requested]
  event_handlers:
    worker.requested:
      data_accumulation:
        source_event: worker.requested
        writes: [label, count]
    worker.ready:
      emit:
        event: leaf.requested
        fields:
          worker_id: payload.worker_id + '-leaf'
          label: "'leaf-' + payload.label"
          count: payload.count + 1
`)
	return root
}
