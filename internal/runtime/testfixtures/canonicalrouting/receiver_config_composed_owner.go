package canonicalrouting

import "testing"

// CopyReceiverConfigComposedOwner keeps the fixed fan-out and typed receiver
// source used by the native engine, publication and contender fault matrices.
func CopyReceiverConfigComposedOwner(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"schema.yaml":   "name: typed-owner-matrix\nstages:\n  pending: {initial: true}\n  review: {}\n  done: {}\npins:\n  outputs:\n    events: [items.child]\nconnect:\n  - {event: items.child, from: ., to: review}\n",
		"entities.yaml": "root:\n  account_id: string\n  handled: boolean\n",
		"events.yaml":   "request: {}\nitems.ready:\n  items: '[string]'\nitems.child:\n  request_id: string\n  label: string\n  nested: json\n",
		"nodes.yaml": `fan-out-source:
  execution_type: system_node
  subscribes_to: [items.ready, request]
  produces: [items.child]
  event_handlers:
    request:
      guard: {check: "true"}
    items.ready:
      fan_out:
        items_from: payload.items
        as: entry
        identity: entry
        emit:
          event: items.child
          fields:
            request_id: {cel: entry}
            label: {cel: "'label-' + entry"}
            nested: {cel: "[7, 7.0]"}
`,
		"review/schema.yaml": `name: review
mode: template
instance: request_id
instance_variables:
  variables:
    label: string
    nested: json
    enabled: {type: boolean, default: true}
stages:
  pending: {initial: true}
pins:
  inputs:
    events:
      - event: items.child
        resolution: {mode: select-or-create}
        initialize: {label: payload.label, nested: payload.nested}
`,
		"review/entities.yaml": "review_item:\n  request_id: string\n",
		"review/events.yaml":   "task.started: {}\n",
		"review/nodes.yaml":    "receiver:\n  execution_type: system_node\n  subscribes_to: [items.child]\n  event_handlers:\n    items.child:\n      guard: {check: \"true\"}\n",
	} {
		writeClosedVariantFile(t, root, name, content)
	}
	return root
}
