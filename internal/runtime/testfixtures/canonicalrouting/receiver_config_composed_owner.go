package canonicalrouting

import "testing"

// CopyReceiverConfigComposedOwner keeps the fixed fan-out and typed receiver
// source used by the native engine, publication and contender fault matrices.
func CopyReceiverConfigComposedOwner(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"schema.yaml": "name: typed-owner-matrix\nstages:\n  pending: {}\n  review: {}\n  done: {}\noutputs:\n  - items.child\nconnect:\n  - {event: items.child, from: ., to: review, resolution: select-or-create}\n",
		"state.yaml":  "account_id: string\nhandled: boolean\n",
		"events.yaml": "request:\nitems.ready:\n  items: [string]\nitems.child:\n  request_id: string\n  label: string\n  nested: json\n",
		"nodes.yaml": `fan-out-source:
  request:
    guard: true
  items.ready:
    fan_out:
      items_from: payload.items
      as: entry
      identity: entry
      emit:
        event: items.child
        fields:
          request_id: entry
          label: |-
            'label-' + entry
          nested: |-
            [7, 7.0]
`,
		"review/schema.yaml": `name: review
instance: request_id
stages:
  pending: {}
inputs:
  - items.child
`,
		"review/state.yaml":  "request_id: string\nlabel: text\nnested: json\nenabled: true\n",
		"review/events.yaml": "task.started:\n",
		"review/nodes.yaml":  "receiver:\n  items.child:\n    guard: true\n",
	} {
		writeClosedVariantFile(t, root, name, content)
	}
	return root
}
