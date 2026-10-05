package canonicalrouting

import "testing"

// CopyServedReceiverInitialization sends a declared typed object through an
// ordinary producer emission and connect, not a direct materializer test hook.
func CopyServedReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml": `name: served-receiver-initialization
pins:
  inputs:
    - work.requested
  outputs:
    - work.ready
connect:
  - {event: work.ready, from: ., to: account, resolution: select-or-create}
`,
		"types.yaml": `types:
  InitializationValues:
    count: integer?
    label: text?
    ratio: numeric?
    active: boolean?
    attributes: json?
`,
		"events.yaml": `work.requested:
  account_id: text
  values: InitializationValues
work.ready:
  key: account_id
  account_id: text
  values: InitializationValues
`,
		"nodes.yaml": `producer:
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.ready]
  event_handlers:
    work.requested:
      emit:
        event: work.ready
        fields:
          account_id: payload.account_id
          values: payload.values
`,
		"account/schema.yaml": `name: account
instance: account_id
pins:
  inputs:
    - work.ready
`,
		"account/entities.yaml": `account_state:
  account_id: {type: text, _unused_reason: receiver instance identity}
  values: InitializationValues
  processed_count: {type: integer, initial: 0}
`,
		"account/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to:
    - work.ready
  event_handlers:
    work.ready:
      data_accumulation:
        writes:
          - target_field: processed_count
            value: |-
              has(entity.processed_count) ? entity.processed_count + 1 : 1
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}
