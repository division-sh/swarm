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
    events:
      - {event: work.requested, source: external}
  outputs:
    events: [work.ready]
connect:
  - {event: work.ready, from: ., to: account}
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
mode: template
instance: account_id
initial_state: active
states: [active]
instance_variables:
  variables:
    count: {type: integer, default: 3}
    label: text
    ratio: {type: numeric, default: 2.0}
    active: boolean
    attributes: json
pins:
  inputs:
    events:
      - event: work.ready
        resolution:
          mode: select-or-create
        initialize:
          count: payload.values.count
          label: payload.values.label
          ratio: payload.values.ratio
          active: payload.values.active
          attributes: payload.values.attributes
`,
		"account/entities.yaml": `account_state:
  account_id: {type: text, _unused_reason: receiver instance identity}
  processed_count: {type: integer, initial: 0}
`,
		"account/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready:
      data_accumulation:
        writes:
          - {target_field: processed_count, expression: "has(entity.processed_count) ? entity.processed_count + 1 : 1"}
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}
