package canonicalrouting

import "testing"

// CopyServedReceiverInitialization sends a declared typed object through an
// ordinary producer emission and connect, not a direct materializer test hook.
func CopyServedReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml": `name: served-receiver-initialization
inputs:
  - work.requested
outputs:
  - work.ready
connect:
  - {event: work.ready, from: ., to: account, resolution: select-or-create}
`,
		"types.yaml": `InitializationValues:
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
  work.requested:
    emit:
      event: work.ready
      fields:
        account_id: payload.account_id
        values: payload.values
`,
		"account/schema.yaml": `name: account
instance: account_id
inputs:
  - work.ready
`,
		"account/state.yaml": `account_id: text
values: InitializationValues
processed_count: 0
`,
		"account/nodes.yaml": `collector:
  work.ready:
    data_accumulation:
      writes:
        - target_field: processed_count
          value: |-
            has(state.processed_count) ? state.processed_count + 1 : 1
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}
