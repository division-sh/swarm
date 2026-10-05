package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkReceiverRecordedConfig creates typed state through ordinary connect
// admission. Its creating route remains explicitly unsupported for selected fork.
func CopyForkReceiverRecordedConfig(t testing.TB) string {
	t.Helper()
	root := CopyServedReceiverInitialization(t)
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "    - work.ready\n", `    - work.ready
auto_emit_on_create:
  event: account.initialized
`)
	writeClosedVariantFile(t, root, "types.yaml", `types:
  InitializationValues:
    count: integer
    label: text
    ratio: numeric
    active: boolean
    attributes: json
`)
	writeClosedVariantFile(t, root, "events.yaml", `work.requested:
  account_id: text
  values: InitializationValues
work.ready:
  key: account_id
  account_id: text
  count: integer
  label: text
  ratio: numeric
  active: boolean
  attributes: json
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `producer:
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.ready]
  event_handlers:
    work.requested:
      emit:
        event: work.ready
        fields:
          account_id: payload.account_id
          count: payload.values.count
          label: payload.values.label
          ratio: payload.values.ratio
          active: payload.values.active
          attributes: payload.values.attributes
`)
	writeClosedVariantFile(t, root, "account/events.yaml", `account.initialized:
  account_id: text
  count: integer
  label: text
  ratio: numeric
  active: boolean
  attributes: json
  status: boolean
  flow_path: text
  instance_id: text
  workflow_version: numeric
`)
	writeClosedVariantFile(t, root, "account/entities.yaml", `account_state:
  account_id: {type: text, _unused_reason: receiver instance identity}
  processed_count: integer?
  count: integer
  label: text
  ratio: numeric
  active: boolean
  attributes: json
  status: {type: boolean, initial: false}
  flow_path: {type: text, initial: 'business/path'}
  instance_id: {type: text, initial: 'business-instance'}
  workflow_version: {type: numeric, initial: 7.0}
`)
	applyClosedReplacement(t, filepath.Join(root, "account/nodes.yaml"), "  subscribes_to:\n    - work.ready\n", "  subscribes_to:\n    - work.ready\n    - account.initialized\n")
	applyClosedReplacement(t, filepath.Join(root, "account/nodes.yaml"), "  event_handlers:\n", `  event_handlers:
    account.initialized:
      data_accumulation:
        writes: [count, label, ratio, active, attributes, status, flow_path, instance_id, workflow_version]
`)
	return root
}
