package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkReceiverRecordedConfig creates typed config through ordinary connect
// admission. Its creating route remains explicitly unsupported for selected fork.
func CopyForkReceiverRecordedConfig(t testing.TB) string {
	t.Helper()
	root := CopyServedReceiverInitialization(t)
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "    attributes: json\n", `    attributes: json
    status: {type: boolean, default: false}
    flow_path: {type: text, default: business/path}
    instance_id: {type: text, default: business-instance}
    workflow_version: {type: numeric, default: 7.0}
auto_emit_on_create:
  event: account.initialized
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
	applyClosedReplacement(t, filepath.Join(root, "account/entities.yaml"), "  processed_count: {type: integer, initial: 0}\n", `  processed_count: {type: integer, initial: 0}
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
	applyClosedReplacement(t, filepath.Join(root, "account/nodes.yaml"), "subscribes_to: [work.ready]", "subscribes_to: [work.ready, account.initialized]")
	applyClosedReplacement(t, filepath.Join(root, "account/nodes.yaml"), "  event_handlers:\n", `  event_handlers:
    account.initialized:
      data_accumulation:
        writes: [count, label, ratio, active, attributes, status, flow_path, instance_id, workflow_version]
`)
	return root
}
