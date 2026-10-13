package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// Q1 is root -> provider; Q2 is provider -> t. The real approval/activity
// records one result while both return obligations are open at its request cut.
func CopyNestedRecordedActivityReply(t testing.TB, providerURL string) string {
	t.Helper()
	root := CopyRecordedActivityReply(t, providerURL)
	copyTree(t, filepath.Join(root, "provider"), filepath.Join(root, "t"))
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "name: provider\n", "name: t\n")
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "inputs: [provider.requested]", "inputs: [t.requested]")
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "outputs: [provider.replied]", "outputs: [t.replied]")
	applyClosedReplacement(t, filepath.Join(root, "t/events.yaml"), "provider.replied:", "t.replied:")
	applyClosedReplacement(t, filepath.Join(root, "t/nodes.yaml"), "subscribes_to: [provider.requested,", "subscribes_to: [t.requested,")
	applyClosedReplacement(t, filepath.Join(root, "t/nodes.yaml"), "    provider.requested:\n", "    t.requested:\n")
	applyClosedReplacement(t, filepath.Join(root, "t/nodes.yaml"), "provider.replied", "t.replied")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    correlation_key: request_id\n", `    correlation_key: request_id
  - {event: t.requested, from: provider, to: t}
  - event: t.replied
    from: t
    to: provider
    replies_to: t.requested
    correlation_key: request_id
`)
	removeClosedVariantFiles(t, root, "provider/entities.yaml", "provider/tools.yaml")
	writeClosedVariantFile(t, root, "provider/schema.yaml", `name: provider
pins:
  inputs: [provider.requested, t.replied]
  outputs: [t.requested, provider.replied]
`)
	writeClosedVariantFile(t, root, "provider/events.yaml", `t.requested:
  account_id: text
  request_id: text
provider.replied:
  account_id: text
  request_id: text
  result: text
`)
	writeClosedVariantFile(t, root, "provider/nodes.yaml", `provider-node:
  execution_type: system_node
  subscribes_to: [provider.requested, t.replied]
  event_handlers:
    provider.requested:
      emit:
        event: t.requested
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
    t.replied:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`)
	return root
}
