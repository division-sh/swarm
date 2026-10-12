package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// The actual ordinal publication is the cut, before its receiver has returned.
// No marker, synthetic delivery or externally seeded intent is introduced.
func CopyPublishedFanOutReply(t testing.TB) string {
	t.Helper()
	root := CopyRootReplyBoundary(t, true, true)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"request.started:\n  account_id: text\n  request_id: text\n",
		"request.started:\n  account_id: text\n  request_id: text\n  items: '[text]'\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"provider.requested:\n  account_id: text\n  request_id: text\n",
		"provider.requested:\n  account_id: text\n  request_id: text\n  items: '[text]'\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"          account_id: payload.account_id\n          request_id: payload.request_id\n",
		"          account_id: payload.account_id\n          request_id: payload.request_id\n          items: payload.items\n")
	writeClosedVariantFile(t, root, "provider/schema.yaml", `name: provider
pins:
  inputs: [provider.requested]
  outputs: [provider.replied]
`)
	writeClosedVariantFile(t, root, "provider/events.yaml", `provider.replied:
  account_id: text
  request_id: text
  result: text
item.ready:
  account_id: text
  request_id: text
`)
	writeClosedVariantFile(t, root, "provider/nodes.yaml", `provider-node:
  execution_type: system_node
  subscribes_to: [provider.requested, item.ready]
  event_handlers:
    provider.requested:
      fan_out:
        items_from: payload.items
        as: entry
        identity: entry
        emit:
          event: item.ready
          fields:
            account_id: payload.account_id
            request_id: entry
    item.ready:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: "from-own-ordinal"
`)
	return root
}

// The inner request's publication cut retains two open returns, before either
// the inner handler or its deferred fan-out has executed.
func CopyNestedFanOutReply(t testing.TB) string {
	t.Helper()
	root := CopyPublishedFanOutReply(t)
	copyTree(t, filepath.Join(root, "provider"), filepath.Join(root, "t"))
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "name: provider\n", "name: t\n")
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "provider.requested", "t.requested")
	applyClosedReplacement(t, filepath.Join(root, "t/schema.yaml"), "provider.replied", "t.replied")
	applyClosedReplacement(t, filepath.Join(root, "t/events.yaml"), "provider.replied", "t.replied")
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
	writeClosedVariantFile(t, root, "provider/schema.yaml", `name: provider
pins:
  inputs: [provider.requested, t.replied]
  outputs: [t.requested, provider.replied]
`)
	writeClosedVariantFile(t, root, "provider/events.yaml", `t.requested:
  account_id: text
  request_id: text
  items: '[text]'
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
          items: payload.items
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
