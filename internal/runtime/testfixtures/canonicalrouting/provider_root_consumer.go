package canonicalrouting

import "testing"

func AddProviderRootConsumer(t testing.TB, root string) {
	t.Helper()
	writeClosedVariantFile(t, root, "nodes.yaml", `root-observer:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message: {}
`)
}
