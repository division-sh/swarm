package canonicalrouting

import "testing"

func AddProviderRootConsumer(t testing.TB, root string) {
	t.Helper()
	writeClosedVariantFile(t, root, "nodes.yaml", `root-observer:
  inbound.telegram.text_message: {}
`)
}
