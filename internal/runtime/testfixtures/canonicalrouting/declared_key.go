package canonicalrouting

import (
	"fmt"
	"strings"
	"testing"
)

// CopyTargetedDeclaredKey publishes through a compiled root-to-template edge;
// tests may pin an exact receiver without granting private root ingress.
func CopyTargetedDeclaredKey(t testing.TB, acquisition string) string {
	t.Helper()
	instanceField := "receiver_id"
	if acquisition != "select" && acquisition != "select_or_create" {
		t.Fatalf("unsupported declared-key acquisition %q", acquisition)
	}
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "review/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", fmt.Sprintf(`# fixture: declared-key-execution
pins:
  inputs: [work.requested]
  outputs: [work.keyed]
connect:
  - {event: work.keyed, from: ., to: review, resolution: %s}
`, strings.ReplaceAll(acquisition, "_", "-")))
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  receiver_id: text\n  account_id: text\n  item: text\nwork.keyed:\n  receiver_id: text\n  account_id: text\n  item: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit:
        event: work.keyed
        fields:
          receiver_id: payload.receiver_id
          account_id: payload.account_id
          item: payload.item
`)
	writeClosedVariantFile(t, root, "review/schema.yaml", fmt.Sprintf(`# fixture: review
instance: %s
stages:
  active: {}
  done: {final: true}
pins:
  inputs:
    - work.keyed
`, instanceField))
	writeClosedVariantFile(t, root, "review/state.yaml", "receiver_id: {type: text, indexed: true}\naccount_id: {type: text, indexed: true}\nowner: text\n")
	writeClosedVariantFile(t, root, "review/nodes.yaml", `key-consumer:
  execution_type: system_node
  subscribes_to: [work.keyed]
  event_handlers:
    work.keyed:
      accumulate:
        into: items
        from: payload
`)
	return root
}
