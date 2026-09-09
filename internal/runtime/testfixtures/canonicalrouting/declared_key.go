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
	writeClosedVariantFile(t, root, "schema.yaml", `name: declared-key-execution
pins:
  inputs:
    events: [{event: work.requested, source: external}]
  outputs:
    events: [work.keyed]
connect:
  - {event: work.keyed, from: ., to: review}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  receiver_id: text\n  account_id: text\n  item: text\nwork.keyed:\n  receiver_id: text\n  account_id: text\n  item: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit:
        event: work.keyed
        fields:
          receiver_id: {expression: payload.receiver_id}
          account_id: {expression: payload.account_id}
          item: {expression: payload.item}
`)
	writeClosedVariantFile(t, root, "review/schema.yaml", fmt.Sprintf(`name: review
mode: template
instance: %s
stages:
  active: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events:
      - event: work.keyed
        resolution: {mode: %s}
`, instanceField, strings.ReplaceAll(acquisition, "_", "-")))
	writeClosedVariantFile(t, root, "review/entities.yaml", "review_entity:\n  receiver_id: {type: text, indexed: true}\n  account_id: {type: text, indexed: true}\n  owner: {type: text, _unused_reason: distinguishes existing receiver state}\n")
	writeClosedVariantFile(t, root, "review/nodes.yaml", `key-consumer:
  id: key-consumer
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
