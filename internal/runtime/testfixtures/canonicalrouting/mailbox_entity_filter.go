package canonicalrouting

import "testing"

// CopyMailboxEntityFilter retains root-ingress routing and opens a real typed
// gate so mailbox filtering is exercised without inserting synthetic cards.
func CopyMailboxEntityFilter(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "schema.yaml", `name: mailbox-entity-filter
stages:
  new: {initial: true}
  waiting:
    gate:
      decision: review
      outcomes:
        approve: {advances_to: done}
  done: {terminal: true}
pins:
  inputs:
    events:
      - event: review.requested
        source: external
`)
	writeClosedVariantFile(t, root, "entities.yaml", "review_item:\n  item_id: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "review.requested:\n  item_id: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `reviewer:
  execution_type: system_node
  subscribes_to: [review.requested]
  event_handlers:
    review.requested:
      advances_to: waiting
      data_accumulation:
        writes:
          - source_field: item_id
            target_field: item_id
`)
	return root
}
