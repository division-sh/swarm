package canonicalrouting

import "testing"

// CopyForkFanOutCarrier declares the immutable source used by fork fan-out
// projection/evaluator proofs. Loop and finite-barrier variants share its site.
func CopyForkFanOutCarrier(t testing.TB, loop, barrier bool) string {
	t.Helper()
	root := t.TempDir()
	schema := `name: fork-fan-out-carrier
stages:
  pending: {initial: true}
  review: {}
  done: {terminal: true}
  exhausted: {terminal: true}
`
	events := "items.ready:\n  items: '[text]'\nitems.child:\n  value: text\nbatch.completed:\n  total: integer\n"
	nodes := `fan-out-source:
  id: fan-out-source
  execution_type: system_node
  subscribes_to: [items.ready]
  event_handlers:
    items.ready:
`
	if loop {
		schema += `loops:
  revision:
    revision_field: revision_id
    max_attempts: 3
    escape: {advances_to: exhausted}
`
		events = "items.ready:\n  items: '[text]'\n  revision_id: text\nitems.child:\n  value: text\n  revision_id: text\nbatch.completed:\n  total: integer\n  revision_id: text\nreview.start: {}\nreview.retry:\n  revision_id: text\nreview.closed:\n  revision_id: text\n"
		nodes += "      loop: {admit: revision, from: review}\n"
	}
	nodes += `      fan_out:
        items_from: payload.items
        as: entry
        identity: entry
        emit:
          event: items.child
          fields:
            value: {cel: entry}
`
	if loop {
		nodes += "            revision_id: {ref: loop.revision_id}\n"
	}
	if barrier {
		nodes += `      join:
        id: all-items-delivered
        members: {from_fan_out: true}
        on_complete:
          emit:
            event: batch.completed
            fields:
              total: {ref: join.total}
`
		if loop {
			nodes += "              revision_id: {ref: loop.revision_id}\n"
		}
	}
	if loop {
		nodes += `loop-controller:
  id: loop-controller
  execution_type: system_node
  subscribes_to: [review.start, review.retry, review.closed]
  event_handlers:
    review.start:
      loop: {start: revision, from: pending}
      advances_to: review
    review.retry:
      loop: {repeat: revision, from: review}
      advances_to: review
    review.closed:
      loop: {close: revision, from: review}
      advances_to: done
`
	}
	for path, body := range map[string]string{"schema.yaml": schema, "events.yaml": events, "nodes.yaml": nodes, "entities.yaml": "root: {}\n"} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
