package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkLoopAccumulator exercises generation-keyed state through ordinary
// handlers. Explicit clear is a separate authored choice, not fork cleanup.
func CopyForkLoopAccumulator(t testing.TB, clearOnAdmit bool) string {
	t.Helper()
	root := CopyForkLoopGenerationState(t)
	nodes := filepath.Join(root, "review", "nodes.yaml")
	applyClosedReplacement(t, nodes, "    review.requested:\n      loop:", "    review.requested:\n      accumulate:\n        into: reviews\n        from: payload\n        dedup_by: payload.token\n      loop:")
	applyClosedReplacement(t, nodes, "  execution_type: system_node\n", "  execution_type: system_node\n  state_schema:\n    fields:\n      reviews: list<Review>\n")
	if clearOnAdmit {
		applyClosedReplacement(t, nodes, "    review.requested:\n      accumulate:", "    review.requested:\n      clear: {targets: [accumulator_state]}\n      accumulate:")
	}
	writeClosedVariantFile(t, root, "review/types.yaml", "types:\n  Review:\n    revision_id: text\n    token: text\n")
	return root
}

// CopyForkLoopGenerationState keeps the fork frontier inside a static loop.
// Its final consumer writes business fields without creating post-R work.
func CopyForkLoopGenerationState(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": `name: fork-loop-generation-state
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - work.requested
      - root.closed
      - review.retry
      - review.closed
  outputs:
    events: [work.started, review.retry, review.closed]
connect:
  - {event: work.started, from: ., to: review}
  - {event: review.retry, from: ., to: review}
  - {event: review.closed, from: ., to: review}
`,
		"entities.yaml": "root: {}\n",
		"events.yaml":   "work.requested:\n  token: text\nroot.closed:\nwork.started:\n  token: text\nreview.retry:\n  revision_id: text\n  token: text\nreview.closed:\n  revision_id: text\n",
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [work.requested, root.closed]
  event_handlers:
    work.requested:
      advances_to: active
      emit:
        event: work.started
        fields: {token: "${payload.token}"}
    root.closed:
      advances_to: done
`,
		"review/schema.yaml": `name: review
stages:
  queued: {initial: true}
  working: {}
  reviewing: {}
  approved: {terminal: true}
  exhausted: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 3
    escape: {advances_to: exhausted}
pins:
  inputs:
    events:
      - work.started
      - review.retry
      - review.closed
`,
		"review/entities.yaml": "work:\n  observed_revision: {type: text, initial: ''}\n  observed_token: {type: text, initial: ''}\n",
		"review/events.yaml": `review.requested:
  revision_id: text
  token: text
`,
		"review/nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [work.started, review.requested, review.retry, review.closed]
  event_handlers:
    work.started:
      loop: {start: revision, from: queued}
      advances_to: working
      emit:
        event: review.requested
        fields:
          revision_id: "${loop.revision_id}"
          token: "${payload.token}"
    review.requested:
      loop: {admit: revision, from: working}
      advances_to: reviewing
      data_accumulation:
        writes:
          - {target_field: observed_revision, value: "${loop.revision_id}"}
          - {target_field: observed_token, value: "${payload.token}"}
    review.retry:
      loop: {repeat: revision, from: reviewing}
      advances_to: working
      emit:
        event: review.requested
        fields:
          revision_id: "${loop.revision_id}"
          token: "${payload.token}"
    review.closed:
      loop: {close: revision, from: reviewing}
      advances_to: approved
`,
	}
	for path, body := range files {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
