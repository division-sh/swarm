package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkLoopAccumulator exercises generation-keyed state through ordinary
// handlers. Explicit clear is a separate authored choice, not fork cleanup.
func CopyForkLoopAccumulator(t testing.TB, clearOnAdmit bool) string {
	t.Helper()
	root := CopyForkLoopGenerationNotice(t)
	nodes := filepath.Join(root, "review", "nodes.yaml")
	applyClosedReplacement(t, nodes, "    review.requested:\n      loop:", "    review.requested:\n      accumulate:\n        into: reviews\n        from: payload\n        dedup_by: payload.token\n      loop:")
	applyClosedReplacement(t, nodes, "  execution_type: system_node\n", "  execution_type: system_node\n  state_schema:\n    fields:\n      reviews: list<Review>\n")
	if clearOnAdmit {
		applyClosedReplacement(t, nodes, "    review.requested:\n      accumulate:", "    review.requested:\n      clear: {targets: [accumulator_state]}\n      accumulate:")
	}
	writeClosedVariantFile(t, root, "review/types.yaml", "types:\n  Review:\n    revision_id: text\n    token: text\n")
	return root
}

// CopyForkLoopGenerationNotice keeps the fork frontier inside a static loop.
// Its final consumer writes a business notice without creating post-R work.
func CopyForkLoopGenerationNotice(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": `name: fork-loop-generation-notice
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
      - {event: root.closed, source: external}
  outputs:
    events: [work.started]
connect:
  - {event: work.started, from: ., to: review}
`,
		"entities.yaml": "root: {}\n",
		"events.yaml":   "work.requested:\n  token: text\nroot.closed: {}\nwork.started:\n  token: text\n",
		"nodes.yaml": `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [work.requested, root.closed]
  event_handlers:
    work.requested:
      advances_to: active
      emit:
        event: work.started
        fields: {token: {ref: payload.token}}
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
      - {event: review.retry, source: external}
      - {event: review.closed, source: external}
`,
		"review/entities.yaml": "work: {}\n",
		"review/events.yaml": `review.requested:
  revision_id: text
  token: text
review.retry:
  revision_id: text
  token: text
review.closed:
  revision_id: text
`,
		"review/nodes.yaml": `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [work.started, review.requested, review.retry, review.closed]
  event_handlers:
    work.started:
      loop: {start: revision, from: queued}
      advances_to: working
      emit:
        event: review.requested
        fields:
          revision_id: {ref: loop.revision_id}
          token: {ref: payload.token}
    review.requested:
      loop: {admit: revision, from: working}
      advances_to: reviewing
      action:
        id: mailbox_write
        mailbox:
          item_type: {literal: fork_loop_review}
          severity: {literal: normal}
          summary: {literal: review processed}
          payload:
            revision_id: {ref: loop.revision_id}
            token: {ref: payload.token}
    review.retry:
      loop: {repeat: revision, from: reviewing}
      advances_to: working
      emit:
        event: review.requested
        fields:
          revision_id: {ref: loop.revision_id}
          token: {ref: payload.token}
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
