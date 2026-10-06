package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyForkLoopRetainedJoinSeparateCheckpoint(t testing.TB) string {
	t.Helper()
	root := CopyForkLoopRetainedJoin(t)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `          emit:
            event: join.observed
            fields:
              revision_id: loop.revision_id
              completed: join.completed
`, "")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    - review.closed\n  event_handlers:", "    - review.closed\n    - checkpoint.requested\n  event_handlers:")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "  event_handlers:\n    work.requested:\n", `  event_handlers:
    checkpoint.requested:
      loop: {admit: revision, from: reviewing}
      advances_to: reviewing
      emit:
        event: join.observed
        fields:
          revision_id: loop.revision_id
          completed: 1
    work.requested:
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - work.requested", "    - work.requested\n    - checkpoint.requested")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "work.requested:\n", "checkpoint.requested:\n  revision_id: text\nwork.requested:\n")
	return root
}

// CopyForkLoopRetainedJoin uses ordinary stage entry, join arrival, completion
// and loop repeat. The externally consumed outcome is a quiescent fork point.
func CopyForkLoopRetainedJoin(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml": `name: fork-loop-retained-join
stages:
  queued: {}
  working: {}
  reviewing: {}
  approved: {final: true}
  exhausted: {final: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 3
    escape: {advances_to: exhausted}
pins:
  inputs:
    - work.requested
    - review.retry
    - review.closed
  outputs:
    - join.observed
`,
		"entities.yaml": `work:
  members: {type: "[text]"}
  window: text
`,
		"events.yaml": `work.requested:
  token: text
review.requested:
  token: text
  revision_id: text
review.retry:
  token: text
  revision_id: text
review.closed:
  revision_id: text
join.observed:
  revision_id: text
  completed: integer
`,
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to:
    - work.requested
    - review.requested
    - review.retry
    - review.closed
  event_handlers:
    work.requested:
      loop:
        start: revision
        from: queued
      data_accumulation:
        writes:
          - target_field: members
            value: |-
              [payload.token]
          - target_field: window
            value: loop.revision_id
      advances_to: working
      emit:
        event: review.requested
        fields:
          token: payload.token
          revision_id: loop.revision_id
    review.requested:
      loop:
        admit: revision
        from: working
      join:
        id: reviews
        stage: working
        members:
          from: state.members
          by: payload.token
        output: payload.token
        on_complete:
          advances_to: reviewing
          emit:
            event: join.observed
            fields:
              revision_id: loop.revision_id
              completed: join.completed
        deadline:
          after: 1h
          from: stage_entry
        on_deadline:
          advances_to: reviewing
    review.closed:
      loop:
        close: revision
        from: reviewing
      advances_to: approved
    review.retry:
      loop:
        repeat: revision
        from: reviewing
      data_accumulation:
        writes:
          - target_field: window
            value: loop.revision_id
      advances_to: working
      emit:
        event: review.requested
        fields:
          token: payload.token
          revision_id: loop.revision_id
`,
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
