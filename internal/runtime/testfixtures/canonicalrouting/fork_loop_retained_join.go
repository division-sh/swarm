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
              revision_id: {ref: loop.revision_id}
              completed: {ref: join.completed}
`, "")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "subscribes_to: [work.requested, review.requested, review.retry, review.closed]", "subscribes_to: [work.requested, review.requested, review.retry, review.closed, checkpoint.requested]")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "  event_handlers:\n    work.requested:\n", `  event_handlers:
    checkpoint.requested:
      loop: {admit: revision, from: reviewing}
      advances_to: reviewing
      emit:
        event: join.observed
        fields:
          revision_id: {ref: loop.revision_id}
          completed: {literal: 1}
    work.requested:
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "      - {event: work.requested, source: external}", "      - {event: work.requested, source: external}\n      - {event: checkpoint.requested, source: external}")
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
      - {event: work.requested, source: external}
      - {event: work.bootstrap, source: external}
      - {event: review.retry, source: external}
      - {event: review.closed, source: external}
`,
		"entities.yaml": `work:
  members: {type: "[text]"}
  window: text
`,
		"events.yaml": `work.bootstrap:
  token: text
work.requested:
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
  swarm:
    consumer: external
`,
		"nodes.yaml": `initializer:
  execution_type: system_node
  subscribes_to: [work.bootstrap]
  event_handlers:
    work.bootstrap:
      create_entity: true
      emit:
        event: work.requested
        fields:
          token: {ref: payload.token}
controller:
  execution_type: system_node
  subscribes_to: [work.requested, review.requested, review.retry, review.closed]
  event_handlers:
    work.requested:
      loop: {start: revision, from: queued}
      data_accumulation:
        writes:
          - {target_field: members, expression: "[payload.token]"}
          - {target_field: window, expression: loop.revision_id}
      advances_to: working
      emit:
        event: review.requested
        fields:
          token: {ref: payload.token}
          revision_id: {ref: loop.revision_id}
    review.requested:
      loop: {admit: revision, from: working}
      join:
        id: reviews
        stage: working
        members: {from: entity.members, by: payload.token}
        window: {from: entity.window, by: payload.revision_id}
        output: payload.token
        on_complete:
          advances_to: reviewing
          emit:
            event: join.observed
            fields:
              revision_id: {ref: loop.revision_id}
              completed: {ref: join.completed}
        timeout:
          after: 1h
          advances_to: reviewing
    review.closed:
      loop: {close: revision, from: reviewing}
      advances_to: approved
    review.retry:
      loop: {repeat: revision, from: reviewing}
      data_accumulation:
        writes:
          - {target_field: window, expression: loop.revision_id}
      advances_to: working
      emit:
        event: review.requested
        fields:
          token: {ref: payload.token}
          revision_id: {ref: loop.revision_id}
`,
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
