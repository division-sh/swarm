package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyWave1Expression(t testing.TB) string {
	t.Helper()
	root := t.TempDir()

	writeClosedVariantFile(t, root, filepath.Join("schema.yaml"), `name: wave1-expression-fixture
pins:
  inputs: [task.assigned, task.feedback]
  outputs: [task.result]
connect:
  - {event: task.assigned, from: ., to: child}
  - {event: task.feedback, from: ., to: child}
  - {event: task.result, from: child, to: .}
`)
	writeClosedVariantFile(t, root, filepath.Join("events.yaml"), "task.assigned:\n  score: numeric\ntask.feedback:\n  comment: string\n")

	writeClosedVariantFile(t, root, filepath.Join("child", "schema.yaml"), `name: child
stages:
  idle: {}
  working: {}
  done: {final: true}
pins:
  inputs: [task.assigned, task.feedback]
  outputs: [task.result]
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "entities.yaml"), `
task:
  retry_count:
    type: integer
    initial: 0
  revision_count:
    type: integer
    initial: 0
  kill_reason:
    type: text
    _unused_reason: optional test surface field
  base_score:
    type: numeric
    _unused_reason: optional test surface field
  adjusted_score:
    type: numeric
    _unused_reason: optional test surface field
  filtered_score:
    type: numeric
    _unused_reason: optional test surface field
  filtered_items:
    type: text
    _unused_reason: optional test surface field
  composite_score:
    type: numeric
    _unused_reason: optional test surface field
  expected_count:
    type: integer
    initial: 1
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "events.yaml"), `
task.result:
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "nodes.yaml"), `
worker:
  execution_type: system_node
  subscribes_to: [task.assigned, task.feedback]
  produces: [task.result]
  event_handlers:
    task.assigned:
      advances_to: working
    task.feedback:
      advances_to: done
      emit: task.result
`)

	return root
}
