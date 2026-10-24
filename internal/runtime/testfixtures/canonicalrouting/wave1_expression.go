package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyWave1Expression(t testing.TB) string {
	t.Helper()
	root := t.TempDir()

	writeClosedVariantFile(t, root, filepath.Join("schema.yaml"), `name: wave1-expression-fixture
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
inputs: [task.assigned, task.feedback]
outputs: [task.result]
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "state.yaml"), `retry_count: 0
revision_count: 0
kill_reason: text
base_score: numeric
adjusted_score: numeric
filtered_score: numeric
filtered_items: text
composite_score: numeric
expected_count: 1
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "events.yaml"), `
task.result:
`)
	writeClosedVariantFile(t, root, filepath.Join("child", "nodes.yaml"), `
worker:
  task.assigned:
    advances_to: working
  task.feedback:
    advances_to: done
    emit: task.result
`)

	return root
}
