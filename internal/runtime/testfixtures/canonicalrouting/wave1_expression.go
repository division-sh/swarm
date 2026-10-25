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
	writeClosedVariantFile(t, root, filepath.Join("child", "agents.yaml"), `writer:
  role: writer
  intent: {inline: "Maintain the declared expression fixture fields."}
  model: regular
  subscriptions: [task.feedback]
  writes: [kill_reason, base_score, adjusted_score, filtered_score, filtered_items, composite_score]
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
