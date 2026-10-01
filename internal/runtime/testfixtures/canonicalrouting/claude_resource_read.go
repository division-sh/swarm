package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyClaudeResourceRead derives the retained two-turn resource proof from
// the canonical Claude lifecycle artifact without exposing routing as caller YAML.
func CopyClaudeResourceRead(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "internal/releasee2e/testdata/claude_cli_managed_lifecycle"), root)
	writeClosedVariantFile(t, root, "schema.yaml", `name: claude-resource-read
stages:
  pending: {initial: true}
  done: {terminal: true}
pins:
  inputs: {events: [task.assigned, task.finished, records.loaded]}
  outputs: {events: [task.assigned, task.finished]}
connect:
  - {event: task.assigned, from: ., to: worker}
  - {event: task.finished, from: ., to: worker}
  - {event: records.loaded, from: worker, to: .}
`)
	writeClosedVariantFile(t, root, "entities.yaml", "reference_state: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "reference:\n  execution_type: system_node\n  subscribes_to: [records.loaded, task.finished]\n  event_handlers:\n    records.loaded:\n      create_entity: true\n    task.finished:\n      advances_to: done\n")
	writeClosedVariantFile(t, root, "events.yaml", "task.assigned:\n  request: \"[text]\"\ntask.finished:\n")
	writeClosedVariantFile(t, root, "worker/schema.yaml", "name: worker\nstages:\n  pending: {initial: true}\n  active: {}\n  done: {terminal: true}\npins:\n  inputs: {events: [task.assigned, task.finished]}\n  outputs: {events: [records.loaded]}\n")
	writeClosedVariantFile(t, root, "worker/agents.yaml", "release-worker:\n  role: release-worker\n  intent: prompts/release-worker.md\n  model: regular\n  memory: true\n  native_tools: {web_search: true}\n  data_access: [{data: worker/records.loaded}]\n  subscriptions: [agent.requested]\n  emit_events: [agent.completed]\n")
	writeClosedVariantFile(t, root, "worker/nodes.yaml", `intake:
  execution_type: system_node
  subscribes_to: [task.assigned]
  produces: [agent.requested]
  event_handlers:
    task.assigned:
      create_entity: true
      advances_to: active
      data_accumulation:
        writes:
          - source_field: request
            target_field: requests
      emit:
        event: agent.requested

worker-completion:
  execution_type: system_node
  subscribes_to: [agent.completed]
  event_handlers:
    agent.completed:
      advances_to: active

finish:
  execution_type: system_node
  subscribes_to: [task.finished]
  event_handlers:
    task.finished:
      advances_to: done
`)
	writeClosedVariantFile(t, root, "worker/events.yaml", "agent.requested:\n  request: \"[text]?\"\nagent.completed:\n  flow_result: text?\n\nrecords.loaded:\n  key: id\n  id: text\n  reference_text: text\n")
	return root
}
