package canonicalrouting

import "testing"

func CopyStageCompletionJourney(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"entities.yaml": "run:\n  topic: string\n",
		"schema.yaml": `initial_state: active
states: [active, done]
terminal_states: [done]
pins:
  inputs:
    events: [flow.started, flow.finish]
  outputs:
    events: [flow.started, flow.finish]
connect:
  - {event: flow.started, from: ., to: discovery}
  - {event: flow.finish, from: ., to: discovery}
`,
		"events.yaml":             "flow.started:\n  topic:\n    type: string?\nflow.finish:\n  topic:\n    type: string?\n",
		"discovery/schema.yaml":   "name: discovery\nstages:\n  ready:\n    initial: true\n  Ready:\n    terminal: true\npins:\n  inputs:\n    events: [flow.started, flow.finish]\n",
		"discovery/entities.yaml": "discovery: {}\n",
		"discovery/nodes.yaml":    "pipeline:\n  execution_type: system_node\n  subscribes_to: [flow.started, flow.finish]\n  event_handlers:\n    flow.started:\n      create_entity: true\n    flow.finish:\n      advances_to: Ready\n",
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}
