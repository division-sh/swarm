package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyReceiverOptionalChild(t testing.TB, existing bool) string {
	t.Helper()
	root := t.TempDir()
	seedPin, seedSchema, seedHandler, create := "", "", "", "      create_entity: true\n"
	activeStage := ""
	if existing {
		activeStage = "  active: {}\n"
		seedPin = "      - {event: work.seeded, source: external}\n"
		seedSchema = "work.seeded:\n  seed: boolean\n"
		seedHandler = "    work.seeded:\n      create_entity: true\n      advances_to: active\n"
		create = ""
	}
	files := map[string]string{
		"schema.yaml": `name: receiver-composition
stages:
  waiting: {initial: true}
` + activeStage + `  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
` + seedPin + `  outputs:
    events: [work.completed]
connect:
  - {event: work.completed, from: ., to: sink}
`,
		"events.yaml":   "work.requested:\n  seed: boolean\nwork.completed:\n  result: text\n" + seedSchema,
		"entities.yaml": "work: {}\n",
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [work.requested` + func() string {
			if existing {
				return ", work.seeded"
			}
			return ""
		}() + `]
  event_handlers:
` + seedHandler + "    work.requested:\n" + create + `      advances_to: done
      emit:
        event: work.completed
        fields: {result: {literal: emitted}}
`,
		"sink/schema.yaml": `name: sink
pins:
  inputs:
    events: [work.completed]
`,
		"sink/entities.yaml": "receipt: {}\n",
		"sink/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed: {}
`,
	}
	for name, content := range files {
		writeClosedVariantFile(t, root, name, content)
	}

	return root
}

func CopyReceiverCreatingChild(t testing.TB, existing bool) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, existing)
	files := map[string]string{
		"sink/schema.yaml": `name: sink
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [work.completed]
`,
		"sink/entities.yaml": "receipt:\n  result: text\n",
		"sink/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      create_entity: true
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      advances_to: done
`,
	}
	for name, content := range files {
		writeClosedVariantFile(t, root, name, content)
	}
	return root
}

func CopyReceiverMixedPolicies(t testing.TB, optionalFirst bool) string {
	t.Helper()
	root := CopyReceiverCreatingChild(t, false)
	optionalScope := "z-observer"
	if optionalFirst {
		optionalScope = "a-observer"
	}
	creating := "  - {event: work.completed, from: ., to: sink}\n"
	optional := "  - {event: work.completed, from: ., to: " + optionalScope + "}\n"
	edges := creating + optional
	if optionalFirst {
		edges = optional + creating
	}
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), creating, edges)
	writeClosedVariantFile(t, root, optionalScope+"/schema.yaml", "name: "+optionalScope+"\npins:\n  inputs:\n    events: [work.completed]\n")
	writeClosedVariantFile(t, root, optionalScope+"/entities.yaml", "receipt: {}\n")
	writeClosedVariantFile(t, root, optionalScope+"/nodes.yaml", "collector:\n  execution_type: system_node\n  subscribes_to: [work.completed]\n  event_handlers:\n    work.completed: {}\n")
	return root
}

func CopyReceiverUnavailableAfterPublish(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": `name: receiver-failure
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.seeded, source: external}
      - {event: work.requested, source: external}
`,
		"events.yaml":   "work.seeded:\n  seed: boolean\nwork.requested:\n  seed: boolean\n",
		"entities.yaml": "work:\n  marker: text\n",
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [work.seeded, work.requested]
  event_handlers:
    work.seeded:
      create_entity: true
      advances_to: active
    work.requested:
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'must-not-write'"}
      advances_to: done
`,
	}
	for name, content := range files {
		writeClosedVariantFile(t, root, name, content)
	}
	return root
}

func CopyReceiverEntitylessOnward(t testing.TB, existing bool) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, existing)
	files := map[string]string{
		"sink/events.yaml": "child.finished:\n  result: text\n",
		"sink/schema.yaml": `name: sink
pins:
  inputs:
    events: [work.completed]
  outputs:
    events: [child.finished]
connect:
  - {event: child.finished, from: ., to: tail}
`,
		"sink/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      emit:
        event: child.finished
        fields: {result: {expression: payload.result}}
`,
		"sink/tail/schema.yaml": `name: tail
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [child.finished]
`,
		"sink/tail/entities.yaml": "receipt:\n  result: text\n",
		"sink/tail/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [child.finished]
  event_handlers:
    child.finished:
      create_entity: true
      data_accumulation:
        writes:
          - {target_field: result, expression: payload.result}
      advances_to: done
`,
	}
	for name, content := range files {
		writeClosedVariantFile(t, root, name, content)
	}
	return root
}

func CopyReceiverAutoMaterializingChild(t testing.TB, existing bool) string {
	t.Helper()
	root := CopyReceiverCreatingChild(t, existing)
	applyClosedReplacement(t, filepath.Join(root, "sink/nodes.yaml"), "      create_entity: true\n", "")
	return root
}

func CopyReceiverEntitylessLocal(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, false)
	writeClosedVariantFile(t, root, "sink/events.yaml", "child.finished:\n  result: text\n")
	writeClosedVariantFile(t, root, "sink/schema.yaml", `name: sink
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [work.completed]
`)
	writeClosedVariantFile(t, root, "sink/entities.yaml", "receipt:\n  result: text\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      emit:
        event: child.finished
        fields: {result: {expression: payload.result}}
local:
  execution_type: system_node
  subscribes_to: [child.finished]
  event_handlers:
    child.finished:
      create_entity: true
      data_accumulation:
        writes:
          - {target_field: result, expression: payload.result}
      advances_to: done
`)
	return root
}

func CopyReceiverEntitylessExternal(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, false)
	writeClosedVariantFile(t, root, "sink/events.yaml", "child.finished:\n  result: text\n  swarm:\n    consumer: external\n")
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events: [work.completed]\n  outputs:\n    events: [child.finished]\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      emit:
        event: child.finished
        fields: {result: {expression: payload.result}}
`)
	return root
}

func CopyReceiverEntitylessUnrouted(t testing.TB) string {
	t.Helper()
	root := CopyReceiverEntitylessExternal(t)
	writeClosedVariantFile(t, root, "sink/events.yaml", "child.finished:\n  result: text\n")
	return root
}

func CopyReceiverMixedAgent(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, true)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "events: [work.completed]", "events: [work.completed, child.seeded]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "connect:\n", "connect:\n  - {event: child.seeded, from: ., to: sink}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "work.seeded:\n", "child.seeded:\n  seed: boolean\nwork.seeded:\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "      advances_to: active\n", "      advances_to: active\n      emit:\n        event: child.seeded\n        fields: {seed: {literal: true}}\n")
	writeClosedVariantFile(t, root, "sink/schema.yaml", `name: sink
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - child.seeded
      - work.completed
      - {event: child.closed, source: external}
`)
	writeClosedVariantFile(t, root, "sink/events.yaml", "child.closed:\n  seed: boolean\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [child.seeded, work.completed, child.closed]
  event_handlers:
    child.seeded:
      create_entity: true
      advances_to: active
    work.completed: {}
    child.closed:
      advances_to: done
`)
	writeClosedVariantFile(t, root, "sink/agents.yaml", `observer:
  id: observer
  role: observer
  model: regular
  intent: {inline: 'Observe the received result.'}
  subscriptions: [work.completed]
  memory: false
  mock:
    kind: python
    module: mocks/observer.py
`)
	writeClosedVariantFile(t, root, "sink/mocks/observer.py", "def handle(input):\n    return {\"text\": \"Observed result.\", \"usage\": {\"input_tokens\": 2, \"output_tokens\": 2}}\n")
	return root
}

func CopyReceiverEntitylessFork(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, false)
	writeClosedVariantFile(t, root, "schema.yaml", `name: receiver-composition
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
  outputs:
    events: [work.completed]
connect:
  - {event: work.completed, from: ., to: sink}
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit:
        event: work.completed
        fields: {result: {literal: emitted}}
`)
	return root
}
