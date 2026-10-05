package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyReceiverOptionalChild(t testing.TB, existing bool) string {
	t.Helper()
	root := t.TempDir()
	seedPin, seedSchema, seedHandler := "", "", ""
	activeStage := ""
	if existing {
		activeStage = "  active: {}\n"
		seedPin = "    - work.seeded\n"
		seedSchema = "work.seeded:\n  seed: boolean\n"
		seedHandler = "    work.seeded:\n      advances_to: active\n"
	}
	files := map[string]string{
		"schema.yaml": `name: receiver-composition
stages:
  waiting: {initial: true}
` + activeStage + `  done: {terminal: true}
pins:
  inputs:
    - work.requested
` + seedPin + `  outputs: [work.completed]
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
` + seedHandler + "    work.requested:\n" + `      advances_to: done
      emit:
        event: work.completed
        fields: {result: "emitted"}
`,
		"sink/schema.yaml": `name: sink
pins:
  inputs:
    - work.completed
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
    - work.completed
`,
		"sink/entities.yaml": "receipt:\n  result: text\n",
		"sink/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      data_accumulation:
        writes:
          - target_field: result
            value: payload.result
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
	writeClosedVariantFile(t, root, optionalScope+"/schema.yaml", "name: "+optionalScope+"\npins:\n  inputs: [work.completed]\n")
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
    - work.seeded
    - work.requested
`,
		"events.yaml":   "work.seeded:\n  seed: boolean\nwork.requested:\n  seed: boolean\n",
		"entities.yaml": "work:\n  marker: text\n",
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to:
    - work.seeded
    - work.requested
  event_handlers:
    work.seeded:
      advances_to: active
    work.requested:
      data_accumulation:
        writes:
          - target_field: marker
            value: |-
              'must-not-write'
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
    - work.completed
  outputs:
    - child.finished
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
        fields: {result: payload.result}
`,
		"sink/tail/schema.yaml": `name: tail
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    - child.finished
`,
		"sink/tail/entities.yaml": "receipt:\n  result: text\n",
		"sink/tail/nodes.yaml": `collector:
  execution_type: system_node
  subscribes_to: [child.finished]
  event_handlers:
    child.finished:
      data_accumulation:
        writes:
          - {target_field: result, value: payload.result}
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
    - work.completed
`)
	writeClosedVariantFile(t, root, "sink/entities.yaml", "receipt:\n  result: text\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      emit:
        event: child.finished
        fields: {result: payload.result}
local:
  execution_type: system_node
  subscribes_to: [child.finished]
  event_handlers:
    child.finished:
      data_accumulation:
        writes:
          - {target_field: result, value: payload.result}
      advances_to: done
`)
	return root
}

// CopyReceiverEntitylessRootExport proves the public observation boundary
// without manufacturing either a private receiver or a delivery.
func CopyReceiverEntitylessRootExport(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-export\npins:\n  inputs:\n    - work.requested\n  outputs:\n    - child.finished\n")
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\nchild.finished:\n  result: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  event_handlers:
    work.requested:
      emit:
        event: child.finished
        fields: {result: "emitted"}
`)
	return root
}

func CopyReceiverEntitylessUnrouted(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, false)
	writeClosedVariantFile(t, root, "sink/events.yaml", "child.finished:\n  result: text\n")
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    - work.completed\n  outputs:\n    - child.finished\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  event_handlers:
    work.completed:
      emit:
        event: child.finished
        fields: {result: payload.result}
`)
	return root
}

func CopyReceiverMixedAgent(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, true)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "outputs: [work.completed]", "outputs: [work.completed, child.seeded]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "connect:\n", "connect:\n  - {event: child.seeded, from: ., to: sink}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "work.seeded:\n", "child.seeded:\n  seed: boolean\nwork.seeded:\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "      advances_to: active\n", "      advances_to: active\n      emit:\n        event: child.seeded\n        fields: {seed: true}\n")
	writeClosedVariantFile(t, root, "sink/schema.yaml", `name: sink
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    - child.seeded
    - work.completed
    - child.closed
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - work.requested\n", "    - work.requested\n    - child.closed\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "outputs: [work.completed, child.seeded]", "outputs: [work.completed, child.seeded, child.closed]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "connect:\n", "connect:\n  - {event: child.closed, from: ., to: sink}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "work.seeded:\n", "child.closed:\n  seed: boolean\nwork.seeded:\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [child.seeded, work.completed, child.closed]
  event_handlers:
    child.seeded:
      advances_to: active
    work.completed: {}
    child.closed:
      advances_to: done
`)
	writeClosedVariantFile(t, root, "sink/agents.yaml", `observer:
  role: observer
  model: regular
  intent: {inline: 'Observe the received result.'}
  subscriptions: [work.completed]
  mock:
    kind: python
    module: mocks/observer.py
`)
	writeClosedVariantFile(t, root, "sink/mocks/observer.py", "def handle(input):\n    return {\"text\": \"Observed result.\", \"usage\": {\"input_tokens\": 2, \"output_tokens\": 2}}\n")
	return root
}

func CopyReceiverFieldlessFork(t testing.TB) string {
	t.Helper()
	root := CopyReceiverOptionalChild(t, false)
	writeClosedVariantFile(t, root, "schema.yaml", `name: receiver-composition
pins:
  inputs:
    - work.requested
    - fork.seeded
  outputs:
    - work.completed
    - fork.seeded
connect:
  - {event: work.completed, from: ., to: sink}
`)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "work.requested:\n", "fork.seeded:\nwork.requested:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit:
        event: work.completed
        fields: {result: "emitted"}
`)
	removeClosedVariantFiles(t, root, "entities.yaml", "sink/entities.yaml")
	return root
}
