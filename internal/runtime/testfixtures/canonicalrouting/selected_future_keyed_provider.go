package canonicalrouting

import "testing"

// CopySelectedFutureKeyedProviderInput creates keyed work only after the root
// receives its run-creating input. The authored provider finishes that instance.
func CopySelectedFutureKeyedProviderInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"schema.yaml": `name: selected-future-keyed-provider
stages:
  pending: {}
  done: {final: true}
pins:
  inputs: [start.requested, work.finished]
  outputs: [work.assigned]
connect:
  - {event: work.assigned, from: ., to: work, resolution: select-or-create}
  - {event: work.finished, from: work, to: .}
`,
		"entities.yaml": `run:
  counter: {type: integer, initial: 0}
`,
		"events.yaml": `start.requested:
  work_id: text
work.assigned:
  work_id: text
`,
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [start.requested, work.finished]
  produces: [work.assigned]
  event_handlers:
    start.requested:
      emit:
        event: work.assigned
        fields: {work_id: payload.work_id}
    work.finished:
      advances_to: done
      data_accumulation:
        source_event: work.finished
        writes:
          - {target_field: counter, value: entity.counter + 1}
`,
		"work/schema.yaml": `name: work
instance: work_id
stages:
  pending: {}
  done: {final: true}
pins:
  inputs: [work.assigned]
  outputs: [work.finished]
required_agents:
  - {role: worker, subscribes_to: [work.assigned]}
`,
		"work/entities.yaml": `job:
  work_id: text
  result: text
`,
		"work/events.yaml": `work.finished:
  work_id: text
  result: text
`,
		"work/nodes.yaml": `finish:
  execution_type: system_node
  subscribes_to: [work.finished]
  event_handlers:
    work.finished:
      advances_to: done
      data_accumulation:
        source_event: work.finished
        writes:
          - {target_field: work_id, value: payload.work_id}
          - {target_field: result, value: payload.result}
`,
		"work/agents.yaml": `worker:
  role: worker
  model: regular
  intent: prompts/worker.md
  subscriptions: [work.assigned]
  emit_events: [work.finished]
  mock:
    kind: python
    module: mocks/future-worker.py
`,
		"work/prompts/worker.md": "Finish the assigned work by calling emit_work_finished once with the exact work_id from the work.assigned payload and result finished, then report completion.\n",
		"work/mocks/future-worker.py": `import json

def handle(input):
    usage = {"input_tokens": 1, "output_tokens": 1}
    if input["round"] == 1:
        frame = {}
        for message in input["messages"]:
            if message["role"] == "user":
                frame = json.loads(message["content"])
        work_id = frame["event"]["payload"]["work_id"]
        return {
            "calls": [{
                "name": "emit_work_finished",
                "arguments": {"work_id": work_id, "result": "finished"},
            }],
            "usage": usage,
        }
    return {"text": "Finished the assigned work.", "usage": usage}
`,
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
