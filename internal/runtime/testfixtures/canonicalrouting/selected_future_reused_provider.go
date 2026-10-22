package canonicalrouting

import "testing"

// CopySelectedFutureReusedProviderInput sends a second assignment only after
// the first turn returns. Both publications select the same constructed worker.
func CopySelectedFutureReusedProviderInput(t testing.TB) string {
	t.Helper()
	root := CopySelectedFutureKeyedProviderInput(t)
	for path, body := range map[string]string{
		"schema.yaml": `name: selected-future-reused-provider
stages:
  pending: {}
  done: {final: true}
pins:
  inputs: [start.requested, work.progress, work.finished]
  outputs: [work.assigned]
connect:
  - {event: work.assigned, from: ., to: work, resolution: select-or-create}
  - {event: work.progress, from: work, to: .}
  - {event: work.finished, from: work, to: .}
`,
		"events.yaml": `start.requested:
  work_id: text
work.assigned:
  work_id: text
  sequence: integer
`,
		"nodes.yaml": `controller:
  execution_type: system_node
  subscribes_to: [start.requested, work.progress, work.finished]
  produces: [work.assigned]
  event_handlers:
    start.requested:
      emit:
        event: work.assigned
        fields: {work_id: payload.work_id, sequence: 1}
    work.progress:
      emit:
        event: work.assigned
        fields: {work_id: payload.work_id, sequence: 2}
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
  outputs: [work.progress, work.finished]
required_agents:
  - {role: worker, subscribes_to: [work.assigned]}
`,
		"work/events.yaml": `work.progress:
  work_id: text
work.finished:
  work_id: text
  result: text
`,
		"work/agents.yaml": `worker:
  role: worker
  model: regular
  intent: prompts/worker.md
  subscriptions: [work.assigned]
  emit_events: [work.progress, work.finished]
  mock:
    kind: python
    module: mocks/future-worker.py
`,
		"work/prompts/worker.md": "For sequence 1 report progress once using emit_work_progress; for sequence 2 finish once using emit_work_finished with result finished. Preserve the assigned work_id.\n",
		"work/mocks/future-worker.py": `import json

def handle(input):
    usage = {"input_tokens": 1, "output_tokens": 1}
    if input["round"] == 1:
        frame = {}
        for message in input["messages"]:
            if message["role"] == "user":
                frame = json.loads(message["content"])
        payload = frame["event"]["payload"]
        arguments = {"work_id": payload["work_id"]}
        if payload["sequence"] == 1:
            name = "emit_work_progress"
        else:
            name = "emit_work_finished"
            arguments["result"] = "finished"
        return {"calls": [{"name": name, "arguments": arguments}], "usage": usage}
    return {"text": "Reported the assigned step.", "usage": usage}
`,
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
