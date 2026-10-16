package canonicalrouting

import "testing"

// CopySelectedFutureProviderInput uses the existing keyed constructor and its
// static descendants. Real mock-provider tools, not an AgentFactory double,
// finish every descendant after the creating event is replayed at run start.
func CopySelectedFutureProviderInput(t testing.TB) string {
	t.Helper()
	root := CopySelectedNestedConstructedAgentInput(t, false)
	writeClosedVariantFile(t, root, "state.yaml", "run: {}\n")
	writeClosedVariantFile(t, root, "schema.yaml", `name: selected-future-provider
stages:
  pending: {}
  done: {final: true}
pins:
  inputs: [task.assigned, construction.started]
  outputs: [task.assigned]
connect:
  - {event: task.assigned, from: ., to: templ, resolution: select-or-create}
`)
	writeClosedVariantFile(t, root, "templ/schema.yaml", "name: templ\ninstance: work_id\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs: [task.assigned]\n  outputs: [construction.started]\nconnect:\n  - {event: construction.started, from: ., to: child}\n  - {event: construction.started, from: ., to: audit}\n")
	for _, path := range []string{"templ/child", "templ/audit"} {
		writeClosedVariantFile(t, root, path+"/schema.yaml", "name: descendant\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs: [construction.started]\n  outputs: [work.ready]\nconnect:\n  - {event: work.ready, from: ., to: leaf}\nrequired_agents:\n  - {role: worker, subscribes_to: [work.ready]}\n")
		writeClosedVariantFile(t, root, path+"/events.yaml", "work.ready:\n  work_id: text\nwork.finish:\n")
		writeClosedVariantFile(t, root, path+"/nodes.yaml", "publish:\n  execution_type: system_node\n  subscribes_to: [construction.started]\n  event_handlers:\n    construction.started:\n      emit:\n        event: work.ready\n        fields: {work_id: payload.work_id}\nfinish:\n  execution_type: system_node\n  subscribes_to: [work.finish]\n  event_handlers:\n    work.finish:\n      advances_to: done\n")
		writeClosedVariantFile(t, root, path+"/leaf/schema.yaml", "name: leaf\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs: [work.ready]\nrequired_agents:\n  - {role: worker, subscribes_to: [work.ready]}\n")
		writeClosedVariantFile(t, root, path+"/leaf/events.yaml", "work.finish:\n")
		writeClosedVariantFile(t, root, path+"/leaf/nodes.yaml", "finish:\n  execution_type: system_node\n  subscribes_to: [work.finish]\n  event_handlers:\n    work.finish:\n      advances_to: done\n")
	}
	writeClosedVariantFile(t, root, "nodes.yaml", `anchor:
  execution_type: system_node
  subscribes_to: [construction.started]
  event_handlers:
    construction.started: {}
finish-root:
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      advances_to: done
`)
	for _, path := range []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"} {
		writeClosedVariantFile(t, root, path+"/agents.yaml", `worker:
  role: worker
  model: regular
  intent: {inline: Finish the exact assigned work using the declared output tool.}
  subscriptions: [work.ready]
  emit_events: [work.finish]
  mock:
    kind: python
    module: mocks/future-worker.py
`)
		writeClosedVariantFile(t, root, path+"/mocks/future-worker.py", `def handle(input):
    if input["round"] == 1:
        return {"calls": [{"name": "emit_work_finish", "arguments": {}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "Finished the exact constructed work.", "usage": {"input_tokens": 1, "output_tokens": 1}}
`)
	}
	return root
}
