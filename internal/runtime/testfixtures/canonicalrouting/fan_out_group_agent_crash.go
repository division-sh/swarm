package canonicalrouting

import "testing"

// CopyFanOutGroupAgentCrash retains the declared fan-out producer and requires
// a real mock-provider agent emission before the business receiver can write.
func CopyFanOutGroupAgentCrash(t testing.TB) string {
	t.Helper()
	root := CopyForkFanOutCarrier(t, false, false)
	writeClosedVariantFile(t, root, "agents.yaml", `item-worker:
  type: generic
  role: item_worker
  intent: {inline: Emit the processed value and exact request event identity.}
  model: regular
  memory: false
  subscriptions: [items.child]
  emit_events: [items.processed]
  mock:
    kind: python
    module: mocks/item-worker.py
`)
	writeClosedVariantFile(t, root, "events.yaml", "items.ready:\n  items: '[text]'\nitems.child:\n  value: text\nitems.processed:\n  value: text\n  request_event_id: text\nbatch.completed:\n  total: integer\n")
	writeClosedVariantFile(t, root, "entities.yaml", "root:\n  processed_value: text\n  account_id: {type: text, initial: preserved}\n  handled: {type: boolean, initial: false}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `fan-out-source:
  execution_type: system_node
  subscribes_to: [items.ready]
  event_handlers:
    items.ready:
      fan_out:
        items_from: payload.items
        as: entry
        identity: entry
        emit:
          event: items.child
          fields:
            value: {cel: entry}
result-consumer:
  execution_type: system_node
  subscribes_to: [items.processed]
  event_handlers:
    items.processed:
      data_accumulation:
        writes:
          - {target_field: processed_value, expression: payload.value}
`)
	writeClosedVariantFile(t, root, "mocks/item-worker.py", `import json

def handle(input):
    frame = {}
    for message in input["messages"]:
        if message["role"] == "user":
            frame = json.loads(message["content"])
    if input["round"] == 1:
        event = frame["event"]
        return {"calls": [{"name": "emit_items_processed", "arguments": {"value": event["payload"]["value"], "request_event_id": event["id"]}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "Processed exact item.", "usage": {"input_tokens": 1, "output_tokens": 1}}
`)
	return root
}
