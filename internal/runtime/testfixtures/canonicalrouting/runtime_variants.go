package canonicalrouting

import "testing"

func CopyGeneratedActivity(t testing.TB, nested, subscribeResults bool) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "entities.yaml")
	flowRoot := ""
	if nested {
		removeClosedVariantFiles(t, root, "events.yaml", "nodes.yaml")

		writeClosedVariantFile(t, root, "schema.yaml", "name: nested-generated-activity-topology\nstages: []\npins:\n  inputs:\n    - request\n  outputs:\n    - request\nconnect:\n  - {event: request, from: ., to: child}\n")
		flowRoot = "child/"
		writeClosedVariantFile(t, root, flowRoot+"schema.yaml", "name: child\nstages: []\npins:\n  inputs:\n    - request\n")
	} else {

		writeClosedVariantFile(t, root, "schema.yaml", "name: generated-activity-topology\nstages: []\npins:\n  inputs:\n    - request\n")
	}
	writeClosedVariantFile(t, root, "events.yaml", "request:\n  message: text\n")
	writeClosedVariantFile(t, root, flowRoot+"tools.yaml", `send:
  description: send one message
  handler_type: http
  effect_class: read_only
  input_schema:
    type: object
    properties:
      message: {type: string}
    required: [message]
  output_schema:
    type: object
    properties:
      delivered: {type: boolean}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: https://example.invalid/send
    body:
      message: "{{input.message}}"
`)
	resultSubscriptions := ""
	resultHandlers := ""
	if subscribeResults {
		prefix := ""
		resultSubscriptions = ", " + prefix + "send.succeeded, " + prefix + "send.failed"
		resultHandlers = "    " + prefix + "send.succeeded:\n      rules:\n        - id: observe_success\n          when: payload.result != null\n        - id: unmatched_success\n          else: true\n    " + prefix + "send.failed:\n      rules:\n        - id: observe_failure\n          when: payload.failure != null\n        - id: unmatched_failure\n          else: true\n"
	}
	nodes := "activity-node:\n  execution_type: system_node\n  subscribes_to: [request" + resultSubscriptions + "]\n  event_handlers:\n    request:\n      activity:\n        id: send\n        tool: send\n        input:\n          message: payload.message\n" + resultHandlers
	if nested {
		nodes += "observer-node:\n  execution_type: system_node\n  subscribes_to: [send.succeeded, send.failed]\n  event_handlers:\n    send.succeeded:\n      rules:\n        - id: observe_success\n          when: payload.result.delivered == true\n        - id: unmatched_success\n          else: true\n    send.failed:\n      rules:\n        - id: observe_failure\n          when: payload.failure != null\n        - id: unmatched_failure\n          else: true\n"
	}
	writeClosedVariantFile(t, root, flowRoot+"nodes.yaml", nodes)
	return root
}

func CopyPayloadNamedField(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", "name: payload-normalizer\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs:\n    - inbound.telegram\n")
	writeClosedVariantFile(t, root, "entities.yaml", "chat:\n  chat_id: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "inbound.telegram:\n  entity_id: text\n  payload: json\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "normalizer:\n  execution_type: system_node\n  subscribes_to: [inbound.telegram]\n  event_handlers:\n    inbound.telegram:\n      data_accumulation:\n        writes:\n          - target_field: chat_id\n            value: payload.payload.message.chat.id\n      advances_to: done\n")
	return root
}

func CopyConstructedStaticHandler(t testing.TB, withTimer bool) string {
	t.Helper()
	root := CopyExample(t, TemplateCreateMintedKey)
	removeClosedVariantFiles(t, root,
		"producer/events.yaml", "producer/nodes.yaml", "producer/schema.yaml", "producer",
		"validator/events.yaml", "validator/entities.yaml", "validator/nodes.yaml", "validator/schema.yaml", "validator")

	writeClosedVariantFile(t, root, "schema.yaml", "name: exact-once-test\npins:\n  inputs:\n    - thing.created\n  outputs:\n    - thing.created\nconnect:\n  - {event: thing.created, from: ., to: validation}\n")
	writeClosedVariantFile(t, root, "events.yaml", "thing.created:\n  amount: integer\n  who: text\n")
	writeClosedVariantFile(t, root, "validation/agents.yaml", "result-observer:\n  intent: {inline: 'Observe the exact created-entity result.'}\n  subscriptions: [thing.emitted]\n")
	inputs := "thing.created"
	produces := "thing.emitted"
	timer := ""
	timerEvent := ""
	if withTimer {
		inputs += ", timer.check"
		produces += ", timer.check"
		timerEvent = "timer.check:\n"
		timer = "  timers:\n    - id: check_timer\n      event: timer.check\n      delay: 1h\n      start_on: event:thing.created\n"
	}
	writeLegacyInstanceFlow(t, root, "validation", "name: validation\nstages:\n  new: {initial: true}\n  done: {terminal: true}\npins:\n  inputs: ["+inputs+"]\n  outputs:\n    - thing.emitted\n", "thing.emitted:\n  amount: integer\n  who: text\n"+timerEvent, "widget:\n  amount:\n    type: integer\n    initial: 0\n  who:\n    type: text\n    initial: \"\"\n  counter:\n    type: integer\n    initial: 0\n", "w-node:\n  execution_type: system_node\n  subscribes_to: ["+inputs+"]\n  produces: ["+produces+"]\n"+timer+"  event_handlers:\n    thing.created:\n      data_accumulation:\n        source_event: thing.created\n        writes:\n          - source_field: amount\n            target_field: amount\n          - source_field: who\n            target_field: who\n          - target_field: counter\n            value: entity.counter + 1\n      sets_gate: ready\n      advances_to: done\n      emit:\n        event: thing.emitted\n        fields:\n          amount: entity.amount\n          who: entity.who\n")
	return root
}
