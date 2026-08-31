package canonicalrouting

import "testing"

func CopyGeneratedActivity(t testing.TB, nested, subscribeResults bool) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "entities.yaml")
	flowRoot := ""
	if nested {
		removeClosedVariantFiles(t, root, "events.yaml", "nodes.yaml")

		writeClosedVariantFile(t, root, "schema.yaml", "name: nested-generated-activity-topology\nstages: []\n")
		flowRoot = "child/"
		writeClosedVariantFile(t, root, flowRoot+"schema.yaml", "name: child\nmode: static\nstages: []\n")
	} else {

		writeClosedVariantFile(t, root, "schema.yaml", "name: generated-activity-topology\nstages: []\n")
	}
	writeClosedVariantFile(t, root, flowRoot+"events.yaml", "request:\n  message: text\n  swarm:\n    source: external\n")
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
		resultHandlers = "    " + prefix + "send.succeeded:\n      rules:\n        - id: observe_success\n          condition: payload.result != null\n    " + prefix + "send.failed:\n      rules:\n        - id: observe_failure\n          condition: payload.failure != null\n"
	}
	nodes := "activity-node:\n  id: activity-node\n  execution_type: system_node\n  subscribes_to: [request" + resultSubscriptions + "]\n  event_handlers:\n    request:\n      activity:\n        id: send\n        tool: send\n        input:\n          message:\n            ref: payload.message\n" + resultHandlers
	if nested {
		nodes += "observer-node:\n  id: observer-node\n  execution_type: system_node\n  subscribes_to: [send.succeeded, send.failed]\n  event_handlers:\n    send.succeeded:\n      rules:\n        - id: observe_success\n          condition: payload.result.delivered == true\n    send.failed:\n      rules:\n        - id: observe_failure\n          condition: payload.failure != null\n"
	}
	writeClosedVariantFile(t, root, flowRoot+"nodes.yaml", nodes)
	return root
}

func CopyPayloadNamedField(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", "name: payload-normalizer\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n")
	writeClosedVariantFile(t, root, "entities.yaml", "chat:\n  chat_id: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "inbound.telegram:\n  entity_id: text\n  payload: json\n  swarm:\n    source: external\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "normalizer:\n  id: normalizer\n  execution_type: system_node\n  subscribes_to: [inbound.telegram]\n  event_handlers:\n    inbound.telegram:\n      data_accumulation:\n        writes:\n          - target_field: chat_id\n            value:\n              ref: payload.payload.message.chat.id\n      advances_to: done\n")
	return root
}

func CopyLegacyStaticCreate(t testing.TB, withTimer bool) string {
	t.Helper()
	root := CopyExample(t, TemplateCreateMintedKey)

	writeClosedVariantFile(t, root, "schema.yaml", "name: exact-once-test\n")
	inputs := "thing.created"
	produces := "thing.emitted"
	timer := ""
	timerEvent := ""
	if withTimer {
		inputs += ", timer.check"
		produces += ", timer.check"
		timerEvent = "timer.check: {}\n"
		timer = "  timers:\n    - id: check_timer\n      event: timer.check\n      delay: 1h\n      start_on: event:thing.created\n"
	}
	writeLegacyInstanceFlow(t, root, "validation", "name: validation\nmode: static\ninitial_state: new\nterminal_states: [done]\nstates: [new, done]\npins:\n  inputs:\n    events: ["+inputs+"]\n  outputs:\n    events:\n      - event: thing.emitted\n        sink: harness\n", "thing.created:\n  swarm:\n    source: external\n  amount: integer\n  who: text\nthing.emitted:\n  amount: integer\n  who: text\n"+timerEvent, "widget:\n  amount:\n    type: integer\n    initial: 0\n  who:\n    type: text\n    initial: \"\"\n  counter:\n    type: integer\n    initial: 0\n", "w-node:\n  id: w-node\n  execution_type: system_node\n  subscribes_to: ["+inputs+"]\n  produces: ["+produces+"]\n"+timer+"  event_handlers:\n    thing.created:\n      create_entity: true\n      data_accumulation:\n        source_event: thing.created\n        writes:\n          - source_field: amount\n            target_field: amount\n          - source_field: who\n            target_field: who\n          - target_field: counter\n            value:\n              cel: entity.counter + 1\n      sets_gate: ready\n      advances_to: done\n      emit:\n        event: thing.emitted\n        fields:\n          amount:\n            cel: entity.amount\n          who:\n            cel: entity.who\n      action:\n        id: mailbox_write\n        mailbox:\n          item_type: {literal: approval}\n          severity: {literal: normal}\n          summary: {literal: created}\n          payload:\n            amount: {ref: payload.amount}\n")
	return root
}
