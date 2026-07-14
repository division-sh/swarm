package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// RootStaticHandler is the closed set of root/default-static handler shapes
// used to prove primary-entity admission semantics.
type RootStaticHandler uint8

const (
	RootStaticMaterialize RootStaticHandler = iota + 1
	RootStaticObserve
)

// RootStaticEntityID controls whether the root input event exposes a
// caller-selected entity identity.
type RootStaticEntityID uint8

const (
	RootStaticNoEntityID RootStaticEntityID = iota
	RootStaticOptionalEntityID
	RootStaticRequiredEntityID
)

type StaticRetirementHandler uint8

const (
	StaticRetirementCreate StaticRetirementHandler = iota + 1
	StaticRetirementSelect
	StaticRetirementSelectOrCreate
	StaticRetirementMaterialize
	StaticRetirementObserve
)

func CopyStaticMultiEntityRetirement(t testing.TB, handler StaticRetirementHandler) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", `name: static-multi-entity-retirement
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: treasury, flow: treasury, mode: static}
`)
	for _, name := range []string{"schema.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, name, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/treasury/schema.yaml", `name: treasury
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [opco.spend_requested]
  outputs: {events: []}
`)
	writeClosedVariantFile(t, root, "flows/treasury/events.yaml", `opco.spend_requested:
  swarm: {source: external}
  vertical_id: string
  amount_usd: number
opco.spend_recorded:
  vertical_id: string
`)
	writeClosedVariantFile(t, root, "flows/treasury/entities.yaml", `budget:
  vertical_id:
    type: string
    indexed: true
    _unused_reason: static multi-entity retirement selection key proof field
  spent_usd:
    type: number
    initial: 0
`)
	for _, name := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/treasury/"+name, "{}\n")
	}
	var body string
	switch handler {
	case StaticRetirementCreate:
		body = "      create_entity: true\n" + staticRetirementWriteBody()
	case StaticRetirementSelect:
		body = "      select_entity:\n        by:\n          vertical_id: payload.vertical_id\n" + staticRetirementWriteBody()
	case StaticRetirementSelectOrCreate:
		body = "      select_or_create_entity:\n        by:\n          vertical_id: payload.vertical_id\n" + staticRetirementWriteBody()
	case StaticRetirementMaterialize:
		body = staticRetirementWriteBody()
	case StaticRetirementObserve:
		body = "      emit:\n        event: opco.spend_recorded\n        fields:\n          vertical_id: payload.vertical_id\n"
	default:
		t.Fatalf("unsupported static retirement handler %d", handler)
	}
	writeClosedVariantFile(t, root, "flows/treasury/nodes.yaml", `treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
`+body)
	return root
}

func staticRetirementWriteBody() string {
	return "      data_accumulation:\n        writes:\n          - source_field: amount_usd\n            target_field: spent_usd\n"
}

func CopyRootDefaultStaticInput(t testing.TB, handler RootStaticHandler, entityID RootStaticEntityID) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", `name: root-default-static-fixture
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: root-default-static-fixture
initial_state: active
states: [active]
pins:
  inputs:
    events: [subject.created]
  outputs:
    events: [subject.observed]
`)
	entityIDField := ""
	entityIDRequired := ""
	switch entityID {
	case RootStaticNoEntityID:
	case RootStaticOptionalEntityID:
		entityIDField = "  entity_id: string\n"
	case RootStaticRequiredEntityID:
		entityIDField = "  entity_id: string\n"
		entityIDRequired = "  required:\n    - entity_id\n"
	default:
		t.Fatalf("unsupported root static entity ID variant %d", entityID)
	}
	writeClosedVariantFile(t, root, "events.yaml", `subject.created:
  swarm:
    source: external
`+entityIDField+`  display_name: string
`+entityIDRequired+`subject.observed:
`+entityIDField+`  display_name: string
`+entityIDRequired)
	writeClosedVariantFile(t, root, "entities.yaml", "subject:\n  display_name: text\n")
	var nodes string
	switch handler {
	case RootStaticMaterialize:
		nodes = `root-writer:
  id: root-writer
  execution_type: system_node
  subscribes_to: [subject.created]
  event_handlers:
    subject.created:
      data_accumulation:
        writes:
          - source_field: display_name
            target_field: display_name
`
	case RootStaticObserve:
		nodes = `root-observer:
  id: root-observer
  execution_type: system_node
  subscribes_to: [subject.created]
  produces: [subject.observed]
  event_handlers:
    subject.created:
      emit:
        event: subject.observed
        fields:
          display_name: payload.display_name
`
	default:
		t.Fatalf("unsupported root static handler variant %d", handler)
	}
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, name, "{}\n")
	}
	return root
}

func CopyServedJoinProof(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	files := map[string]string{
		"package.yaml": `name: served-join-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`,
		"schema.yaml": `name: served-join-proof
stages:
  new:
    initial: true
  dispatching: {}
  awaiting: {}
  ready:
    terminal: true
  attention:
    terminal: true
pins:
  inputs:
    events:
      - {name: order_started, event: order.started, source: external}
      - {name: order_dispatched, event: order.dispatched, source: external}
      - {name: item_completed, event: item.completed, source: external}
`,
		"entities.yaml": `order:
  expected:
    type: "[text]"
    initial: []
  dispatch_id: text
  probe: text
`,
		"types.yaml": `types:
  JoinResult:
    ok: boolean
`,
		"events.yaml": `order.started:
  swarm: {source: external}
  expected: "[text]"
  dispatch_id: text
order.dispatched:
  swarm: {source: external}
item.completed:
  swarm: {source: external}
  dispatch_id: text
  member_id: text
  result: JoinResult
fork.probe:
  swarm: {source: external}
  marker: text
`,
		"nodes.yaml": `starter:
  id: starter
  execution_type: system_node
  subscribes_to: [order.started]
  event_handlers:
    order.started:
      create_entity: true
      data_accumulation:
        source_event: order.started
        writes:
          - {source_field: expected, target_field: expected}
          - {source_field: dispatch_id, target_field: dispatch_id}
      advances_to: dispatching
dispatcher:
  id: dispatcher
  execution_type: system_node
  subscribes_to: [order.dispatched]
  event_handlers:
    order.dispatched:
      advances_to: awaiting
join-node:
  id: join-node
  execution_type: system_node
  subscribes_to: [item.completed]
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: entity.expected, by: payload.member_id}
        window: {from: entity.dispatch_id, by: payload.dispatch_id}
        output: payload.result
        on_complete: {advances_to: ready}
        timeout: {after: 1h, advances_to: attention}
fork-probe:
  id: fork-probe
  execution_type: system_node
  subscribes_to: [fork.probe]
  event_handlers:
    fork.probe:
      data_accumulation:
        source_event: fork.probe
        writes:
          - {source_field: marker, target_field: probe}
`,
		"policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyTestSetupValidation(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	files := map[string]string{
		"package.yaml": `name: review
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: operating, flow: operating, mode: static}
  - {id: secondary, flow: secondary, mode: static}
`,
		"schema.yaml": `name: review
initial_state: new
terminal_states: [done]
states: [new, done]
pins:
  inputs:
    events: [scan.requested]
`,
		"entities.yaml": "{}\n",
		"events.yaml":   "scan.requested:\n  swarm: {source: external}\n  topic: text\n",
		"nodes.yaml":    "scan-orchestrator:\n  id: scan-orchestrator\n  execution_type: system_node\n  subscribes_to: [scan.requested]\n",
		"policy.yaml":   "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n",
		"flows/operating/schema.yaml": `name: operating
mode: static
initial_state: initializing
terminal_states: [ready]
states: [initializing, waiting, ready]
pins:
  inputs:
    events: [opco.product_review_requested]
`,
		"flows/operating/entities.yaml": `product:
  product_id: text
  note: text
  review_score: integer
  business_brief: Brief
  feature_list: list<Feature>
  review_scores: map[text]integer
`,
		"flows/operating/types.yaml":  "types:\n  Brief:\n    summary: text\n  Feature:\n    name: text\n",
		"flows/operating/events.yaml": "opco.product_review_requested:\n  swarm: {source: external}\n  note: text\n",
		"flows/operating/nodes.yaml": `reviewer:
  id: reviewer
  execution_type: system_node
  subscribes_to: [opco.product_review_requested]
  gate_state:
    gates: [review_ready]
  event_handlers:
    opco.product_review_requested:
      sets_gate: review_ready
      advances_to: ready
`,
		"flows/operating/policy.yaml": "{}\n", "flows/operating/tools.yaml": "{}\n", "flows/operating/agents.yaml": "{}\n",
		"flows/secondary/schema.yaml":   "name: secondary\nmode: static\ninitial_state: open\nterminal_states: [closed]\nstates: [open, closed]\n",
		"flows/secondary/entities.yaml": "ticket:\n  ticket_id: text\n",
		"flows/secondary/events.yaml":   "{}\n", "flows/secondary/nodes.yaml": "{}\n", "flows/secondary/policy.yaml": "{}\n", "flows/secondary/tools.yaml": "{}\n", "flows/secondary/agents.yaml": "{}\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyTemplateConnectRollback(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	files := map[string]string{
		"package.yaml": `name: test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: producer, flow: producer, mode: static}
  - {id: consumer, flow: consumer, mode: template}
connect:
  - {from: producer.deploy_done, to: consumer.deploy_completed, delivery: one}
  - {from: producer.deploy_done, to: consumer.deploy_audited, delivery: one}
`,
		"schema.yaml": "name: test\n", "policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n", "events.yaml": "{}\n", "nodes.yaml": "{}\n",
		"flows/producer/schema.yaml": `name: producer
mode: static
pins:
  outputs:
    events:
      - {name: deploy_done, event: deploy.done, key: vertical_id, carries: [vertical_id]}
`,
		"flows/producer/policy.yaml": "{}\n", "flows/producer/agents.yaml": "{}\n", "flows/producer/events.yaml": "deploy.done:\n  vertical_id: string\n", "flows/producer/entities.yaml": "{}\n", "flows/producer/nodes.yaml": "{}\n",
		"flows/consumer/schema.yaml": `name: consumer
mode: template
instance:
  by: vertical_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - {name: deploy_completed, event: deploy.done}
      - {name: deploy_audited, event: deploy.done}
`,
		"flows/consumer/policy.yaml": "{}\n", "flows/consumer/agents.yaml": "{}\n", "flows/consumer/events.yaml": "deploy.done:\n  vertical_id: string\n",
		"flows/consumer/entities.yaml": "deployment:\n  vertical_id:\n    type: string\n",
		"flows/consumer/nodes.yaml":    "consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    deploy.done: {}\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyProviderRollback(t testing.TB, withCarrier bool) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	nodes := "{}\n"
	if withCarrier {
		nodes = "consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    inbound.telegram.text_message: {}\n"
	}
	files := map[string]string{
		"package.yaml": `name: provider-rollback-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: consumer, flow: consumer, mode: template}
`,
		"schema.yaml": "name: provider-rollback-proof\n", "policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n", "nodes.yaml": "{}\n",
		"events.yaml": "inbound.telegram:\n  raw: boolean\ninbound.telegram.text_message:\n  chat_id: text\n",
		"flows/consumer/schema.yaml": `name: consumer
mode: template
instance:
  by: chat_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: telegram_text
        event: inbound.telegram.text_message
        source: external
        resolution: {mode: select-or-create, instance_key: chat_id}
        carries:
          chat_id: {from: payload.chat_id, type: text}
`,
		"flows/consumer/policy.yaml": "{}\n", "flows/consumer/agents.yaml": "{}\n", "flows/consumer/events.yaml": "{}\n",
		"flows/consumer/entities.yaml": "chat:\n  chat_id:\n    type: text\n    indexed: true\n",
		"flows/consumer/nodes.yaml":    nodes,
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyStandingTelegramServe(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	toolURL := strings.TrimRight(strings.TrimSpace(telegramBaseURL), "/")
	if toolURL == "" {
		t.Fatal("standing Telegram fixture requires a base URL")
	}
	files := map[string]string{
		"package.yaml": `name: standing-telegram-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: telegram-ingress
    flow: telegram-ingress
    mode: singleton
    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
  - {id: telegram-chat, flow: telegram-chat, mode: template}
`,
		"schema.yaml": "name: standing-telegram-proof\n", "policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n", "events.yaml": "{}\n", "nodes.yaml": "{}\n",
		"flows/telegram-ingress/schema.yaml": `name: telegram-ingress
mode: singleton
stages:
  active:
    initial: true
    gate:
      decision: standing_review
      outcomes:
        keep:
          advances_to: done
  done:
    terminal: true
pins:
  inputs:
    events:
      - {name: telegram_update, event: inbound.telegram, source: external}
  outputs: {events: []}
`,
		"flows/telegram-ingress/types.yaml": "{}\n", "flows/telegram-ingress/events.yaml": "{}\n", "flows/telegram-ingress/nodes.yaml": "{}\n", "flows/telegram-ingress/tools.yaml": "{}\n", "flows/telegram-ingress/policy.yaml": "{}\n", "flows/telegram-ingress/agents.yaml": "{}\n",
		"flows/telegram-ingress/entities.yaml": "telegram_service:\n  service_id:\n    type: text\n    initial: standing\n  active_chats:\n    type: map[text]json\n    initial: {}\n",
		"flows/telegram-chat/schema.yaml": `name: telegram-chat
mode: template
instance:
  by: chat_id
  on_missing: create
  on_conflict: reuse
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: telegram_text_message
        event: inbound.telegram.text_message
        source: external
        resolution: {mode: select-or-create, instance_key: chat_id}
        carries:
          chat_id: {from: payload.chat_id, type: text}
  outputs: {events: []}
`,
		"flows/telegram-chat/types.yaml": "{}\n", "flows/telegram-chat/policy.yaml": "{}\n",
		"flows/telegram-chat/entities.yaml": "chat:\n  chat_id:\n    type: text\n    indexed: true\n    _unused_reason: populated from the normalized input resolution carry\n  last_message:\n    type: text\n    initial: \"\"\n",
		"flows/telegram-chat/events.yaml":   "telegram.reply_requested:\n  chat_id: text\n  text: text\n",
		"flows/telegram-chat/nodes.yaml": `telegram-responder:
  id: telegram-responder
  execution_type: system_node
  subscribes_to: [telegram.reply_requested]
  event_handlers:
    telegram.reply_requested:
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        input:
          chat_id: {cel: payload.chat_id}
          text: {cel: payload.text}
`,
		"flows/telegram-chat/tools.yaml": fmt.Sprintf(`telegram.send_message:
  category: provider_connector
  description: send Telegram messages
  handler_type: http
  effect_class: non_idempotent_write
  credentials: [telegram_bot_token]
  input_schema:
    type: object
    properties:
      chat_id: {type: string}
      text: {type: string}
    required: [chat_id, text]
  output_schema: {type: object}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: %s/bot{{credentials.telegram_bot_token}}/sendMessage
    body:
      chat_id: "{{input.chat_id}}"
      text: "{{input.text}}"
`, toolURL),
		"flows/telegram-chat/agents.yaml":           "phrase-bot:\n  id: phrase-bot-{instance_id}\n  role: phrase_bot\n  prompt_ref: phrase-bot\n  model: regular\n  memory: true\n  subscriptions: [inbound.telegram.text_message]\n  emit_events: [telegram.reply_requested]\n",
		"flows/telegram-chat/prompts/phrase-bot.md": "Reply to each Telegram message by emitting telegram.reply_requested with the same chat_id.\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, filepath.ToSlash(name), source)
	}
	return root
}

// CopyStandingTelegramMemoryServe extends the supported Telegram route with a
// standing singleton so memory continuity is proven for both flow modes.
func CopyStandingTelegramMemoryServe(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	root := CopyStandingTelegramServe(t, telegramBaseURL)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "  - {id: telegram-chat, flow: telegram-chat, mode: template}\n", `  - {id: telegram-chat, flow: telegram-chat, mode: template}
  - id: memory-singleton
    flow: memory-singleton
    mode: singleton
    activation: standing
`)
	files := map[string]string{
		"flows/memory-singleton/schema.yaml": `name: memory-singleton
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - {name: memory_ping, event: memory.ping, source: external}
  outputs: {events: []}
`,
		"flows/memory-singleton/events.yaml": "memory.ping:\n  text: text\n",
		"flows/memory-singleton/agents.yaml": `memory-bot:
  id: memory-bot
  role: memory_bot
  prompt_ref: memory-bot
  model: regular
  memory: true
  subscriptions: [memory.ping]
`,
		"flows/memory-singleton/prompts/memory-bot.md": "Remember each singleton ping and answer with observed.\n",
		"flows/memory-singleton/types.yaml":            "{}\n",
		"flows/memory-singleton/entities.yaml": `memory_state:
  last_ping:
    type: text
    initial: ""
  pings:
    type: map[text]json
    initial: {}
`,
		"flows/memory-singleton/nodes.yaml":  "{}\n",
		"flows/memory-singleton/tools.yaml":  "{}\n",
		"flows/memory-singleton/policy.yaml": "{}\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}
