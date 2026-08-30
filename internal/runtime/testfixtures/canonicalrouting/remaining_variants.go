package canonicalrouting

import (
	"path/filepath"
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
	removeClosedVariantFiles(t, root, "entities.yaml", "events.yaml", "nodes.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", "{}\n")
	writeClosedVariantFile(t, root, "treasury/schema.yaml", `name: treasury
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [opco.spend_requested]
`)
	writeClosedVariantFile(t, root, "treasury/events.yaml", `opco.spend_requested:
  swarm: {source: external}
  vertical_id: string
  amount_usd: number
opco.spend_recorded:
  vertical_id: string
`)
	writeClosedVariantFile(t, root, "treasury/entities.yaml", `budget:
  vertical_id:
    type: string
    indexed: true
    _unused_reason: static multi-entity retirement selection key proof field
  spent_usd:
    type: number
    initial: 0
`)
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
	writeClosedVariantFile(t, root, "treasury/nodes.yaml", `treasury-node:
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
	switch entityID {
	case RootStaticNoEntityID:
	case RootStaticOptionalEntityID:
		entityIDField = "  entity_id: string?\n"
	case RootStaticRequiredEntityID:
		entityIDField = "  entity_id: string\n"
	default:
		t.Fatalf("unsupported root static entity ID variant %d", entityID)
	}
	writeClosedVariantFile(t, root, "events.yaml", `subject.created:
  swarm:
    source: external
`+entityIDField+`  display_name: string?
subject.observed:
`+entityIDField+`  display_name: string?
`)
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
	return root
}

func CopyServedJoinProof(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "entities.yaml")
	files := map[string]string{

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
      - {event: order.started, source: external}
      - {event: order.dispatched, source: external}
      - {event: item.completed, source: external}
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

		"schema.yaml": `name: review
initial_state: new
terminal_states: [done]
states: [new, done]
`,
		"events.yaml": "scan.requested:\n  swarm: {source: external}\n  topic: text\n",
		"nodes.yaml":  "scan-orchestrator:\n  id: scan-orchestrator\n  execution_type: system_node\n  subscribes_to: [scan.requested]\n",
		"operating/schema.yaml": `name: operating
mode: static
initial_state: initializing
terminal_states: [ready]
states: [initializing, waiting, ready]
`,
		"operating/entities.yaml": `product:
  product_id: text
  note: text
  review_score: integer
  business_brief: Brief
  feature_list: list<Feature>
  review_scores: map[text]integer
`,
		"operating/types.yaml":  "types:\n  Brief:\n    summary: text\n  Feature:\n    name: text\n",
		"operating/events.yaml": "opco.product_review_requested:\n  swarm: {source: external}\n  note: text\n",
		"operating/nodes.yaml": `reviewer:
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
		"secondary/schema.yaml":   "name: secondary\nmode: static\ninitial_state: open\nterminal_states: [closed]\nstates: [open, closed]\n",
		"secondary/entities.yaml": "ticket:\n  ticket_id: text\n",
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

		"schema.yaml": "name: test\nconnect:\n  - {event: deploy.done, from: producer, to: consumer}\n",
		"producer/schema.yaml": `name: producer
mode: static
pins:
  outputs:
    events:
      - deploy.done
`,
		"producer/events.yaml": "deploy.done:\n  key: vertical_id\n  vertical_id: string\n",
		"consumer/schema.yaml": `name: consumer
mode: template
instance: vertical_id
pins:
  inputs:
    events:
      - event: deploy.done
        resolution: {mode: select-or-create}
`,
		"consumer/entities.yaml": "deployment:\n  vertical_id:\n    type: string\n",
		"consumer/nodes.yaml":    "consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    deploy.done: {}\n",
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyTemplateInstanceEmpireOutbox(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	files := map[string]string{

		"events.yaml": `approval.completed:
  swarm:
    source: external
  entity_id: string?
  instance_id: string?
  product_id: string?
opco.spinup_requested:
  instance_id: string?
  product_id: string?
`,
		"nodes.yaml": `approval-router:
  id: approval-router
  execution_type: system_node
  subscribes_to: [approval.completed]
  produces: [opco.spinup_requested]
  event_handlers:
    approval.completed:
      emit:
        event: opco.spinup_requested
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
`,
		"operating/schema.yaml": `name: operating
mode: template
instance: instance_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state: {}\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  product_id: string?
component_scaffold.spawn_requested:
  product_id: string?
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      advances_to: ready
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
`,
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyProviderRollback(t testing.TB, withHandler bool) string {
	t.Helper()
	root := CopyExample(t, TemplateSelectOrCreate)
	nodes := ""
	if withHandler {
		nodes = "consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    inbound.telegram.text_message: {}\n"
	}
	files := map[string]string{
		"manifest.yaml": `name: provider-rollback-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`,
		"schema.yaml": "name: provider-rollback-proof\n",
		"events.yaml": "inbound.telegram:\n  raw: boolean\ninbound.telegram.text_message:\n  chat_id: text\n",
		"consumer/schema.yaml": `name: consumer
mode: template
instance: chat_id
pins:
  inputs:
    events:
      - event: inbound.telegram.text_message
        source: external
        resolution: {mode: select-or-create}
`,
		"consumer/entities.yaml": "chat:\n  chat_id:\n    type: text\n    indexed: true\n",
		"consumer/nodes.yaml":    nodes,
	}
	if nodes == "" {
		delete(files, "consumer/nodes.yaml")
	}
	for name, source := range files {
		writeClosedVariantFile(t, root, name, source)
	}
	return root
}

func CopyProviderRollbackRenamedSource(t testing.TB, withCarrier bool) string {
	t.Helper()
	root := CopyProviderRollback(t, withCarrier)
	applyClosedReplacement(t, filepath.Join(root, "consumer", "schema.yaml"),
		"        resolution: {mode: select-or-create}",
		"        resolution: {mode: select-or-create, from: payload.external_chat_id}")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"inbound.telegram.text_message:\n  chat_id: text\n",
		"inbound.telegram.text_message:\n  chat_id: text\n  external_chat_id: text\n")
	return root
}

func CopyProviderRollbackInvalidSourceType(t testing.TB) string {
	t.Helper()
	root := CopyProviderRollback(t, true)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"inbound.telegram.text_message:\n  chat_id: text\n",
		"inbound.telegram.text_message:\n  chat_id: integer\n")
	return root
}

func CopyProviderRollbackSyntheticCollision(t testing.TB, mint CreateMint) string {
	t.Helper()
	root := CopyProviderRollback(t, true)
	var source string
	switch mint {
	case CreateMintUUID:
		source = "generated.uuid"
	case CreateMintEventID:
		source = "event.id"
	default:
		t.Fatalf("provider rollback synthetic collision requires UUID or event-ID source, got %d", mint)
	}
	applyClosedReplacement(t, filepath.Join(root, "consumer", "schema.yaml"),
		"        resolution: {mode: select-or-create}",
		"        resolution: {mode: create, from: "+source+"}")
	applyClosedReplacement(t, filepath.Join(root, "consumer", "entities.yaml"),
		"    type: text", "    type: uuid")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"  chat_id: text", "  chat_id: uuid")
	return root
}

func CopyProviderRollbackSyntheticProjection(t testing.TB, mint CreateMint) string {
	t.Helper()
	root := CopyProviderRollbackSyntheticCollision(t, mint)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"  chat_id: uuid", "  message: text")
	return root
}

func CopyTelegramAgentImportedSyntheticProjection(t testing.TB, mint CreateMint, collision bool) string {
	t.Helper()
	root := CopyExample(t, TelegramAgent)
	botRoot := filepath.Join(root, "bot")
	var source string
	switch mint {
	case CreateMintUUID:
		source = "generated.uuid"
	case CreateMintEventID:
		source = "event.id"
	default:
		t.Fatalf("imported synthetic projection requires UUID or event-ID source, got %d", mint)
	}
	field := "chat_id"
	if collision {
		field = "conversation_reference"
	}
	applyClosedReplacement(t, filepath.Join(botRoot, "telegram-chat", "schema.yaml"),
		"instance: conversation_reference", "instance: "+field)
	applyClosedReplacement(t, filepath.Join(botRoot, "telegram-chat", "schema.yaml"),
		"          mode: select-or-create", "          mode: create\n          from: "+source)
	applyClosedReplacement(t, filepath.Join(botRoot, "telegram-chat", "entities.yaml"),
		"  conversation_reference:\n    type: text", "  "+field+":\n    type: uuid")
	return root
}
