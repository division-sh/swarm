package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyRootIngressServedFollowUp derives the fixed event-publish follow-up
// runtime proof from the canonical root-ingress artifact.
func CopyRootIngressServedFollowUp(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `name: routing-root-ingress
stages:
  pending: {initial: true}
  processed: {}
  done: {terminal: true}
`, `name: served-event-publish-followup
stages:
  new: {initial: true}
  waiting: {}
  done: {terminal: true}
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `    item.received:
      advances_to: processed
      emit:
        event: item.processed
        fields:
          item_id: ${payload.item_id}
`, `    item.received:
      rules:
        initialize:
          when: "payload.item_id != 'emit'"
          advances_to: waiting
        emit_processed:
          when: "payload.item_id == 'emit'"
          emit:
            event: item.processed
            fields:
              item_id: ${payload.item_id}
        unmatched:
          else: true
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `item-observer:
  execution_type: system_node
  subscribes_to: [item.processed]
  event_handlers:
    item.processed:
      advances_to: done
`, `item-observer:
  execution_type: system_node
  subscribes_to: [item.processed]
  event_handlers:
    item.processed:
      rules:
        complete:
          when: "has(payload.item_id) && payload.item_id == 'review'"
          advances_to: done
        unmatched:
          else: true
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - item.received\n", "    - item.received\n    - item.processed\n")
	return root
}

// CopyRootIngressServedDecisionControl declares the lifecycle that the mailbox
// and run-control fixtures materialize, rather than borrowing an unrelated graph.
func CopyRootIngressServedDecisionControl(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedFollowUp(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `stages:
  new: {initial: true}
  waiting: {}
  done: {terminal: true}
`, `stages:
  awaiting_review:
    initial: true
    gate:
      decision: launch_review
      outcomes:
        approve: {advances_to: done}
        reject: {advances_to: rework}
  waiting: {}
  rework: {}
  done: {terminal: true}
`)
	return root
}

// CopyRootIngressServedExternalEvent derives the fixed externally handled
// event proof without exposing event-source authority as caller YAML.
func CopyRootIngressServedExternalEvent(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedFollowUp(t)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), `item.processed:
  item_id: text?
`, `item.processed:
  item_id: text?
external.observed:
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - item.processed\n", "    - item.processed\n    - external.observed\n  outputs: [external.observed]\n")
	return root
}

// CopyRootIngressServedConversationFork declares the source agent through the
// same startup topology used by the live conversation-fork proof.
func CopyRootIngressServedConversationFork(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedExternalEvent(t)

	writeClosedVariantFile(t, root, "fork-source/schema.yaml", `name: fork-source
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    - fork.source_message
    - item.processed
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - external.observed\n", "    - external.observed\n    - fork.source_message\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "outputs: [external.observed]\n", "outputs: [external.observed, fork.source_message, item.processed]\nconnect:\n  - {event: fork.source_message, from: ., to: fork-source}\n  - {event: item.processed, from: ., to: fork-source}\n")
	writeClosedVariantFile(t, root, "fork-source/entities.yaml", "conversation: {}\n")
	writeClosedVariantFile(t, root, "fork-source/nodes.yaml", "owner:\n  execution_type: system_node\n  event_handlers:\n    fork.source_message:\n      create_entity: true\n      advances_to: active\n    item.processed:\n      advances_to: done\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "external.observed:\n", "external.observed:\nfork.source_message:\n  note: text\n")
	writeClosedVariantFile(t, root, "fork-source/agents.yaml", `fork-source-agent:
  role: researcher
  intent: prompts/fork-source-agent.md
  model: regular
  memory: true
  subscriptions:
    - fork.source_message
`)
	writeClosedVariantFile(t, root, "fork-source/prompts/fork-source-agent.md", "Preserve the source conversation used by the operator fork proof.\n")
	return root
}

// CopyRootIngressServedActiveLoad derives the fixed agent-subscription load
// proof from the canonical root-ingress route.
func CopyRootIngressServedActiveLoad(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedFollowUp(t)
	addServedItemProcessedAgent(t, root, "Handle the active-load event and wait for test release.\n")
	return root
}

// CopyRootIngressServedSessionCleanup derives the destructive-cleanup proof
// with a separately addressable live-agent event from the canonical route.
func CopyRootIngressServedSessionCleanup(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedFollowUp(t)

	writeClosedVariantFile(t, root, "hold/schema.yaml", `name: hold
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    - item.agent_hold
    - item.processed
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    - item.processed\n", "    - item.processed\n    - item.agent_hold\n  outputs: [item.agent_hold, item.processed]\nconnect:\n  - {event: item.agent_hold, from: ., to: hold}\n  - {event: item.processed, from: ., to: hold}\n")
	writeClosedVariantFile(t, root, "hold/entities.yaml", "session: {}\n")
	writeClosedVariantFile(t, root, "hold/nodes.yaml", "owner:\n  execution_type: system_node\n  event_handlers:\n    item.agent_hold:\n      create_entity: true\n      advances_to: active\n    item.processed:\n      advances_to: done\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "item.processed:\n", "item.agent_hold:\n  note: text\nitem.processed:\n")
	writeClosedVariantFile(t, root, "hold/agents.yaml", `load-agent:
  role: load_agent
  intent: prompts/load-agent.md
  model: regular
  memory: true
  subscriptions:
    - item.agent_hold
`)
	writeClosedVariantFile(t, root, "hold/prompts/load-agent.md", "Hold one lifecycle-authorized live session until destructive cleanup closes runtime admission.\n")
	return root
}

// CopyRootIngressServedLiveAgent derives the fixed live-agent parity proof
// from the canonical root-ingress route.
func CopyRootIngressServedLiveAgent(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedFollowUp(t)
	addServedItemProcessedAgent(t, root, "Handle live-agent parity events.\n")
	return root
}

func addServedItemProcessedAgent(t testing.TB, root, prompt string) {
	t.Helper()
	writeClosedVariantFile(t, root, "agents.yaml", `load-agent:
  role: load_agent
  intent: prompts/load-agent.md
  model: regular
  subscriptions:
    - item.processed
`)
	writeClosedVariantFile(t, root, "prompts/load-agent.md", prompt)
}

// CopyRootIngressLegacyTemplateTargetRoute keeps the tracked legacy template
// runtime proof behind a fixed constructor until issue #1738 retires it.
func CopyRootIngressLegacyTemplateTargetRoute(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	addLegacyTemplateRoot(t, root)
	writeClosedVariantFile(t, root, "operating/schema.yaml", `name: operating
instance: instance_id
instance_variables:
  variables:
    product_id: text
stages:
  initializing: {initial: true}
  waiting: {}
  ready: {terminal: true}
pins:
  inputs:
    - event: opco.create_requested
      initialize:
        product_id: payload.product_id
    - opco.product_initialization_requested
    - opco.product_review_requested
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeClosedVariantFile(t, root, "operating/entities.yaml", `
product:
  instance_id: {type: text, _unused_reason: receiver instance identity}
  product_id: text
  note: text
`)
	writeClosedVariantFile(t, root, "operating/events.yaml", `
opco.product_initialization_requested:
  instance_id: string
  product_id: string
`)
	writeClosedVariantFile(t, root, "operating/nodes.yaml", `
lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.create_requested, opco.product_initialization_requested, opco.product_review_requested]
  event_handlers:
    opco.create_requested: {}
    opco.product_initialization_requested:
      data_accumulation:
        source_event: opco.product_initialization_requested
        writes:
          - source_field: product_id
            target_field: product_id
      advances_to: waiting
    opco.product_review_requested:
      data_accumulation:
        source_event: opco.product_review_requested
        writes:
          - source_field: note
            target_field: note
      advances_to: ready
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `    - opco.spinup_requested
  outputs:
    - opco.create_requested
connect:
  - {event: opco.create_requested, from: ., to: operating, resolution: create}
`, `    - opco.spinup_requested
    - opco.product_review_requested
  outputs: [opco.create_requested, opco.product_review_requested]
connect:
  - {event: opco.create_requested, from: ., to: operating, resolution: create}
  - {event: opco.product_review_requested, from: ., to: operating, resolution: select}
`)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "opco.spinup_requested:\n", `opco.product_review_requested:
  instance_id: text
  product_id: text
  note: text
opco.spinup_requested:
`)
	return root
}

// CopyRootIngressLegacyTemplateAutoEmit keeps the tracked legacy auto-emit
// proof behind a fixed constructor until issue #1738 retires it.
func CopyRootIngressLegacyTemplateAutoEmit(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	addLegacyTemplateRoot(t, root)
	writeClosedVariantFile(t, root, "operating/schema.yaml", `name: operating
instance: instance_id
instance_variables:
  variables:
    product_id: text
stages:
  initializing: {initial: true}
  spawning: {}
  ready: {terminal: true}
pins:
  inputs:
    - event: opco.create_requested
      initialize:
        product_id: payload.product_id
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeClosedVariantFile(t, root, "operating/entities.yaml", `
product:
  instance_id: {type: text, _unused_reason: receiver instance identity}
  product_id: text
`)
	writeClosedVariantFile(t, root, "operating/events.yaml", `
opco.product_initialization_requested:
  instance_id: string
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`)
	writeClosedVariantFile(t, root, "operating/nodes.yaml", `
lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.create_requested, opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.create_requested: {}
    opco.product_initialization_requested:
      data_accumulation:
        source_event: opco.product_initialization_requested
        writes:
          - source_field: product_id
            target_field: product_id
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: ${payload.product_id}
      advances_to: spawning
component-scaffold:
  execution_type: system_node
  subscribes_to: [component_scaffold.spawn_requested]
  event_handlers:
    component_scaffold.spawn_requested:
      advances_to: ready
`)
	return root
}

func addLegacyTemplateRoot(t testing.TB, root string) {
	t.Helper()

	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `name: routing-root-ingress
stages:
  pending: {initial: true}
  processed: {}
  done: {terminal: true}
pins:
  inputs:
    - item.received
`, `name: routing-root-ingress
stages:
  new: {initial: true}
  waiting: {}
  done: {terminal: true}
pins:
  inputs:
    - item.received
    - opco.bootstrap_requested
    - opco.spinup_requested
  outputs:
    - opco.create_requested
connect:
  - {event: opco.create_requested, from: ., to: operating, resolution: create}
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "      advances_to: processed\n", "      advances_to: waiting\n")
	applyClosedReplacement(t, filepath.Join(root, "entities.yaml"), `item:
  item_id:
    type: text
    _unused_reason: external root instance identity
`, `portfolio:
  owner: text
`)
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), `item.processed:
  item_id: text?
`, `item.processed:
  item_id: text?
opco.bootstrap_requested:
  owner: text
opco.spinup_requested:
  instance_id: text
  product_id: text
opco.create_requested:
  key: instance_id
  instance_id: text
  product_id: text
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    item.processed:\n      advances_to: done\n", `    item.processed:
      advances_to: done
portfolio-bootstrap:
  execution_type: system_node
  subscribes_to: [opco.bootstrap_requested]
  event_handlers:
    opco.bootstrap_requested:
      data_accumulation:
        source_event: opco.bootstrap_requested
        writes:
          - source_field: owner
            target_field: owner
      advances_to: waiting
portfolio-node:
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  produces: [opco.create_requested]
  event_handlers:
    opco.spinup_requested:
      emit:
        event: opco.create_requested
        fields:
          instance_id: ${payload.instance_id}
          product_id: ${payload.product_id}
      advances_to: done
`)
}
