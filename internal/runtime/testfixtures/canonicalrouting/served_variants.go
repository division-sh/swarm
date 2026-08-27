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
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "name: routing-root-ingress\n", "name: served-event-publish-followup\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `name: routing-root-ingress
initial_state: pending
terminal_states: [done]
states: [pending, processed, done]
`, `name: served-event-publish-followup
initial_state: new
terminal_states: [done]
states: [new, waiting, done]
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `    item.received:
      advances_to: processed
      emit:
        event: item.processed
        fields:
          item_id: payload.item_id
`, `    item.received:
      rules:
        initialize:
          element_id: 00000000-0000-4000-8000-000000000005
          condition: "payload.item_id != 'emit'"
          advances_to: waiting
        emit_processed:
          element_id: 00000000-0000-4000-8000-000000000006
          condition: "payload.item_id == 'emit'"
          emit:
            event: item.processed
            fields:
              item_id: payload.item_id
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `item-observer:
  id: item-observer
  execution_type: system_node
  subscribes_to: [item.processed]
  event_handlers:
    item.processed:
      advances_to: done
`, `item-observer:
  id: item-observer
  execution_type: system_node
  subscribes_to: [item.processed]
  event_handlers:
    item.processed:
      rules:
        complete:
          element_id: 00000000-0000-4000-8000-000000000007
          condition: "payload.item_id == 'review'"
          advances_to: done
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
  swarm:
    source: external
    consumer: external
`)
	return root
}

// CopyRootIngressServedConversationFork declares the source agent through the
// same startup topology used by the live conversation-fork proof.
func CopyRootIngressServedConversationFork(t testing.TB) string {
	t.Helper()
	root := CopyRootIngressServedExternalEvent(t)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "flows: []\n", `flows:
  - id: fork-source
    flow: fork-source
    mode: static
`)
	writeClosedVariantFile(t, root, "flows/fork-source/schema.yaml", `name: fork-source
mode: static
pins:
  inputs:
    events:
      - event: fork.source_message
        source: external
`)
	writeClosedVariantFile(t, root, "flows/fork-source/events.yaml", `fork.source_message:
  note: text
`)
	writeClosedVariantFile(t, root, "flows/fork-source/agents.yaml", `fork-source-agent:
  id: fork-source-agent
  role: researcher
  intent: prompts/fork-source-agent.md
  model: regular
  memory: true
  subscriptions:
    - fork.source_message
`)
	writeClosedVariantFile(t, root, "flows/fork-source/prompts/fork-source-agent.md", "Preserve the source conversation used by the operator fork proof.\n")
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
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "flows: []\n", `flows:
  - id: hold
    flow: hold
    mode: static
`)
	writeClosedVariantFile(t, root, "flows/hold/schema.yaml", `name: hold
mode: static
pins:
  inputs:
    events:
      - event: item.agent_hold
        source: external
`)
	writeClosedVariantFile(t, root, "flows/hold/events.yaml", `item.agent_hold:
  note: text
`)
	writeClosedVariantFile(t, root, "flows/hold/agents.yaml", `load-agent:
  id: load-agent
  role: load_agent
  intent: prompts/load-agent.md
  model: regular
  memory: true
  subscriptions:
    - item.agent_hold
`)
	writeClosedVariantFile(t, root, "flows/hold/prompts/load-agent.md", "Hold one lifecycle-authorized live session until destructive cleanup closes runtime admission.\n")
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
  id: load-agent
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
	writeClosedVariantFile(t, root, "flows/operating/schema.yaml", `
name: operating
mode: template
instance: product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, waiting, ready]
pins:
  inputs:
    events:
      - opco.product_initialization_requested
      - event: opco.product_review_requested
        source: external
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeClosedVariantFile(t, root, "flows/operating/entities.yaml", `
product:
  product_id: text
  note: text
`)
	writeClosedVariantFile(t, root, "flows/operating/events.yaml", `
opco.product_initialization_requested:
  swarm:
    source: external
  product_id: string
opco.product_review_requested:
  swarm:
    source: external
  note: string
`)
	writeClosedVariantFile(t, root, "flows/operating/nodes.yaml", `
lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested, opco.product_review_requested]
  event_handlers:
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
	return root
}

// CopyRootIngressLegacyTemplateAutoEmit keeps the tracked legacy auto-emit
// proof behind a fixed constructor until issue #1738 retires it.
func CopyRootIngressLegacyTemplateAutoEmit(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	addLegacyTemplateRoot(t, root)
	writeClosedVariantFile(t, root, "flows/operating/schema.yaml", `
name: operating
mode: template
instance: product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, spawning, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeClosedVariantFile(t, root, "flows/operating/entities.yaml", `
product:
  product_id: text
`)
	writeClosedVariantFile(t, root, "flows/operating/events.yaml", `
opco.product_initialization_requested:
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`)
	writeClosedVariantFile(t, root, "flows/operating/nodes.yaml", `
lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      data_accumulation:
        source_event: opco.product_initialization_requested
        writes:
          - source_field: product_id
            target_field: product_id
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
      advances_to: spawning
component-scaffold:
  id: component-scaffold
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
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "flows: []\n", `flows:
  - id: operating
    flow: operating
    mode: template
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), `name: routing-root-ingress
initial_state: pending
terminal_states: [done]
states: [pending, processed, done]
pins:
  inputs:
    events:
      - event: item.received
        source: external
`, `name: routing-root-ingress
initial_state: new
terminal_states: [done]
states: [new, waiting, done]
pins:
  inputs:
    events:
      - event: item.received
        source: external
      - event: opco.bootstrap_requested
        source: external
      - event: opco.spinup_requested
        source: external
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "      advances_to: processed\n", "      advances_to: waiting\n")
	applyClosedReplacement(t, filepath.Join(root, "entities.yaml"), "item: {}\n", `portfolio:
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
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    item.processed:\n      advances_to: done\n", `    item.processed:
      advances_to: done
portfolio-bootstrap:
  id: portfolio-bootstrap
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
      advances_to: done
`)
}
