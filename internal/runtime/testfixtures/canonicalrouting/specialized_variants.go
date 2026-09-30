package canonicalrouting

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CopyOutputModeVerify derives the CLI output-mode fixture from the checked-in
// root-ingress owner while keeping its route declaration closed in this
// package.
func CopyOutputModeVerify(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", "name: output-mode-verify\npins:\n  inputs:\n    events: [task.assigned]\n  outputs:\n    events: [task.assigned]\nconnect:\n  - {event: task.assigned, from: ., to: child}\n")
	removeClosedVariantFiles(t, root, "nodes.yaml", "events.yaml", "entities.yaml")
	writeClosedVariantFile(t, root, "events.yaml", "task.assigned:\n")
	writeLegacyInstanceFlow(t, root, "child", `name: child
stages:
  idle: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events:
      - task.assigned
    reads: [priority]
`, "", `case:
  priority:
    type: integer
    _unused_reason: output-mode child primary entity proof field
`, `reader:
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      guard:
        check: "has(entity.priority) && entity.priority >= 0"
      advances_to: done
`)
	return root
}

// CopyManagedNativeLifecycle owns the route-bearing shell used by the runtime
// startup capability test. The test varies only the declared agent identity.
func CopyManagedNativeLifecycle(t testing.TB, agentID string) string {
	t.Helper()
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		t.Fatal("managed-native lifecycle fixture requires an agent ID")
	}
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "nodes.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", `name: managed-native-lifecycle
stages:
  pending: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [task.requested]
`)
	writeClosedVariantFile(t, root, "agents.yaml", fmt.Sprintf(`%s:
  id: %s
  type: stub
  role: researcher
  model: regular
  intent: prompts/%s.md
  native_tools:
    web_search: true
  subscriptions:
    - task.requested
  emit_events: []
`, agentID, agentID, agentID))
	writeClosedVariantFile(t, root, filepath.Join("prompts", agentID+".md"), "Operate as the managed native lifecycle test agent.\n")
	for file, contents := range map[string]string{
		"events.yaml": "task.requested:\n",
	} {
		writeClosedVariantFile(t, root, file, contents)
	}
	return root
}

// CopyDescribeStageGraph owns the route-bearing shell around the CLI stage
// graph fixture; stage/timer/join content remains its distinct test concept.
func CopyDescribeStageGraph(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", "name: stage-graph\npins:\n  inputs:\n    events: [ticket.opened, ticket.closed]\n  outputs:\n    events: [ticket.opened, ticket.closed]\nconnect:\n  - {event: ticket.opened, from: ., to: support}\n  - {event: ticket.closed, from: ., to: support}\n")
	removeClosedVariantFiles(t, root, "nodes.yaml", "events.yaml", "entities.yaml")
	writeLegacyInstanceFlow(t, root, "support", `name: support
pins:
  inputs:
    events: [ticket.opened, ticket.closed]
stages:
  waiting:
    initial: true
  active:
    timers:
      - after: 48h
        emit: ticket.sla_escalated
      - after: 72h
        advances_to: timed_out
  review:
    terminal: true
  timed_out:
    terminal: true
`, `ticket.sla_escalated:
line_item.requested:
  line_item_id: string
  line_item_index: integer
`, `ticket:
  expected_line_item_ids:
    type: "[text]"
    initial: []
`, `support-node:
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
  event_handlers:
    ticket.opened:
      create_entity: true
      fan_out:
        items_from: payload.line_items
        as: line_item
        identity: line_item
        emit:
          event: line_item.requested
          fields:
            line_item_id: ${line_item}
            line_item_index: ${fan_out.index}
      advances_to: active
    ticket.closed:
      join:
        stage: active
        members:
          from: entity.expected_line_item_ids
          by: payload.line_item_id
        output: payload.result
        on_complete:
          advances_to: review
        timeout:
          after: 1h
          advances_to: timed_out
    ticket.sla_escalated: {}
    line_item.requested: {}
`)
	writeClosedVariantFile(t, root, "events.yaml", "ticket.opened:\n  line_items: \"[text]\"\nticket.closed:\n  line_item_id: string\n  result: string\n")
	return root
}

func CopyInboundAdmissionPolicyMatrix(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "nodes.yaml", "events.yaml", "entities.yaml")
	files := map[string]string{

		"schema.yaml": "name: inbound-admission-policy-matrix\n",
		"matrix/schema.yaml": `name: matrix
mode: singleton
activation: standing
stages:
  active: {initial: true}
ingress:
  alias: matrix
  providers:
    - provider: telegram
      signing_secret: webhook_signing.telegram
      admission:
        pack: {id: provider.telegram}
    - provider: intercom
      signing_secret: webhook_signing.intercom
      admission:
        pack: {id: provider.intercom}
    - provider: acme_public
      admission:
        kind: raw
        acknowledge: unsigned_webhook
        authentication: {kind: none}
        event: inbound.acme_public
        delivery_id: {source: body_sha256}
        payload: json
    - provider: partner_auth
      signing_secret: webhook_signing.partner
      admission:
        kind: raw
        authentication: {kind: hmac_sha256, header: X-Partner-Signature, encoding: hex}
        event: inbound.partner_auth
        delivery_id: {source: header, header: X-Partner-Delivery}
        payload: json
    - provider: partner_open
      admission:
        kind: raw
        authentication: {kind: none}
        event: inbound.partner_open
        delivery_id: {source: json_path, json_path: $.delivery.id}
        payload: raw
    - provider: partner_ack
      admission:
        kind: raw
        acknowledge: unsigned_webhook
        authentication: {kind: none}
        event: inbound.partner_ack
        delivery_id: {source: body_sha256}
        payload: raw
pins:
  inputs:
    events:
      - inbound.telegram
      - inbound.telegram.text_message
      - inbound.intercom
      - inbound.acme_public
      - inbound.partner_auth
      - inbound.partner_open
      - inbound.partner_ack
`,
		"matrix/entities.yaml": "matrix_service:\n  service_id:\n    type: text\n    initial: standing\n",
		"matrix/events.yaml":   inboundAdmissionEvents(),
		"matrix/nodes.yaml":    inboundAdmissionNodes(),
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

func inboundAdmissionEvents() string {
	var out strings.Builder
	for _, event := range []string{"inbound.acme_public", "inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"} {
		fmt.Fprintf(&out, "%s:\n  provider: text\n  provider_event_id: text\n  provider_event_type: text\n  data: json\n", event)
	}
	return out.String()
}

func inboundAdmissionNodes() string {
	events := []string{"inbound.telegram", "inbound.telegram.text_message", "inbound.intercom", "inbound.acme_public", "inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"}
	var out strings.Builder
	out.WriteString("matrix-sink:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(events, ", ") + "]\n  event_handlers:\n")
	for _, event := range events {
		fmt.Fprintf(&out, `    %s:
      data_accumulation:
        writes:
          - target_field: service_id
            value: {literal: standing}
`, event)
	}
	return out.String()
}

func CopyServedTestSetup(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", `name: served-test-setup
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events:
      - widget.started
      - widget.scored
`)
	writeClosedVariantFile(t, root, "entities.yaml", "widget:\n  score: integer\n")
	writeClosedVariantFile(t, root, "events.yaml", `widget.scored:
  delta: integer
widget.started:
  seed: boolean
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `scorer:
  execution_type: system_node
  subscribes_to: [widget.scored]
  event_handlers:
    widget.scored:
      guard: {check: "has(entity.score)"}
      data_accumulation:
        source_event: widget.scored
        writes:
          - target_field: score
            value: "${entity.score + payload.delta}"
      advances_to: done
`)
	return root
}

func CopyVerifyLintEvidence(t testing.TB, missingEmitSchema bool) string {
	t.Helper()
	root := CopyOutputModeVerify(t)

	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "name: output-mode-verify", "name: verify-lint-evidence")
	writeClosedVariantFile(t, root, "entities.yaml", `case:
  untouched:
    type: integer
    _unused_reason: verify command lint evidence proof field
  priority:
    type: integer
    _unused_reason: child read-pin coverage proof field
`)
	writeClosedVariantFile(t, root, "child/entities.yaml", `case:
  priority:
    type: integer
    _unused_reason: verify lint evidence child primary entity proof field
`)
	if missingEmitSchema {
		writeClosedVariantFile(t, root, "child/agents.yaml", `strict-schema-agent:
  id: strict-schema-agent
  role: strict_schema_agent
  intent: prompts/strict-schema-agent.md
  model: regular
  subscriptions: [task.assigned]
  emit_events: [missing.event]
`)
		writeClosedVariantFile(t, root, "child/prompts/strict-schema-agent.md", "Emit the missing event when requested.\n")
	}
	return root
}

func CopyFirstFlowTutorial(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	writeClosedVariantFile(t, root, "schema.yaml", "name: ticket-flow\nstages:\n  open: {initial: true}\n  assigned: {}\n  resolved: {terminal: true}\npins:\n  inputs:\n    events: [ticket.classified]\n")
	writeClosedVariantFile(t, root, "entities.yaml", `ticket:
  category:
    type: text
    initial: ""
  priority:
    type: text
    initial: ""
  resolution:
    type: text
    initial: ""
    _unused_reader_reason: External operator readout from the persisted ticket record
`)
	writeClosedVariantFile(t, root, "events.yaml", `ticket.classified:
  category: text
  priority: text
ticket.assigned:
  category: text
  priority: text
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `classifier:
  execution_type: system_node
  subscribes_to: [ticket.classified]
  produces: [ticket.assigned]
  event_handlers:
    ticket.classified:
      guard:
        check: "entity.category != '' && entity.priority != ''"
      emit:
        event: ticket.assigned
        fields:
          category: ${entity.category}
          priority: ${entity.priority}
      advances_to: assigned
assignee:
  execution_type: system_node
  subscribes_to: [ticket.assigned]
  event_handlers:
    ticket.assigned:
      guard:
        check: "entity.category != ''"
      advances_to: resolved
`)
	return root
}

func CopyAgentSlugAdmission(t testing.TB, workflowName, agentKey, agentID string) string {
	t.Helper()
	workflowName = closedScalarLiteral(t, "workflow name", workflowName, "agent-slug-admission")
	agentKey = closedScalarLiteral(t, "agent key", agentKey, "worker")
	agentID = closedScalarLiteral(t, "agent ID", agentID, "worker")
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "nodes.yaml", "entities.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", "mode: static\nstages:\n  pending: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [agent.requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "agent.requested:\n")
	writeClosedVariantFile(t, root, "agents.yaml", agentKey+":\n  id: "+agentID+"\n  role: "+agentID+"\n  intent: prompts/"+agentID+".md\n  model: regular\n  memory: false\n  subscriptions: [agent.requested]\n")
	writeClosedVariantFile(t, root, "prompts/"+agentID+".md", "Handle assigned work.\n")
	return root
}

func CopyVerifyMissingPin(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)

	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-missing-pin-warning\nstages:\n  pending: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [task.requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\ntask.completed:\nchild/task.assigned:\nchild/task.result:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `dispatcher:
  execution_type: system_node
  subscribes_to: [task.requested, child/task.result]
  produces: [task.completed, child/task.assigned]
  event_handlers:
    task.requested:
      emit: child/task.assigned
    child/task.result:
      advances_to: done
      emit:
        event: task.completed
`)
	writeLegacyInstanceFlow(t, root, "child", "name: child\nstages:\n  idle: {initial: true}\n  working: {}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [task.assigned, task.feedback]\n", "task.assigned:\ntask.feedback:\n  comment: string\ntask.result:\n", "work_item: {}\n", `worker:
  execution_type: system_node
  subscribes_to: [task.assigned, task.feedback]
  produces: [task.result]
  event_handlers:
    task.assigned:
      advances_to: working
    task.feedback:
      advances_to: done
      emit:
        event: task.result
`)
	return root
}

func CopyRunForkTarget(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "entities.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", "stages:\n  pending: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [task.requested]\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "test-node:\n  execution_type: system_node\n  subscribes_to: [task.requested]\n  produces: []\n  event_handlers:\n    task.requested:\n      advances_to: done\n")
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\n")
	return root
}

func CopyScenarioSetup(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	removeClosedVariantFiles(t, root, "entities.yaml", "events.yaml", "nodes.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", "name: scenario-setup-fixture\nstages:\n  waiting: {initial: true}\n  ready: {terminal: true}\npins:\n  inputs:\n    events: [opco.product_review_requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "opco.product_review_requested:\n  product_id: text\n  note: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "product:\n  product_id:\n    type: text\n    _unused_reason: scenario setup identity\n  note: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `reviewer:
  execution_type: system_node
  subscribes_to: [opco.product_review_requested]
  gate_state:
    gates: [review_ready]
  event_handlers:
    opco.product_review_requested:
      data_accumulation:
        source_event: opco.product_review_requested
        writes:
          - source_field: note
            target_field: note
      clear_gates: [review_ready]
      advances_to: ready
`)
	writeClosedVariantFile(t, root, "tests/setup-target.yaml", `name: setup target and expectation
setup:
  entities:
    - as: product
      type: product
      current_state: waiting
      fields: {product_id: p-1, note: seeded}
      gates: {review_ready: true}
steps:
  - publish: opco.product_review_requested
    payload: {product_id: p-1, note: approved}
expect:
  entities:
    - ref: product
      current_state: ready
      fields: {product_id: p-1, note: approved}
      gates: {review_ready: false}
`)
	return root
}

func CopyScenarioRootSetup(t testing.TB) string {
	t.Helper()
	root := CopyServedTestSetup(t)
	removeInheritedScenarios(t, root)

	writeClosedVariantFile(t, root, "schema.yaml", "name: scenario-root-setup-fixture\nstages:\n  waiting: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [widget.scored]\n")
	writeClosedVariantFile(t, root, "events.yaml", "widget.scored:\n  delta: integer\n")
	writeClosedVariantFile(t, root, "tests/root-setup.yaml", `name: root setup and expectation
setup:
  entities:
    - as: widget
      type: widget
      current_state: waiting
      fields: {score: 5}
steps:
  - publish: widget.scored
    payload: {delta: 7}
expect:
  entities:
    - ref: widget
      current_state: done
      fields: {score: 12}
`)
	return root
}

func removeInheritedScenarios(t testing.TB, root string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(root, "tests")); err != nil {
		t.Fatalf("remove inherited canonical scenarios: %v", err)
	}
}

func CopyInputPinExternalScope(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "events.yaml", "nodes.yaml", "entities.yaml")

	writeClosedVariantFile(t, root, "schema.yaml", "name: input-pin-external-scope\n")
	writeLegacyInstanceFlow(t, root, "external_consumer", "name: external_consumer\nstages:\n  idle: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events:\n      - event: ticket.ready\n        source: external\n", "ticket.ready:\n  entity_id: string\n", "", "")
	writeLegacyInstanceFlow(t, root, "plain_consumer", "name: plain_consumer\nstages:\n  idle: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events:\n      - ticket.ready\n", "ticket.ready:\n  entity_id: string\n", "", "")
	return root
}
