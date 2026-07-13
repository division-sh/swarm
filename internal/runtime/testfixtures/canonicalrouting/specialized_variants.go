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
	writeClosedVariantFile(t, root, "package.yaml", `name: output-mode-verify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeClosedVariantFile(t, root, "schema.yaml", "name: output-mode-verify\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "nodes.yaml", "events.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "child", `name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:
      - name: task.assigned
        source: external
    reads: [priority]
  outputs:
    events: []
`, `task.assigned:
  swarm:
    source: external (output mode verify test)
`, `case:
  priority:
    type: integer
    _unused_reason: output-mode child primary entity proof field
`, `reader:
  id: reader
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      guard:
        check: "entity.priority >= 0"
      advances_to: done
`)
	return root
}

// CopyDescribeStageGraph owns the route-bearing shell around the CLI stage
// graph fixture; stage/timer/join content remains its distinct test concept.
func CopyDescribeStageGraph(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", `name: stage-graph
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
`)
	writeClosedVariantFile(t, root, "schema.yaml", "name: stage-graph\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "support", `name: support
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
`, `ticket.opened:
  swarm:
    source: external
  entity_id: string
  line_items: "[text]"
ticket.closed:
  swarm:
    source: external
  entity_id: string
  line_item_id: string
  result: string
ticket.sla_escalated:
  swarm:
    consumer: [operator]
  entity_id: string
line_item.requested:
  swarm:
    consumer: [worker]
  line_item_id: string
  line_item_index: integer
accumulate.timeout:
  swarm:
    source: platform
`, `ticket:
  expected_line_item_ids:
    type: "[text]"
    initial: []
`, `support-node:
  id: support-node
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
            line_item_id: line_item
            line_item_index: fan_out.index
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
`)
	return root
}

func CopyInboundAdmissionPolicyMatrix(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	files := map[string]string{
		"package.yaml": `name: inbound-admission-policy-matrix
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: matrix
    flow: matrix
    mode: singleton
    activation: standing
    ingress:
      alias: matrix
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
          admission:
            pack: {id: provider.telegram}
        - provider: intercom
          admission:
            pack: {id: provider.intercom}
            acknowledge: unsigned_webhook
        - provider: acme_public
          admission:
            pack: {id: provider.acme_public}
            acknowledge: unsigned_webhook
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
`,
		"schema.yaml": "name: inbound-admission-policy-matrix\n",
		"policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n", "events.yaml": "{}\n", "nodes.yaml": "{}\n",
		"flows/matrix/schema.yaml": `name: matrix
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - {name: telegram, event: inbound.telegram, source: external}
      - {name: intercom, event: inbound.intercom, source: external}
      - {name: acme_public, event: inbound.acme_public, source: external}
      - {name: partner_auth, event: inbound.partner_auth, source: external}
      - {name: partner_open, event: inbound.partner_open, source: external}
      - {name: partner_ack, event: inbound.partner_ack, source: external}
  outputs: {events: []}
`,
		"flows/matrix/entities.yaml": "matrix_service:\n  service_id:\n    type: text\n    initial: standing\n  records:\n    type: map[text]json\n    initial: {}\n",
		"flows/matrix/types.yaml":    "{}\n", "flows/matrix/policy.yaml": "{}\n", "flows/matrix/tools.yaml": "{}\n", "flows/matrix/agents.yaml": "{}\n",
		"flows/matrix/events.yaml": inboundAdmissionEvents(),
		"flows/matrix/nodes.yaml":  inboundAdmissionNodes(),
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

func inboundAdmissionEvents() string {
	var out strings.Builder
	for _, event := range []string{"inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"} {
		fmt.Fprintf(&out, "%s:\n  entity_id: text\n  provider: text\n  event_type: text\n  provider_event_type: text\n  provider_event_id: text\n  provider_delivery_id: text\n  headers: json\n  received_at: text\n", event)
	}
	return out.String()
}

func inboundAdmissionNodes() string {
	events := []string{"inbound.telegram", "inbound.intercom", "inbound.acme_public", "inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"}
	var out strings.Builder
	out.WriteString("matrix-sink:\n  id: matrix-sink\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(events, ", ") + "]\n  event_handlers:\n")
	for _, event := range events {
		fmt.Fprintf(&out, `    %s:
      data_accumulation:
        writes:
          - op: set
            target: entity.records
            key: {ref: payload.provider_event_id}
            value: {ref: payload.provider_event_id}
`, event)
	}
	return out.String()
}

func CopyServedTestSetup(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", "name: served-test-setup\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\n")
	writeClosedVariantFile(t, root, "schema.yaml", `name: served-test-setup
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events:
      - name: widget_started
        event: widget.started
        source: external
      - name: widget_scored
        event: widget.scored
        source: external
`)
	writeClosedVariantFile(t, root, "entities.yaml", "widget:\n  score: integer\n")
	writeClosedVariantFile(t, root, "events.yaml", `widget.scored:
  swarm:
    source: external
  delta: integer
widget.started:
  swarm:
    source: external
  seed: boolean
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `scorer:
  id: scorer
  execution_type: system_node
  subscribes_to: [widget.scored]
  event_handlers:
    widget.scored:
      data_accumulation:
        source_event: widget.scored
        writes:
          - target_field: score
            expression: entity.score + payload.delta
      advances_to: done
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

func CopyVerifyLintEvidence(t testing.TB, missingEmitSchema bool) string {
	t.Helper()
	root := CopyOutputModeVerify(t)
	writeClosedVariantFile(t, root, "package.yaml", `name: verify-lint-evidence
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-lint-evidence\n")
	writeClosedVariantFile(t, root, "entities.yaml", `case:
  untouched:
    type: integer
    _unused_reason: verify command lint evidence proof field
  priority:
    type: integer
    _unused_reason: child read-pin coverage proof field
`)
	writeClosedVariantFile(t, root, "flows/child/events.yaml", `task.assigned:
  swarm:
    source: external (verify lint evidence test)
`)
	writeClosedVariantFile(t, root, "flows/child/entities.yaml", `case:
  priority:
    type: integer
    _unused_reason: verify lint evidence child primary entity proof field
`)
	if missingEmitSchema {
		writeClosedVariantFile(t, root, "agents.yaml", `strict-schema-agent:
  id: strict-schema-agent
  role: strict_schema_agent
  prompt_ref: strict-schema-agent
  model: regular
  mode: task
  subscriptions: [task.assigned]
  emit_events: [missing.event]
`)
		writeClosedVariantFile(t, root, "prompts/strict-schema-agent.md", "Emit the missing event when requested.\n")
	}
	return root
}

func CopyFirstFlowTutorial(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", "name: ticket-flow\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: ticket-flow\ninitial_state: open\nterminal_states: [resolved]\nstates: [open, assigned, resolved]\n")
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
  swarm:
    source: external (first-flow verify proof)
  category: text
  priority: text
ticket.assigned:
  category: text
  priority: text
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `classifier:
  id: classifier
  execution_type: system_node
  subscribes_to: [ticket.classified]
  produces: [ticket.assigned]
  event_handlers:
    ticket.classified:
      guard:
        check: "entity.category != '' && entity.priority != ''"
      emit:
        event: ticket.assigned
        broadcast: true
        fields:
          category: entity.category
          priority: entity.priority
      advances_to: assigned
assignee:
  id: assignee
  execution_type: system_node
  subscribes_to: [ticket.assigned]
  event_handlers:
    ticket.assigned:
      guard:
        check: "entity.category != ''"
      advances_to: resolved
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

func CopyAgentSlugAdmission(t testing.TB, workflowName, agentKey, agentID string) string {
	t.Helper()
	workflowName = closedScalarLiteral(t, "workflow name", workflowName, "agent-slug-admission")
	agentKey = closedScalarLiteral(t, "agent key", agentKey, "worker")
	agentID = closedScalarLiteral(t, "agent ID", agentID, "worker")
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", "name: "+workflowName+"\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "initial_state: pending\nterminal_states: [done]\nstates: [pending, done]\npins:\n  inputs:\n    events: [agent.requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "agent.requested:\n  swarm:\n    source: external\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "{}\n")
	writeClosedVariantFile(t, root, "agents.yaml", agentKey+":\n  id: "+agentID+"\n  role: "+agentID+"\n  prompt_ref: "+agentID+"\n  model: regular\n  mode: task\n  subscriptions: [agent.requested]\n")
	writeClosedVariantFile(t, root, "prompts/"+agentID+".md", "Handle assigned work.\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

func CopyArtifactRepoCommitAdmission(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", "name: artifact-root-startup\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "initial_state: ready\nterminal_states: [done]\nstates: [ready, done]\npins:\n  inputs:\n    events: [artifact.requested]\n")
	writeClosedVariantFile(t, root, "entities.yaml", `core:
  repo_url: {type: text, _unused_reason: artifact startup admission proof output field}
  current_ref: {type: text, _unused_reason: artifact startup admission proof output field}
  file_manifest: {type: text, _unused_reason: artifact startup admission proof output field}
  status: {type: text, _unused_reason: artifact startup admission proof output field}
  failure: {type: text, _unused_reason: artifact startup admission proof output field}
  last_request_id: {type: text, _unused_reason: artifact startup admission proof output field}
  last_source_event_id: {type: text, _unused_reason: artifact startup admission proof output field}
`)
	writeClosedVariantFile(t, root, "events.yaml", "artifact.requested:\n  swarm:\n    source: external\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `artifact-writer:
  id: artifact-writer
  execution_type: system_node
  subscribes_to: [artifact.requested]
  event_handlers:
    artifact.requested:
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id: {literal: "11111111-1111-1111-1111-111111111111"}
          namespace: {literal: local-proof}
          request_id: {literal: "22222222-2222-2222-2222-222222222222"}
          allowed_paths: [readme.md]
          files:
            - path: {literal: readme.md}
              content: {literal: "# Demo\n"}
              content_type: markdown
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure: failure
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

func CopyVerifyMissingPin(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "package.yaml", `name: verify-missing-pin-warning
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-missing-pin-warning\ninitial_state: pending\nterminal_states: [done]\nstates: [pending, done]\npins:\n  inputs:\n    events: [task.requested]\n  outputs:\n    events: [task.completed]\n")
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\n  swarm:\n    source: external\ntask.completed: {}\nchild/task.assigned: {}\nchild/task.result: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `dispatcher:
  id: dispatcher
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
        broadcast: true
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "child", "name: child\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, working, done]\npins:\n  inputs:\n    events: [task.assigned, task.feedback]\n  outputs:\n    events: [task.result]\n", "task.assigned: {}\ntask.feedback:\n  comment: string\ntask.result: {}\n", "work_item: {}\n", `worker:
  id: worker
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
        broadcast: true
`)
	return root
}

func CopyRunForkTarget(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", "name: cross-bundle-target\nversion: 1.0.0\ndescription: Cross-bundle target fixture for run.fork.\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "initial_state: pending\nterminal_states: [done]\nstates: [pending, done]\npins:\n  inputs:\n    events: [task.requested]\n  outputs:\n    events: [task.completed]\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "test-node:\n  id: test-node\n  execution_type: system_node\n  subscribes_to: [task.requested]\n  produces: [task.completed]\n  event_handlers:\n    task.requested:\n      advances_to: done\n      emit:\n        event: task.completed\n        broadcast: true\n")
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\n  swarm:\n    source: external\ntask.completed:\n  swarm:\n    source: external\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

func CopyScenarioSetup(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: scenario-setup-fixture\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: operating\n    flow: operating\n    mode: static\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: scenario-setup-fixture\ninitial_state: new\nterminal_states: [done]\nstates: [new, done]\n")
	for _, file := range []string{"entities.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "operating", "name: operating\nmode: static\ninitial_state: initializing\nterminal_states: [ready]\nstates: [initializing, waiting, ready]\npins:\n  inputs:\n    events: [opco.product_review_requested]\n", "opco.product_review_requested:\n  swarm:\n    source: external\n  note: text\n", "product:\n  product_id: text\n  note: text\n", `reviewer:
  id: reviewer
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
	writeClosedVariantFile(t, root, "flows/operating/tests/setup-target.yaml", `name: setup target and expectation
setup:
  entities:
    - as: product
      type: product
      current_state: waiting
      fields: {product_id: p-1, note: seeded}
      gates: {review_ready: true}
steps:
  - publish: opco.product_review_requested
    target: product
    payload: {note: approved}
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
	writeClosedVariantFile(t, root, "package.yaml", "name: scenario-root-setup-fixture\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: scenario-root-setup-fixture\ninitial_state: waiting\nterminal_states: [done]\nstates: [waiting, done]\npins:\n  inputs:\n    events: [widget.scored]\n")
	writeClosedVariantFile(t, root, "events.yaml", "widget.scored:\n  swarm:\n    source: external\n  delta: integer\n")
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
	writeClosedVariantFile(t, root, "package.yaml", "name: input-pin-external-scope\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: external_consumer\n    flow: external_consumer\n    mode: static\n  - id: plain_consumer\n    flow: plain_consumer\n    mode: static\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: input-pin-external-scope\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "external_consumer", "name: external_consumer\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events:\n      - name: ticket.ready\n        source: external\n  outputs:\n    events: []\n", "ticket.ready:\n  entity_id: string\n", "{}\n", "{}\n")
	writeLegacyInstanceFlow(t, root, "plain_consumer", "name: plain_consumer\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events:\n      - ticket.ready\n  outputs:\n    events: []\n", "ticket.ready:\n  entity_id: string\n", "{}\n", "{}\n")
	return root
}
