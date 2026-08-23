package canonicalrouting

import "testing"

// CopyHandlerRuleSelectionProof owns the complete routing bundle used to prove
// stable handler-rule selection through the durable EventBus surface.
func CopyHandlerRuleSelectionProof(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", `name: handler-rule-selection-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: handler-rule-selection-proof
pins:
  inputs:
    events:
      - {name: rules_selected, event: rules.selected, source: external}
      - {name: rules_no_match, event: rules.no_match, source: external}
      - {name: complete_selected, event: complete.selected, source: external}
      - {name: complete_no_match, event: complete.no_match, source: external}
      - {name: direct, event: direct, source: external}
  outputs:
    events: []
`)
	writeClosedVariantFile(t, root, "events.yaml", `rules.selected: {swarm: {source: external}}
rules.no_match: {swarm: {source: external}}
complete.selected: {swarm: {source: external}}
complete.no_match: {swarm: {source: external}}
direct: {swarm: {source: external}}
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `selection-node:
  id: selection-node
  execution_type: system_node
  subscribes_to: [rules.selected, rules.no_match, complete.selected, complete.no_match, direct]
  event_handlers:
    rules.selected:
      rules:
        selected:
          element_id: 00000000-0000-4000-8000-000000000421
          id: rules-label
          condition: else
    rules.no_match:
      rules:
        - element_id: 00000000-0000-4000-8000-000000000422
          id: never-rules
          condition: "false"
    complete.selected:
      on_complete:
        - element_id: 00000000-0000-4000-8000-000000000423
          id: complete-label
          condition: else
    complete.no_match:
      on_complete:
        - element_id: 00000000-0000-4000-8000-000000000424
          id: never-complete
          condition: "false"
    direct: {}
`)
	for _, file := range []string{"entities.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "types.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

// CopyExactJoinEventBusProof owns the route-bearing contract for the durable
// root and template-flow join proofs.
func CopyExactJoinEventBusProof(t testing.TB, flowID string) string {
	return CopyExactJoinEventBusProofWithTimeout(t, flowID, "1h")
}

func CopyExactJoinEventBusProofWithTimeout(t testing.TB, flowID, timeout string) string {
	t.Helper()
	root := CopyExample(t, RootIngress)

	joinSchema := `name: join-eventbus-proof
stages:
  awaiting:
    initial: true
  ready:
    terminal: true
  attention:
    terminal: true
pins:
  inputs:
    events:
      - name: item_completed
        event: item.completed
        source: external
`
	joinEntities := `join_state:
  expected:
    type: "[text]"
    initial: []
`
	joinEvents := `item.completed:
  swarm:
    source: external
  member_id: text
  result: JoinResult
`
	joinTypes := `types:
  JoinResult:
    value: text
`
	joinNodes := `join-node:
  id: join-node
  execution_type: system_node
  subscribes_to: [item.completed]
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: entity.expected, by: payload.member_id}
        output: payload.result
        on_complete: {element_id: 00000000-0000-4000-8000-000000000013, advances_to: ready}
        timeout: {element_id: 00000000-0000-4000-8000-000000000014, after: ` + timeout + `, advances_to: attention}
`

	switch flowID {
	case "":
		writeClosedVariantFile(t, root, "package.yaml", `name: join-eventbus-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
		writeClosedVariantFile(t, root, "schema.yaml", joinSchema)
		writeClosedVariantFile(t, root, "entities.yaml", joinEntities)
		writeClosedVariantFile(t, root, "events.yaml", joinEvents)
		writeClosedVariantFile(t, root, "types.yaml", joinTypes)
		writeClosedVariantFile(t, root, "nodes.yaml", joinNodes)
		for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
			writeClosedVariantFile(t, root, file, "{}\n")
		}
	case "orders":
		writeClosedVariantFile(t, root, "package.yaml", `name: join-eventbus-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: orders
    flow: orders
    mode: template
`)
		writeClosedVariantFile(t, root, "schema.yaml", "name: join-eventbus-proof\n")
		for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "entities.yaml", "events.yaml", "nodes.yaml", "types.yaml"} {
			writeClosedVariantFile(t, root, file, "{}\n")
		}
		writeLegacyInstanceFlow(t, root, "orders", "mode: template\ninstance: order_id\n"+joinSchema,
			joinEvents, joinEntities, joinNodes)
		writeClosedVariantFile(t, root, "flows/orders/types.yaml", joinTypes)
	default:
		t.Fatalf("unsupported exact join flow %q", flowID)
	}
	return root
}

// CopyRecurringTimerCancellation owns the root controller route used by the
// recurring timer cancellation proof. Timer scheduling remains test-owned.
func CopyRecurringTimerCancellation(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	writeClosedVariantFile(t, root, "package.yaml", `name: timer-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: timer-proof
stages:
  waiting:
    initial: true
  done:
    terminal: true
pins:
  inputs:
    events:
      - name: timer_cancel
        event: timer.cancel
        source: external
`)
	writeClosedVariantFile(t, root, "events.yaml", `timer.cancel:
  swarm:
    source: external
`)
	writeClosedVariantFile(t, root, "entities.yaml", "timer_state: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [timer.cancel]
  event_handlers:
    timer.cancel:
      advances_to: done
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	return root
}

// CopyTemplateOutputRootConnect owns the concrete template-output-to-root
// route used by the emit-tool supported-surface proof.
func CopyTemplateOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "package.yaml", `name: root
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: template
connect:
  - from: producer.deploy_done
    to: .deploy_completed
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: root
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeClosedVariantFile(t, root, "events.yaml", "deploy.done: {}\n")
	writeClosedVariantFile(t, root, "entities.yaml", "root_state: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `root-receiver:
  id: root-receiver
  execution_type: system_node
  subscribes_to: [deploy.done]
  event_handlers:
    deploy.done:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
mode: template
instance: producer_id
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`, "deploy.done: {}\n", "producer_state:\n  producer_id: string\n", "{}\n")
	return root
}
