package canonicalrouting

import "testing"

// CopyHandlerRuleSelectionProof owns the complete routing bundle used to prove
// stable handler-rule selection through the durable EventBus surface.
func CopyHandlerRuleSelectionProof(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "entities.yaml")
	writeClosedVariantFile(t, root, "schema.yaml", `name: handler-rule-selection-proof
pins:
  inputs:
    events:
      - {event: rules.selected, source: external}
      - {event: rules.no_match, source: external}
      - {event: rules.evaluation_failed, source: external}
      - {event: complete.selected, source: external}
      - {event: complete.no_match, source: external}
      - {event: direct, source: external}
`)
	writeClosedVariantFile(t, root, "events.yaml", `rules.selected: {swarm: {source: external}}
rules.no_match: {swarm: {source: external}}
rules.evaluation_failed: {swarm: {source: external}}
complete.selected: {swarm: {source: external}}
complete.no_match: {swarm: {source: external}}
direct: {swarm: {source: external}}
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `selection-node:
  id: selection-node
  execution_type: system_node
  subscribes_to: [rules.selected, rules.no_match, rules.evaluation_failed, complete.selected, complete.no_match, direct]
  event_handlers:
    rules.selected:
      rules:
        selected:
          id: rules-label
          condition: else
    rules.no_match:
      rules:
        - id: never-rules
          condition: "false"
    rules.evaluation_failed:
      rules:
        - id: failed-rules
          condition: "payload.proof > 0"
    complete.selected:
      on_complete:
        - id: complete-label
          condition: else
    complete.no_match:
      on_complete:
        - id: never-complete
          condition: "false"
    direct: {}
`)
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
      - event: item.completed
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
        on_complete: {advances_to: ready}
        timeout: {after: ` + timeout + `, advances_to: attention}
`

	switch flowID {
	case "":
		writeClosedVariantFile(t, root, "schema.yaml", joinSchema)
		writeClosedVariantFile(t, root, "entities.yaml", joinEntities)
		writeClosedVariantFile(t, root, "events.yaml", joinEvents)
		writeClosedVariantFile(t, root, "types.yaml", joinTypes)
		writeClosedVariantFile(t, root, "nodes.yaml", joinNodes)
	case "orders":
		writeClosedVariantFile(t, root, "schema.yaml", "name: join-eventbus-proof\n")
		removeClosedVariantFiles(t, root, "entities.yaml", "events.yaml", "nodes.yaml")
		writeLegacyInstanceFlow(t, root, "orders", "mode: template\ninstance: order_id\n"+joinSchema,
			joinEvents, joinEntities, joinNodes)
		writeClosedVariantFile(t, root, "orders/types.yaml", joinTypes)
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

	writeClosedVariantFile(t, root, "schema.yaml", `name: timer-proof
stages:
  waiting:
    initial: true
  done:
    terminal: true
pins:
  inputs:
    events:
      - event: timer.cancel
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
	return root
}

// CopyTemplateOutputRootConnect owns the concrete template-output-to-root
// route used by the emit-tool supported-surface proof.
func CopyTemplateOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)

	writeClosedVariantFile(t, root, "schema.yaml", `name: root
pins:
  inputs:
    events:
      - deploy.done
connect:
  - event: deploy.done
    from: producer
    to: .
`)
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
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
mode: template
instance: producer_id
pins:
  outputs:
    events:
      - deploy.done
`, "deploy.done: {}\n", "producer_state:\n  producer_id: string\n", "")
	return root
}
