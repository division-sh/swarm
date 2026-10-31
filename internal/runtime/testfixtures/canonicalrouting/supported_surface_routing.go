package canonicalrouting

import "testing"

// CopyHandlerRuleSelectionProof owns the complete routing bundle used to prove
// stable handler-rule selection through the durable EventBus surface.
func CopyHandlerRuleSelectionProof(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeClosedVariantFiles(t, root, "state.yaml")
	writeClosedVariantFile(t, root, "schema.yaml", `# fixture: handler-rule-selection-proof
pins:
  inputs:
    - rules.selected
    - rules.fallback
    - rules.evaluation_failed
    - complete.selected
    - complete.no_match
    - direct
`)
	writeClosedVariantFile(t, root, "events.yaml", `rules.selected:
rules.fallback:
rules.evaluation_failed:
complete.selected:
complete.no_match:
direct:
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `selection-node:
  execution_type: system_node
  subscribes_to: [rules.selected, rules.fallback, rules.evaluation_failed, complete.selected, complete.no_match, direct]
  event_handlers:
    rules.selected:
      rules:
        - when: true
    rules.fallback:
      rules:
        - when: |-
                  false
        - when: true
    rules.evaluation_failed:
      rules:
        - when: payload.proof > 0
        - when: true
    complete.selected:
      on_complete:
        - condition: true
    complete.no_match:
      on_complete:
        - condition: |-
                       false
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

	joinSchema := `# fixture: join-eventbus-proof
stages:
  awaiting: {}
  ready:
    final: true
  attention:
    final: true
pins:
  inputs:
    - item.completed
`
	joinEntities := `expected: [text]
`
	joinEvents := `item.completed:
  member_id: text
  result: JoinResult
`
	joinTypes := `types:
  JoinResult:
    value: text
`
	joinNodes := `join-node:
  execution_type: system_node
  subscribes_to: [item.completed]
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: ready}
        deadline: {after: ` + timeout + `, from: stage_entry}
        on_deadline: {advances_to: attention}
`

	switch flowID {
	case "":
		writeClosedVariantFile(t, root, "schema.yaml", joinSchema)
		writeClosedVariantFile(t, root, "state.yaml", joinEntities)
		writeClosedVariantFile(t, root, "events.yaml", joinEvents)
		writeClosedVariantFile(t, root, "types.yaml", joinTypes)
		writeClosedVariantFile(t, root, "nodes.yaml", joinNodes)
	case "orders":
		writeClosedVariantFile(t, root, "schema.yaml", "stages: []\n# fixture: join-eventbus-proof\n")
		removeClosedVariantFiles(t, root, "state.yaml", "events.yaml", "nodes.yaml")
		writeLegacyInstanceFlow(t, root, "orders", "instance: order_id\n"+joinSchema,
			joinEvents, joinEntities+"order_id: text\n", joinNodes)
		writeClosedVariantFile(t, root, "orders/types.yaml", joinTypes)
	default:
		t.Fatalf("unsupported exact join flow %q", flowID)
	}
	return root
}

// CopyTemplateOutputRootConnect owns the concrete template-output-to-root
// route used by the emit-tool supported-surface proof.
func CopyTemplateOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	// This variant replaces the example's child consumer with the root receiver.
	removeClosedVariantFiles(t, root, "consumer/nodes.yaml", "consumer/schema.yaml", "consumer/manifest.yaml", "consumer")

	writeClosedVariantFile(t, root, "schema.yaml", `# fixture: root
pins:
  inputs:
    - deploy.done
connect:
  - event: deploy.done
    from: producer
    to: .
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `root-receiver:
  execution_type: system_node
  subscribes_to: [deploy.done]
  event_handlers:
    deploy.done:
      guard:
        id: selected_owner
        check: instance.id != ""
`)
	removeClosedVariantFiles(t, root, "producer/nodes.yaml", "producer/agents.yaml")
	writeClosedVariantFile(t, root, "producer/schema.yaml", `# fixture: producer
instance: producer_id
pins:
  outputs:
    - deploy.done
`)
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.done:\n")
	writeClosedVariantFile(t, root, "producer/state.yaml", "producer_id: string\n")
	return root
}
