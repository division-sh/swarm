package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyParentConnectRequiredAgent derives the one closed agent-authority
// specialization from the checked-in parent-connect route. The agent's
// subscription and output declarations are fixed here rather than accepted as
// route-bearing overlay YAML.
func CopyParentConnectRequiredAgent(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "schema.yaml"), "pins:\n", `required_agents:
  - role: analyzer
    subscribes_to: [work.requested]
    emits: [producer/work.ready]
    description: Analyzes work before delivery
pins:
`)
	writeClosedVariantFile(t, root, "flows/producer/agents.yaml", `analyzer:
  model: regular
  subscriptions: [work.requested]
  emit_events: [producer/work.ready]
`)
	return root
}

// CopyParentConnectEventMetadataAuthority derives the fixed routed event
// surfaces used to prove that event metadata never owns routing.
func CopyParentConnectEventMetadataAuthority(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	producerSchema := filepath.Join(root, "flows", "producer", "schema.yaml")
	consumerSchema := filepath.Join(root, "flows", "consumer", "schema.yaml")
	applyClosedReplacement(t, producerSchema, "pins:\n", "auto_emit_on_create:\n  event: flow.started\npins:\n")
	applyClosedReplacement(t, producerSchema,
		"      - name: work_ready\n        event: work.ready\n",
		"      - name: work_ready\n        event: work.ready\n      - name: deploy_done\n        event: deploy.done\n")
	applyClosedReplacement(t, consumerSchema,
		"      - name: work_ready\n        event: work.ready\n",
		"      - name: work_ready\n        event: work.ready\n      - name: deploy_completed\n        event: deploy.completed\n        source: external\n")
	return root
}

type ParentConnectEventMetadataInvalidity uint8

const (
	ParentConnectMetadataProducerFlowAutoEmit ParentConnectEventMetadataInvalidity = iota + 1
	ParentConnectMetadataProducerFlowOutput
	ParentConnectMetadataConsumerFlowInput
	ParentConnectMetadataProducerConnectOutput
	ParentConnectMetadataConsumerConnectInput
	ParentConnectMetadataProducerWrongFlowEvent
	ParentConnectMetadataConsumerWrongFlowEvent
	ParentConnectMetadataProducerWrongConnectEvent
	ParentConnectMetadataConsumerWrongConnectEvent
)

// CopyParentConnectEventMetadataInvalidity derives the closed fail-closed
// matrix for event metadata that attempts to restate flow/connect authority.
func CopyParentConnectEventMetadataInvalidity(t testing.TB, invalidity ParentConnectEventMetadataInvalidity) string {
	t.Helper()
	root := CopyParentConnectEventMetadataAuthority(t)
	flowStartedRole := ""
	producerWorkReadyRole := ""
	consumerWorkReadyRole := ""
	switch invalidity {
	case ParentConnectMetadataProducerFlowAutoEmit:
		flowStartedRole = "producer: producer"
	case ParentConnectMetadataProducerFlowOutput:
		producerWorkReadyRole = "producer: deploy_done"
	case ParentConnectMetadataConsumerFlowInput:
		consumerWorkReadyRole = "consumer: consumer"
	case ParentConnectMetadataProducerConnectOutput:
		producerWorkReadyRole = "producer: producer.work_ready"
	case ParentConnectMetadataConsumerConnectInput:
		consumerWorkReadyRole = "consumer: consumer.work_ready"
	case ParentConnectMetadataProducerWrongFlowEvent:
		flowStartedRole = "producer: deploy_done"
	case ParentConnectMetadataConsumerWrongFlowEvent:
		producerWorkReadyRole = "consumer: deploy_completed"
	case ParentConnectMetadataProducerWrongConnectEvent:
		flowStartedRole = "producer: producer.work_ready"
	case ParentConnectMetadataConsumerWrongConnectEvent:
		producerWorkReadyRole = "consumer: consumer.work_ready"
	default:
		t.Fatalf("unsupported parent-connect event metadata invalidity %d", invalidity)
	}
	writeClosedVariantFile(t, root, "flows/producer/events.yaml",
		closedMetadataEvent("flow.started", flowStartedRole, false)+
			closedMetadataEvent("work.requested", "", true)+
			closedMetadataEvent("work.ready", producerWorkReadyRole, true)+
			closedMetadataEvent("deploy.done", "", true))
	writeClosedVariantFile(t, root, "flows/consumer/events.yaml",
		closedMetadataEvent("work.ready", consumerWorkReadyRole, true)+
			closedMetadataEvent("deploy.completed", "", true))
	return root
}

func closedMetadataEvent(name, role string, workID bool) string {
	entry := name + ":\n"
	if role != "" {
		entry += "  swarm:\n    " + role + "\n"
	}
	if workID {
		entry += "  work_id: text\n"
	}
	return entry
}
