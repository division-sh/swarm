package canonicalrouting

import "testing"

type SelectEntityAcquisition uint8

const (
	SelectEntityNoAcquisition SelectEntityAcquisition = iota
	SelectEntityAcquire
	SelectOrCreateEntityAcquire
)

type SelectEntityDemotionOptions struct {
	TemplateReceiver       bool
	Acquisition            SelectEntityAcquisition
	External               bool
	WithProducer           bool
	ConnectProducerToOther bool
	RenameReceiverPin      bool
}

func CopySelectEntityDemotion(t testing.TB, opts SelectEntityDemotionOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	consumerMode := "static"
	if opts.TemplateReceiver {
		consumerMode = "template"
	}
	flows := "\n  - {id: consumer, flow: consumer, mode: " + consumerMode + "}"
	if opts.WithProducer {
		flows = "\n  - {id: producer, flow: producer}" + flows
		if opts.ConnectProducerToOther {
			flows += "\n  - {id: other_consumer, flow: other_consumer, mode: static}"
		}
	}
	connect := ""
	if opts.WithProducer {
		targetFlow := "consumer"
		targetPin := "deploy_done"
		if opts.ConnectProducerToOther {
			targetFlow = "other_consumer"
		}
		if opts.RenameReceiverPin && targetFlow == "consumer" {
			targetPin = "deploy_completed"
		}
		connect = "\nconnect:\n  - from: producer.deploy_done\n    to: " + targetFlow + "." + targetPin
		if !opts.TemplateReceiver {
			connect += "\n    map:\n      vertical_id:\n        source: payload.vertical_id\n        target: entity.vertical_id"
		}
	}
	writeClosedVariantFile(t, root, "package.yaml", "name: select-entity-demotion\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:"+flows+connect+"\n")
	for _, name := range []string{"schema.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "entities.yaml"} {
		body := "{}\n"
		if name == "schema.yaml" {
			body = "name: select-entity-demotion\n"
		}
		writeClosedVariantFile(t, root, name, body)
	}
	if opts.WithProducer {
		writeSelectEntityDemotionProducer(t, root)
		if opts.ConnectProducerToOther {
			writeSelectEntityDemotionOther(t, root)
		}
	}
	writeSelectEntityDemotionConsumer(t, root, opts)
	return root
}

func writeSelectEntityDemotionProducer(t testing.TB, root string) {
	writeClosedVariantFile(t, root, "flows/producer/schema.yaml", `name: producer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - {name: deploy_requested, event: deploy.requested, source: external}
  outputs:
    events:
      - {name: deploy_done, event: deploy.done, key: vertical_id, carries: [vertical_id]}
`)
	writeClosedVariantFile(t, root, "flows/producer/entities.yaml", "producer_request:\n  vertical_id:\n    type: string\n    _unused_reason: select_entity demotion producer proof field\n")
	writeClosedVariantFile(t, root, "flows/producer/events.yaml", "deploy.requested:\n  vertical_id: string\ndeploy.done:\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "flows/producer/nodes.yaml", `producer-node:
  id: producer-node
  execution_type: system_node
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields: {vertical_id: payload.vertical_id}
      advances_to: done
`)
	for _, name := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/producer/"+name, "{}\n")
	}
}

func writeSelectEntityDemotionOther(t testing.TB, root string) {
	writeClosedVariantFile(t, root, "flows/other_consumer/schema.yaml", `name: other-consumer
mode: static
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - name: deploy_done
        event: deploy.done
        address: {by: vertical_id, source: payload.vertical_id, target: entity.vertical_id, cardinality: one}
  outputs: {events: []}
`)
	writeClosedVariantFile(t, root, "flows/other_consumer/events.yaml", "deploy.done:\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "flows/other_consumer/entities.yaml", "deployment:\n  vertical_id:\n    type: string\n    indexed: true\n    _unused_reason: select_entity demotion other receiver route-key proof field\n")
	writeClosedVariantFile(t, root, "flows/other_consumer/nodes.yaml", "other-consumer-node:\n  id: other-consumer-node\n  execution_type: system_node\n  subscribes_to: [deploy.done]\n  event_handlers:\n    deploy.done: {advances_to: done}\n")
	for _, name := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/other_consumer/"+name, "{}\n")
	}
}

func writeSelectEntityDemotionConsumer(t testing.TB, root string, opts SelectEntityDemotionOptions) {
	mode := "static"
	instance := ""
	if opts.TemplateReceiver {
		mode = "template"
		instance = "instance:\n  by: vertical_id\n  on_missing: reject\n  on_conflict: reject\n"
	}
	pinName := "deploy_done"
	eventName := "deploy.done"
	if opts.RenameReceiverPin {
		pinName = "deploy_completed"
		eventName = "deploy.completed"
	}
	pin := "      - name: " + pinName + "\n        event: " + eventName + "\n"
	if !opts.TemplateReceiver {
		pin += "        address: {by: vertical_id, source: payload.vertical_id, target: entity.vertical_id, cardinality: one}\n"
	}
	if opts.External {
		pin += "        source: external\n"
	}
	writeClosedVariantFile(t, root, "flows/consumer/schema.yaml", "name: consumer\nmode: "+mode+"\n"+instance+"initial_state: pending\nstates: [pending, done]\nterminal_states: [done]\npins:\n  inputs:\n    events:\n"+pin+"  outputs: {events: []}\n")
	writeClosedVariantFile(t, root, "flows/consumer/events.yaml", eventName+":\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "flows/consumer/entities.yaml", "deployment:\n  vertical_id:\n    type: string\n    indexed: true\n    _unused_reason: select_entity demotion route-key proof field\n")
	acquisition := ""
	switch opts.Acquisition {
	case SelectEntityNoAcquisition:
	case SelectEntityAcquire:
		acquisition = "      select_entity:\n        by:\n          vertical_id: payload.vertical_id\n"
	case SelectOrCreateEntityAcquire:
		acquisition = "      select_or_create_entity:\n        by:\n          vertical_id: payload.vertical_id\n"
	default:
		t.Fatalf("unsupported select-entity acquisition %d", opts.Acquisition)
	}
	writeClosedVariantFile(t, root, "flows/consumer/nodes.yaml", "consumer-node:\n  id: consumer-node\n  execution_type: system_node\n  subscribes_to: ["+eventName+"]\n  event_handlers:\n    "+eventName+":\n"+acquisition+"      advances_to: done\n")
	for _, name := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/consumer/"+name, "{}\n")
	}
}
