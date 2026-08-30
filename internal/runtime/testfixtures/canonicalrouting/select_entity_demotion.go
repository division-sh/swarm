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
	connect := ""
	if opts.WithProducer {
		targetFlow := "consumer"
		if opts.ConnectProducerToOther {
			targetFlow = "other_consumer"
		}
		rename := ""
		if opts.RenameReceiverPin && targetFlow == "consumer" {
			rename = "\n    rename: deploy.completed"
		}
		connect = "\nconnect:\n  - event: deploy.done\n    from: producer\n    to: " + targetFlow + rename
	}

	writeClosedVariantFile(t, root, "schema.yaml", "name: select-entity-demotion\n"+connect+"\n")
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
	writeClosedVariantFile(t, root, "producer/schema.yaml", `name: producer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - {event: deploy.requested, source: external}
  outputs:
    events:
      - deploy.done
`)
	writeClosedVariantFile(t, root, "producer/entities.yaml", "producer_request:\n  vertical_id:\n    type: string\n    _unused_reason: select_entity demotion producer proof field\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.requested:\n  vertical_id: string\ndeploy.done:\n  key: vertical_id\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "producer/nodes.yaml", `producer-node:
  id: producer-node
  execution_type: system_node
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields: {vertical_id: payload.vertical_id}
      advances_to: done
`)
}

func writeSelectEntityDemotionOther(t testing.TB, root string) {
	writeClosedVariantFile(t, root, "other_consumer/schema.yaml", `name: other-consumer
mode: static
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - deploy.done
`)
	writeClosedVariantFile(t, root, "other_consumer/events.yaml", "deploy.done:\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "other_consumer/entities.yaml", "deployment:\n  vertical_id:\n    type: string\n    indexed: true\n    _unused_reason: select_entity demotion other receiver route-key proof field\n")
	writeClosedVariantFile(t, root, "other_consumer/nodes.yaml", "other-consumer-node:\n  id: other-consumer-node\n  execution_type: system_node\n  subscribes_to: [deploy.done]\n  event_handlers:\n    deploy.done: {advances_to: done}\n")
}

func writeSelectEntityDemotionConsumer(t testing.TB, root string, opts SelectEntityDemotionOptions) {
	mode := "static"
	instance := ""
	if opts.TemplateReceiver {
		mode = "template"
		instance = "instance: vertical_id\n"
	}
	eventName := "deploy.done"
	if opts.RenameReceiverPin {
		eventName = "deploy.completed"
	}
	pin := "      - " + eventName + "\n"
	if opts.TemplateReceiver || opts.External {
		pin = "      - event: " + eventName + "\n"
	}
	if opts.TemplateReceiver {
		pin += "        resolution:\n          mode: select\n"
	}
	if opts.External {
		pin += "        source: external\n"
	}
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "name: consumer\nmode: "+mode+"\n"+instance+"initial_state: pending\nstates: [pending, done]\nterminal_states: [done]\npins:\n  inputs:\n    events:\n"+pin)
	writeClosedVariantFile(t, root, "consumer/events.yaml", eventName+":\n  vertical_id: string\n")
	writeClosedVariantFile(t, root, "consumer/entities.yaml", "deployment:\n  vertical_id:\n    type: string\n    indexed: true\n    _unused_reason: select_entity demotion route-key proof field\n")
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
	writeClosedVariantFile(t, root, "consumer/nodes.yaml", "consumer-node:\n  id: consumer-node\n  execution_type: system_node\n  subscribes_to: ["+eventName+"]\n  event_handlers:\n    "+eventName+":\n"+acquisition+"      advances_to: done\n")
}
