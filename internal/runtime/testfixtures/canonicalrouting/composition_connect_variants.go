package canonicalrouting

import (
	"strings"
	"testing"
)

type CompositionConnectVariant uint8

const (
	CompositionConnectValid CompositionConnectVariant = iota
	CompositionConnectTemplateInstance
	CompositionConnectMissingProducerFlow
	CompositionConnectMissingProducerPin
	CompositionConnectMissingReceiverFlow
	CompositionConnectMissingReceiverPin
	CompositionConnectRootReceiver
	CompositionConnectWithoutRename
)

type compositionConnectFixtureOptions struct {
	connectEvent             string
	connectFrom              string
	connectTo                string
	connectRename            string
	omitRename               bool
	consumerMode             string
	consumerTemplateInstance bool
	rootReceiver             bool
}

func CopyCompositionConnect(t testing.TB, variant CompositionConnectVariant) string {
	t.Helper()
	opts := compositionConnectFixtureOptions{}
	switch variant {
	case CompositionConnectValid:
	case CompositionConnectTemplateInstance:
		opts.consumerMode, opts.consumerTemplateInstance = "template", true
	case CompositionConnectMissingProducerFlow:
		opts.connectFrom = "missing"
	case CompositionConnectMissingProducerPin:
		opts.connectEvent = "missing.event"
	case CompositionConnectMissingReceiverFlow:
		opts.connectTo = "missing"
	case CompositionConnectMissingReceiverPin:
		opts.connectRename = "missing.event"
	case CompositionConnectRootReceiver:
		opts.connectTo, opts.rootReceiver = ".", true
	case CompositionConnectWithoutRename:
		opts.omitRename = true
	default:
		t.Fatalf("unsupported composition-connect variant %d", variant)
	}
	return writeCompositionConnectFixture(t, opts)
}

func writeCompositionConnectFixture(t testing.TB, opts compositionConnectFixtureOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	rename := "    rename: " + valueOr(opts.connectRename, "deploy.completed") + "\n"
	if opts.omitRename {
		rename = ""
	}
	consumerMode := valueOr(opts.consumerMode, "static")

	rootSchema := "name: composition-connect\nconnect:\n  - event: " + valueOr(opts.connectEvent, "deploy.done") + "\n    from: " + valueOr(opts.connectFrom, "producer") + "\n    to: " + valueOr(opts.connectTo, "consumer") + "\n" + rename
	if opts.rootReceiver {
		rootSchema += "pins:\n  inputs:\n    events:\n      - deploy.completed\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
mode: static
pins:
  outputs:
    events:
      - deploy.done
`, `deploy.requested:
  vertical_id: string
deploy.done:
  key: vertical_id
  vertical_id: string
`, "", `producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [deploy.requested]
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields:
          vertical_id: payload.vertical_id
`)
	instance := ""
	input := "      - deploy.completed\n"
	entities := ""
	if opts.consumerTemplateInstance {
		instance = "instance: vertical_id\n"
		input = "      - event: deploy.completed\n        resolution:\n          mode: select\n"
		entities = "deployment:\n  vertical_id:\n    type: string\n    _unused_reason: composition connect route-key proof field\n"
	}
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: `+consumerMode+`
`+instance+`initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:
`+input, "", entities, `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [deploy.completed]
  event_handlers:
    deploy.completed:
      advances_to: done
`)
	return root
}

func CopyCompositionConnectTopology(t testing.TB) string {
	t.Helper()
	return writeCompositionConnectFixture(t, compositionConnectFixtureOptions{})
}

func CopyCompositionConnectAmbiguity(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)

	writeClosedVariantFile(t, root, "schema.yaml", "name: composition-connect-ambiguity\n")
	removeClosedVariantFiles(t, root,
		"consumer/agents.yaml", "consumer/entities.yaml", "consumer/events.yaml", "consumer/nodes.yaml",
	)
	for _, flowID := range []string{"producer_a", "producer_b"} {
		writeLegacyInstanceFlow(t, root, flowID, "name: "+flowID+"\nmode: static\npins:\n  outputs:\n    events:\n      - ticket.ready\n", "ticket.ready:\n  key: entity_id\n  entity_id: string\n", "", "")
	}
	writeLegacyInstanceFlow(t, root, "consumer", "name: consumer\nmode: static\npins:\n  inputs:\n    events:\n      - ticket.ready\n", "", "", "")
	return root
}

type CompositionConnectReceiverFanoutVariant uint8

const (
	CompositionConnectReceiverFanoutStatic CompositionConnectReceiverFanoutVariant = iota + 1
	CompositionConnectReceiverFanoutRuntimeIncomplete
)

func CopyCompositionConnectReceiverFanout(t testing.TB, variant CompositionConnectReceiverFanoutVariant) string {
	t.Helper()
	includeDynamic := variant == CompositionConnectReceiverFanoutRuntimeIncomplete
	if variant != CompositionConnectReceiverFanoutStatic && !includeDynamic {
		t.Fatalf("unsupported composition-connect receiver fanout variant %d", variant)
	}
	root := CopyExample(t, ParentConnect)
	rootSchema := `name: composition-connect-receiver-fanout
pins:
  inputs:
    events:
      - work.ready
connect:
  - event: work.ready
    from: producer
    to: .
  - event: work.ready
    from: producer
    to: consumer
    rename: consumer.work.ready
`
	if includeDynamic {
		rootSchema += "  - event: work.ready\n    from: producer\n    to: dynamic\n    rename: dynamic.work.ready\n"
	}

	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	writeClosedVariantFile(t, root, "nodes.yaml", `root-node:
  id: root-node
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready: {}
`)
	writeClosedVariantFile(t, root, "agents.yaml", `root-agent:
  id: root-agent
  model: regular
  intent:
    inline: Consume connected root work events.
  subscriptions: [work.ready]
`)
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.requested:\n  work_id: text?\nwork.ready:\n  key: work_id\n  work_id: text\n")
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\nmode: static\npins:\n  outputs:\n    events:\n      - work.ready\n")
	writeLegacyInstanceFlow(t, root, "consumer", "name: consumer\nmode: static\npins:\n  inputs:\n    events:\n      - consumer.work.ready\n", "", "", `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [consumer.work.ready]
  event_handlers:
    consumer.work.ready: {}
`)
	if includeDynamic {
		writeLegacyInstanceFlow(t, root, "dynamic", "name: dynamic\nmode: template\ninstance: work_id\npins:\n  inputs:\n    events:\n      - event: dynamic.work.ready\n        resolution:\n          mode: select\n", "", "dynamic_state:\n  work_id: string\n", `dynamic-node:
  id: dynamic-node-{instance_id}
  execution_type: system_node
  subscribes_to: [dynamic.work.ready]
  event_handlers:
    dynamic.work.ready: {}
`)
	}
	return root
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func writeBootverifyFixtureFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := writeFixtureFile(t, path, strings.TrimLeft(contents, "\n")); err != nil {
		t.Fatalf("write composition fixture %s: %v", path, err)
	}
}
