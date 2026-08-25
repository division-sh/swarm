package canonicalrouting

import (
	"path/filepath"
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
	CompositionConnectAdapterWithoutRename
	CompositionConnectMissingOutputKey
	CompositionConnectMissingOutputCarries
	CompositionConnectKeyNotCarried
	CompositionConnectDuplicateCarry
	CompositionConnectAmbiguousOutputKey
	CompositionConnectMissingPayloadKey
	CompositionConnectNonScalarKey
	CompositionConnectEmitMissingKey
	CompositionConnectAgentEmitUnproven
	CompositionConnectAutoEmitUnproven
	CompositionConnectTimerUnproven
	CompositionConnectInputAlias
	CompositionConnectOutputAlias
)

func CopyCompositionConnect(t testing.TB, variant CompositionConnectVariant) string {
	t.Helper()
	opts := compositionConnectFixtureOptions{}
	switch variant {
	case CompositionConnectValid:
	case CompositionConnectTemplateInstance:
		opts.consumerMode, opts.consumerScalarInput, opts.consumerTemplateInstance = "template", true, true
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
	case CompositionConnectAdapterWithoutRename:
		opts.omitRename = true
	case CompositionConnectMissingOutputKey:
		opts.omitProducerOutputKey = true
	case CompositionConnectMissingOutputCarries:
		opts.omitProducerOutputCarries = true
	case CompositionConnectKeyNotCarried:
		opts.producerOutputCarries = "[component_id]"
	case CompositionConnectDuplicateCarry:
		opts.producerOutputCarries = "[vertical_id, vertical_id]"
	case CompositionConnectAmbiguousOutputKey:
		opts.duplicateProducerOutputKey = true
	case CompositionConnectMissingPayloadKey:
		opts.producerOutputKey, opts.producerOutputCarries = "component_id", "[component_id]"
	case CompositionConnectNonScalarKey:
		opts.producerVerticalIDType = "[string]"
	case CompositionConnectEmitMissingKey:
		opts.omitProducerEmitField = true
	case CompositionConnectAgentEmitUnproven:
		opts.producerAgentEmit = true
	case CompositionConnectAutoEmitUnproven:
		opts.producerAutoEmit = true
	case CompositionConnectTimerUnproven:
		opts.producerTimer = true
	case CompositionConnectInputAlias:
		opts.consumerRequiresInput, opts.consumerScalarInput, opts.noAdapter = true, true, true
		opts.consumerInputBind, opts.omitRename = "deploy.done", true
	case CompositionConnectOutputAlias:
		opts.producerRequiresOutput, opts.consumerScalarInput, opts.noAdapter = true, true, true
		opts.producerOutputBind, opts.connectEvent, opts.omitRename = "deploy.completed", "deploy.completed", true
	default:
		t.Fatalf("unsupported composition-connect variant %d", variant)
	}
	return writeCompositionConnectBootverifyFixture(t, opts)
}

func CopyCompositionConnectTopology(t testing.TB) string {
	t.Helper()
	return writeCompositionConnectTopologyFixture(t)
}

func CopyCompositionConnectAmbiguity(t testing.TB) string {
	t.Helper()
	return writeCompositionConnectAmbiguityFixture(t)
}

type CompositionConnectReceiverFanoutVariant uint8

const (
	CompositionConnectReceiverFanoutStatic CompositionConnectReceiverFanoutVariant = iota + 1
	CompositionConnectReceiverFanoutRuntimeIncomplete
)

// CopyCompositionConnectReceiverFanout owns the closed root/child receiver
// policy fixture used by selected-contract routing admission.
func CopyCompositionConnectReceiverFanout(t testing.TB, variant CompositionConnectReceiverFanoutVariant) string {
	t.Helper()
	includeRuntimeReceiver := false
	switch variant {
	case CompositionConnectReceiverFanoutStatic:
	case CompositionConnectReceiverFanoutRuntimeIncomplete:
		includeRuntimeReceiver = true
	default:
		t.Fatalf("unsupported composition-connect receiver fanout variant %d", variant)
	}
	root := CopyExample(t, ParentConnect)
	packageBody := `name: composition-connect-receiver-fanout
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
  - event: work.ready
    from: producer
    to: .
  - event: work.ready
    from: producer
    to: consumer
    rename: consumer.work.ready
    adapter: work_ready_to_consumer
`
	if includeRuntimeReceiver {
		packageBody = strings.Replace(packageBody, "connect:\n", "  - id: dynamic\n    flow: dynamic\n    mode: template\nconnect:\n", 1)
		packageBody += "  - event: work.ready\n    from: producer\n    to: dynamic\n    rename: dynamic.work.ready\n    adapter: work_ready_to_dynamic\n"
	}
	writeClosedVariantFile(t, root, "package.yaml", packageBody)
	writeClosedVariantFile(t, root, "schema.yaml", `name: composition-connect-receiver-fanout
pins:
  inputs:
    events:
      - name: work_ready
        event: work.ready
`)
	writeClosedVariantFile(t, root, "events.yaml", "{}\n")
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
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: static
pins:
  inputs:
    events:
      - name: work_ready
        event: consumer.work.ready
`, "{}\n", "{}\n", `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [consumer.work.ready]
  event_handlers:
    consumer.work.ready: {}
`)
	if includeRuntimeReceiver {
		writeLegacyInstanceFlow(t, root, "dynamic", `name: dynamic
mode: template
instance: work_id
pins:
  inputs:
    events:
      - name: work_ready
        event: dynamic.work.ready
        resolution:
          mode: select
        carries:
          work_id:
            from: payload.work_id
`, "{}\n", `dynamic_state:
  work_id: string
`, `dynamic-node:
  id: dynamic-node-{instance_id}
  execution_type: system_node
  subscribes_to: [dynamic.work.ready]
  event_handlers:
    dynamic.work.ready: {}
`)
	}
	return root
}

type compositionConnectFixtureOptions struct {
	connectEvent               string
	connectFrom                string
	connectTo                  string
	connectRename              string
	omitRename                 bool
	noAdapter                  bool
	consumerMode               string
	consumerScalarInput        bool
	consumerRequiresInput      bool
	consumerInputBind          string
	producerRequiresOutput     bool
	producerOutputBind         string
	producerOutputKey          string
	producerOutputCarries      string
	omitProducerOutputKey      bool
	omitProducerOutputCarries  bool
	omitProducerEmitField      bool
	producerVerticalIDType     string
	producerAgentEmit          bool
	producerAutoEmit           bool
	producerTimer              bool
	duplicateProducerOutputKey bool
	consumerTemplateInstance   bool
	rootReceiver               bool
}

func writeCompositionConnectBootverifyFixture(t testing.TB, opts compositionConnectFixtureOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	connectEvent := firstTestValue(opts.connectEvent, "deploy.done")
	connectFrom := firstTestValue(opts.connectFrom, "producer")
	connectTo := firstTestValue(opts.connectTo, "consumer")
	rename := "    rename: " + firstTestValue(opts.connectRename, "deploy.completed") + "\n"
	if opts.omitRename {
		rename = ""
	}
	adapter := "    adapter: deploy_done_to_completed\n"
	if opts.noAdapter {
		adapter = ""
	}
	flowBind := ""
	if opts.consumerRequiresInput {
		bind := firstTestValue(opts.consumerInputBind, "deploy.done")
		flowBind = `
    bind:
      inputs:
        deploy.completed: ` + bind
	}
	producerBind := ""
	if opts.producerRequiresOutput {
		bind := firstTestValue(opts.producerOutputBind, "deploy.completed")
		producerBind = `
    bind:
      outputs:
        deploy.done: ` + bind
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-bootverify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static`+producerBind+`
  - id: consumer
    flow: consumer
    mode: `+firstTestValue(opts.consumerMode, "static")+flowBind+`
connect:
  - event: `+connectEvent+`
    from: `+connectFrom+`
    to: `+connectTo+`
`+rename+adapter+`
`)
	rootSchema := "name: composition-connect-bootverify\n"
	if opts.rootReceiver {
		rootSchema = `
name: composition-connect-bootverify
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.completed
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), rootSchema)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	rootEvents := "{}\n"
	if opts.producerRequiresOutput {
		rootEvents = "deploy.completed:\n  vertical_id: string\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), rootEvents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCompositionConnectProducerFlow(t, root, opts)
	writeCompositionConnectConsumerFlow(t, root, opts)
	return root
}

func writeCompositionConnectTopologyFixture(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeCompositionConnectRootPackage(t, root, "composition-connect-topology", `
  - event: deploy.done
    from: producer
    to: consumer
    rename: deploy.completed
    adapter: deploy_done_to_completed
`)
	writeCompositionConnectProducerSchemaOnlyFlow(t, root)
	writeCompositionConnectConsumerSchemaOnlyFlow(t, root)
	return root
}

func writeCompositionConnectAmbiguityFixture(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-ambiguity
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer_a
    flow: producer_a
    mode: static
  - id: producer_b
    flow: producer_b
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
  - event: ticket.ready
    from: producer_a
    to: consumer
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-ambiguity\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	for _, flowID := range []string{"producer_a", "producer_b"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
pins:
  outputs:
    events:
      - ticket.ready
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), `
ticket.ready:
  entity_id: string
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
pins:
  inputs:
    events:
      - ticket.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func writeCompositionConnectRootPackage(t testing.TB, root, name, connectEntries string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: `+name+`
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
`+connectEntries)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: "+name+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func writeCompositionConnectProducerSchemaOnlyFlow(t testing.TB, root string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.done:
  vertical_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
}

func writeCompositionConnectConsumerSchemaOnlyFlow(t testing.TB, root string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.completed
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
    indexed: true
    _unused_reason: composition connect topology proof field
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
}

func writeCompositionConnectProducerFlow(t testing.TB, root string, opts compositionConnectFixtureOptions) {
	t.Helper()
	if opts.producerRequiresOutput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), `
name: producer
version: "1.0.0"
requires:
  inputs: []
  outputs: [deploy.done]
`)
	}
	outputKeyBlock := ""
	if !opts.omitProducerOutputKey {
		outputKeyBlock += "\n        key: " + firstTestValue(opts.producerOutputKey, "vertical_id")
	}
	if !opts.omitProducerOutputCarries {
		outputKeyBlock += "\n        carries: " + firstTestValue(opts.producerOutputCarries, "[vertical_id]")
	}
	duplicateOutputPinBlock := ""
	if opts.duplicateProducerOutputKey {
		duplicateOutputPinBlock = `
      - name: deploy_done_alias
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`
	}
	emitField := "          vertical_id: payload.vertical_id\n"
	if opts.omitProducerEmitField {
		emitField = ""
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
`+producerAutoEmitOnCreateBlock(opts)+`
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`+outputKeyBlock+`
`+duplicateOutputPinBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	if !opts.producerRequiresOutput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	}
	verticalIDType := firstTestValue(opts.producerVerticalIDType, "string")
	verticalIDSchema := "  vertical_id: " + verticalIDType + "\n"
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.requested:
  vertical_id: string
deploy.done:
`+verticalIDSchema+`
`)
	producerAgents := "{}\n"
	if opts.producerAgentEmit {
		producerAgents = `
producer-agent:
  id: producer-agent
  type: claude
  role: producer
  intent: {inline: "Produce the declared deployment event."}
  emit_events: [deploy.done]
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), producerAgents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [deploy.requested]
`+producerWorkflowTimerBlock(opts)+`
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields:
`+emitField+`
`)
}

func producerWorkflowTimerBlock(opts compositionConnectFixtureOptions) string {
	if !opts.producerTimer {
		return ""
	}
	return `  timers:
    - id: deploy_done_timer
      owner: producer-node
      event: deploy.done
      delay: 1m
      start_on: event:deploy.requested
`
}

func producerAutoEmitOnCreateBlock(opts compositionConnectFixtureOptions) string {
	if !opts.producerAutoEmit {
		return ""
	}
	return `auto_emit_on_create:
  event: deploy.done
`
}

func writeCompositionConnectConsumerFlow(t testing.TB, root string, opts compositionConnectFixtureOptions) {
	t.Helper()
	if opts.consumerRequiresInput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "package.yaml"), `
name: consumer
version: "1.0.0"
requires:
  inputs: [deploy.completed]
  outputs: []
`)
	}
	inputEvents := `
      - name: deploy_completed
        event: deploy.completed
`
	if opts.consumerScalarInput && !opts.consumerTemplateInstance {
		inputEvents = `
      - deploy.completed
`
	}
	instanceBlock := ""
	if opts.consumerTemplateInstance {
		instanceBlock = "instance: vertical_id\n"
		inputEvents = `
      - name: deploy_completed
        event: deploy.completed
        resolution:
          mode: select
        carries:
          vertical_id:
            from: payload.vertical_id
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: `+firstTestValue(opts.consumerMode, "static")+`
`+instanceBlock+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:`+inputEvents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "{}\n")
	if opts.consumerTemplateInstance {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
    _unused_reason: composition connect route-key proof field
`)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [deploy.completed]
  event_handlers:
    deploy.completed:
      create_entity: true
      advances_to: done
`)
}

func firstTestValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func writeBootverifyFixtureFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := writeFixtureFile(path, strings.TrimLeft(contents, "\n")); err != nil {
		t.Fatalf("write composition fixture %s: %v", path, err)
	}
}
