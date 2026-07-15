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
	CompositionConnectCompositeTemplateInstance
	CompositionConnectMissingProducerFlow
	CompositionConnectMissingProducerPin
	CompositionConnectMissingReceiverFlow
	CompositionConnectMissingReceiverPin
	CompositionConnectRootReceiver
	CompositionConnectMissingAdapter
	CompositionConnectMissingAddressKey
	CompositionConnectIncompatibleKeyType
	CompositionConnectUnindexedTarget
	CompositionConnectNestedTarget
	CompositionConnectTemplateMissingAddress
	CompositionConnectTemplateLegacyMap
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
		opts.connectTo, opts.omitMap = "consumer.deploy.completed", true
	case CompositionConnectCompositeTemplateInstance:
		opts.consumerMode, opts.consumerScalarInput, opts.consumerTemplateInstance = "template", true, true
		opts.consumerTemplateInstanceBy, opts.consumerTemplateInstanceRegion = "[vertical_id, region]", true
		opts.producerOutputCarries, opts.connectTo, opts.omitMap = "[vertical_id, region]", "consumer.deploy.completed", true
	case CompositionConnectMissingProducerFlow:
		opts.connectFrom = "missing.deploy_done"
	case CompositionConnectMissingProducerPin:
		opts.connectFrom = "producer.missing_pin"
	case CompositionConnectMissingReceiverFlow:
		opts.connectTo = "missing.deploy_completed"
	case CompositionConnectMissingReceiverPin:
		opts.connectTo = "consumer.missing_pin"
	case CompositionConnectRootReceiver:
		opts.connectTo = ".deploy_completed"
	case CompositionConnectMissingAdapter:
		opts.noAdapter = true
	case CompositionConnectMissingAddressKey:
		opts.mapSource = "payload.missing_vertical_id"
	case CompositionConnectIncompatibleKeyType:
		opts.consumerEntityType = "integer"
	case CompositionConnectUnindexedTarget:
		opts.consumerEntityUnindexed = true
	case CompositionConnectNestedTarget:
		opts.mapTarget = "entity.profile.vertical_id"
	case CompositionConnectTemplateMissingAddress:
		opts.consumerMode, opts.consumerScalarInput = "template", true
		opts.connectTo, opts.omitMap = "consumer.deploy.completed", true
	case CompositionConnectTemplateLegacyMap:
		opts.consumerMode, opts.consumerScalarInput, opts.consumerTemplateInstance = "template", true, true
		opts.connectTo = "consumer.deploy.completed"
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
		opts.producerOutputKey, opts.producerOutputCarries, opts.mapSource = "component_id", "[component_id]", "payload.component_id"
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
		opts.consumerRequiresInput, opts.consumerScalarInput, opts.noAdapter, opts.omitMap = true, true, true, true
		opts.consumerInputBind, opts.connectTo = "deploy.done", "consumer.deploy.completed"
	case CompositionConnectOutputAlias:
		opts.producerRequiresOutput, opts.consumerScalarInput, opts.noAdapter, opts.omitMap = true, true, true, true
		opts.producerOutputBind = "deploy.completed"
	default:
		t.Fatalf("unsupported composition-connect variant %d", variant)
	}
	return writeCompositionConnectBootverifyFixture(t, opts)
}

type CompositionConnectAdapterVariant uint8

const (
	CompositionAdapterScalar CompositionConnectAdapterVariant = iota + 1
	CompositionAdapterComposite
	CompositionAdapterMissing
	CompositionAdapterMissingSource
	CompositionAdapterMissingTarget
	CompositionAdapterSourceNotCarried
	CompositionAdapterTargetNotKey
	CompositionAdapterPartial
	CompositionAdapterCardinality
	CompositionAdapterDuplicateSource
	CompositionAdapterDuplicateTarget
	CompositionAdapterIncompatibleTypes
	CompositionAdapterLegacyMap
)

func CopyCompositionConnectAdapter(t testing.TB, variant CompositionConnectAdapterVariant) string {
	t.Helper()
	opts := compositionConnectAdapterFixtureOptions{
		sourceFields: []compositionConnectAdapterField{{Name: "source_vertical_id", Type: "string"}},
		outputKey:    "source_vertical_id", outputCarries: "[source_vertical_id]",
		receiverInstanceBy: "vertical_id",
		receiverFields:     []compositionConnectAdapterField{{Name: "vertical_id", Type: "string"}},
		usingSource:        "source_vertical_id", usingTarget: "vertical_id",
	}
	if variant != CompositionAdapterScalar {
		opts.sourceFields = []compositionConnectAdapterField{{Name: "source_vertical_id", Type: "string"}, {Name: "region_code", Type: "string"}}
		opts.outputCarries = "[source_vertical_id, region_code]"
		opts.receiverInstanceBy = "[vertical_id, region]"
		opts.receiverFields = []compositionConnectAdapterField{{Name: "vertical_id", Type: "string"}, {Name: "region", Type: "string"}}
		opts.usingSource, opts.usingTarget = "[source_vertical_id, region_code]", "[vertical_id, region]"
	}
	switch variant {
	case CompositionAdapterScalar, CompositionAdapterComposite:
	case CompositionAdapterMissing:
		opts.omitUsing = true
	case CompositionAdapterMissingSource:
		opts.usingSource = ""
	case CompositionAdapterMissingTarget:
		opts.usingTarget = ""
	case CompositionAdapterSourceNotCarried:
		opts.usingSource = "[source_vertical_id, missing_region]"
	case CompositionAdapterTargetNotKey:
		opts.usingTarget = "[vertical_id, missing_region]"
	case CompositionAdapterPartial:
		opts.usingSource, opts.usingTarget = "source_vertical_id", "vertical_id"
	case CompositionAdapterCardinality:
		opts.usingTarget = "vertical_id"
	case CompositionAdapterDuplicateSource:
		opts.usingSource = "[source_vertical_id, source_vertical_id]"
	case CompositionAdapterDuplicateTarget:
		opts.usingTarget = "[vertical_id, vertical_id]"
	case CompositionAdapterIncompatibleTypes:
		opts.sourceFields[0].Type = "integer"
	case CompositionAdapterLegacyMap:
		opts.omitUsing, opts.includeLegacyMap = true, true
	default:
		t.Fatalf("unsupported composition-connect adapter variant %d", variant)
	}
	return writeCompositionConnectAdapterBootverifyFixture(t, opts)
}

func CopyCompositionConnectTopology(t testing.TB) string {
	t.Helper()
	return writeCompositionConnectTopologyFixture(t)
}

func CopyCompositionConnectAmbiguity(t testing.TB) string {
	t.Helper()
	return writeCompositionConnectAmbiguityFixture(t)
}

type compositionConnectFixtureOptions struct {
	connectFrom                    string
	connectTo                      string
	noAdapter                      bool
	mapSource                      string
	mapTarget                      string
	omitMap                        bool
	consumerMode                   string
	consumerScalarInput            bool
	consumerEntityType             string
	consumerEntityUnindexed        bool
	consumerRequiresInput          bool
	consumerInputBind              string
	producerRequiresOutput         bool
	producerOutputBind             string
	producerOutputKey              string
	producerOutputCarries          string
	omitProducerOutputKey          bool
	omitProducerOutputCarries      bool
	omitProducerEmitField          bool
	producerVerticalIDType         string
	producerAgentEmit              bool
	producerAutoEmit               bool
	producerTimer                  bool
	duplicateProducerOutputKey     bool
	consumerTemplateInstance       bool
	consumerTemplateInstanceBy     string
	consumerTemplateInstanceRegion bool
}

type compositionConnectAdapterField struct {
	Name string
	Type string
}

type compositionConnectAdapterFixtureOptions struct {
	sourceFields       []compositionConnectAdapterField
	outputKey          string
	outputCarries      string
	receiverInstanceBy string
	receiverFields     []compositionConnectAdapterField
	usingSource        string
	usingTarget        string
	omitUsing          bool
	includeLegacyMap   bool
}

func (o compositionConnectAdapterFixtureOptions) clone() compositionConnectAdapterFixtureOptions {
	out := o
	out.sourceFields = append([]compositionConnectAdapterField(nil), o.sourceFields...)
	out.receiverFields = append([]compositionConnectAdapterField(nil), o.receiverFields...)
	return out
}

func writeCompositionConnectBootverifyFixture(t testing.TB, opts compositionConnectFixtureOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	connectFrom := firstTestValue(opts.connectFrom, "producer.deploy_done")
	connectTo := firstTestValue(opts.connectTo, "consumer.deploy_completed")
	adapter := "    adapter: deploy_done_to_completed\n"
	if opts.noAdapter {
		adapter = ""
	}
	mapSource := firstTestValue(opts.mapSource, "payload.vertical_id")
	mapTarget := firstTestValue(opts.mapTarget, "entity.vertical_id")
	mapBlock := ""
	if !opts.omitMap {
		mapBlock = `
    map:
      vertical_id:
        source: ` + mapSource + `
        target: ` + mapTarget
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
  - from: `+connectFrom+`
    to: `+connectTo+`
`+adapter+mapBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-bootverify\n")
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
  - from: producer.deploy_done
    to: consumer.deploy_completed
    adapter: deploy_done_to_completed
    map:
      vertical_id:
        source: payload.vertical_id
        target: entity.vertical_id
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
  - from: producer_a.ticket.ready
    to: consumer.ticket.ready
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  entity_id: string
`)
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
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.completed:
  vertical_id: string
`)
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
	} else if opts.consumerTemplateInstanceRegion {
		emitField += "          region: payload.region\n"
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
	if opts.consumerTemplateInstanceRegion {
		verticalIDSchema += "  region: string\n"
	}
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
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
`
	if opts.consumerScalarInput {
		inputEvents = `
      - deploy.completed
`
	}
	instanceBlock := ""
	if opts.consumerTemplateInstance {
		instanceBy := firstTestValue(opts.consumerTemplateInstanceBy, "vertical_id")
		instanceBlock = `instance:
  by: ` + instanceBy + `
  on_missing: reject
  on_conflict: reject
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.completed:
  vertical_id: string
`+compositionConnectConsumerRegionEventSchema(opts))
	if !opts.consumerScalarInput || opts.consumerTemplateInstance {
		entityType := firstTestValue(opts.consumerEntityType, "string")
		indexed := "\n    indexed: true"
		if opts.consumerEntityUnindexed {
			indexed = ""
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: `+entityType+`
`+indexed+`
    _unused_reason: composition connect route-key proof field
`+compositionConnectConsumerRegionEntitySchema(opts)+`
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

func compositionConnectConsumerRegionEventSchema(opts compositionConnectFixtureOptions) string {
	if !opts.consumerTemplateInstanceRegion {
		return ""
	}
	return "  region: string\n"
}

func compositionConnectConsumerRegionEntitySchema(opts compositionConnectFixtureOptions) string {
	if !opts.consumerTemplateInstanceRegion {
		return ""
	}
	return "  region:\n" +
		"    type: string\n" +
		"    _unused_reason: composite template instance key proof field\n"
}

func writeCompositionConnectAdapterBootverifyFixture(t testing.TB, opts compositionConnectAdapterFixtureOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	outputKey := firstTestValue(opts.outputKey, "source_vertical_id")
	outputCarries := firstTestValue(opts.outputCarries, "["+outputKey+"]")
	receiverInstanceBy := firstTestValue(opts.receiverInstanceBy, "vertical_id")
	usingBlock := ""
	if !opts.omitUsing {
		usingBlock = "\n" +
			"    using:\n" +
			"      instance:\n" +
			"        source: " + opts.usingSource + "\n" +
			"        target: " + opts.usingTarget
	}
	legacyMapBlock := ""
	if opts.includeLegacyMap {
		legacyMapBlock = "\n" +
			"    map:\n" +
			"      vertical_id:\n" +
			"        source: payload." + outputKey + "\n" +
			"        target: entity.vertical_id"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-adapter-bootverify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
`+usingBlock+legacyMapBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-adapter-bootverify\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: `+outputKey+`
        carries: `+outputCarries+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.requested:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields)+`deploy.done:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [deploy.requested]
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields:
`+compositionConnectAdapterEmitFields(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: `+receiverInstanceBy+`
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.done:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
`+compositionConnectAdapterEntityFields(opts.receiverFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func compositionConnectAdapterFieldsSchema(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "  source_vertical_id: string\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "source_vertical_id")
		typ := firstTestValue(field.Type, "string")
		out.WriteString("  " + name + ": " + typ + "\n")
	}
	return out.String()
}

func compositionConnectAdapterEmitFields(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "          source_vertical_id: payload.source_vertical_id\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "source_vertical_id")
		out.WriteString("          " + name + ": payload." + name + "\n")
	}
	return out.String()
}

func compositionConnectAdapterEntityFields(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "  vertical_id:\n    type: string\n    _unused_reason: connect adapter receiver instance-key proof field\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "vertical_id")
		typ := firstTestValue(field.Type, "string")
		out.WriteString("  " + name + ":\n    type: " + typ + "\n    _unused_reason: connect adapter receiver instance-key proof field\n")
	}
	return out.String()
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
