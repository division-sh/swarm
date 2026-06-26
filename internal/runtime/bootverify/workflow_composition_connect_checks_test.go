package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_AllowsParentCompositionConnectAsVerifyRouteProof(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "pin_target_resolution", "deploy.done") {
		t.Fatalf("parent connect should satisfy output pin target proof, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "input_pin_wiring", "deploy.completed") {
		t.Fatalf("parent connect should satisfy input pin wiring proof, got %#v", report.Warnings())
	}
}

func TestRun_FailsClosedForInvalidParentCompositionConnect(t *testing.T) {
	tests := []struct {
		name      string
		opts      compositionConnectFixtureOptions
		want      string
		wantExtra string
	}{
		{
			name: "missing producer flow",
			opts: compositionConnectFixtureOptions{
				connectFrom: "missing.deploy_done",
			},
			want: "producer_flow_missing",
		},
		{
			name: "missing producer output pin",
			opts: compositionConnectFixtureOptions{
				connectFrom: "producer.missing_pin",
			},
			want: "producer_output_pin_missing",
		},
		{
			name: "missing receiver flow",
			opts: compositionConnectFixtureOptions{
				connectTo: "missing.deploy_completed",
			},
			want: "receiver_flow_missing",
		},
		{
			name: "missing receiver input pin",
			opts: compositionConnectFixtureOptions{
				connectTo: "consumer.missing_pin",
			},
			want: "receiver_input_pin_missing",
		},
		{
			name: "event names differ without adapter",
			opts: compositionConnectFixtureOptions{
				noAdapter: true,
			},
			want: "event_alias_or_adapter_invalid",
		},
		{
			name: "missing address key",
			opts: compositionConnectFixtureOptions{
				mapSource: "payload.missing_vertical_id",
			},
			want:      "output_carries_address_key",
			wantExtra: "missing_vertical_id",
		},
		{
			name: "incompatible address key types",
			opts: compositionConnectFixtureOptions{
				consumerEntityType: "integer",
			},
			want: "key_types_incompatible",
		},
		{
			name: "unindexed business-field target",
			opts: compositionConnectFixtureOptions{
				consumerEntityUnindexed: true,
			},
			want:      "receiver_address_rule_invalid",
			wantExtra: "indexed: true",
		},
		{
			name: "invalid delivery topology",
			opts: compositionConnectFixtureOptions{
				delivery: "many",
			},
			want: "delivery_topology_invalid",
		},
		{
			name: "broadcast delivery with singular address",
			opts: compositionConnectFixtureOptions{
				delivery: "broadcast",
			},
			want:      "delivery_topology_invalid",
			wantExtra: "broadcast",
		},
		{
			name: "missing reply lineage",
			opts: compositionConnectFixtureOptions{
				delivery: "reply",
			},
			want: "reply_lineage_missing",
		},
		{
			name: "template receiver missing address rule",
			opts: compositionConnectFixtureOptions{
				consumerMode:        "template",
				consumerScalarInput: true,
				connectTo:           "consumer.deploy.completed",
			},
			want: "receiver_address_rule_missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectBootverifyFixture(t, tc.opts)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
			if tc.wantExtra != "" && !reportContains(report.Errors(), "composition_connect_validation", tc.wantExtra) {
				t.Fatalf("expected composition_connect_validation detail %q, got %#v", tc.wantExtra, report.Errors())
			}
		})
	}
}

func TestRun_AllowsImportBoundaryAliasAsConnectEventAdapter(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		consumerRequiresInput: true,
		consumerInputBind:     "deploy.done",
		consumerScalarInput:   true,
		connectTo:             "consumer.deploy.completed",
		noAdapter:             true,
		omitMap:               true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "event_alias_or_adapter_invalid") {
		t.Fatalf("import-boundary alias should satisfy connect event adaptation, got %#v", report.Errors())
	}
}

func TestRun_AllowsOutputBoundaryAliasAsConnectEventAdapter(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		producerRequiresOutput: true,
		producerOutputBind:     "deploy.completed",
		consumerScalarInput:    true,
		noAdapter:              true,
		omitMap:                true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "event_alias_or_adapter_invalid") {
		t.Fatalf("producer import-boundary alias should satisfy connect event adaptation, got %#v", report.Errors())
	}
}

func TestRun_AllowsParentCompositionConnectAsCrossFlowAmbiguityProof(t *testing.T) {
	root := writeCompositionConnectAmbiguityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("parent connect should disambiguate cross-flow input pin, got %#v", report.Errors())
	}
}

func TestRun_TreatsParentCompositionConnectAsEventTopologyProof(t *testing.T) {
	root := writeCompositionConnectTopologyFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "event_producer_exists", "consumer/deploy.completed") {
		t.Fatalf("parent connect should prove receiver input has a producer, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_consumer_exists", "producer/deploy.done") {
		t.Fatalf("parent connect should prove producer output has a consumer, got %#v", report.Warnings())
	}
	for _, eventRef := range []string{"producer/deploy.done", "consumer/deploy.completed"} {
		if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", eventRef) {
			t.Fatalf("parent connect should mark %s as active, got %#v", eventRef, report.Warnings())
		}
	}
}

type compositionConnectFixtureOptions struct {
	connectFrom             string
	connectTo               string
	delivery                string
	noAdapter               bool
	mapSource               string
	mapTarget               string
	omitMap                 bool
	consumerMode            string
	consumerScalarInput     bool
	consumerEntityType      string
	consumerEntityUnindexed bool
	consumerRequiresInput   bool
	consumerInputBind       string
	producerRequiresOutput  bool
	producerOutputBind      string
}

func writeCompositionConnectBootverifyFixture(t *testing.T, opts compositionConnectFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	connectFrom := firstTestValue(opts.connectFrom, "producer.deploy_done")
	connectTo := firstTestValue(opts.connectTo, "consumer.deploy_completed")
	delivery := firstTestValue(opts.delivery, "one")
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
platform_version: ">=1.6.0"
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
`+adapter+`    delivery: `+delivery+`
`+mapBlock+`
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

func writeCompositionConnectTopologyFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeCompositionConnectRootPackage(t, root, "composition-connect-topology", `
  - from: producer.deploy_done
    to: consumer.deploy_completed
    adapter: deploy_done_to_completed
    delivery: one
    map:
      vertical_id:
        source: payload.vertical_id
        target: entity.vertical_id
`)
	writeCompositionConnectProducerSchemaOnlyFlow(t, root)
	writeCompositionConnectConsumerSchemaOnlyFlow(t, root)
	return root
}

func writeCompositionConnectAmbiguityFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-ambiguity
version: "1.0.0"
platform_version: ">=1.6.0"
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
    delivery: one
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

func writeCompositionConnectRootPackage(t *testing.T, root, name, connectEntries string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: `+name+`
version: "1.0.0"
platform_version: ">=1.6.0"
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

func writeCompositionConnectProducerSchemaOnlyFlow(t *testing.T, root string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
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

func writeCompositionConnectConsumerSchemaOnlyFlow(t *testing.T, root string) {
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

func writeCompositionConnectProducerFlow(t *testing.T, root string, opts compositionConnectFixtureOptions) {
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	if !opts.producerRequiresOutput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.requested:
  vertical_id: string
deploy.done:
  vertical_id: string
`)
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
          vertical_id: payload.vertical_id
`)
}

func writeCompositionConnectConsumerFlow(t *testing.T, root string, opts compositionConnectFixtureOptions) {
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: `+firstTestValue(opts.consumerMode, "static")+`
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
`)
	if !opts.consumerScalarInput {
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
