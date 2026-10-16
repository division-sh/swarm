package contracts

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestS1ProviderDirectoryKeySurvivesSchemaReferences(t *testing.T) {
	const event = "inbound.telegram-sales.message"
	snapshot, err := yamlsource.Load([]byte(fmt.Sprintf("pins:\n  inputs: [%s]\n  outputs: [%s]\nconnect:\n  - {event: %s, from: ., to: child, resolution: select}\n", event, event, event)))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
	if err != nil {
		t.Fatalf("directory-derived provider reference refused: %v", err)
	}
	if len(schema.Pins.Inputs.EventPins) != 1 || schema.Pins.Inputs.EventPins[0].Event != event || len(schema.Pins.Outputs.EventPins) != 1 || schema.Pins.Outputs.EventPins[0].Event != event || len(schema.Connect) != 1 || schema.Connect[0].Event != event {
		t.Fatalf("provider key was rewritten: %+v", schema)
	}
	for _, flow := range []string{".", "child"} {
		context := FlowPinCompilationContext{FlowID: flow, FlowPath: flow, SourceFile: "schema.yaml"}
		input, err := CompileFlowInputPin(context, schema.Pins.Inputs.EventPins[0])
		if err != nil {
			t.Fatal(err)
		}
		output, err := CompileFlowOutputPin(context, schema.Pins.Outputs.EventPins[0])
		if err != nil {
			t.Fatal(err)
		}
		entry := EventCatalogEntry{Payload: EventPayloadSpec{Required: []string{"text"}, Properties: map[string]EventFieldSpec{"text": {Type: "text"}}}}
		compiled, err := CompileImportedEventSchema(flow, event, entry, CompiledEventSchemaSource{Layer: "provider_trigger_pack", File: "telegram-sales/trigger.yaml"})
		if err != nil {
			t.Fatalf("compiled provider schema rejected its directory key: %v", err)
		}
		boundInput, err := input.BindImportedEventSchema(compiled)
		if err != nil {
			t.Fatal(err)
		}
		boundOutput, err := output.BindImportedEventSchema(compiled)
		if err != nil {
			t.Fatal(err)
		}
		inputSchema, inputPresent := boundInput.ReceiverEventSchema()
		outputSchema, outputPresent := boundOutput.EventSchema()
		if !inputPresent || !outputPresent || inputSchema.EventName() != event || outputSchema.EventName() != event || inputSchema.FlowPath() != flow || outputSchema.FlowPath() != flow {
			t.Fatal("pin binding lost exact provider or flow identity")
		}
		wrong, err := CompileImportedEventSchema(flow, strings.ReplaceAll(event, "-", "_"), entry, CompiledEventSchemaSource{Layer: "provider_trigger_pack", File: "telegram_sales/trigger.yaml"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := input.BindImportedEventSchema(wrong); err == nil {
			t.Fatal("input accepted another provider directory's schema")
		}
		if _, err := output.BindImportedEventSchema(wrong); err == nil {
			t.Fatal("output accepted another provider directory's schema")
		}
	}
}

func TestS1ProviderDirectoryKeyDoesNotGrantAuthoredCatalogOwnership(t *testing.T) {
	for _, name := range []string{"inbound.telegram-sales.message", "inbound.telegram-sales.*", "child/inbound.telegram-sales.message"} {
		snapshot, err := yamlsource.Load([]byte(fmt.Sprintf("%q:\n", name)))
		if err != nil {
			t.Fatal(err)
		}
		entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
		if len(entries) != 0 || err == nil || !strings.Contains(err.Error(), "reserved provider namespace inbound.*") {
			t.Fatalf("directory key did not reach reserved-namespace owner: entries=%v error=%v", entries, err)
		}
	}
}
