package contracts

import (
	"fmt"
	"strings"
	"testing"

	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
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

func TestS1ProviderKeyBoundariesReachEveryReferenceAndSchemaOwner(t *testing.T) {
	for _, tc := range []struct {
		label, key string
		valid      bool
	}{
		{"plain", "team", true},
		{"hyphen", "team-bot", true},
		{"underscore", "team_bot", true},
		{"plain_100", strings.Repeat("a", 100), true},
		{"plain_101", strings.Repeat("a", 101), false},
		{"hyphen_100", strings.Repeat("a", 98) + "-b", true},
		{"hyphen_101", strings.Repeat("a", 99) + "-b", false},
		{"leading_underscore", "_team", false},
		{"reserved_con", "con", false},
		{"reserved_prn", "prn", false},
		{"reserved_aux", "aux", false},
		{"reserved_nul", "nul", false},
		{"reserved_com", "com1", false},
		{"reserved_lpt", "lpt9", false},
		{"leading_space", " team", false},
		{"trailing_space", "team ", false},
		{"leading_tab", "\tteam", false},
		{"trailing_tab", "team\t", false},
		{"newline", "team\n", false},
		{"empty", "", false},
	} {
		t.Run(tc.label, func(t *testing.T) {
			if got := runtimepaths.ValidateFlowSegment(tc.key) == nil; got != tc.valid {
				t.Fatalf("directory-owner control=%v want %v for %q", got, tc.valid, tc.key)
			}
			event := "inbound." + tc.key + ".message"
			for _, authored := range []struct{ label, body string }{
				{"input", fmt.Sprintf("pins: {inputs: [%q]}\n", event)},
				{"output", fmt.Sprintf("pins: {outputs: [%q]}\n", event)},
				{"connect", fmt.Sprintf("connect: [{event: %q, from: ., to: child, resolution: select}]\n", event)},
			} {
				t.Run(authored.label, func(t *testing.T) {
					snapshot, err := yamlsource.Load([]byte(authored.body))
					if err != nil {
						t.Fatal(err)
					}
					_, err = AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
					if (err == nil) != tc.valid {
						t.Errorf("authored %s disagrees with directory owner for %q: %v", authored.label, tc.key, err)
					}
				})
			}
			for _, flow := range []string{".", "child"} {
				context := FlowPinCompilationContext{FlowID: flow, FlowPath: flow, SourceFile: "schema.yaml"}
				if _, err := CompileFlowInputPin(context, FlowInputEventPin{Event: event}); (err == nil) != tc.valid {
					t.Errorf("compiled input disagrees with directory owner: %s %q %v", flow, tc.key, err)
				}
				if _, err := CompileFlowOutputPin(context, FlowOutputEventPin{Event: event}); (err == nil) != tc.valid {
					t.Errorf("compiled output disagrees with directory owner: %s %q %v", flow, tc.key, err)
				}
				entry := EventCatalogEntry{Payload: EventPayloadSpec{Required: []string{"text"}, Properties: map[string]EventFieldSpec{"text": {Type: "text"}}}}
				for _, prefix := range []string{"", "child/"} {
					name := prefix + event
					compiled, err := CompileImportedEventSchema(flow, name, entry, CompiledEventSchemaSource{Layer: "provider_trigger_pack", File: "trigger.yaml"})
					if (err == nil) != tc.valid {
						t.Errorf("imported schema disagrees with directory owner: %s %q %v", flow, name, err)
					}
					if err == nil && compiled.EventName() != name {
						t.Errorf("imported schema rewrote provider spelling: %q -> %q", name, compiled.EventName())
					}
				}
			}
		})
	}
}
