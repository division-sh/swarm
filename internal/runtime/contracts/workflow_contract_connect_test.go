package contracts

import (
	"strings"
	"testing"
)

func TestImportedOutputPinSchemaBindingIsImmutableAndSingleOwner(t *testing.T) {
	for _, flow := range []string{".", "ingress"} {
		t.Run(flow, func(t *testing.T) {
			pin, err := CompileFlowOutputPin(FlowPinCompilationContext{FlowID: flow, FlowPath: flow, SourceFile: "schema.yaml"}, FlowOutputEventPin{Event: "inbound.message"})
			if err != nil {
				t.Fatal(err)
			}
			entry := EventCatalogEntry{Payload: EventPayloadSpec{Required: []string{"text"}, Properties: map[string]EventFieldSpec{"text": {Type: "text"}}}}
			schema, err := CompileImportedEventSchema(flow, "inbound.message", entry, CompiledEventSchemaSource{Layer: "provider_trigger_pack", File: "verified-provider"})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := pin.BindImportedEventSchema(schema)
			if err != nil {
				t.Fatal(err)
			}
			if _, exists := pin.EventSchema(); exists {
				t.Fatal("binding mutated the original schema-less pin")
			}
			got, ok := bound.EventSchema()
			if !ok || got.AcceptanceSchemaDigest() != schema.AcceptanceSchemaDigest() || bound.Digest() == pin.Digest() || bound.Provenance() != pin.Provenance() {
				t.Fatal("binding lost immutable schema identity or authored provenance")
			}
			if _, err := bound.BindImportedEventSchema(schema); err == nil {
				t.Fatal("already-bound output accepted a second owner")
			}
			wrong, err := CompileImportedEventSchema(flow, "other.message", entry, CompiledEventSchemaSource{Layer: "provider_trigger_pack", File: "verified-provider"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pin.BindImportedEventSchema(wrong); err == nil {
				t.Fatal("output accepted another event's schema")
			}
			if _, err := pin.BindImportedEventSchema(CompiledEventSchema{}); err == nil {
				t.Fatal("output accepted absent schema evidence")
			}
		})
	}
}

func TestW2CanonicalPinEvidenceIsImmutable(t *testing.T) {
	context := FlowPinCompilationContext{FlowID: "collector", FlowPath: "collector", SourceFile: "collector/schema.yaml"}
	first, err := CompileFlowInputPin(context, FlowInputEventPin{Event: "work.reported"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileFlowInputPin(context, FlowInputEventPin{Event: "work.reported"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() == "" || first.Digest() != second.Digest() {
		t.Fatal("equivalent pins lost canonical identity")
	}
	readback := first.Provenance()
	readback.SourceFile = "changed"
	if first.Provenance().SourceFile != context.SourceFile {
		t.Fatal("readback mutation changed compiled pin")
	}
}

func TestW2CompiledResolutionRejectsFieldsOutsideClosedMode(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resolution string
	}{
		{name: "fan-in from", resolution: "mode: fan-in, from: payload.ignored, aggregation: stream, window: payload.batch_id, dedup_by: [event.id], singleton: collector"},
		{name: "reply from", resolution: "mode: reply, from: payload.ignored, replies_to: work.requested"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitSchemaFragment("pins: {inputs: [{event: work.completed, resolution: {" + tc.resolution + "}}]}\n")
			if err == nil || !strings.Contains(err.Error(), "must be a scalar text") {
				t.Fatalf("admission error = %v, want retired input-resolution rejection", err)
			}
		})
	}
}
