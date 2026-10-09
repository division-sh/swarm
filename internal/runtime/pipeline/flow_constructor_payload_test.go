package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

func TestFlowConstructorPayloadConsumesExactDeliveryProjection(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "name: constructor-projection\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
		"entities.yaml": "item:\n  item_id: text\n  precise: double?\n  message: text?\n",
		"events.yaml":   "item.created:\n  precise: double\n  message: text\n",
	})
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.created", "constructor-fixture", "", []byte(`{"precise":7.0,"message":"unchanged"}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"item_id": "minted-key"})
	if err != nil {
		t.Fatal(err)
	}
	request := FlowInstanceActivationRequest{ContractBundle: source, Instance: flowidentity.Instance{TemplateID: "."}, ConstructorInput: "item.created", ResolvedKey: "minted-key", TriggerEvent: event, PayloadProjection: projection}
	payload, err := request.ConstructorPayload()
	if err != nil || payload["item_id"] != nil || payload["precise"] != json.Number("7.0") || payload["message"] != "unchanged" {
		t.Fatalf("constructor did not preserve its producer payload: %+v %v", payload, err)
	}
	constructor, err := CompileFlowConstructor(source, ".", "item.created")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := constructor.InitialFields(payload, request.ResolvedKey)
	if err != nil || fields["item_id"] != "minted-key" || fields["message"] != "unchanged" {
		t.Fatalf("constructor lost its separate resolved key or supplied fields: %+v %v", fields, err)
	}
	if string(event.Payload()) != `{"precise":7.0,"message":"unchanged"}` {
		t.Fatal("constructor changed the original publication payload")
	}
	for _, test := range []struct {
		name       string
		projection map[string]string
		key        any
	}{
		{"wrong_key", map[string]string{"item_id": "other-key"}, "minted-key"},
		{"unrelated_field", map[string]string{"unrelated": "minted-key"}, "minted-key"},
		{"extra_field", map[string]string{"item_id": "minted-key", "unrelated": "other"}, "minted-key"},
		{"missing_resolution", map[string]string{"item_id": "minted-key"}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := request
			changed.ResolvedKey = test.key
			changed.PayloadProjection, err = events.NewDeliveryPayloadProjection(test.projection)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := changed.ConstructorPayload(); err == nil {
				t.Fatal("constructor accepted unrelated or contradictory projection authority")
			}
		})
	}
	for _, key := range []string{"minted-key", "other-key"} {
		changed := request
		changed.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "source.created", "constructor-fixture", "", []byte(`{"precise":7.0,"message":"unchanged","item_id":"`+key+`"}`), 0, event.RunID(), events.EventEnvelope{}, time.Now().UTC())
		if _, err := changed.ConstructorPayload(); err == nil {
			t.Fatal("constructor accepted producer/synthetic-key collision")
		}
	}
	request.ConstructorInput = ""
	if _, err := request.ConstructorPayload(); err == nil || !strings.Contains(err.Error(), "keyless construction") {
		t.Fatalf("keyless constructor silently ignored a projected argument: %v", err)
	}
	request.PayloadProjection = events.DeliveryPayloadProjection{}
	request.ResolvedKey = nil
	if payload, err := request.ConstructorPayload(); err != nil || payload != nil {
		t.Fatalf("keyless constructor consumed its parent's message as arguments: %+v %v", payload, err)
	}
}

func TestFlowInstanceKeyRequiresScalarAdmissionBeforeFormatting(t *testing.T) {
	for _, test := range []struct {
		name, fieldType, material string
		value                     any
		invalid                   any
	}{
		{"text", "text", "business-key", "business-key", 123},
		{"integer", "integer", "7", int64(7), "7"},
		{"numeric", "double", "7.5", json.Number("7.5"), "7.5"},
		{"boolean", "boolean", "true", true, "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "name: typed-instance-key\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
				"entities.yaml": "item:\n  item_id: " + test.fieldType + "\n",
				"events.yaml":   "item.created:\n  item_id: " + test.fieldType + "\n",
			})
			material, err := AdmitFlowInstanceKey(source, ".", test.value)
			if err != nil || material != test.material {
				t.Fatalf("typed key material=%q err=%v, want %q", material, err, test.material)
			}
			for _, value := range []any{test.invalid, nil, map[string]any{"item_id": test.value}, []any{test.value}} {
				if material, err := AdmitFlowInstanceKey(source, ".", value); err == nil {
					t.Fatalf("unadmitted %T became identity %q", value, material)
				}
			}
		})
	}
	source := loadWorkflowTempSource(t, map[string]string{"schema.yaml": "name: keyless\n"})
	if material, err := AdmitFlowInstanceKey(source, ".", nil); err != nil || material != "" {
		t.Fatalf("keyless admission: material=%q err=%v", material, err)
	}
	if _, err := AdmitFlowInstanceKey(source, ".", "foreign"); err == nil {
		t.Fatal("keyless admission accepted a key")
	}
}

func TestFlowConstructorResolvedKeySatisfiesOnlyItsDeclaredSlot(t *testing.T) {
	for _, declaresKey := range []bool{false, true} {
		name := "message_only_input"
		eventSchema := "item.created:\n  message: text\n"
		if declaresKey {
			name = "declared_key_input"
			eventSchema += "  item_id: text\n"
		}
		t.Run(name, func(t *testing.T) {
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "name: constructor-key-slot\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
				"entities.yaml": "item:\n  item_id: text\n  message: text?\n",
				"events.yaml":   eventSchema,
			})
			constructor, err := CompileFlowConstructor(source, ".", "item.created")
			if err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name    string
				payload map[string]any
				key     any
				valid   bool
			}{
				{"resolved_key", map[string]any{"message": "supplied"}, "minted-key", true},
				{"matching_payload_key", map[string]any{"message": "supplied", "item_id": "minted-key"}, "minted-key", declaresKey},
				{"conflicting_payload_key", map[string]any{"message": "supplied", "item_id": "other"}, "minted-key", false},
				{"missing_message", map[string]any{}, "minted-key", false},
				{"unrelated_field", map[string]any{"message": "supplied", "unrelated": "value"}, "minted-key", false},
				{"invalid_resolved_key", map[string]any{"message": "supplied"}, 123, false},
				{"missing_resolved_key", map[string]any{"message": "supplied"}, nil, false},
			} {
				t.Run(test.name, func(t *testing.T) {
					before, err := json.Marshal(test.payload)
					if err != nil {
						t.Fatal(err)
					}
					fields, err := constructor.InitialFields(test.payload, test.key)
					if (err == nil) != test.valid {
						t.Fatalf("constructor fields=%+v err=%v, want valid=%t", fields, err, test.valid)
					}
					if test.valid && (fields["item_id"] != "minted-key" || fields["message"] != "supplied") {
						t.Fatalf("constructor lost its resolved key or supplied message: %+v", fields)
					}
					after, err := json.Marshal(test.payload)
					if err != nil || string(before) != string(after) {
						t.Fatal("constructor changed the original publication payload")
					}
				})
			}
		})
	}
}
