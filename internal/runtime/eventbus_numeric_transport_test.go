package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

// Admission receives execution payloads after their semantic-to-CEL handoff.
// Schema validation and optional-null normalization must not erase their kinds.
func TestRuntimePayloadAdmissionPreservesExecutionNumberKinds(t *testing.T) {
	for _, tc := range []struct {
		name, schema, payload string
	}{
		{"authored", "numeric.completed:\n  integer: integer\n  explicit_double: numeric\n", `{"integer":8,"explicit_double":8.0}`},
		{"optional_null", "numeric.completed:\n  integer: integer\n  explicit_double: numeric\n  optional: text?\n", `{"integer":8,"explicit_double":8.0,"optional":null}`},
		{"schema_less", "", `{"integer":8,"explicit_double":8.0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadRootPayloadBundle(t, tc.schema, "")
			event := eventtest.RuntimeControl("numeric-kind-proof", "numeric.completed", "numeric-proof", "", []byte(tc.payload), 0, "", "", events.EventEnvelope{}, time.Time{})
			admitted, err := testRuntimePayloadAdmitter(t, bundle)(context.Background(), event, "")
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(admitted.Payload(), &decoded); err != nil {
				t.Fatal(err)
			}
			projected, err := workflowexpr.ProjectCELValue(decoded)
			if err != nil {
				t.Fatal(err)
			}
			values := projected.(map[string]any)
			if values["integer"] != int64(8) || values["explicit_double"] != float64(8) {
				t.Fatalf("admission erased execution kinds: integer=%T double=%T bytes=%s", values["integer"], values["explicit_double"], admitted.Payload())
			}
			if bytes.Contains(admitted.Payload(), []byte("null")) {
				t.Fatal("optional-null normalization was bypassed")
			}
		})
	}
}

func TestRuntimePayloadAdmissionPreservesNestedExecutionKindsAndImmutability(t *testing.T) {
	bundle := loadRootPayloadBundle(t, "numeric.completed:\n  record: json\n", "")
	payload := []byte(`{"record":{"rows":[{"integer":8,"double":8.0,"exponent":8e0,"fraction":8.25},[-8,-8.0,0,0.0]]}}`)
	want := map[string]any{"record": map[string]any{"rows": []any{
		map[string]any{"integer": int64(8), "double": float64(8), "exponent": float64(8), "fraction": 8.25},
		[]any{int64(-8), float64(-8), int64(0), float64(0)},
	}}}
	event := eventtest.RuntimeControl("nested-kinds", "numeric.completed", "numeric-proof", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{})
	admission, err := testRuntimePayloadAdmitter(t, bundle)(context.Background(), event, "")
	if err != nil {
		t.Fatal(err)
	}
	readback := admission.Payload()
	frozen := bytes.Clone(readback)
	readback[0], payload[0] = '[', '['
	if !bytes.Equal(admission.Payload(), frozen) {
		t.Fatal("admission aliases its producer or readback bytes")
	}
	var decoded map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(admission.Payload(), &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := workflowexpr.ProjectCELValue(decoded)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("nested execution kinds: got=%#v want=%#v err=%v", got, want, err)
	}
}

func TestRuntimePayloadAdmissionStrictOriginalBytes(t *testing.T) {
	bundle := loadRootPayloadBundle(t, "numeric.completed:\n  value: numeric\n  optional: text?\n", "")
	admitter := testRuntimePayloadAdmitter(t, bundle)
	for name, raw := range map[string]string{
		"duplicate":         `{"value":1,"value":2}`,
		"escaped_duplicate": `{"value":1,"val\u0075e":2}`,
		"nested_duplicate":  `{"value":1,"optional":{"x":1,"x":2}}`,
		"negative_zero":     `{"value":-0.0}`,
		"unsafe_integer":    `{"value":9007199254740992}`,
		"unsafe_exponent":   `{"value":9.007199254740992e15}`,
		"negative_unsafe":   `{"value":-9007199254740992}`,
		"overflow":          `{"value":1e400}`,
		"underflow":         `{"value":1e-400}`,
		"trailing":          `{"value":1} {}`,
		"invalid_utf8":      "{\"value\":1,\"optional\":\"\xff\"}",
		"array":             `[1]`,
		"null":              `null`,
		"required_null":     `{"value":null}`,
		"required_absent":   `{}`,
		"wrong_type":        `{"value":"8"}`,
		"unknown_field":     `{"value":8,"unknown":8}`,
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if failure := recover(); failure != nil && json.Valid([]byte(raw)) {
					t.Fatalf("valid-JSON probe failed before payload admission: %v", failure)
				}
			}()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "numeric.completed", "numeric-proof", "", []byte(raw), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if _, err := admitter(context.Background(), event, ""); err == nil {
				t.Fatalf("accepted hostile original bytes %q", raw)
			}
		})
	}
	for _, raw := range []string{`{"value":9007199254740991}`, `{"value":5e-324}`, `{"value":8.0,"optional":null}`} {
		if _, err := admitRuntimePayload(admitter, "numeric.completed", []byte(raw)); err != nil {
			t.Fatalf("rejected lawful control %s: %v", raw, err)
		}
	}
}

func TestRuntimePayloadAdmissionEmptyObjectIsNotNull(t *testing.T) {
	admitter := testRuntimePayloadAdmitter(t, loadRootPayloadBundle(t, "empty.completed: {}\n", ""))
	for _, raw := range []string{"", "{}", "{ }"} {
		event, err := admitRuntimePayload(admitter, "empty.completed", []byte(raw))
		if err != nil {
			t.Fatalf("empty object %q: %v", raw, err)
		}
		if string(event.Payload()) != "{}" {
			t.Fatalf("empty object changed shape: %s", event.Payload())
		}
	}
	for _, raw := range []string{"null", "[]", "1", "true", `""`} {
		if _, err := admitRuntimePayload(admitter, "empty.completed", []byte(raw)); err == nil {
			t.Fatalf("accepted non-object %q", raw)
		}
	}
}

func TestRuntimePayloadAdmissionSelectedForkPreservesNumericBytes(t *testing.T) {
	bundle := loadRootPayloadBundle(t, "numeric.completed:\n  integer: integer\n  explicit_double: numeric\n", "")
	payload := json.RawMessage("{\n  \"explicit_double\": 8e0, \"integer\": 8\n}")
	lineage, err := events.NewSelectedForkLineage(uuid.NewString(), uuid.NewString(), uuid.NewString(), "selected-contract", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts:   events.EventFacts{ID: uuid.NewString(), Type: "numeric.completed", Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "selected-contract"}, Payload: payload, CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live},
		Lineage: lineage,
	})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := testRuntimePayloadAdmitter(t, bundle)(context.Background(), event, "")
	if err != nil || !bytes.Equal(admission.Payload(), payload) {
		t.Fatalf("selected-fork bytes changed: %q err=%v", admission.Payload(), err)
	}
}
