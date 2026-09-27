package contracts

import (
	"fmt"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestExpressionValueR2Authoring(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		kind    ExpressionKind
		literal any
		cel     string
	}{
		{"bare string", "ready", ExpressionKindLiteral, "ready", ""},
		{"empty string", `""`, ExpressionKindLiteral, "", ""},
		{"number", "42", ExpressionKindLiteral, 42, ""},
		{"typed expression", `"${payload.count}"`, ExpressionKindCEL, nil, "payload.count"},
		{"mixed string", `"count=${payload.count}!"`, ExpressionKindCEL, nil, `"count=" + string((payload.count)) + "!"`},
		{"escaped", `{literal: "${payload.count}"}`, ExpressionKindLiteral, "${payload.count}", ""},
		{"object", `{a: "${payload.count}", b: [true, "x"]}`, ExpressionKindCEL, nil, `{"a": (payload.count), "b": [true, "x"]}`},
		{"escaped nested", `{a: {literal: "${payload.count}"}}`, ExpressionKindLiteral, map[string]any{"a": "${payload.count}"}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got ExpressionValue
			if err := yaml.Unmarshal([]byte(test.source), &got); err != nil {
				t.Fatal(err)
			}
			if got.Kind != test.kind || got.CEL != test.cel || !reflect.DeepEqual(got.Literal, test.literal) {
				t.Fatalf("value = %#v, want kind=%q literal=%#v cel=%q", got, test.kind, test.literal, test.cel)
			}
		})
	}
}

func TestExpressionValueR2SharedAuthoringSurfaces(t *testing.T) {
	surfaces := []struct {
		name   string
		decode func(string) (ExpressionValue, error)
	}{
		{"emit.fields", func(source string) (ExpressionValue, error) {
			var spec EmitSpec
			err := yaml.Unmarshal([]byte("event: observed\nfields:\n  value: "+source+"\n"), &spec)
			return spec.Fields["value"], err
		}},
		{"activity.input", func(source string) (ExpressionValue, error) {
			var spec ActivitySpec
			err := yaml.Unmarshal([]byte("tool: record\ninput:\n  value: "+source+"\n"), &spec)
			return spec.Input["value"], err
		}},
		{"stage gate context", func(source string) (ExpressionValue, error) {
			var spec FlowStageGateDeclaration
			err := yaml.Unmarshal([]byte("decision: review\ncontext:\n  value: "+source+"\noutcomes:\n  accepted: {advances_to: done}\n"), &spec)
			return spec.Context["value"], err
		}},
		{"guard escalation", func(source string) (ExpressionValue, error) {
			var spec GuardSpec
			err := yaml.Unmarshal([]byte("check: false\non_fail:\n  escalate:\n    event: review.failed\n    fields:\n      value: "+source+"\n"), &spec)
			return spec.OnFailSpec.Escalation.Fields["value"], err
		}},
		{"data write value", func(source string) (ExpressionValue, error) {
			var write WorkflowDataWrite
			err := yaml.Unmarshal([]byte("target_field: result\nvalue: "+source+"\n"), &write)
			return write.Value, err
		}},
		{"data write key", func(source string) (ExpressionValue, error) {
			var write WorkflowDataWrite
			err := yaml.Unmarshal([]byte("op: append\ntarget: entity.by_id.items\nkey: "+source+"\nvalue: 1\n"), &write)
			return write.Key, err
		}},
		{"data write index", func(source string) (ExpressionValue, error) {
			var write WorkflowDataWrite
			err := yaml.Unmarshal([]byte("op: update\ntarget: entity.items\nindex: "+source+"\nvalue: 1\n"), &write)
			return write.Index, err
		}},
	}
	cases := []struct {
		name, source, cel string
		literal           any
		kind              ExpressionKind
	}{
		{"null", "null", "", nil, ExpressionKindLiteral},
		{"false", "false", "", false, ExpressionKindLiteral},
		{"zero", "0", "", 0, ExpressionKindLiteral},
		{"empty text", `""`, "", "", ExpressionKindLiteral},
		{"empty list", "[]", "", []any{}, ExpressionKindLiteral},
		{"empty object", "{}", "", map[string]any{}, ExpressionKindLiteral},
		{"typed", `"${payload.count}"`, "payload.count", nil, ExpressionKindCEL},
		{"mixed", `"count=${payload.count}"`, `"count=" + string((payload.count))`, nil, ExpressionKindCEL},
		{"escaped", `{literal: "${payload.count}"}`, "", "${payload.count}", ExpressionKindLiteral},
	}
	for _, surface := range surfaces {
		t.Run(surface.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := surface.decode(tc.source)
					if err != nil {
						t.Fatal(err)
					}
					if got.Kind != tc.kind || got.CEL != tc.cel || !reflect.DeepEqual(got.Literal, tc.literal) {
						t.Fatalf("value = %#v, want kind=%q literal=%#v cel=%q", got, tc.kind, tc.literal, tc.cel)
					}
				})
			}
			for _, retired := range []string{`{ref: payload.count}`, `{cel: payload.count}`, `{expression: payload.count}`} {
				if _, err := surface.decode(retired); err == nil {
					t.Errorf("accepted retired value %s", retired)
				}
			}
		})
	}
}

func TestExpressionValueR2RejectsDuplicateNormalizedFields(t *testing.T) {
	for _, source := range []string{
		"event: observed\nfields: {value: 1, ' value ': 2}\n",
		"tool: record\ninput: {value: 1, ' value ': 2}\n",
		"decision: review\ncontext: {value: 1, ' value ': 2}\noutcomes: {accepted: {advances_to: done}}\n",
	} {
		t.Run(fmt.Sprintf("surface-%d", len(source)), func(t *testing.T) {
			var err error
			switch {
			case len(source) >= 5 && source[:5] == "event":
				var value EmitSpec
				err = yaml.Unmarshal([]byte(source), &value)
			case len(source) >= 4 && source[:4] == "tool":
				var value ActivitySpec
				err = yaml.Unmarshal([]byte(source), &value)
			default:
				var value FlowStageGateDeclaration
				err = yaml.Unmarshal([]byte(source), &value)
			}
			if err == nil {
				t.Fatal("accepted duplicate normalized field")
			}
		})
	}
}

func TestExpressionValueR2RejectsRetiredAndMalformedForms(t *testing.T) {
	for _, source := range []string{
		`{cel: payload.id}`, `{expression: payload.id}`, `{ref: payload.id}`,
		`{kind: cel, cel: payload.id}`, `"${}"`, `"${payload.id"`,
	} {
		t.Run(source, func(t *testing.T) {
			var got ExpressionValue
			if err := yaml.Unmarshal([]byte(source), &got); err == nil {
				t.Fatalf("accepted %s", source)
			}
		})
	}
	var escaped ExpressionValue
	if err := yaml.Unmarshal([]byte(`{literal: {kind: cel, cel: payload.id}}`), &escaped); err != nil {
		t.Fatal(err)
	}
	if !escaped.HasLiteralValue() || !reflect.DeepEqual(escaped.Literal, map[string]any{"kind": "cel", "cel": "payload.id"}) {
		t.Fatalf("escaped literal = %#v", escaped)
	}
}

func TestExpressionValueR2DataWritePreservesAuthoredNull(t *testing.T) {
	var write WorkflowDataWrite
	if err := yaml.Unmarshal([]byte("op: set\ntarget: entity.items\nkey: null\nvalue: null\n"), &write); err != nil {
		t.Fatal(err)
	}
	if !write.Key.HasLiteralValue() || !write.Value.HasLiteralValue() || !reflect.DeepEqual(write.Key.Literal, nil) || !reflect.DeepEqual(write.Value.Literal, nil) {
		t.Fatalf("null operands were lost: key=%#v value=%#v", write.Key, write.Value)
	}
}
