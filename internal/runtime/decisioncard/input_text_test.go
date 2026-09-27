package decisioncard

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestParseDecisionInputFieldTextUsesCanonicalGateTypes(t *testing.T) {
	for _, tc := range []struct {
		name, kind, text string
		want             any
	}{
		{name: "text preserves content", kind: "text", text: "  a reason  ", want: "  a reason  "},
		{name: "integer", kind: "integer", text: "42", want: float64(42)},
		{name: "numeric", kind: "numeric", text: "-1.25", want: float64(-1.25)},
		{name: "boolean true", kind: "boolean", text: "true", want: true},
		{name: "boolean false", kind: "boolean", text: "false", want: false},
		{name: "timestamp", kind: "timestamp", text: "2026-09-27T20:00:00Z", want: "2026-09-27T20:00:00Z"},
		{name: "uuid", kind: "uuid", text: "45b9e7b1-92fb-4bed-88c2-53100a4b28e1", want: "45b9e7b1-92fb-4bed-88c2-53100a4b28e1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := ParseInputFieldText(tc.kind, tc.text)
			if err != nil || value.Interface() != tc.want {
				t.Fatalf("parse %q as %s = %v, %v; want %v", tc.text, tc.kind, value.Interface(), err, tc.want)
			}
		})
	}
	for _, tc := range []struct{ kind, text string }{
		{"text", "  "}, {"integer", "1.0"}, {"integer", "1e2"}, {"integer", "9007199254740993"},
		{"numeric", "NaN"}, {"numeric", "01"}, {"boolean", "True"}, {"boolean", "1"},
		{"timestamp", "tomorrow"}, {"uuid", "not-an-id"}, {"jsonb", "{}"},
	} {
		t.Run(tc.kind+"/invalid/"+tc.text, func(t *testing.T) {
			value, err := ParseInputFieldText(tc.kind, tc.text)
			if err == nil || value.Kind() != semanticvalue.KindNull {
				t.Fatalf("invalid %q as %s = %v, %v", tc.text, tc.kind, value, err)
			}
		})
	}
}

func TestAdvanceInputFieldKeepsDeclaredOrderAndRejectsRepeat(t *testing.T) {
	outcome := FrozenOutcome{Verdict: "revise", Input: map[string]contracts.WorkflowGateInputField{
		"zeta": {Type: "text", Required: true}, "alpha": {Type: "integer", Required: true},
	}, InputOrder: []string{"zeta", "alpha"}}
	first, err := AdvanceInputField(outcome, semanticvalue.EmptyObject(), 0, "why")
	if err != nil || first.AcceptedField != "zeta" || first.NextField != "alpha" || first.Complete {
		t.Fatalf("first declared field = %+v, %v", first, err)
	}
	if _, err := AdvanceInputField(outcome, first.Fields, 0, "again"); err == nil {
		t.Fatal("replayed field overwrote recorded answer")
	}
	if _, err := AdvanceInputField(outcome, semanticvalue.EmptyObject(), 1, "17"); err == nil {
		t.Fatal("index advanced without its preceding field")
	}
	foreign := semanticvalue.MustObject(map[string]semanticvalue.Value{"foreign": semanticvalue.MustString("value")})
	if _, err := AdvanceInputField(outcome, foreign, 1, "17"); err == nil {
		t.Fatal("undeclared stored field advanced the draft")
	}
	if _, err := AdvanceInputField(outcome, first.Fields, 1, "01"); err == nil {
		t.Fatal("invalid integer advanced the draft")
	}
	second, err := AdvanceInputField(outcome, first.Fields, 1, "17")
	if err != nil || !second.Complete || second.NextField != "" || second.AcceptedField != "alpha" {
		t.Fatalf("final declared field = %+v, %v", second, err)
	}
	if value, ok := second.Fields.Lookup("zeta"); !ok || value.Interface() != "why" {
		t.Fatalf("first answer was lost: %v", second.Fields)
	}
	if value, ok := second.Fields.Lookup("alpha"); !ok || value.Interface() != float64(17) {
		t.Fatalf("second answer was lost: %v", second.Fields)
	}
	if _, err := AdvanceInputField(outcome, second.Fields, 2, "extra"); err == nil {
		t.Fatal("completed draft accepted an extra answer")
	}
}

func TestSkipInputFieldRequiresExplicitOptionalField(t *testing.T) {
	outcome := FrozenOutcome{Verdict: "revise", Input: map[string]contracts.WorkflowGateInputField{
		"optional": {Type: "text", Required: false}, "required": {Type: "text", Required: true},
	}, InputOrder: []string{"optional", "required"}}
	skipped, err := SkipInputField(outcome, semanticvalue.EmptyObject(), 0)
	if err != nil || !skipped.Skipped || skipped.NextField != "required" || skipped.Fields.Len() != 0 {
		t.Fatalf("optional skip = %+v, %v", skipped, err)
	}
	if _, err := SkipInputField(outcome, skipped.Fields, 1); err == nil {
		t.Fatal("required field was skipped")
	}
	answer, err := AdvanceInputField(outcome, skipped.Fields, 1, "skip")
	if err != nil || !answer.Complete || answer.Skipped {
		t.Fatalf("literal text answer after optional skip = %+v, %v", answer, err)
	}
	if _, present := answer.Fields.Lookup("optional"); present {
		t.Fatal("skipped optional field was materialized")
	}
	if value, present := answer.Fields.Lookup("required"); !present || value.Interface() != "skip" {
		t.Fatalf("literal answer was reinterpreted: %v", answer.Fields)
	}
}
