package workflowexpr

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEvalValueExpression_AllowsNullPresenceCheckOnMissingField(t *testing.T) {
	value, err := EvalValueExpression(`entity.kill_reason == null`, ValueContext{
		Entity: map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalValueExpression error = %v", err)
	}
	got, ok := value.(bool)
	if !ok {
		t.Fatalf("EvalValueExpression value = %#v (%T), want bool", value, value)
	}
	if !got {
		t.Fatal("expected sparse field == null presence check to evaluate true")
	}
}

func TestEvalValueExpression_AllowsComputedNamespace(t *testing.T) {
	value, err := EvalValueExpression(`computed.template_path`, ValueContext{
		Computed: map[string]any{"template_path": "templates/service/go"},
	})
	if err != nil {
		t.Fatalf("EvalValueExpression computed namespace error = %v", err)
	}
	if got := value; got != "templates/service/go" {
		t.Fatalf("EvalValueExpression computed namespace = %#v, want templates/service/go", got)
	}
}

func TestEvalValueExpression_FailsClosedOnMissingEntityValueRead(t *testing.T) {
	_, err := EvalValueExpression(`entity.revision_count + 1`, ValueContext{
		Entity: map[string]any{},
	})
	if err == nil {
		t.Fatal("expected missing entity field read to fail closed")
	}
	if got := err.Error(); got == "" || got == "no such key: revision_count" {
		t.Fatalf("expected explicit missing-field error, got %q", got)
	}
}

func TestEvalValueExpression_ExposesFanOutItemAlias(t *testing.T) {
	value, err := EvalValueExpressionWithOptions(`[line_item]`, ValueContext{
		FanOut: map[string]any{"item": "industry-a"},
	}, ValueExpressionOptions{ItemAlias: "line_item"})
	if err != nil {
		t.Fatalf("EvalValueExpression error = %v", err)
	}
	got, ok := value.([]any)
	if !ok || len(got) != 1 || got[0] != "industry-a" {
		t.Fatalf("EvalValueExpression value = %#v, want [industry-a]", value)
	}
}

func TestEvalValueExpression_RejectsBareItemByDefault(t *testing.T) {
	if err := ValidateValueExpression(`item`); err == nil {
		t.Fatal("expected bare item to be rejected by default")
	}
	_, err := EvalValueExpression(`item`, ValueContext{
		FanOut: map[string]any{"item": "industry-a"},
	})
	if err == nil {
		t.Fatal("expected bare item eval to be rejected by default")
	}
}

func TestValidateValueExpression_RejectsAccumulatedNamespace(t *testing.T) {
	err := ValidateValueExpression(`accumulated.size()`)
	if err == nil {
		t.Fatal("expected accumulated namespace to be rejected for data expressions")
	}
}

func TestJoinExpressionTypeCheckingMatchesRuntimeContext(t *testing.T) {
	opts := ValueExpressionOptions{AllowJoin: true, RequireBool: true, JoinResultType: runtimecontracts.CatalogTypeReference{Type: "text"}}
	for _, expression := range []string{
		`join.completed <= join.expected`,
		`join.missing.size() > 0`,
		`join.results.all(result, result != "")`,
		`join.timed_out == false`,
	} {
		t.Run("valid "+expression, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expression, opts); err != nil {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %v", expression, err)
			}
		})
	}
	for _, expression := range []string{
		`join.missing > 1`,
		`join.timed_out > 0`,
		`join.results[0] > 1`,
	} {
		t.Run("invalid "+expression, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expression, opts)
			if err == nil || !strings.Contains(err.Error(), "no matching overload") {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %v, want typed overload rejection", expression, err)
			}
			_, evalErr := EvalValueExpressionWithOptions(expression, ValueContext{Join: map[string]any{
				"expected": 2, "completed": 1, "missing": []any{"b"}, "results": []any{"ok"}, "timed_out": false,
			}}, opts)
			if evalErr == nil || !strings.Contains(evalErr.Error(), "no matching overload") {
				t.Fatalf("EvalValueExpressionWithOptions(%q) error = %v, want the same typed rejection before evaluation", expression, evalErr)
			}
		})
	}
}

func TestJoinExpressionTypeCheckingPreservesCatalogTypes(t *testing.T) {
	catalog := runtimecontracts.TypeCatalogDocument{
		Scalars: map[string]runtimecontracts.ScalarTypeDecl{"Score": {Base: "integer"}},
		Enums:   map[string]runtimecontracts.EnumTypeDecl{"Decision": {Values: []string{"accept", "reject"}}},
		Types: map[string]runtimecontracts.NamedTypeDecl{
			"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{
				"value": {Type: "text"},
				"score": {Type: "Score"},
			}},
		},
	}
	for _, tc := range []struct {
		name       string
		resultType string
		expression string
		wantErr    bool
	}{
		{name: "named object field", resultType: "JoinResult", expression: `join.results[0].value == "ok"`},
		{name: "named object operator", resultType: "JoinResult", expression: `join.results[0] > 1`, wantErr: true},
		{name: "named object unknown field", resultType: "JoinResult", expression: `join.results[0].missing == "x"`, wantErr: true},
		{name: "nested scalar alias field", resultType: "JoinResult", expression: `join.results[0].score > 1`},
		{name: "enum equality", resultType: "Decision", expression: `join.results[0] == "accept"`},
		{name: "enum numeric operator", resultType: "Decision", expression: `join.results[0] > 1`, wantErr: true},
		{name: "scalar alias", resultType: "Score", expression: `join.results[0] > 1`},
		{name: "scalar alias mismatch", resultType: "Score", expression: `join.results[0].startsWith("1")`, wantErr: true},
		{name: "list", resultType: "list<Score>", expression: `join.results[0][0] > 1`},
		{name: "map", resultType: "map[text]Score", expression: `join.results[0]["a"] > 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(tc.expression, ValueExpressionOptions{
				AllowJoin: true, RequireBool: true,
				JoinResultType: runtimecontracts.CatalogTypeReference{Type: tc.resultType, Catalog: catalog},
			})
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) succeeded, want typed rejection", tc.expression)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %v", tc.expression, err)
			}
		})
	}
}

func TestValidateValueExpression_RejectsRetiredFanOutTarget(t *testing.T) {
	tests := []string{
		`fan_out.target`,
		`fan_out.target.flow_instance`,
		`fan_out["target"]`,
		`fan_out['target']`,
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{AllowBareItem: true})
			if err == nil {
				t.Fatalf("expected %q to reject retired fan_out.target", expression)
			}
			if got := err.Error(); got == "" || !containsAll(got, "fan_out.target", "retired") {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %q, want retired fan_out.target", expression, got)
			}
		})
	}
}

func TestValidateValueExpression_RejectsLegacyEventReceiverProjections(t *testing.T) {
	for _, expression := range []string{
		`event.entity_id`,
		`event.flow_instance`,
		`event.entity_id == "ent-1"`,
		`event["entity_id"]`,
		`event['flow_instance']`,
		`event["entity_id"] == "ent-1"`,
		`event[payload.key]`,
	} {
		t.Run(expression, func(t *testing.T) {
			err := ValidateValueExpression(expression)
			if err == nil {
				t.Fatalf("expected %q to reject legacy event receiver projection", expression)
			}
			if !strings.Contains(err.Error(), "unsupported event context reference") {
				t.Fatalf("error = %q, want unsupported event context reference", err.Error())
			}
		})
	}
}

func TestEventReferences_OnlyMatchesRootEventContext(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       []string
	}{
		{
			name:       "root",
			expression: `event.entity_id`,
			want:       []string{"entity_id"},
		},
		{
			name:       "root after delimiter",
			expression: `(event.flow_instance == "flow-1") || payload.ok`,
			want:       []string{"flow_instance"},
		},
		{
			name:       "bracket root",
			expression: `event["entity_id"]`,
			want:       []string{"entity_id"},
		},
		{
			name:       "single-quote bracket root",
			expression: `event['flow_instance']`,
			want:       []string{"flow_instance"},
		},
		{
			name:       "mixed route access",
			expression: `event["source"].entity_id == event.target["flow_instance"]`,
			want:       []string{"source.entity_id", "target.flow_instance"},
		},
		{
			name:       "nested bracket route access",
			expression: `event["source"]["flow_id"]`,
			want:       []string{"source.flow_id"},
		},
		{
			name:       "nested payload event object",
			expression: `payload.event.entity_id`,
			want:       nil,
		},
		{
			name:       "nested platform entity event object",
			expression: `_entity.event.flow_instance`,
			want:       nil,
		},
		{
			name:       "nested event object with spaced dot",
			expression: `payload . event.entity_id`,
			want:       nil,
		},
		{
			name:       "nested event bracket object",
			expression: `payload.event["entity_id"]`,
			want:       nil,
		},
		{
			name:       "identifier prefix",
			expression: `some_event.entity_id`,
			want:       nil,
		},
		{
			name:       "string literal",
			expression: `"event.entity_id"`,
			want:       nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventReferences(tt.expression)
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("EventReferences(%q) = %#v, want %#v", tt.expression, got, tt.want)
			}
		})
	}
}

func TestValidateValueExpression_RejectsUnsupportedEventBracketRefs(t *testing.T) {
	tests := []struct {
		expression string
		want       string
	}{
		{expression: `event["entity_id"]`, want: "event.entity_id is unsupported"},
		{expression: `event['flow_instance']`, want: "event.flow_instance is unsupported"},
		{expression: `event["source"]["entity_id"]["extra"]`, want: "event.source.entity_id is a route identity scalar"},
		{expression: `event["source.entity_id"]`, want: `event["source.entity_id"] is not a supported handler event context field`},
		{expression: `event[payload.key]`, want: "event[...] dynamic field access is unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			err := ValidateValueExpression(tt.expression)
			if err == nil {
				t.Fatalf("expected %q to reject unsupported event bracket ref", tt.expression)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestValidateValueExpression_AllowsSupportedEventBracketRefs(t *testing.T) {
	for _, expression := range []string{
		`event["id"]`,
		`event["source"]["entity_id"]`,
		`event["source"].flow_instance`,
		`event.target["flow_id"]`,
		`event["target_set"]`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpression(expression); err != nil {
				t.Fatalf("ValidateValueExpression(%q) error = %v", expression, err)
			}
		})
	}
}

func TestValidateValueExpression_AllowsNestedAuthorEventFields(t *testing.T) {
	for _, expression := range []string{
		`payload.event.entity_id`,
		`payload.event.flow_instance == "flow-1"`,
		`_entity.event.flow_instance`,
		`payload.event.entity_id == event.source.entity_id`,
		`payload.event["entity_id"]`,
		`_entity.event["flow_instance"]`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpression(expression); err != nil {
				t.Fatalf("ValidateValueExpression(%q) error = %v", expression, err)
			}
		})
	}
}

func TestEvalValueExpression_RejectsLegacyEventReceiverProjectionEvenWhenMapContainsValue(t *testing.T) {
	_, err := EvalValueExpression(`event.entity_id`, ValueContext{
		Event: map[string]any{"entity_id": "legacy-ent"},
	})
	if err == nil {
		t.Fatal("expected eval to reject legacy event receiver projection")
	}
	if !strings.Contains(err.Error(), "event.entity_id is unsupported") {
		t.Fatalf("error = %q, want event.entity_id unsupported", err.Error())
	}
}

func TestValidateValueExpression_AllowsSupportedEventContextRefs(t *testing.T) {
	for _, expression := range []string{
		`event.id`,
		`event.type`,
		`event.source.entity_id`,
		`event.source.flow_instance`,
		`event.source.flow_id`,
		`event.target.entity_id`,
		`event.target.flow_instance`,
		`event.target.flow_id`,
		`event.target_set`,
		`event.source_event_id`,
		`event.emitted_at`,
		`event.trigger_event_type`,
		`event.current_state`,
		`event.run_id`,
		`event.scope`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpression(expression); err != nil {
				t.Fatalf("ValidateValueExpression(%q) error = %v", expression, err)
			}
		})
	}
}

func TestValidateValueExpression_AllowsFanOutAliasAndStringLiteralTargetText(t *testing.T) {
	tests := []string{
		`line_item.target`,
		`"fan_out.target"`,
		`payload.note == "fan_out.target"`,
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{ItemAlias: "line_item"}); err != nil {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %v", expression, err)
			}
		})
	}
}

func TestValidateValueExpression_RejectsRetiredFanOutItem(t *testing.T) {
	for _, expression := range []string{`fan_out.item`, `fan_out.item.target`, `fan_out["item"]`} {
		t.Run(expression, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{ItemAlias: "line_item"})
			if err == nil {
				t.Fatalf("expected %q to reject retired fan_out.item", expression)
			}
			if got := err.Error(); got == "" || !containsAll(got, "fan_out.item", "retired") {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %q, want retired fan_out.item", expression, got)
			}
		})
	}
}

func TestExpressionReferencesEntity_IgnoresStringLiterals(t *testing.T) {
	if ExpressionReferencesEntity(`payload.reason == "entity.kill_reason"`) {
		t.Fatal("expected quoted entity reference text to be ignored")
	}
	if !ExpressionReferencesEntity(`has(entity.kill_reason) ? entity.kill_reason : payload.reason`) {
		t.Fatal("expected real entity reference to be detected")
	}
}

func TestEvalValueExpressionSupportsPublicLoopRootWithoutRewritingStrings(t *testing.T) {
	value, err := EvalValueExpression(`loop.revision_id + ":loop.revision_id"`, ValueContext{Loop: map[string]any{"revision_id": "rev-2"}})
	if err != nil {
		t.Fatal(err)
	}
	if value != "rev-2:loop.revision_id" {
		t.Fatalf("value = %#v", value)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
