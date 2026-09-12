package contracts

import "testing"

func TestLoopRevisionCarriageExpression(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value ExpressionValue
		want  bool
	}{
		{"ref", RefExpression("loop.revision_id"), true},
		{"cel", ExpressionValue{Kind: ExpressionKindCEL, CEL: "loop.revision_id"}, true},
		{"accepted_whitespace", RefExpression(" loop.revision_id "), true},
		{"business_reference", RefExpression("payload.revision_id"), false},
		{"literal", ExpressionValue{Kind: ExpressionKindLiteral, Literal: "loop.revision_id"}, false},
		{"computed", ExpressionValue{Kind: ExpressionKindCEL, CEL: "string(loop.revision_id)"}, false},
		{"suffix", RefExpression("loop.revision_id.extra"), false},
		{"zero", ExpressionValue{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CarriesLoopRevision(tc.value); got != tc.want {
				t.Fatalf("CarriesLoopRevision = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLoopRevisionCarriageSchema(t *testing.T) {
	for _, tc := range []struct {
		name, kind, required string
		present, want        bool
	}{
		{"text", "text", "revision", true, true},
		{"string", "string", "revision", true, true},
		{"accepted_type_normalization", " TEXT ", "revision", true, true},
		{"optional", "text", "", true, false},
		{"different_required", "text", "other", true, false},
		{"required_not_normalized", "text", " revision ", true, false},
		{"numeric", "integer", "revision", true, false},
		{"missing_property", "text", "revision", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := EventPayloadSpec{Properties: map[string]EventFieldSpec{}, Required: []string{tc.required}}
			if tc.present {
				payload.Properties["revision"] = EventFieldSpec{Type: tc.kind}
			}
			if got := RequiresLoopRevision(payload, "revision"); got != tc.want {
				t.Fatalf("RequiresLoopRevision = %v, want %v", got, tc.want)
			}
		})
	}
}
