package workflowexpr

import (
	"reflect"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityAssignmentExpressionFactsRespectScope(t *testing.T) {
	for _, tc := range []struct {
		expression string
		facts      []string
		want       []string
	}{
		{"state.score + 1", nil, []string{"score"}},
		{"state.score + 1", []string{"state.score"}, nil},
		{"has(state.score) && state.score > 0", nil, nil},
		{"state.?score.orValue(0)", nil, nil},
		{"has(state.record.score) && state.record.score > 0", nil, []string{"record"}},
		{"[1].exists(entity, entity > 0)", []string{"state.score"}, nil},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			got := RequiredEntityReferences(tc.expression, tc.facts)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("references=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestEntityAssignmentKnownPresenceDoesNotChangeDeclaration(t *testing.T) {
	typ := c.ResolvedCatalogType{Kind: c.CatalogTypeObject, Fields: []c.ResolvedCatalogField{
		{Name: "score", Type: c.ResolvedCatalogType{Kind: c.CatalogTypeInteger}, IsOptional: true},
	}}
	options := ValueExpressionOptions{EntityType: &typ}
	if err := ValidateValueExpressionWithOptions("state.score + 1", options); err == nil {
		t.Fatal("unproved optional read accepted")
	}
	options.KnownPresence = []string{"state.score"}
	if err := ValidateValueExpressionWithOptions("state.score + 1", options); err != nil {
		t.Fatal(err)
	}
	if !typ.Fields[0].IsOptional {
		t.Fatal("program fact mutated declaration")
	}
	options.KnownPresence = nil
	if err := ValidateValueExpressionWithOptions("state.score + 1", options); err == nil {
		t.Fatal("proof survived into another program point")
	}
}

func TestEntityAssignmentStageNarrowingIsBounded(t *testing.T) {
	for _, tc := range []struct {
		expression string
		truth      bool
		want       bool
	}{
		{"instance.stage == 'ready'", true, true},
		{"instance.stage == 'ready'", false, false},
		{"instance.stage == 'other' && state.score > 0", true, false},
		{"instance.stage == 'other' || state.score > 0", true, true},
		{"!(instance.stage != 'ready')", true, true},
		{"payload.event_name == 'ready'", true, true},
		{"payload.event_name == 'ready'", false, true},
	} {
		if got := ConditionCanHoldAtStage(tc.expression, "ready", tc.truth); got != tc.want {
			t.Errorf("%s truth=%t: %t, want %t", tc.expression, tc.truth, got, tc.want)
		}
	}
}
