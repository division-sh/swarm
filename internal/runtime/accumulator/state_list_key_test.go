package accumulator

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func Test2293Q1AccumulateAdmitsRequiredScalarItemKeys(t *testing.T) {
	for _, typeRef := range []string{"text", "uuid", "timestamp", "integer", "numeric", "boolean", "Rating"} {
		t.Run(typeRef, func(t *testing.T) {
			source := accumulateQ1KeySource(t, typeRef, true)
			if err := ValidateSpecForHandler(source, identitytest.RootNode(t, "collector"), "review.received", &runtimecontracts.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}); err != nil {
				t.Fatalf("required %s item key refused: %v", typeRef, err)
			}
		})
	}
}

func Test2293Q1AccumulateRefusesNonRequiredScalarItemKeys(t *testing.T) {
	for _, tc := range []struct {
		name, typeRef, key string
		required           bool
	}{
		{"missing_key", "text", "", true},
		{"rooted_key", "text", "payload.reviewer_id", true},
		{"optional_key", "text", "reviewer_id", false},
		{"list_key", "[text]", "reviewer_id", true},
		{"map_key", "map[text]text", "reviewer_id", true},
		{"object_key", "Review", "reviewer_id", true},
		{"dynamic_key", "json", "reviewer_id", true},
		{"absent_key", "text", "absent", true},
		{"nested_key", "Review", "reviewer_id.id", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := accumulateQ1KeySource(t, tc.typeRef, tc.required)
			err := ValidateSpecForHandler(source, identitytest.RootNode(t, "collector"), "review.received", &runtimecontracts.AccumulateSpec{Target: "reviews", Key: tc.key})
			if err == nil {
				t.Fatal("accumulate accepted a key that is not a required scalar item field")
			}
			if tc.name == "rooted_key" && !strings.Contains(err.Error(), "key: reviewer_id") {
				t.Fatalf("rooted key refusal lost canonical teaching: %v", err)
			}
		})
	}
}

func accumulateQ1KeySource(t *testing.T, typeRef string, required bool) semanticview.Source {
	t.Helper()
	payload := runtimecontracts.EventPayloadSpec{Type: "object", Properties: map[string]runtimecontracts.EventFieldSpec{
		"reviewer_id": {Type: typeRef}, "message": {Type: "text"},
	}, Required: []string{"message"}}
	if required {
		payload.Required = append(payload.Required, "reviewer_id")
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Name: "state-list-key"},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Enums: map[string]runtimecontracts.EnumTypeDecl{"Rating": {Values: []string{"low", "high"}, Default: "low"}},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Review": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}}},
				"CollectedItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{
					"reviewer_id": {Type: typeRef, IsOptional: !required}, "message": {Type: "text"},
				}},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{"work": {Fields: map[string]runtimecontracts.EntityFieldDecl{
			"reviews": {Type: "[CollectedItem]", IsOptional: true},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"review.received": {Payload: payload}},
	}
	source := semanticviewtest.WrapRootAgents(bundle)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	resolution := semanticview.ResolveEventSchema(source, ".", "review.received")
	field, found := resolution.Field("reviewer_id")
	if !found || field.IsOptional == required {
		t.Fatalf("fixture lost the declared key presence: %#v", resolution)
	}
	return source
}
