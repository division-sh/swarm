package accprojection

import (
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func projectionResolverBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension": {Type: "text"},
						"score":     {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {
						Type:            "[DimensionScore]",
						MaterializeFrom: "valid-node.dimensions_received",
					},
					"bad_scores": {
						Type:            "[DimensionScore]",
						MaterializeFrom: "bad-node.missing_buffer",
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"valid-node": {
				StateSchema: runtimecontracts.NodeStateSchema{
					Fields: []runtimecontracts.NodeStateField{{Name: "dimensions_received", Type: "[DimensionScore]"}},
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
					},
				},
			},
			"bad-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.bad_dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "missing_buffer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"dimension": {Type: "text"},
					"score":     {Type: "integer"},
				}},
			},
			"score.bad_dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"dimension": {Type: "text"},
					"score":     {Type: "integer"},
				}},
			},
		},
	}
}

func TestForHandler_FiltersProjectionIssuesToActiveHandler(t *testing.T) {
	source := semanticview.Wrap(projectionResolverBundle())
	resolved := Resolve(source)
	if len(resolved.Issues) == 0 {
		t.Fatal("Resolve issues = 0, want global invalid bad-node declaration")
	}

	bindings, issues := ForHandler(source, "", "valid-node", "score.dimension_complete")
	if len(issues) != 0 {
		t.Fatalf("ForHandler valid issues = %#v, want none", issues)
	}
	if len(bindings) != 1 {
		t.Fatalf("ForHandler valid bindings = %d, want 1", len(bindings))
	}

	_, issues = ForHandler(source, "", "bad-node", "score.bad_dimension_complete")
	if !issuesContain(issues, "missing_buffer") {
		t.Fatalf("ForHandler bad issues = %#v, want missing_buffer issue", issues)
	}
}

func issuesContain(issues []Issue, want string) bool {
	for _, issue := range issues {
		if strings.Contains(issue.Message, want) {
			return true
		}
	}
	return false
}
