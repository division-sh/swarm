package bootverify

import (
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

// Invariant: an instance state list has one accumulating handler and no ordinary
// authored writer; the negative reaches cover both competing writer classes.
func TestAccumulateTargetRejectsAdditionalWriters(t *testing.T) {
	for _, kind := range []string{"single_owner", "second_accumulator", "ordinary_write"} {
		t.Run(kind, func(t *testing.T) {
			bundle := &c.WorkflowContractBundle{
				RootTypes: c.TypeCatalogDocument{Types: map[string]c.NamedTypeDecl{
					"Review": {Fields: map[string]c.TypeFieldSpec{"reviewer_id": {Type: "text"}}},
				}},
				RootEntities: c.EntityContractsDocument{"state": {Fields: map[string]c.EntityFieldDecl{
					"reviews": {Type: "[Review]"},
				}}},
				Nodes: map[string]c.SystemNodeContract{"collector": {EventHandlers: map[string]c.SystemNodeEventHandler{
					"review.received": {Accumulate: &c.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}},
				}}},
				Events: map[string]c.EventCatalogEntry{
					"review.received": {Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"reviewer_id": {Type: "text"}}, Required: []string{"reviewer_id"}}},
					"review.reset":    {},
				},
			}
			want := ""
			switch kind {
			case "second_accumulator":
				bundle.Nodes["second"] = c.SystemNodeContract{EventHandlers: map[string]c.SystemNodeEventHandler{
					"review.received": {Accumulate: &c.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}},
				}}
				want = "multiple writers"
			case "ordinary_write":
				bundle.Nodes["reset"] = c.SystemNodeContract{EventHandlers: map[string]c.SystemNodeEventHandler{
					"review.reset": {DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetField: "reviews", Value: c.LiteralExpression([]any{})}}}},
				}}
				want = "ordinary writer"
			}
			checker := &checkerContext{source: compileBootverifyRootSource(bundle)}
			findings := checker.accumulatorEntityProjection()
			if want == "" && len(findings) != 0 {
				t.Fatalf("single owner refused: %+v", findings)
			}
			if want != "" {
				found := false
				for _, finding := range findings {
					found = found || strings.Contains(finding.Message, want)
				}
				if !found {
					t.Fatalf("missing %s refusal: %+v", want, findings)
				}
			}
		})
	}
}
