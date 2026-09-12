package pipeline

import (
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntitySnapshotHydrationPreservesSparseFields(t *testing.T) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{RootEntities: rc.EntityContractsDocument{
		"work": {Fields: map[string]rc.EntityFieldDecl{"note": {Type: "text", IsOptional: true, Initial: "seed"}}},
	}})
	for _, fields := range []map[string]any{nil, {}, {"note": ""}} {
		snapshot, found, err := workflowInstanceEngineStateSnapshot(source, "", identity.NormalizeEntityID("entity-1"), WorkflowInstance{EntityType: "work", Fields: fields})
		if err != nil {
			t.Fatal(err)
		}
		if !found || len(snapshot.StateCarrier.Fields) != len(fields) {
			t.Fatalf("hydrate=%#v, found=%v", snapshot.StateCarrier.Fields, found)
		}
	}
	for _, fields := range []map[string]any{{"note": nil}, {"note": 17}, {"unknown": "x"}} {
		if _, _, err := workflowInstanceEngineStateSnapshot(source, "", identity.NormalizeEntityID("entity-1"), WorkflowInstance{EntityType: "work", Fields: fields}); err == nil {
			t.Fatalf("malformed snapshot accepted: %#v", fields)
		}
	}
}
