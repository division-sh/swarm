package accumulator

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestA2AccumulatorKeyRequiresRequiredCatalogScalarField(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Name: "accumulator-test"},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"BusinessRecord": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}, "number": {Type: "integer"}}},
			"CollectedItem":  {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}, "number": {Type: "integer"}, "record": {Type: "BusinessRecord"}, "event_id": {Type: "text"}}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{"work": {Fields: map[string]runtimecontracts.EntityFieldDecl{"items": {Type: "[CollectedItem]", IsOptional: true}}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Type: "object", Properties: map[string]runtimecontracts.EventFieldSpec{
				"id": {Type: "text"}, "number": {Type: "integer"}, "record": {Type: "BusinessRecord"}, "event_id": {Type: "text"},
			}, Required: []string{"id", "number", "record", "event_id"}}},
		},
	})
	bundle, _ := semanticview.Bundle(source)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "collector")
	for _, key := range []string{"id", "number", "event_id"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Target: "items", Key: key}); err != nil {
			t.Fatalf("key %q: %v (schema: %#v)", key, err, semanticview.ResolveEventSchema(source, node.FlowPath(), "item.received"))
		}
	}
	for _, key := range []string{"record", "", "payload.id", "record.number", "payload.record.absent", "payload.absent", "event.id", "payload", "payload..id", "payload.id.", " payload.id", "payload. id", "${payload.id}"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Target: "items", Key: key}); err == nil {
			t.Fatalf("key %q admitted without a required catalog scalar field", key)
		}
	}
	if err := ValidateSpecForHandler(source, node, "unknown", &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}); err == nil {
		t.Fatal("unknown event catalog admitted a business key")
	}
}
