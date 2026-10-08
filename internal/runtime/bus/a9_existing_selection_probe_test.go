package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA9TriageExistingSelectionUsesStoredConstructionAndEdgeKey(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	graph := pinrouting.CompileConnectGraph(source)
	var plan pinrouting.ConnectRoutePlan
	for _, candidate := range graph.Plans() {
		if candidate.ReceiverEndpoint().Readback().FlowID == "parent" {
			plan = candidate
		}
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	stored, err := flowidentity.KeyedChild(source, root, "parent", "stored-one")
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.ValidateConstruction(source, busInternalTestRunID); err != nil {
		t.Fatalf("existing constructor owner rejects setup: %v", err)
	}
	table := &RouteTable{instanceOwners: map[flowidentity.RunScopedFlowInstance]flowidentity.Instance{}}
	for _, owner := range []flowidentity.Instance{root, stored} {
		coordinate, err := flowidentity.NewRunScopedFlowInstance(busInternalTestRunID, owner.Route())
		if err != nil {
			t.Fatal(err)
		}
		table.instanceOwners[coordinate] = owner
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("existing-edge-key"), "start", "test", "", []byte(`{"parent_key":"left","leaf_key":"same"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	owner := newTemplateInstanceLifecycleOwner(source, table, nil)
	materialized, selected, _, err := owner.Materialize(withConnectRoutePlanPreview(context.Background()), event, plan,
		map[string]string{"payload.parent_key": "left", "payload.leaf_key": "same"},
		[]pinrouting.Descriptor{{EntityID: stored.EntityID, FlowInstance: stored.InstancePath, AddressFields: map[string]string{"entity.id": "left"}}})
	if err != nil || !materialized.Failure.Empty() || selected.identity != stored {
		t.Fatalf("per-edge key failed to reuse admitted stored construction: failure=%v selection=%+v want=%+v err=%v", materialized.Failure, selected, stored, err)
	}
}

func TestA9TriageSchemalessAncestryConsumesAdmittedFlowNodes(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyNestedProducerTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{
		Mode: canonicalrouting.SelectResolutionSelect,
	}))
	scope, found := source.FlowScopeByID("left")
	if !found || scope.Mode != "static" {
		t.Fatalf("admitted descendant did not establish its keyless ancestor: %+v found=%t", scope, found)
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, flowidentity.EntityID(busInternalTestRunID), "")
	if _, err := flowidentity.KeylessChild(source, root, "left"); err != nil {
		t.Fatalf("constructor rejected admitted schema-omitted FlowNode: %v", err)
	}
}
