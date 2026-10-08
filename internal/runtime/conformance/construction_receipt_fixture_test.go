package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
)

// Component fixtures admit explicit construction facts independently of their
// active receiver lists. Native construction proofs use the selected store.
type conformanceConstructionReceipts map[flowidentity.RunScopedFlowInstance]pipeline.FlowConstructionPublicationEvidence

func (r conformanceConstructionReceipts) add(t *testing.T, source semanticview.Source, runID string, instance flowidentity.Instance) {
	t.Helper()
	if err := instance.ValidateConstruction(source, runID); err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: instance.Route()}
	if previous, found := r[owner]; found && previous.Identity != instance {
		t.Fatal("fixture construction evidence changed its exact owner")
	}
	r[owner] = pipeline.FlowConstructionPublicationEvidence{Identity: instance}
}

func (r conformanceConstructionReceipts) LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	if owner != owner.Normalize() || owner.Validate() != nil || entityID == "" {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("fixture observation requires its exact owner")
	}
	evidence, found := r[owner]
	if !found || evidence.Identity.EntityID != entityID {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("fixture construction observation is absent")
	}
	return evidence, nil
}

func TestConformanceConstructionReceiptsRequireExactOwner(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	runID := eventtest.UUID("conformance-construction-observation")
	root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: root.Route()}
	receipts := conformanceConstructionReceipts{}
	receipts.add(t, source, runID, root)
	got, err := receipts.LoadFlowConstructionPublication(context.Background(), owner, root.EntityID)
	if err != nil || got.Identity != root {
		t.Fatalf("exact independent observation = %+v %v", got, err)
	}
	for _, test := range []struct {
		name   string
		owner  flowidentity.RunScopedFlowInstance
		entity string
	}{
		{name: "missing owner", entity: root.EntityID},
		{name: "foreign run", owner: flowidentity.RunScopedFlowInstance{RunID: eventtest.UUID("foreign-run"), Route: root.Route()}, entity: root.EntityID},
		{name: "foreign route", owner: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.DeriveRoute("portfolio", "")}, entity: root.EntityID},
		{name: "missing entity", owner: owner},
		{name: "foreign entity", owner: owner, entity: eventtest.UUID("foreign-entity")},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := receipts.LoadFlowConstructionPublication(context.Background(), test.owner, test.entity)
			if err == nil || !reflect.DeepEqual(got, pipeline.FlowConstructionPublicationEvidence{}) {
				t.Fatalf("invalid observation = %+v %v", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := receipts.LoadFlowConstructionPublication(ctx, owner, root.EntityID); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, pipeline.FlowConstructionPublicationEvidence{}) {
		t.Fatalf("cancelled observation = %+v %v", got, err)
	}
}
