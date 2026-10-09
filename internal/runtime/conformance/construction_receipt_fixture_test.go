package conformance

import (
	"context"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
)

// Component fixtures admit explicit construction facts independently of their
// active receiver lists. Native construction proofs use the selected store.
type conformanceConstructionReceipts map[flowidentity.RunScopedFlowInstance]pipeline.FlowInstanceObservation

func (r conformanceConstructionReceipts) add(t *testing.T, source semanticview.Source, runID string, instance flowidentity.Instance, key string) {
	t.Helper()
	if err := instance.ValidateConstruction(source, runID); err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: instance.Route()}
	if previous, found := r[owner]; found && previous.Identity() != instance {
		t.Fatal("fixture construction evidence changed its exact owner")
	}

	fact := conformanceSourceArtifactFact(t, source)
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, owner)
	if err != nil {
		t.Fatal(err)
	}
	schema, found := source.FlowSchemaByID(instance.TemplateID)
	if !found {
		t.Fatal("construction fixture has no declaration")
	}
	at := time.Unix(1700000000, 0).UTC()
	header := pipeline.WorkflowInstance{
		WorkflowName: instance.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), Status: "active",
		InstanceID: instance.InstanceID, StorageRef: instance.InstancePath, EntityID: instance.EntityID, InstanceKey: key,
		ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance, ParentEntityID: instance.ParentEntityID,
		CurrentState: "active", Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	if entity, declared := entityruntime.ResolveForFlow(source, instance.TemplateID); declared {
		header.EntityType = entity.EntityType
	}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := pipeline.FlowConstructionPublicationEvidence{Identity: instance, InstanceKey: key}
	readiness := pipeline.DynamicFlowRuntimeReadiness{Plan: pipeline.DynamicFlowRuntimeReadinessPlan{Identity: instance, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live}, OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active"}
	observation, err := pipeline.AdmitNativeFlowInstanceObservation(request, header, run, 1, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	r[owner] = observation
}

func (r conformanceConstructionReceipts) LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowConstructionPublicationEvidence{}, err
	}
	if owner != owner.Normalize() || owner.Validate() != nil || entityID == "" {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("fixture observation requires its exact owner")
	}
	evidence, found := r[owner]
	if !found || evidence.Identity().EntityID != entityID {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("fixture construction observation is absent")
	}
	receipt, native, err := evidence.NativeConstruction()
	if err != nil || !native {
		return pipeline.FlowConstructionPublicationEvidence{}, fmt.Errorf("fixture lacks native construction: %w", err)
	}
	return receipt, nil
}

func (r conformanceConstructionReceipts) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	var selected pipeline.FlowInstanceObservation
	for owner, observed := range r {
		identity := observed.Identity()
		if owner.RunID != request.RunID() || identity.TemplateID != request.FlowID() {
			continue
		}
		if request.ExactPath() != "" && request.ExactPath() != identity.InstancePath || request.DeclaredSelection() && (identity.ParentRoute.FlowInstance != request.ParentInstance() || observed.InstanceKey() != request.InstanceKey()) {
			continue
		}
		if err := observed.ValidateSelection(request); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
		if selected.Valid() {
			return pipeline.FlowInstanceObservation{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), Cause: fmt.Errorf("duplicate native fixture selector")}
		}
		selected = observed
	}
	return selected, selected.Valid(), nil
}

func (r conformanceConstructionReceipts) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var selected []pipeline.FlowInstanceObservation
	for owner, observed := range r {
		if owner.RunID != scope.RunID() {
			continue
		}
		included := slices.Contains(scope.FlowIDs(), observed.Identity().TemplateID)
		for _, coordinate := range scope.Coordinates() {
			if owner.Key() != coordinate.Key() {
				continue
			}
			request, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), coordinate)
			if err != nil {
				return nil, err
			}
			if err := observed.ValidateSelection(request); err != nil {
				return nil, err
			}
			included = true
		}
		if included {
			selected = append(selected, observed)
		}
	}
	return selected, nil
}

func TestConformanceConstructionReceiptsRequireExactOwner(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	runID := eventtest.UUID("conformance-construction-observation")
	root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: root.Route()}
	receipts := conformanceConstructionReceipts{}
	receipts.add(t, source, runID, root, "")
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
