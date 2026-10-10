package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func workflowPersistenceForTest(store *workflowInstanceStore) WorkflowPersistence {
	return WorkflowPersistence{store: store}
}

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

func testPipelineRunContextNoSeed(t *testing.T) context.Context {
	t.Helper()
	return runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
}

func materializedWorkflowInstanceForTest(instance WorkflowInstance) WorkflowInstance {
	occurredAt := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	instance.Fields = cloneStringAnyMap(instance.Fields)
	if instance.Fields == nil {
		instance.Fields = map[string]any{}
	}
	instance.Bookkeeping = cloneStringAnyMap(instance.Bookkeeping)
	if instance.Bookkeeping == nil {
		instance.Bookkeeping = map[string]any{}
	}
	instance.Gates = cloneWorkflowGates(instance.Gates)
	storageRef := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	if storageRef != "" {
		instance.InstanceID = runtimeflowidentity.LogicalInstanceID(storageRef)
	} else {
		canonicalRoute := strings.Trim(strings.TrimSpace(instance.WorkflowName), "/")
		if canonicalRoute != "" {
			instance.StorageRef = canonicalRoute
			instance.InstanceID = runtimeflowidentity.LogicalInstanceID(canonicalRoute)
			storageRef = canonicalRoute
		}
	}
	if strings.TrimSpace(instance.EntityID) == "" && storageRef != "" {
		instance.EntityID = FlowInstanceEntityID(storageRef)
	}
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = occurredAt
	}
	if instance.CreatedAt.IsZero() {
		instance.CreatedAt = occurredAt
	}
	return instance
}

func testEntityContractsForType(entityType string) runtimecontracts.EntityContractsDocument {
	return runtimecontracts.EntityContractsDocument{
		strings.TrimSpace(entityType): {Fields: map[string]runtimecontracts.EntityFieldDecl{}},
	}
}

func testRootEntityContractSource(workflowName, entityType string) semanticview.Source {
	root := runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: runtimecontracts.FlowSchemaDocument{Name: strings.TrimSpace(workflowName)},
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics:    runtimecontracts.WorkflowSemanticView{Name: strings.TrimSpace(workflowName)},
		RootEntities: testEntityContractsForType(entityType),
		RootSchema:   &root.Schema,
		FlowTree: runtimecontracts.FlowTree{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
		},
	})
}

func admitSyntheticEntityContractsForTest(
	t *testing.T,
	base *runtimecontracts.WorkflowContractBundle,
	rootEntityType string,
	flowEntityTypes map[string]string,
) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	if base == nil {
		t.Fatal("synthetic contract bundle is required")
	}
	flowIDs := make([]string, 0, len(flowEntityTypes))
	for flowID := range flowEntityTypes {
		flowIDs = append(flowIDs, strings.TrimSpace(flowID))
	}
	sort.Strings(flowIDs)
	files := map[string]string{
		"schema.yaml": "name: synthetic-contract\n",
	}
	for _, flowID := range flowIDs {
		entityType := strings.TrimSpace(flowEntityTypes[flowID])
		if flowID == "" || entityType == "" {
			t.Fatalf("synthetic flow entity contract requires nonblank flow and entity type: flow=%q type=%q", flowID, entityType)
		}
		files[""+flowID+"/schema.yaml"] = fmt.Sprintf("name: %s\nstages:\n  active: {}\n", flowID)
		files[""+flowID+"/entities.yaml"] = fmt.Sprintf("%s:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n", entityType)
	}
	if rootEntityType = strings.TrimSpace(rootEntityType); rootEntityType != "" {
		files["entities.yaml"] = fmt.Sprintf("%s: {}\n", rootEntityType)
	}

	admitted := loadWorkflowTempBundle(t, files)
	admitted.Semantics = base.Semantics
	admitted.Nodes = base.Nodes
	admitted.Events = base.Events
	admitted.Agents = base.Agents
	admitted.Tools = base.Tools
	admitted.Policy = base.Policy
	admitted.RootTypes = base.RootTypes
	admitted.Platform = base.Platform
	if base.RootSchema != nil {
		admitted.RootSchema = base.RootSchema
	}
	if base.FlowSchemas != nil {
		admitted.FlowSchemas = base.FlowSchemas
	}
	if base.FlowTree.Root != nil {
		admitted.FlowTree = base.FlowTree
	}
	if failures := admitted.PrepareFanOutPlans(); len(failures) != 0 {
		t.Fatalf("prepare synthetic fan-out plans: %v", failures)
	}
	return admitted
}

func configureWorkflowLifecycleForTest(t testing.TB, pc *PipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.workflowStore == nil {
		return
	}
	if pc.workflowTimers == nil {
		pc.workflowTimers = newWorkflowTimerLifecycle(pc.workflowStore, pc.SemanticSource(), pc.bus, pc.workOwner, pc.timerScheduler, executionposture.Live)
	}
	if pc.workflowStore.lifecycleOwner == nil {
		pc.workflowStore.lifecycleOwner = pipelineWorkflowLifecycleOwner{coordinator: pc}
	}
}

func testPipelineCoordinatorRunContext(t *testing.T, pc *PipelineCoordinator) context.Context {
	t.Helper()
	if pc == nil {
		t.Fatal("test pipeline coordinator run context requires coordinator")
	}
	configureWorkflowLifecycleForTest(t, pc)
	ctx := testPipelineRunContextNoSeed(t)
	if !pc.runtimeReceiver {
		return ctx
	}
	bound, err := pc.receiverExecution.Bind(ctx, executionmode.Live)
	if err != nil {
		t.Fatalf("bind test Pipeline receiver execution: %v", err)
	}
	return bound
}

func testWorkflowSourceEnvelope(flowID, instancePath, entityID string) events.EventEnvelope {
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath)
	return events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: instancePath,
		EntityID:     entityID,
	})
}

func testWorkflowRoutingSource(flowID, instancePath, entityID string) events.RoutingSource {
	if flowID != "." && instancePath != flowID {
		return eventtest.ConcreteTemplateRoutingSource(flowID, instancePath, entityID)
	}
	return eventtest.StaticFlowRoutingSource(flowID, instancePath, entityID)
}

func testWorkflowStateTransitionContext(ctx context.Context, route runtimeflowidentity.Route, entityID, eventType string) context.Context {
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), route.InstancePath)
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType(strings.TrimSpace(eventType)), "test", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", envelope, time.Now().UTC(),
	)
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}
