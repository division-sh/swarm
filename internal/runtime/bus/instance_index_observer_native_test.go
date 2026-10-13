package bus_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestR7NativeObserverCreationOrdersPreserveCompiledDeclarationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, observerFirst := range []bool{false, true} {
			order := "sources_first"
			if observerFirst {
				order = "observer_first"
			}
			t.Run(backend+"/"+order, func(t *testing.T) {
				verifyNativeObserverCreationOrder(t, backend, observerFirst)
			})
		}
	}
}

func verifyNativeObserverCreationOrder(t *testing.T, backend string, observerFirst bool) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyConstructibleObserverDependencies(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	table, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	definitions := table.ConnectDeclarationDefinitions("observer")
	if len(definitions) != 3 {
		t.Fatalf("observer declarations=%d, want both source families and its constructor", len(definitions))
	}
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
	selected := newScalarTemplateInstanceParityStore(t, backend, ctx)
	at := time.Now().UTC()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
	constructed := map[string]runtimeflowidentity.Instance{}
	originalReceipts := map[string]runtimepipeline.FlowConstructionPublicationEvidence{}
	index := runtimepipeline.FlowInstanceIndexReader(selected)
	construct := func(flow, key string) runtimeflowidentity.Instance {
		t.Helper()
		identity := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flow, key, runID)
		if flow != "." {
			identity.EntityID = uuid.NewString()
		}
		instance := runtimepipeline.WorkflowInstance{
			InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
			ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
			WorkflowName: flow, WorkflowVersion: source.WorkflowVersion(), InstanceKey: key, EnteredStageAt: at, CreatedAt: at,
		}
		if contract, present := entityruntime.ResolveForFlow(source, flow); present {
			constructor, err := runtimepipeline.CompileFlowConstructor(source, flow, "item.created")
			if err != nil {
				t.Fatal(err)
			}
			instance.EntityType = contract.EntityType
			instance.Fields, err = constructor.InitialFields(map[string]any{"item_id": key}, key)
			if err != nil {
				t.Fatal(err)
			}
		}
		seedComponentFlowConstruction(t, ctx, selected, source, instance)
		constructed[identity.InstancePath] = identity
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, identity.Route())
		if err != nil {
			t.Fatal(err)
		}
		request, err := runtimepipeline.NewExactFlowInstanceLookup(source, testSourceArtifactFact(source), owner)
		if err != nil {
			t.Fatal(err)
		}
		observation, found, err := index.LookupFlowInstance(ctx, request)
		if err != nil || !found || observation.Identity() != identity {
			t.Fatalf("original native construction absent or changed: found=%t identity=%+v err=%v", found, observation.Identity(), err)
		}
		receipt, native, err := observation.NativeConstruction()
		if err != nil || !native || receipt.Identity != identity {
			t.Fatalf("original native construction receipt invalid: native=%t receipt=%+v err=%v", native, receipt, err)
		}
		originalReceipts[identity.InstancePath] = receipt
		if !reflect.DeepEqual(definitions, table.ConnectDeclarationDefinitions("observer")) {
			t.Fatal("native construction changed compiled observer declarations")
		}
		return identity
	}
	construct(".", "")
	var observer runtimeflowidentity.Instance
	if observerFirst {
		observer = construct("observer", "one")
	}
	var producers []runtimeflowidentity.Instance
	for _, flow := range []string{"producer", "other"} {
		for _, key := range []string{"old-a", "old-b"} {
			producers = append(producers, construct(flow, key))
		}
	}
	if !observerFirst {
		observer = construct("observer", "one")
	}
	eventBus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source, RouteTable: table})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(producer runtimeflowidentity.Instance) {
		t.Helper()
		local := "work.ready"
		if producer.TemplateID == "other" {
			local = "other.ready"
		}
		event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(producer.InstancePath+"/"+local), "fixture", "", json.RawMessage(`{"item_id":"one"}`), 0, runID, events.EventEnvelope{},
			eventtest.ConcreteTemplateRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID), time.Now().UTC())
		preview, err := eventBus.CheckPublishRecipientPlan(ctx, event)
		want := events.RouteIdentity{FlowID: "observer", FlowInstance: observer.InstancePath, EntityID: observer.EntityID}
		if err != nil || preview.TargetFailure != "" || len(preview.DeliveryRoutes) != 1 || preview.DeliveryRoutes[0].Target.Route() != want {
			t.Fatalf("native observer preview=%+v err=%v, want exact stored receiver %+v", preview, err, want)
		}
		if err := eventBus.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
		routes, err := selected.ListEventDeliveryRoutes(ctx, event.ID())
		if err != nil || len(routes) != 1 || !reflect.DeepEqual(routes[0], preview.DeliveryRoutes[0]) || routes[0].Recipient.LocalID() != "observe" || routes[0].ConnectClaim.Empty() {
			t.Fatalf("native observer delivery=%+v err=%v", routes, err)
		}
		if !reflect.DeepEqual(definitions, table.ConnectDeclarationDefinitions("observer")) {
			t.Fatal("publication specialized or replaced the source declarations")
		}
	}
	for _, producer := range producers {
		publish(producer)
	}
	newProducer := construct("producer", "new")
	publish(newProducer)
	for _, producer := range producers {
		publish(producer)
	}
	lookup, err := runtimepipeline.NewFlowInstanceLookupScope(source, testSourceArtifactFact(source), runID, []string{".", "producer", "other", "observer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := selected.ListFlowInstances(ctx, lookup)
	if err != nil || len(observations) != len(constructed) {
		t.Fatalf("constructed inventory=%d err=%v, want%d", len(observations), err, len(constructed))
	}
	for _, observation := range observations {
		identity := observation.Identity()
		receipt, native, err := observation.NativeConstruction()
		if err != nil || !native || !observation.Valid() || observation.SourceFact() != testSourceArtifactFact(source) || identity != constructed[identity.InstancePath] || !reflect.DeepEqual(receipt, originalReceipts[identity.InstancePath]) {
			t.Fatalf("publication changed native construction: identity=%+v err=%v", identity, err)
		}
	}
}
