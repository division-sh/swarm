package runforkexecution

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type materializedTargetTestStore interface {
	bus.EventStore
	bus.FlowInstanceActivationCommitOwner
	pipeline.WorkflowPersistenceOwner
	runlifecycle.OperationOwner
	storetest.RunFixtureStore
}

// This bridges the real fork recipient producer to native target planning. It
// does not claim selected-fork execution, attachment, or persistence admission.
func TestSelectedForkMaterializedTargetLookupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected materializedTargetTestStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			for _, topology := range []string{"root", "nested-keyed"} {
				t.Run(topology, func(t *testing.T) { proveMaterializedTargetLookup(t, selected, topology) })
			}
		})
	}
}

func proveMaterializedTargetLookup(t *testing.T, selected materializedTargetTestStore, topology string) {
	t.Helper()
	rootPath := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	if topology == "nested-keyed" {
		rootPath = canonicalrouting.CopyNestedKeyedConnectionSelection(t)
	}
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: rootPath, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := loader.LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	ctx := correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), runID), loaded.SourceArtifactFact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
	bundle, _ := semanticview.Bundle(loaded.Source)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, BundleHash: loaded.SourceArtifactFact.BundleHash()})
	root := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	constructMaterializedTarget(t, ctx, selected, loaded, root, "")
	target := root
	node := mustRunForkRootNode("item-handler")
	localEvent := "item.received"
	missingPath := uuid.NewString()
	if topology == "nested-keyed" {
		parent, err := flowidentity.KeyedChild(loaded.Source, root, "parent", "parent")
		if err != nil {
			t.Fatal(err)
		}
		constructMaterializedTarget(t, ctx, selected, loaded, parent, "parent")
		middle, err := flowidentity.KeylessChild(loaded.Source, parent, "parent/middle")
		if err != nil {
			t.Fatal(err)
		}
		constructMaterializedTarget(t, ctx, selected, loaded, middle, "")
		for _, key := range []string{"selected", "sibling"} {
			leaf, err := flowidentity.KeyedChild(loaded.Source, middle, "parent/middle/leaf", key)
			if err != nil {
				t.Fatal(err)
			}
			constructMaterializedTarget(t, ctx, selected, loaded, leaf, key)
			if key == "selected" {
				target = leaf
			}
		}
		node, err = identity.AdmitExecutableNodeDeclaration("parent/middle/leaf", "receiver")
		if err != nil {
			t.Fatal(err)
		}
		localEvent = "start"
		missing, err := flowidentity.KeyedChild(loaded.Source, middle, "parent/middle/leaf", "missing")
		if err != nil {
			t.Fatal(err)
		}
		missingPath = missing.InstancePath
	}
	workflow := pipeline.NewWorkflowPersistence(selected)
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: target.Route()}
	lookup, err := pipeline.NewExactFlowInstanceLookup(loaded.Source, loaded.SourceArtifactFact, owner)
	if err != nil {
		t.Fatal(err)
	}
	before, found, err := workflow.LookupFlowInstance(ctx, lookup)
	if err != nil || !found {
		t.Fatalf("constructed target not observed: found=%t err=%v", found, err)
	}
	process := worklifetime.NewProcess()
	work, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runForkTestRuntimeInstanceID, BundleHash: loaded.SourceArtifactFact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = work.RetireAndWait(context.Background()) })
	path := target.InstancePath
	explicitFlow := ""
	materializer := func(_ context.Context, _ events.Event, _ bus.PublishRecipientPlan) ([]bus.DeliveryRouteBlueprint, error) {
		routes, err := selectedContractNodeDeliveryRoutes(loaded.Source, localEvent, []runfork.RunForkContractFrontierRecipient{
			testNodeFrontierRecipient(node, events.EventType(localEvent), path, "subscription"),
		})
		if err == nil && len(routes) == 1 && explicitFlow != "" {
			routes[0].Target.FlowID = explicitFlow
		}
		return routes, err
	}
	eventBus, err := bus.NewEphemeralEventBusWithOptions(selected, bus.EventBusOptions{
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact, RuntimeInstanceID: runForkTestRuntimeInstanceID,
		WorkOwner: work, RecipientPlanMaterializer: materializer,
		ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live,
		Durable: bus.DurableDependencies{Instances: workflow, ConstructionPublications: workflow, RunLifecycle: selected},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eventBus.ResetInMemoryState() })
	lineage, err := events.NewSelectedForkLineage(runID, uuid.NewString(), uuid.NewString(), "selected-contract-test", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	route := events.RouteIdentity{FlowID: target.TemplateID, FlowInstance: target.InstancePath, EntityID: target.EntityID}
	routing := eventtest.RootRoutingSource(runID)
	if topology != "root" {
		routing = eventtest.ConcreteTemplateRoutingSource(route.FlowID, route.FlowInstance, route.EntityID)
	}
	eventType := loaded.Source.ResolveFlowEventReference(target.TemplateID, localEvent)
	if topology != "root" {
		eventType = eventidentity.ExternalizeForFlow(target.InstancePath, []string{localEvent}, localEvent)
	}
	event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: events.EventType(eventType),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runfork.RunForkSelectedContractExecutionOwner},
		Payload:  []byte(`{}`), RoutingSource: routing, CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
	}, Lineage: lineage})
	if err != nil {
		t.Fatal(err)
	}
	for _, form := range []string{"path-only", "empty"} {
		if form == "empty" && topology != "root" {
			continue
		}
		t.Run(form, func(t *testing.T) {
			if form == "empty" {
				path = ""
			}
			plan, err := eventBus.PrepareSelectedForkPublish(ctx, event, bus.SelectedInputValidation{})
			routes := plan.DeliveryRoutes()
			if err != nil || len(routes) != 1 || routes[0].Target != events.MustExistingEntityTarget(route) {
				t.Fatalf("%s materialized target: routes=%+v err=%v want=%+v", form, routes, err, route)
			}
			if err := eventBus.AbandonPreparedPublish(ctx, plan); err != nil {
				t.Fatal(err)
			}
		})
	}
	path = missingPath
	if _, err := eventBus.PrepareSelectedForkPublish(ctx, event, bus.SelectedInputValidation{}); err == nil {
		t.Fatal("foreign or absent materialized target was admitted")
	}
	path = target.InstancePath
	explicitFlow = "other"
	if _, err := eventBus.PrepareSelectedForkPublish(ctx, event, bus.SelectedInputValidation{}); err == nil {
		t.Fatal("materialized target contradicted its admitted handler declaration")
	}
	explicitFlow = ""
	foreign, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eventBus.PrepareSelectedForkPublish(correlation.WithSourceArtifactFact(ctx, foreign), event, bus.SelectedInputValidation{}); err == nil {
		t.Fatal("materialization crossed admitted source evidence")
	}
	wrongLineage, err := events.NewSelectedForkLineage(uuid.NewString(), lineage.SourceRunID(), lineage.SourceEventID(), "selected-contract-test", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	wrongEvent, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: event.Type(), Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runfork.RunForkSelectedContractExecutionOwner},
		Payload: event.Payload(), RoutingSource: routing, CreatedAt: event.CreatedAt(), ExecutionMode: executionmode.Live,
	}, Lineage: wrongLineage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eventBus.PrepareSelectedForkPublish(ctx, wrongEvent, bus.SelectedInputValidation{}); err == nil {
		t.Fatal("materialization borrowed a receiver from another run")
	}
	after, found, err := workflow.LookupFlowInstance(ctx, lookup)
	if err != nil || !found || after.Identity() != before.Identity() || after.InstanceKey() != before.InstanceKey() {
		t.Fatalf("planning changed native construction: found=%t before=%+v after=%+v err=%v", found, before.Identity(), after.Identity(), err)
	}
	beforeHeader, err := before.WorkflowInstance()
	if err != nil {
		t.Fatal(err)
	}
	afterHeader, err := after.WorkflowInstance()
	if err != nil || !reflect.DeepEqual(beforeHeader, afterHeader) {
		t.Fatalf("target planning mutated the native header or fields: before=%+v after=%+v err=%v", beforeHeader, afterHeader, err)
	}
}

func constructMaterializedTarget(t *testing.T, ctx context.Context, selected materializedTargetTestStore, loaded LoadedSelectedContractSource, target flowidentity.Instance, key string) {
	t.Helper()
	input := ""
	if key != "" {
		input = "start"
	}
	payload := map[string]any{"parent_key": key, "leaf_key": key}
	constructor, err := pipeline.CompileFlowConstructor(loaded.Source, target.TemplateID, input)
	if err != nil {
		t.Fatal(err)
	}
	var resolved any
	if input != "" {
		resolved = key
	} else {
		payload = nil
	}
	fields, err := constructor.InitialFields(payload, resolved)
	if err != nil {
		t.Fatal(err)
	}
	instanceKey, err := pipeline.AdmitFlowInstanceKey(loaded.Source, target.TemplateID, resolved)
	if err != nil {
		t.Fatal(err)
	}
	graph, found := semanticview.WorkflowStageTopology(loaded.Source, target.TemplateID)
	if !found {
		t.Fatal("constructor lacks its compiled stages")
	}
	stage, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	schema, _ := loaded.Source.FlowSchemaByID(target.TemplateID)
	entity, _ := entityruntime.ResolveForFlow(loaded.Source, target.TemplateID)
	at := time.Now().UTC()
	initialized := pipeline.WorkflowInstance{
		WorkflowName: target.TemplateID, WorkflowVersion: loaded.Source.WorkflowVersion(), Mode: schema.EffectiveMode(),
		InstanceID: target.InstanceID, StorageRef: target.InstancePath, EntityID: target.EntityID, InstanceKey: instanceKey,
		ParentFlowID: target.ParentRoute.FlowID, ParentFlowInstance: target.ParentRoute.FlowInstance, ParentEntityID: target.ParentEntityID,
		EntityType: entity.EntityType, CurrentState: stage.ID(), StageDefined: graph.StageCount() != 0, Fields: fields, CreatedAt: at, EnteredStageAt: at,
	}
	command, err := flowactivationfixture.Command(effects.WithExecutionMode(ctx, executionmode.Live), initialized, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct native materializer receiver: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
}
