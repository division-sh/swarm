package runtimepersistence_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type s6CreationActivityStore interface {
	dynamicFlowCreationAtomicityStore
	authoractivity.Reader
}

type s6CreationActivityFixture struct {
	store     s6CreationActivityStore
	bus       *bus.EventBus
	ctx       context.Context
	source    semanticview.Source
	plan      pipeline.DynamicFlowRuntimeReadinessPlan
	attempt   pipeline.DynamicFlowRuntimeActivationAttempt
	parent    events.Event
	event     events.Event
	workflow  *pipeline.PipelineCoordinator
	construct func(context.Context, pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, pipeline.DynamicFlowRuntimeActivationAttempt)
}

func newS6CreationActivityFixture(t *testing.T, backend, frontier string) s6CreationActivityFixture {
	t.Helper()
	var selected s6CreationActivityStore
	switch backend {
	case "sqlite":
		selected = storetest.StartSQLiteRuntimeStore(t)
	case "postgres":
		selected = storetest.StartPostgresRuntimeStore(t)
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	repo := pipeline.WorkflowRepoRoot()
	rootPath := canonicalrouting.CopySelectedForkLocalReadiness(t, 0, "node")
	if frontier == "own_schema" {
		path := filepath.Join(rootPath, "worker-flow/schema.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var declaration yaml.Node
		if err := yaml.Unmarshal(raw, &declaration); err != nil {
			t.Fatal(err)
		}
		// Preserve the authored stage order while changing only the creation input.
		mapping := declaration.Content[0]
		for i := 0; i < len(mapping.Content); i += 2 {
			key, value := mapping.Content[i], mapping.Content[i+1]
			for j := 0; j < len(value.Content) && (key.Value == "auto_emit_on_create" || key.Value == "pins"); j += 2 {
				switch {
				case key.Value == "auto_emit_on_create" && value.Content[j].Value == "event":
					value.Content[j+1].Value = "worker.inspect"
				case key.Value == "pins" && value.Content[j].Value == "inputs":
					value.Content[j+1].Content = append(value.Content[j+1].Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "worker.inspect"})
				}
			}
		}
		raw, err = yaml.Marshal(&declaration)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, rootPath, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	childScope, found := source.FlowScopeByID("worker-flow")
	if !found || childScope.AutoEmitEvent == "" {
		t.Fatal("fixture omitted the child's authored creation emission")
	}
	_, ownSchema := childScope.Events[childScope.AutoEmitEvent]
	if ownSchema != (frontier == "own_schema") {
		t.Fatal("fixture lost its reference-only/own-schema distinction")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	run := uuid.NewString()
	ctx := authoractivity.WithScope(correlation.WithRunID(correlation.WithSourceArtifactFact(context.Background(), fact), run), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	ctx = effects.WithExecutionMode(ctx, executionmode.Live)
	ctx = storeTestWorkContext(t, ctx)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: run, BundleHash: fact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC()})
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := authoractivity.ScopeFromContext(ctx)
	catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	routes, err := bus.DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, RouteTable: routes})
	if err != nil {
		t.Fatal(err)
	}
	workflow := pipeline.NewPipelineCoordinatorWithOptions(publisher, pipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live, Module: dynamicFlowCreationWorkflowModule{source: source},
		Persistence: pipeline.NewWorkflowPersistence(selected), RunLifecycle: selected, DeliveryStore: selected, DeadLetters: selected,
		PipelineObligations: selected.PipelineObligations(), DecisionCards: selected, ProposedEffects: selected, HumanTasks: selected,
		DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected, DeliveryRuntime: publisher, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	am := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(publisher, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact, SemanticSource: source,
		WorkflowInstances: workflow, DeliveryStore: selected, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
	}, selected))
	process, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "s6-creation-activity", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: set.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	construct := func(ctx context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, pipeline.DynamicFlowRuntimeActivationAttempt) {
		plan, err := am.PrepareFlowInstanceActivation(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		committed, err := publisher.CommitFlowInstanceActivation(ctx, plan)
		if err != nil || !committed.Acknowledged || !committed.Created {
			t.Fatalf("canonical construction: %+v err=%v", committed, err)
		}
		row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, plan.Readiness.RunID, plan.Identity.Route())
		if err != nil || !found {
			t.Fatalf("constructed readiness: found=%t err=%v", found, err)
		}
		admitted, err := selected.BeginDynamicFlowRuntimeActivation(ctx, pipeline.NewDynamicFlowRuntimeActivationRequest(row.Plan, row.AttemptOrdinal, row.AttemptState, binding))
		if err != nil || !admitted.Acknowledged {
			t.Fatalf("attachment admission: %+v err=%v", admitted, err)
		}
		owner, err := plan.Readiness.FlowIdentity()
		if err != nil {
			t.Fatal(err)
		}
		requestRoute := bus.FlowInstanceRouteMaterializationRequest{Identity: owner, Instance: plan.Identity}
		staged, err := publisher.StageFlowInstanceRouteContext(ctx, requestRoute)
		if err != nil || !staged.Acknowledged {
			t.Fatalf("stage exact constructed route: %+v err=%v", staged, err)
		}
		if ready, err := completeFlowAttachmentFixture(ctx, workflow, admitted.Attempt, time.Now().UTC()); err != nil || !ready.Acknowledged {
			t.Fatalf("component attachment: %+v err=%v", ready, err)
		}
		if _, err := publisher.PublishPersistedFlowInstanceRouteForAttempt(ctx, requestRoute, admitted.Attempt); err != nil {
			t.Fatal(err)
		}
		return plan, admitted.Attempt
	}
	root := flowidentity.Stored(source, ".", run, run, "", "")
	construct(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: root, OccurredAt: time.Now().UTC()})
	parent := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.recorded", "operator-api", "", []byte(`{"worker_id":"worker-001"}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
	if err := publisher.Publish(ctx, parent); err != nil {
		t.Fatal(err)
	}
	waitS6CreationPublication(t, ctx, publisher)
	child, err := flowidentity.KeyedChild(source, root, "worker-flow", "worker-001")
	if err != nil {
		t.Fatal(err)
	}
	activation, attempt := construct(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: child, ConstructorInput: childScope.AutoEmitEvent, ResolvedKey: "worker-001", TriggerEvent: parent, OccurredAt: time.Now().UTC()})
	event := newS6CreationActivityEvent(t, source, activation.Readiness)
	return s6CreationActivityFixture{store: selected, bus: publisher, ctx: ctx, source: source, plan: activation.Readiness, attempt: attempt, parent: parent, event: event, workflow: workflow, construct: construct}
}

func newS6CreationActivityEvent(t *testing.T, source semanticview.Source, plan pipeline.DynamicFlowRuntimeReadinessPlan) events.Event {
	t.Helper()
	creation := plan.CreationEvent
	if creation == nil {
		t.Fatal("fixture omitted actual auto-emit")
	}
	route := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
	routing, err := pinrouting.AdmitFlowExecutionRoutingSource(source, plan.RunID, plan.Identity, route)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewChildEvent(events.ChildEventInput{Facts: events.EventFacts{ID: creation.EventID, Type: events.EventType(creation.EventType), Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "flow-instance-activator"}, Payload: creation.Payload, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{FlowInstance: route.FlowInstance, EntityID: route.EntityID}, route), RoutingSource: routing, CreatedAt: creation.CreatedAt, ExecutionMode: executionmode.Live}, Lineage: events.EventLineage{RunID: plan.RunID, ParentEventID: creation.ParentEventID, ExecutionMode: executionmode.Live}})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func (f s6CreationActivityFixture) commit(event events.Event) error {
	return f.bus.CommitDynamicFlowRuntimeCreationOccurrence(f.ctx, pipeline.DynamicFlowRuntimeCreationOccurrenceRequest{RunID: f.plan.RunID, InstancePath: f.plan.Identity.InstancePath, Plan: f.plan, Attempt: f.attempt, Event: event, OccurredAt: time.Now().UTC()})
}

func waitS6CreationPublication(t *testing.T, ctx context.Context, publisher *bus.EventBus) {
	t.Helper()
	join, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := publisher.WaitForQuiescence(join); err != nil {
		t.Fatal(err)
	}
}

func (f s6CreationActivityFixture) activity(t *testing.T, flow string) []authoractivity.Occurrence {
	t.Helper()
	page, err := f.store.ListAuthorActivity(f.ctx, authoractivity.ListOptions{RunID: f.plan.RunID, FlowID: flow, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Occurrences) == 100 {
		t.Fatal("fixture activity page was truncated")
	}
	return page.Occurrences
}

func TestS6ReferenceOnlyCreationEmitsNativeActivityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, frontier := range []string{"node", "own_schema"} {
			t.Run(backend+"/"+frontier, func(t *testing.T) {
				f := newS6CreationActivityFixture(t, backend, frontier)
				if err := f.commit(f.event); err != nil {
					t.Fatal(err)
				}
				waitS6CreationPublication(t, f.ctx, f.bus)
				var creation, parent authoractivity.Occurrence
				count := 0
				all := f.activity(t, "")
				for _, item := range all {
					if item.Kind != authoractivity.KindEventEmitted {
						continue
					}
					if item.SourceIdentity == f.parent.ID() {
						parent = item
					}
					if item.SourceIdentity == f.event.ID() {
						creation = item
						count++
					}
				}
				if count != 1 || creation.SourceOwner != "events" || creation.DedupKey != "emit:"+f.event.ID() || creation.Sequence <= parent.Sequence || parent.Sequence <= 0 || creation.FlowID != f.plan.Identity.InstancePath || creation.EntityID != f.plan.Identity.EntityID {
					t.Fatalf("native creation emission has no exact child/order occurrence: count=%d creation=%+v parent=%+v", count, creation, parent)
				}
				child := f.activity(t, f.plan.Identity.InstancePath)
				found := false
				for _, item := range child {
					found = found || item.OccurrenceID == creation.OccurrenceID
				}
				if !found {
					t.Fatal("child story omitted its creation emission")
				}
				for _, item := range f.activity(t, f.plan.RunID) {
					if item.OccurrenceID == creation.OccurrenceID {
						t.Fatal("child creation emission appeared in the parent story")
					}
				}
				stored, found, err := f.store.LoadPreparedPublishEvent(f.ctx, f.event.ID())
				if err != nil || !found || len(stored.DeliveryRoutes) != 1 {
					t.Fatalf("native creation/delivery: found=%t routes=%+v err=%v", found, stored.DeliveryRoutes, err)
				}
				if route := stored.DeliveryRoutes[0]; !route.Recipient.IsNode() || !route.Target.ExistingEntity() || !events.SameRouteIdentity(route.Target.Route(), f.event.SourceRoute()) {
					t.Fatalf("creation delivery lost its exact constructed receiver: %+v", route)
				}
				row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, f.plan.RunID, f.plan.Identity.Route())
				if err != nil || !found || row.CreationEventEmittedAt.IsZero() {
					t.Fatalf("creation completion: %+v found=%t err=%v", row, found, err)
				}
				if err := f.commit(f.event); err != nil {
					t.Fatal(err)
				}
				waitS6CreationPublication(t, f.ctx, f.bus)
				if after := f.activity(t, ""); !reflect.DeepEqual(after, all) {
					t.Fatalf("exact creation replay changed activity: before=%+v after=%+v", all, after)
				}
				conflict, err := events.NewChildEvent(events.ChildEventInput{
					Facts: events.EventFacts{ID: f.event.ID(), Type: f.event.Type(), Producer: events.ProducerClaim{Type: f.event.ProducerType(), ID: f.event.SourceAgent()},
						Payload: f.event.Payload(), Envelope: f.event.Envelope(), RoutingSource: f.event.RoutingSource(), CreatedAt: f.event.CreatedAt().Add(time.Microsecond), ExecutionMode: f.event.ExecutionMode()},
					Lineage: events.EventLineage{RunID: f.event.RunID(), ParentEventID: f.event.ParentEventID(), ExecutionMode: f.event.ExecutionMode()},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.commit(conflict); !errors.Is(err, events.ErrEventIdentityConflict) {
					t.Fatalf("conflicting creation replay did not refuse its immutable identity: %v", err)
				}
				afterEvent, found, err := f.store.LoadPreparedPublishEvent(f.ctx, f.event.ID())
				if err != nil || !found || !reflect.DeepEqual(afterEvent, stored) {
					t.Fatalf("creation replay/refusal changed publication or deliveries: found=%t err=%v", found, err)
				}
				afterRow, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, f.plan.RunID, f.plan.Identity.Route())
				if err != nil || !found || !reflect.DeepEqual(afterRow, row) || !reflect.DeepEqual(f.activity(t, ""), all) {
					t.Fatalf("creation replay/refusal changed readiness or activity: found=%t err=%v", found, err)
				}
			})
		}
	}
}
