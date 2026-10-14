package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

type poolBoundPublicationStore struct {
	storeTestDurableEventBusStore
	db      *sql.DB
	passes  int
	command *bus.PublicationCommand
	refusal error
}

func (s *poolBoundPublicationStore) CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error) {
	s.command = &command
	return bus.CommittedPublication{}, s.refusal
}

func (s *poolBoundPublicationStore) WithFlowInstancePlanningSnapshot(ctx context.Context, scope pipeline.FlowInstanceLookupScope, prepare func(context.Context, pipeline.FlowInstanceIndexReader) error) error {
	// The direct entry already owns its publication lease here. Leave exactly
	// one additional connection, without constraining that prior lease setup.
	capacity := s.db.Stats().MaxOpenConnections
	s.db.SetMaxOpenConns(s.db.Stats().InUse + 1)
	defer s.db.SetMaxOpenConns(capacity)
	s.passes++
	return s.storeTestDurableEventBusStore.(pipeline.FlowInstancePlanningSnapshotOwner).WithFlowInstancePlanningSnapshot(ctx, scope, prepare)
}

func TestOrdinaryInstancePlanningConstructorUsesOneConnectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			index := pipeline.NewWorkflowPersistence(f.store.(pipeline.WorkflowPersistenceOwner))
			scope, err := pipeline.NewFlowInstanceLookupScope(request.Lookup.Source(), request.Lookup.SourceFact(), request.Lookup.RunID(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			capacity := f.db.Stats().MaxOpenConnections
			f.db.SetMaxOpenConns(f.db.Stats().InUse + 1)
			defer f.db.SetMaxOpenConns(capacity)
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			control, err := pipeline.PrepareFlowInstanceSelection(ctx, index, f.manager, request)
			if err != nil || control.Activation == nil {
				t.Fatalf("unscoped constructor control: %+v %v", control, err)
			}
			var retained pipeline.FlowInstanceIndexReader
			waits := f.db.Stats().WaitCount
			if err := index.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				retained = reader
				selected, err := pipeline.PrepareFlowInstanceSelection(ctx, reader, f.manager, request)
				if err == nil && (selected.Activation == nil || !reflect.DeepEqual(selected.Activation, control.Activation)) {
					t.Fatal("scoped preparation changed the real constructor or initial lifecycle")
				}
				return err
			}); err != nil {
				t.Fatalf("real constructor requires a second free connection: %v", err)
			}
			if f.db.Stats().WaitCount != waits {
				t.Fatal("constructor queued an independent pool connection inside the index cut")
			}
			timers, ok := retained.(pipeline.FlowInstancePlanningInitialTimerReader)
			if !ok {
				t.Fatal("constructor read was not composed through the finite timer owner")
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: request.Lookup.RunID(), Route: control.Identity().Route()}
			if values, err := timers.ListFlowInstancePlanningInitialTimers(ctx, owner, control.Identity().EntityID); err == nil || len(values) != 0 {
				t.Fatal("initial-entry timer reader survived the index cut")
			}
			if _, err := f.store.(bus.ActiveAgentDescriptorLister).ListActiveAgentDescriptors(ctx, request.Lookup.RunID()); err != nil {
				t.Fatalf("ended constructor cut retained the only free connection: %v", err)
			}
		})
	}
}

func TestOrdinaryPublicationPlanningPoolBoundariesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
			files := make(map[string]string)
			for _, entry := range bundle.SourceArtifact.Entries() {
				files[entry.Label()] = string(entry.Bytes())
			}
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
			constructed, producer := constructR7PublicationReceiver(t, f, true)
			source := semanticview.Wrap(f.bundle)
			agents, err := f.store.(bus.ActiveAgentDescriptorLister).ListActiveAgentDescriptors(f.ctx, correlation.RunIDFromContext(f.ctx))
			if err != nil || len(agents) == 0 {
				t.Fatalf("projection control requires real durable agents: count=%d err=%v", len(agents), err)
			}
			publisher := f.newRuntimeEventBus(t, bus.EventBusOptions{TemplateInstancePlanner: f.manager})
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			capacity := f.db.Stats().MaxOpenConnections
			f.db.SetMaxOpenConns(f.db.Stats().InUse + 1)
			defer f.db.SetMaxOpenConns(capacity)
			for _, cell := range []struct {
				name, event, key string
			}{
				{"missing_constructor", "account.registered", "new-planning-receiver"},
				{"existing_projection", "account.notify.requested", "existing"},
			} {
				t.Run(cell.name, func(t *testing.T) {
					payload := map[string]any{"account_id": cell.key}
					if cell.name == "existing_projection" {
						payload["command"] = "inspect"
					}
					raw, err := json.Marshal(payload)
					if err != nil {
						t.Fatal(err)
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, cell.event)), notifyallchildren.OwnerFlowID, "", raw, 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID), time.Now().UTC())
					ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
					defer cancel()
					waits := f.db.Stats().WaitCount
					plan, err := publisher.CheckPublishRecipientPlan(ctx, event)
					if err != nil || len(plan.DeliveryRoutes) == 0 || plan.TargetFailure != "" {
						t.Fatalf("real publication planning exhausted the pool: %+v %v", plan, err)
					}
					if f.db.Stats().WaitCount != waits {
						t.Fatal("real publication attempted a nested pool connection")
					}
					canceled, release := context.WithCancel(f.ctx)
					release()
					if _, err := publisher.CheckPublishRecipientPlan(canceled, event); !errors.Is(err, context.Canceled) {
						t.Fatalf("publication planning lost cancellation: %v", err)
					}
				})
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("planning changed durable receiver/agent/timer evidence: %v", err)
			}
			// Direct publication uses the other callback entrance with the same
			// durable agent owner. Stop at the prepared command rather than launch
			// a provider: this proof covers the real pre-commit planner entrance.
			f.db.SetMaxOpenConns(capacity)
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			authorScope, _ := authoractivity.ScopeFromContext(f.ctx)
			catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(authorScope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			refusal := errors.New("effect-free direct commit refusal after real planning")
			boundedStore := &poolBoundPublicationStore{storeTestDurableEventBusStore: f.store.(storeTestDurableEventBusStore), db: f.db, refusal: refusal}
			evidence, err := f.grant.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
			authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, evidence.GrantID, evidence.RuntimeGeneration)
			if err != nil {
				t.Fatal(err)
			}
			direct, err := newStoreTestEventBus(t, boundedStore, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, RuntimeInstanceID: evidence.RuntimeInstanceID, DeliveryAuthority: authority})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, "account.notify.requested")), notifyallchildren.OwnerFlowID, "", []byte(`{"account_id":"existing","command":"inspect"}`), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID), time.Now().UTC())
			event = eventtest.TargetRouted(event, events.RouteIdentity{FlowID: constructed.Identity.TemplateID, FlowInstance: constructed.Identity.InstancePath, EntityID: agents[0].EntityID})
			beforeDirect, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			if err := direct.PublishDirect(ctx, event, []string{agents[0].Identity.AgentID()}); !errors.Is(err, refusal) {
				t.Fatalf("direct publication requires a second pool connection: %v", err)
			}
			if boundedStore.passes != 1 {
				t.Fatalf("direct publication bypassed the one-free-connection planning cut: %d", boundedStore.passes)
			}
			if boundedStore.command == nil {
				t.Fatal("direct publication did not reach its effect-free commit cut")
			}
			routes := boundedStore.command.Commit.DeliveryRoutes
			if len(routes) != 1 || routes[0].AgentIdentity != agents[0].Identity || routes[0].Target.Route().FlowInstance != constructed.Identity.InstancePath {
				t.Fatalf("direct planning changed the exact receiver command: %+v", routes)
			}
			if afterDirect, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(beforeDirect, afterDirect) {
				t.Fatalf("effect-free direct refusal changed durable evidence or retained a claim: %v", err)
			}
		})
	}
}

func TestOrdinaryInstancePlanningInitialTimerScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithTimer(t, backend, false, false, true, false)
			f.constructKeylessRoot(t)
			request := f.request("timer-key", "timer-receiver", "initial timer")
			control, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil || len(control.Lifecycle.Timers) != 1 {
				t.Fatalf("authored initial timer control: %+v %v", control.Lifecycle, err)
			}
			fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
			scope, err := pipeline.NewFlowInstanceLookupScope(request.ContractBundle, fact, control.Readiness.RunID, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			index := pipeline.NewWorkflowPersistence(f.store.(pipeline.WorkflowPersistenceOwner))
			capacity := f.db.Stats().MaxOpenConnections
			f.db.SetMaxOpenConns(f.db.Stats().InUse + 1)
			defer f.db.SetMaxOpenConns(capacity)
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			fault := errors.New("planning consumer refused after timer read")
			var retained pipeline.FlowInstancePlanningInitialTimerReader
			if err := index.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				retained = reader.(pipeline.FlowInstancePlanningInitialTimerReader)
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, request)
				if err != nil || !reflect.DeepEqual(plan, control) {
					t.Fatalf("scoped authored timer changed lifecycle: %+v %v", plan.Lifecycle, err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: control.Readiness.RunID, Route: control.Identity.Route()}
				foreign := owner
				foreign.RunID = uuid.NewString()
				if _, err := retained.ListFlowInstancePlanningInitialTimers(ctx, foreign, control.Identity.EntityID); err == nil {
					t.Fatal("timer reader admitted a foreign run")
				}
				foreign = owner
				foreign.Route.ScopeKey = "unknown-declaration"
				if _, err := retained.ListFlowInstancePlanningInitialTimers(ctx, foreign, control.Identity.EntityID); err == nil {
					t.Fatal("timer reader admitted an unknown declaration")
				}
				for _, entity := range []string{"", " " + control.Identity.EntityID} {
					if _, err := retained.ListFlowInstancePlanningInitialTimers(ctx, owner, entity); err == nil {
						t.Fatal("timer reader admitted an inexact entity")
					}
				}
				canceled, release := context.WithCancel(ctx)
				release()
				if _, err := retained.ListFlowInstancePlanningInitialTimers(canceled, owner, control.Identity.EntityID); !errors.Is(err, context.Canceled) {
					t.Fatalf("timer reader lost cancellation: %v", err)
				}
				return fault
			}); !errors.Is(err, fault) {
				t.Fatalf("planning failed to preserve the exact consumer refusal: %v", err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: control.Readiness.RunID, Route: control.Identity.Route()}
			if _, err := retained.ListFlowInstancePlanningInitialTimers(ctx, owner, control.Identity.EntityID); err == nil {
				t.Fatal("failed planning retained timer read authority")
			}
			if _, err := f.manager.PrepareFlowInstanceActivation(ctx, request); err != nil {
				t.Fatalf("failed planning retained the pool connection: %v", err)
			}
		})
	}
}
