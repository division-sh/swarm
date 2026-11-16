package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type postgresScalarTemplateInstanceStore struct {
	*store.PostgresStore
	indexCalls  int
	indexErr    error
	lookupFault func(runtimepipeline.FlowInstanceLookupRequest) error
}

func (s *postgresScalarTemplateInstanceStore) LookupFlowInstance(ctx context.Context, request runtimepipeline.FlowInstanceLookupRequest) (runtimepipeline.FlowInstanceObservation, bool, error) {
	s.indexCalls++
	if s.indexErr != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, s.indexErr
	}
	if s.lookupFault != nil {
		if err := s.lookupFault(request); err != nil {
			return runtimepipeline.FlowInstanceObservation{}, false, err
		}
	}
	return s.PostgresStore.LookupFlowInstance(ctx, request)
}

func (s *postgresScalarTemplateInstanceStore) ListFlowInstances(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope) ([]runtimepipeline.FlowInstanceObservation, error) {
	s.indexCalls++
	if s.indexErr != nil {
		return nil, s.indexErr
	}
	return s.PostgresStore.ListFlowInstances(ctx, scope)
}

type sqliteScalarTemplateInstanceStore struct {
	*store.SQLiteRuntimeStore
	indexCalls  int
	indexErr    error
	lookupFault func(runtimepipeline.FlowInstanceLookupRequest) error
}

func (s *sqliteScalarTemplateInstanceStore) LookupFlowInstance(ctx context.Context, request runtimepipeline.FlowInstanceLookupRequest) (runtimepipeline.FlowInstanceObservation, bool, error) {
	s.indexCalls++
	if s.indexErr != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, s.indexErr
	}
	if s.lookupFault != nil {
		if err := s.lookupFault(request); err != nil {
			return runtimepipeline.FlowInstanceObservation{}, false, err
		}
	}
	return s.SQLiteRuntimeStore.LookupFlowInstance(ctx, request)
}

func (s *sqliteScalarTemplateInstanceStore) ListFlowInstances(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope) ([]runtimepipeline.FlowInstanceObservation, error) {
	s.indexCalls++
	if s.indexErr != nil {
		return nil, s.indexErr
	}
	return s.SQLiteRuntimeStore.ListFlowInstances(ctx, scope)
}

type scalarPlanningIndexReader struct {
	runtimepipeline.FlowInstanceIndexReader
	read        func() error
	lookupFault func(runtimepipeline.FlowInstanceLookupRequest) error
}

func (r scalarPlanningIndexReader) LookupFlowInstance(ctx context.Context, request runtimepipeline.FlowInstanceLookupRequest) (runtimepipeline.FlowInstanceObservation, bool, error) {
	if err := r.read(); err != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, err
	}
	if r.lookupFault != nil {
		if err := r.lookupFault(request); err != nil {
			return runtimepipeline.FlowInstanceObservation{}, false, err
		}
	}
	return r.FlowInstanceIndexReader.LookupFlowInstance(ctx, request)
}

func (r scalarPlanningIndexReader) ListFlowInstances(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope) ([]runtimepipeline.FlowInstanceObservation, error) {
	if err := r.read(); err != nil {
		return nil, err
	}
	return r.FlowInstanceIndexReader.ListFlowInstances(ctx, scope)
}

func (s *postgresScalarTemplateInstanceStore) WithFlowInstancePlanningSnapshot(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope, prepare func(context.Context, runtimepipeline.FlowInstanceIndexReader) error) error {
	return s.PostgresStore.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader runtimepipeline.FlowInstanceIndexReader) error {
		return prepare(ctx, scalarPlanningIndexReader{FlowInstanceIndexReader: reader, lookupFault: s.lookupFault, read: func() error {
			s.indexCalls++
			return s.indexErr
		}})
	})
}

func (s *sqliteScalarTemplateInstanceStore) WithFlowInstancePlanningSnapshot(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope, prepare func(context.Context, runtimepipeline.FlowInstanceIndexReader) error) error {
	return s.SQLiteRuntimeStore.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader runtimepipeline.FlowInstanceIndexReader) error {
		return prepare(ctx, scalarPlanningIndexReader{FlowInstanceIndexReader: reader, lookupFault: s.lookupFault, read: func() error {
			s.indexCalls++
			return s.indexErr
		}})
	})
}

type scalarTemplateInstanceParityStore interface {
	componentFlowConstructionStore
	storetest.RunFixtureStore
	runtimepipeline.FlowInstanceIndexReader
	runtimebus.PreparedPublishEventReader
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	setScalarTemplateInstanceIndexError(error)
	resetScalarTemplateInstanceIndexCalls()
	scalarTemplateInstanceIndexCalls() int
}

func (s *postgresScalarTemplateInstanceStore) setScalarTemplateInstanceIndexError(err error) {
	s.indexErr = err
}

func (s *postgresScalarTemplateInstanceStore) resetScalarTemplateInstanceIndexCalls() {
	s.indexCalls = 0
}

func (s *postgresScalarTemplateInstanceStore) scalarTemplateInstanceIndexCalls() int {
	return s.indexCalls
}

func (s *sqliteScalarTemplateInstanceStore) setScalarTemplateInstanceIndexError(err error) {
	s.indexErr = err
}

func (s *sqliteScalarTemplateInstanceStore) resetScalarTemplateInstanceIndexCalls() {
	s.indexCalls = 0
}

func (s *sqliteScalarTemplateInstanceStore) scalarTemplateInstanceIndexCalls() int {
	return s.indexCalls
}

func TestScalarTemplateInstanceResolutionPersistsAndReplaysOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				shape canonicalrouting.CrossInstanceConnectionShape
			}{
				{"same_flow_parent_binding", canonicalrouting.CrossInstanceSameKeyedFlow},
				{"nested_parent_binding", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_account_absent", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_receiver_unavailable", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_foreign_run", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_account_read_error", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_receiver_read_error", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_intermediate_read_error", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_interleaved_plans", canonicalrouting.CrossInstanceNestedKeyless},
				{"nested_recorded_replay", canonicalrouting.CrossInstanceNestedKeyless},
			} {
				t.Run(tc.name, func(t *testing.T) {
					proveCrossInstanceNativeBoundary(t, backend, tc.name, tc.shape)
				})
			}
			for _, fixture := range []canonicalrouting.ArtifactID{canonicalrouting.TemplateSelectExisting, canonicalrouting.TemplateSelectOrCreate} {
				t.Run(string(fixture), func(t *testing.T) {
					repo := canonicalrouting.RepoRoot(t)
					root := canonicalrouting.CopyExample(t, fixture)
					bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
					if err != nil {
						t.Fatalf("load scalar resolution fixture: %v", err)
					}
					source := semanticview.Wrap(bundle)
					sourceFact := testSourceArtifactFact(source)
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
					selected := newScalarTemplateInstanceParityStore(t, backend, ctx)
					ctx = originalEventBusFixtureContext(t, ctx, selected)
					storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: sourceFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC().Add(-time.Minute)})
					entityID := runtimeflowidentity.EntityID("account/one")
					constructor, err := runtimepipeline.CompileFlowConstructor(source, "account", "account.ready")
					if err != nil {
						t.Fatal(err)
					}
					fields, err := constructor.InitialFields(map[string]any{"account_id": "acct-1"}, "acct-1")
					if err != nil {
						t.Fatal(err)
					}
					contract, declared := entityruntime.ResolveForFlow(source, "account")
					if !declared {
						t.Fatal("account fixture lost its state contract")
					}
					at := time.Now().UTC()
					for _, flowID := range []string{".", "producer"} {
						constructed := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, "", runID)
						seedComponentFlowConstruction(t, ctx, selected, source, runtimepipeline.WorkflowInstance{
							InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
							ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
							WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), EnteredStageAt: at, CreatedAt: at,
						})
					}
					seedComponentFlowConstruction(t, ctx, selected, source, runtimepipeline.WorkflowInstance{
						InstanceID: "one", StorageRef: "account/one", EntityID: entityID, WorkflowName: "account", InstanceKey: "acct-1",
						WorkflowVersion: source.WorkflowVersion(), InstanceKind: "template", EntityType: contract.EntityType,
						ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: runtimeflowidentity.EntityID(runID),
						Fields: fields, EnteredStageAt: at, CreatedAt: at,
					})
					eventBus, err := newNativeScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}

					eventID := uuid.NewString()
					eventTime := time.Now().UTC()
					producerEntityID := runtimeflowidentity.EntityID("producer")
					evt := eventtest.ExistingRunRootIngressWithRoutingSource(
						eventID,
						events.EventType("producer/account.ready"),
						"producer",
						"",
						json.RawMessage(`{"account_id":"acct-1"}`),
						0,
						runID,
						events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
						eventtest.StaticFlowRoutingSource("producer", "producer", producerEntityID), eventTime,
					)
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, evt)
					if err != nil {
						t.Fatalf("CheckPublishRecipientPlan: %v", err)
					}
					wantTarget := events.RouteIdentity{FlowID: "account", FlowInstance: "account/one", EntityID: entityID}
					if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route().Normalized() != wantTarget.Normalized() {
						t.Fatalf("preflight failure/routes = %q/%#v, want scalar target %#v", plan.TargetFailure, plan.DeliveryRoutes, wantTarget)
					}
					if err := eventBus.Publish(ctx, evt); err != nil {
						t.Fatalf("Publish: %v", err)
					}
					persistedRoutes, err := selected.ListEventDeliveryRoutes(ctx, eventID)
					if err != nil {
						t.Fatalf("ListEventDeliveryRoutes: %v", err)
					}
					if len(persistedRoutes) != 1 || persistedRoutes[0].Recipient.LocalID() != "account-node" || persistedRoutes[0].Target.Route().Normalized() != wantTarget.Normalized() {
						t.Fatalf("persisted routes = %#v, want account-node at %#v", persistedRoutes, wantTarget)
					}

					selected.setScalarTemplateInstanceIndexError(errors.New("instance lookup must not run for a durable duplicate"))
					selected.resetScalarTemplateInstanceIndexCalls()
					if err := eventBus.Publish(ctx, evt); err != nil {
						t.Fatalf("Publish exact duplicate after index failure: %v", err)
					}
					if calls := selected.scalarTemplateInstanceIndexCalls(); calls != 0 {
						t.Fatalf("duplicate index calls = %d, want durable identity short-circuit", calls)
					}
					conflicting := eventtest.ExistingRunRootIngressWithRoutingSource(
						eventID,
						events.EventType("producer/account.ready"),
						"producer",
						"",
						json.RawMessage(`{"account_id":"acct-2"}`),
						0,
						runID,
						events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
						evt.RoutingSource(), eventTime,
					)
					if err := eventBus.Publish(ctx, conflicting); !errors.Is(err, events.ErrEventIdentityConflict) {
						t.Fatalf("Publish conflicting duplicate error = %v, want event identity conflict", err)
					}
					if calls := selected.scalarTemplateInstanceIndexCalls(); calls != 0 {
						t.Fatalf("conflicting duplicate index calls = %d, want durable identity short-circuit", calls)
					}
					selected.setScalarTemplateInstanceIndexError(nil)

					replayed := subscribeInternalDeliveriesForTest(t, eventBus, persistedRoutes[0].Recipient.ID())
					indexFailure := errors.New("committed scalar replay must not query the current instance index")
					selected.setScalarTemplateInstanceIndexError(indexFailure)
					selected.resetScalarTemplateInstanceIndexCalls()
					if _, err := eventBus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{
						Event: evt,
						Scope: runtimepipelineobligation.ScopeSubscribed,
					}, nil); err != nil {
						t.Fatalf("RecoverPersistedPipeline: %v", err)
					}
					select {
					case delivery := <-replayed:
						replayEvent := delivery.Event()
						if err := delivery.Complete(); err != nil {
							t.Fatal(err)
						}
						if replayEvent.FlowInstance() != "account/one" || replayEvent.EntityID() != entityID {
							t.Fatalf("replayed target = %q/%q, want account/one/%s", replayEvent.FlowInstance(), replayEvent.EntityID(), entityID)
						}
					case <-time.After(time.Second):
						t.Fatal("timed out waiting for committed scalar-resolution replay")
					}
					if calls := selected.scalarTemplateInstanceIndexCalls(); calls != 0 {
						t.Fatalf("replay index calls = %d, want persisted route authority", calls)
					}
					fresh := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), evt.Type(), "producer", "", evt.Payload(), 0, runID,
						events.EnvelopeForSourceRoute(events.EventEnvelope{}, evt.RoutingSource().Route()), evt.RoutingSource(), eventTime)
					if _, err := eventBus.CheckPublishRecipientPlan(ctx, fresh); !errors.Is(err, indexFailure) || selected.scalarTemplateInstanceIndexCalls() == 0 {
						t.Fatalf("fresh scalar plan bypassed poisoned index: calls=%d err=%v", selected.scalarTemplateInstanceIndexCalls(), err)
					}
					if persisted, found, err := selected.LoadPreparedPublishEvent(ctx, fresh.ID()); err != nil || found || len(persisted.DeliveryRoutes) != 0 {
						t.Fatalf("refused scalar preflight mutated storage: %+v found=%t err=%v", persisted, found, err)
					}
				})
			}
		})
	}
}

func proveCrossInstanceNativeBoundary(t *testing.T, backend, name string, shape canonicalrouting.CrossInstanceConnectionShape) {
	t.Helper()
	root := canonicalrouting.CopyCrossInstanceConnection(t, shape)
	boundOuter := strings.HasSuffix(name, "parent_binding")
	if boundOuter {
		root = canonicalrouting.CopyWrappedCrossInstanceConnection(t, shape, false)
		canonicalrouting.BindCrossInstanceOuterKey(t, root)
	} else {
		canonicalrouting.AddCrossInstanceKeylessIntermediate(t, root)
		if name == "nested_account_absent" {
			canonicalrouting.AddCrossInstancePreparedAccount(t, root)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
	selected := newScalarTemplateInstanceParityStore(t, backend, ctx)
	ctx = originalEventBusFixtureContext(t, ctx, selected)
	at := time.Now().UTC()
	ensureRun := func(id string) {
		t.Helper()
		storetest.RequireRun(t, runtimecorrelation.WithRunID(ctx, id), selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: id, Artifact: bundle.SourceArtifact, StartedAt: at})
	}
	seed := func(id, flow, key string, parent runtimeflowidentity.Instance) runtimeflowidentity.Instance {
		t.Helper()
		var identity runtimeflowidentity.Instance
		var err error
		schema, found := source.FlowSchemaByID(flow)
		if !found {
			t.Fatalf("undeclared native fixture flow %q", flow)
		}
		switch {
		case flow == ".":
			identity = runtimeflowidentity.Stored(source, ".", id, id, runtimeflowidentity.EntityID(id), "")
		case !schema.Instance.Empty():
			identity, err = runtimeflowidentity.KeyedChild(source, parent, flow, key)
		default:
			identity, err = runtimeflowidentity.KeylessChild(source, parent, flow)
		}
		if err != nil {
			t.Fatal(err)
		}
		instance := runtimepipeline.WorkflowInstance{InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID, WorkflowName: flow, WorkflowVersion: source.WorkflowVersion(), InstanceKey: key, CreatedAt: at, EnteredStageAt: at}
		if !schema.Instance.Empty() {
			instance.InstanceKind = "template"
		}
		if entity, present := entityruntime.ResolveForFlow(source, flow); present {
			constructor, err := runtimepipeline.CompileFlowConstructor(source, flow, "")
			if err != nil {
				t.Fatal(err)
			}
			payload := map[string]any{}
			if !schema.Instance.Empty() {
				payload[schema.Instance.Path()] = key
			}
			instance.Fields, err = constructor.InitialFields(payload, key)
			if err != nil {
				t.Fatal(err)
			}
			instance.EntityType = entity.EntityType
		}
		seedComponentFlowConstruction(t, runtimecorrelation.WithRunID(ctx, id), selected, source, instance)
		return identity
	}
	ensureRun(runID)
	parent := seed(runID, ".", "", runtimeflowidentity.Instance{})
	prefix := ""
	if boundOuter {
		prefix = "envelope/"
		parent = seed(runID, "envelope", "left", parent)
	}
	accountFlow := prefix + "account"
	sender := seed(runID, accountFlow, "7", parent)
	var alternateReceiver runtimeflowidentity.Instance
	if name == "nested_interleaved_plans" {
		bridge := seed(runID, accountFlow+"/bridge", "", sender)
		alternateReceiver = seed(runID, accountFlow+"/bridge/audit", "", bridge)
	}
	if shape == canonicalrouting.CrossInstanceNestedKeyless {
		sender = seed(runID, accountFlow+"/ticket", "42", sender)
	}
	targetParent, targetRun := parent, runID
	if name == "nested_foreign_run" {
		targetRun = uuid.NewString()
		ensureRun(targetRun)
		targetParent = seed(targetRun, ".", "", runtimeflowidentity.Instance{})
	}
	var target runtimeflowidentity.Instance
	if name != "nested_account_absent" {
		target = seed(targetRun, accountFlow, "8", targetParent)
		if shape == canonicalrouting.CrossInstanceNestedKeyless {
			if !boundOuter {
				target = seed(targetRun, accountFlow+"/bridge", "", target)
			}
			flow := accountFlow + "/audit"
			if !boundOuter {
				flow = accountFlow + "/bridge/audit"
			}
			target = seed(targetRun, flow, "", target)
		}
	}
	if boundOuter {
		rootOwner := runtimeflowidentity.Stored(source, ".", runID, runID, runtimeflowidentity.EntityID(runID), "")
		otherParent := seed(runID, "envelope", "right", rootOwner)
		other := seed(runID, accountFlow, "8", otherParent)
		if shape == canonicalrouting.CrossInstanceNestedKeyless {
			other = seed(runID, accountFlow+"/audit", "", other)
		}
		if other.EntityID == target.EntityID || target.ParentEntityID == sender.EntityID {
			t.Fatal("repeated-key parent proof lost distinct structural owners")
		}
	}
	var native any
	setReadFault := func(fault func(runtimepipeline.FlowInstanceLookupRequest) error) {
		switch owner := selected.(type) {
		case *sqliteScalarTemplateInstanceStore:
			native, owner.lookupFault = owner.SQLiteRuntimeStore, fault
		case *postgresScalarTemplateInstanceStore:
			native, owner.lookupFault = owner.PostgresStore, fault
		}
	}
	setReadFault(nil)
	if name == "nested_receiver_unavailable" {
		if count, err := storetest.SetWorkflowProjectionDraining(ctx, native, runID, target.InstancePath); err != nil || count != 1 {
			t.Fatalf("unavailable receiver fixture changed %d headers: %v", count, err)
		}
	}
	before, err := selected.ListWorkflowInstances(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var beforeForeign []runtimepipeline.WorkflowInstance
	if targetRun != runID {
		beforeForeign, err = selected.ListWorkflowInstances(runtimecorrelation.WithRunID(ctx, targetRun), targetRun)
		if err != nil {
			t.Fatal(err)
		}
	}
	readFailure := errors.New("independent cross-instance read failure")
	faultReads := 0
	accountLookups := 0
	if name == "nested_account_absent" {
		setReadFault(func(request runtimepipeline.FlowInstanceLookupRequest) error {
			if request.DeclaredSelection() && request.FlowID() == accountFlow {
				accountLookups++
			}
			return nil
		})
	}
	readError := strings.HasSuffix(name, "read_error")
	if readError {
		faultFlow := accountFlow
		if name == "nested_receiver_read_error" {
			faultFlow += "/bridge/audit"
		} else if name == "nested_intermediate_read_error" {
			faultFlow += "/bridge"
		}
		setReadFault(func(request runtimepipeline.FlowInstanceLookupRequest) error {
			if request.DeclaredSelection() && request.FlowID() == faultFlow {
				faultReads++
				return readFailure
			}
			return nil
		})
	}
	eventName := "account.cross"
	if shape == canonicalrouting.CrossInstanceNestedKeyless {
		eventName = "ticket.cross"
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(sender.InstancePath+"/"+eventName), "fixture", "", json.RawMessage(`{"target_account_id":"8"}`), 0, runID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource(sender.TemplateID, sender.InstancePath, sender.EntityID), at)
	opts := runtimebus.EventBusOptions{ContractBundle: source}
	var activationPlanner runtimepipeline.FlowInstanceActivationPlanner
	if name == "nested_account_absent" {
		opts.TemplateInstancePlanner = runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, request runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			return activationPlanner.PrepareFlowInstanceActivation(ctx, request)
		})
	}
	bus, err := newNativeScopedTestEventBus(t, selected, opts)
	if err != nil {
		t.Fatal(err)
	}
	if name == "nested_account_absent" {
		coordinator := newEventBusWorkflowCoordinator(bus, selected, newFixtureWorkflowModule(t, bundle))
		if coordinator == nil {
			t.Fatal("prepared-account control requires its original native construction owner")
		}
		planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
			ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: testSourceArtifactFact(source),
			SemanticSource: source, WorkflowInstances: coordinator, WorkOwner: nativeBusWorkOwner(t, ctx), ReceiverExecution: eventreceiver.NormalExecution(),
		})
		activationPlanner = planner
		t.Cleanup(func() {
			if err := planner.Shutdown(); err != nil {
				t.Error(err)
			}
		})
	}
	plan, err := bus.CheckPublishRecipientPlan(ctx, event)
	positive := boundOuter || name == "nested_recorded_replay" || name == "nested_interleaved_plans"
	switch {
	case readError:
		if !errors.Is(err, readFailure) || faultReads == 0 || len(plan.DeliveryRoutes) != 0 {
			t.Fatalf("independent lookup error became absence/route: reads=%d plan=%+v err=%v", faultReads, plan, err)
		}
		if err := bus.Publish(ctx, event); !errors.Is(err, readFailure) {
			t.Fatalf("native publication swallowed the named read error: %v", err)
		}
	case positive:
		want := events.RouteIdentity{FlowID: target.TemplateID, FlowInstance: target.InstancePath, EntityID: target.EntityID}
		if err != nil || plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route() != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
			t.Fatalf("cross-instance row escaped its recorded outer parent: plan=%+v target=%+v err=%v", plan, target, err)
		}
		if name == "nested_interleaved_plans" {
			other := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), event.Type(), "fixture", "", json.RawMessage(`{"target_account_id":"7"}`), 0, runID, events.EventEnvelope{}, event.RoutingSource(), at)
			otherPlan, err := bus.CheckPublishRecipientPlan(ctx, other)
			otherTarget := events.RouteIdentity{FlowID: alternateReceiver.TemplateID, FlowInstance: alternateReceiver.InstancePath, EntityID: alternateReceiver.EntityID}
			if err != nil || otherPlan.TargetFailure != "" || len(otherPlan.DeliveryRoutes) != 1 || otherPlan.DeliveryRoutes[0].Target.Route() != otherTarget || plan.DeliveryRoutes[0].Target.Route() != want {
				t.Fatalf("interleaved receiver selection changed an earlier plan: first=%+v other=%+v err=%v", plan, otherPlan, err)
			}
			if err := bus.Publish(ctx, other); err != nil {
				t.Fatal(err)
			}
			otherRoutes, err := selected.ListEventDeliveryRoutes(ctx, other.ID())
			if err != nil || !reflect.DeepEqual(otherRoutes, otherPlan.DeliveryRoutes) {
				t.Fatalf("interleaved publication lost its own immutable receiver: routes=%+v err=%v", otherRoutes, err)
			}
		}
		if err := bus.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
		routes, err := selected.ListEventDeliveryRoutes(ctx, event.ID())
		if err != nil || len(routes) != 1 || !reflect.DeepEqual(routes, plan.DeliveryRoutes) {
			t.Fatalf("native parent-bound route differs from immutable plan: %+v err=%v", routes, err)
		}
		stored, found, err := selected.LoadPreparedPublishEvent(ctx, event.ID())
		if err != nil || !found || stored.Event.Event().SourceRoute().EntityID != sender.EntityID {
			t.Fatalf("native parent-bound publication replaced its sender: %+v found=%t err=%v", stored, found, err)
		}
		if name == "nested_recorded_replay" {
			restarted, err := newNativeScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			carrier := subscribeInternalDeliveriesForTest(t, restarted, routes[0].Recipient.ID())
			selected.setScalarTemplateInstanceIndexError(errors.New("nested recorded replay must not reselect account or keyless descendants"))
			selected.resetScalarTemplateInstanceIndexCalls()
			if _, err := restarted.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{Event: event, Scope: runtimepipelineobligation.ScopeSubscribed}, nil); err != nil {
				t.Fatal(err)
			}
			replayed := requireBusEvent(t, carrier, "recorded nested keyless receiver")
			if replayed.TargetRoute() != want || replayed.SourceRoute() != event.SourceRoute() || selected.scalarTemplateInstanceIndexCalls() != 0 {
				t.Fatalf("recorded nested delivery reinterpreted current ancestry: target=%+v source=%+v reads=%d", replayed.TargetRoute(), replayed.SourceRoute(), selected.scalarTemplateInstanceIndexCalls())
			}
			selected.setScalarTemplateInstanceIndexError(nil)
		}
	default:
		wantFailure := runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetUnresolved).Code()
		if name == "nested_receiver_unavailable" {
			wantFailure = runtimepinrouting.FailureTargetUnreachableTerminated.Code()
		}
		if err != nil || plan.TargetFailure != wantFailure || len(plan.DeliveryRoutes) != 0 {
			t.Fatalf("absent/unavailable receiver was selected or constructed: %+v err=%v", plan, err)
		}
		if name == "nested_account_absent" && accountLookups != 2 {
			t.Fatalf("nested path borrowed the earlier prepared account instead of native absence: lookups=%d", accountLookups)
		}
		if err := bus.Publish(ctx, event); err != nil {
			t.Fatalf("publish counted nested refusal: %v", err)
		}
		if routes, err := selected.ListEventDeliveryRoutes(ctx, event.ID()); err != nil || len(routes) != 0 {
			t.Fatalf("refused nested path committed deliveries: %+v err=%v", routes, err)
		}
	}
	setReadFault(nil)
	after, err := selected.ListWorkflowInstances(ctx, runID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("selection constructed/reinitialized unrelated state: before=%+v after=%+v err=%v", before, after, err)
	}
	if targetRun != runID {
		afterForeign, err := selected.ListWorkflowInstances(runtimecorrelation.WithRunID(ctx, targetRun), targetRun)
		if err != nil || !reflect.DeepEqual(beforeForeign, afterForeign) {
			t.Fatalf("foreign run changed during refusal: before=%+v after=%+v err=%v", beforeForeign, afterForeign, err)
		}
	}
}

func newScalarTemplateInstanceParityStore(t *testing.T, backend string, ctx context.Context) scalarTemplateInstanceParityStore {
	t.Helper()
	var selected scalarTemplateInstanceParityStore
	switch backend {
	case "sqlite":
		selected = &sqliteScalarTemplateInstanceStore{SQLiteRuntimeStore: storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)}
	case "postgres":
		selected = &postgresScalarTemplateInstanceStore{PostgresStore: storetest.StartPostgresRuntimeStore(t)}
	default:
		t.Fatalf("unsupported backend %q", backend)
		return nil
	}
	nativeBusProcessContext(t, ctx, selected)
	return selected
}

func TestParentLocalReturnPersistsExactNativeReceiverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyNestedFlowConnect(t), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
			var selected scalarTemplateInstanceParityStore
			if backend == "postgres" {
				selected = &postgresScalarTemplateInstanceStore{PostgresStore: storetest.StartPostgresRuntimeStore(t)}
			} else {
				selected = &sqliteScalarTemplateInstanceStore{SQLiteRuntimeStore: storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)}
			}
			ctx = originalEventBusFixtureContext(t, ctx, selected)
			at := time.Now().UTC()
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
			for _, flowID := range []string{".", "child", "child/grandchild"} {
				identity := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, "", runID)
				instance := runtimepipeline.WorkflowInstance{
					InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
					ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
					WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), EnteredStageAt: at, CreatedAt: at,
				}
				if entity, declared := entityruntime.ResolveForFlow(source, flowID); declared {
					instance.EntityType = entity.EntityType
					constructor, err := runtimepipeline.CompileFlowConstructor(source, flowID, "")
					if err != nil {
						t.Fatal(err)
					}
					instance.Fields, err = constructor.InitialFields(nil, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				seedComponentFlowConstruction(t, ctx, selected, source, instance)
			}
			eventBus, err := newNativeScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			sender := eventtest.StaticFlowRoutingSource("child/grandchild", "child/grandchild", runtimeflowidentity.EntityID("child/grandchild"))
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "child/grandchild/micro.done", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, sender, at)
			plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
			want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: runtimeflowidentity.EntityID("child")})
			if err != nil || plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
				t.Fatalf("parent-local plan=%+v err=%v, want exact admitted receiver %+v", plan, err, want)
			}
			node, localEvent, claimed := plan.DeliveryRoutes[0].ConnectClaim.NodeHandlerOwner()
			if !claimed || node.FlowPath() != "child" || node.NodeID() != "child-aggregator" || localEvent != "micro.done" {
				t.Fatalf("parent-local claim=%+v event=%q admitted=%t", node, localEvent, claimed)
			}
			if err := eventBus.Publish(ctx, event); err != nil {
				t.Fatal(err)
			}
			routes, err := selected.ListEventDeliveryRoutes(ctx, event.ID())
			if err != nil || len(routes) != 1 || !reflect.DeepEqual(routes[0], plan.DeliveryRoutes[0]) {
				t.Fatalf("persisted parent-local routes=%+v err=%v", routes, err)
			}
			carrier := subscribeInternalDeliveriesForTest(t, eventBus, routes[0].Recipient.ID())
			indexFailure := errors.New("committed parent-local replay must not query the current instance index")
			selected.setScalarTemplateInstanceIndexError(indexFailure)
			selected.resetScalarTemplateInstanceIndexCalls()
			if _, err := eventBus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{Event: event, Scope: runtimepipelineobligation.ScopeSubscribed}, nil); err != nil {
				t.Fatal(err)
			}
			select {
			case delivery := <-carrier:
				if got := delivery.Event(); got.ID() != event.ID() || got.FlowInstance() != want.Route().FlowInstance || got.EntityID() != want.Route().EntityID || got.Type() != event.Type() {
					t.Fatalf("parent-local replay crossed receiver or event: %+v", got)
				}
				if err := delivery.Complete(); err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("parent-local replay did not reach its committed receiver")
			}
			if calls := selected.scalarTemplateInstanceIndexCalls(); calls != 0 {
				t.Fatalf("committed parent-local replay consulted the current instance index %d times", calls)
			}
			fresh := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), event.Type(), "test", "", event.Payload(), 0, runID, events.EventEnvelope{}, sender, at)
			if _, err := eventBus.CheckPublishRecipientPlan(ctx, fresh); !errors.Is(err, indexFailure) || selected.scalarTemplateInstanceIndexCalls() == 0 {
				t.Fatalf("fresh parent-local plan bypassed poisoned index: calls=%d err=%v", selected.scalarTemplateInstanceIndexCalls(), err)
			}
			if persisted, found, err := selected.LoadPreparedPublishEvent(ctx, fresh.ID()); err != nil || found || len(persisted.DeliveryRoutes) != 0 {
				t.Fatalf("refused parent-local preflight mutated storage: %+v found=%t err=%v", persisted, found, err)
			}
			selected.setScalarTemplateInstanceIndexError(nil)
			foreign := eventtest.StaticFlowRoutingSource("child/grandchild", "child/grandchild", uuid.NewString())
			rejected := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "child/grandchild/micro.done", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, foreign, at)
			if err := eventBus.Publish(ctx, rejected); err == nil || !strings.Contains(err.Error(), "contradicts") {
				t.Fatalf("foreign parent-local publication=%v, want exact source-entity refusal", err)
			}
			if persisted, found, err := selected.LoadPreparedPublishEvent(ctx, rejected.ID()); err != nil || found || len(persisted.DeliveryRoutes) != 0 {
				t.Fatalf("rejected parent-local publication mutated store: %+v found=%t err=%v", persisted, found, err)
			}
		})
	}
}
