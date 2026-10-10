package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestTemplateFlowPilotRuntime_ParentConnectCreatesTemplateInstanceAndPersistedDeliveryRoute(t *testing.T) {
	bundle := templateflowpilot.LoadBundle(t, templateflowpilot.Options{})
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-flow pilot hard invalidities = %#v, want none", got)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := seedRuntimeTestRunForSource(t, pg, source)
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager not initialized")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager not initialized")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	pc := newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              newRuntimeTestWorkflowModule(t, source),
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		DeliveryStore:       pg,
		PipelineObligations: pg.PipelineObligations(),
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus), ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	admitExternalManagerTestGeneration(t, ctx, pg, manager, source)
	producer := seedRuntimeTestKeylessSource(t, ctx, pg, pc, "producer")

	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		"99999999-9999-4999-8999-999999999952",
		events.EventType("producer/account.ready"),
		"producer",
		"",
		json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: producer.TemplateID, FlowInstance: producer.InstancePath, EntityID: producer.EntityID,
		}),
		eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID),
		time.Now().UTC(),
	)
	preflight, err := bus.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one deterministic template route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if !preflight.DeliveryRoutes[0].Target.MaterializingEntity() {
		t.Fatalf("preflight target ownership = %q, want materializing_entity", preflight.DeliveryRoutes[0].Target.Code())
	}
	if target := preflight.DeliveryRoutes[0].Target.Route(); target.FlowID != "account" || !strings.HasPrefix(target.FlowInstance, "account/") || target.EntityID == "" {
		t.Fatalf("preflight target = %#v, want account template flow instance", target)
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM flow_instances
		WHERE flow_template = 'account'
	`, 0)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish validation request: %v", err)
	}
	accountNodeID := identitytest.FlowNode(t, "account", "account-node").Key()
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, 1, evt.ID(), accountNodeID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id IN ('workflow-runtime', 'raw-source-listener')
	`, 0, evt.ID())

	flowInstance, entityID := loadTemplateFlowPilotInstanceIdentity(t, ctx, db)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $3
		  AND delivery_target_route @> $2::jsonb
	`, 1, evt.ID(), templateFlowPilotDeliveryTargetRouteJSON(t, events.RouteIdentity{
		FlowID:       "account",
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}), accountNodeID)
	loaded, ok, err := pc.Load(ctx, runtimeflowidentity.RunScopedFlowInstance{
		RunID: templateInstanceDeliveryRunID,
		Route: runtimeflowidentity.RouteForInstancePath(flowInstance),
	})
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.StorageRef != flowInstance || loaded.WorkflowName != "account" || loaded.CurrentState != "pending" {
		t.Fatalf("loaded account instance = storage:%q workflow:%q state:%q, want %s/account/pending", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState, flowInstance)
	}
	if loaded.Fields["account_id"] != "acct-1" {
		t.Fatalf("loaded account fields = %#v, want account_id from route activation", loaded.Fields)
	}
}

func TestTemplateFlowPilotRuntime_FailsClosedForMissingAndAmbiguousKeys(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	tests := []struct {
		name        string
		payload     json.RawMessage
		duplicate   bool
		wantFailure string
	}{
		{
			name:        "missing producer key",
			payload:     json.RawMessage(`{"score":"91","decision":"approved"}`),
			wantFailure: runtimepinrouting.ConnectFailureInstanceSourceValueMissing.Code(),
		},
		{
			name:      "ambiguous receiver key",
			payload:   json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
			duplicate: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &templateFlowPilotMemoryStore{}
			root := runtimeflowidentity.Stored(source, ".", templateInstanceDeliveryRunID, templateInstanceDeliveryRunID, runtimeflowidentity.EntityID(templateInstanceDeliveryRunID), "")
			producer, err := runtimeflowidentity.KeylessChild(source, root, "producer")
			if err != nil {
				t.Fatal(err)
			}
			store.constructions = []runtimeflowidentity.Instance{root, producer}
			store.addObservation(t, source, root, "")
			store.addObservation(t, source, producer, "")
			if tc.duplicate {
				for _, id := range []string{"one", "two"} {
					instance, err := runtimeflowidentity.KeyedChild(source, root, "account", id)
					if err != nil {
						t.Fatal(err)
					}
					store.constructions = append(store.constructions, instance)
					store.addObservation(t, source, instance, "acct-1")
				}
			}
			fact := runtimeTestSourceArtifactFact(t, source)
			ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), fact)
			ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
			bus, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact,
				Durable: runtimebus.DurableDependencies{ConstructionPublications: store, RunLifecycle: store,
					Instances: store},
				TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(context.Context, runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
					t.Fatal("fail-closed route must not plan a template instance")
					return runtimepipeline.FlowInstanceActivationPlan{}, nil
				}),
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				"99999999-9999-4999-8999-999999999953",
				events.EventType("producer/account.ready"),
				"producer",
				"",
				tc.payload,
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID: producer.TemplateID, FlowInstance: producer.InstancePath, EntityID: producer.EntityID,
				}),
				eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID),
				time.Now().UTC(),
			)
			plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
			var corruption *runtimepipeline.FlowInstanceConstructionCorruption
			if tc.duplicate && !errors.As(err, &corruption) {
				t.Fatalf("duplicate native selector was not typed construction corruption: %v", err)
			}
			if !tc.duplicate && err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if plan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", plan.TargetFailure, tc.wantFailure)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("fail-closed route exposed executable plan: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			before := len(store.constructions)
			err = bus.Publish(ctx, evt)
			if tc.duplicate && (!errors.As(err, &corruption) || store.commits != 0) {
				t.Fatalf("corrupt selector reached publication: err=%v commits=%d", err, store.commits)
			}
			if !tc.duplicate && err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(store.constructions) != before {
				t.Fatal("refused publication changed construction evidence")
			}
			if routes := store.deliveryRoutes[evt.ID()]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none", routes)
			}
		})
	}
}

type templateFlowPilotMemoryStore struct {
	runtimebus.InMemoryEventStore
	runtimerunlifecycle.OperationOwner
	constructions  []runtimeflowidentity.Instance
	observations   []runtimepipeline.FlowInstanceObservation
	deliveryRoutes map[string][]events.DeliveryRoute
	commits        int
}

func (s *templateFlowPilotMemoryStore) addObservation(t testing.TB, source semanticview.Source, identity runtimeflowidentity.Instance, key string) {
	t.Helper()
	fact := runtimeTestSourceArtifactFact(t, source)
	owner := runtimeflowidentity.RunScopedFlowInstance{RunID: templateInstanceDeliveryRunID, Route: identity.Route()}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, owner)
	if err != nil {
		t.Fatal(err)
	}
	schema, found := source.FlowSchemaByID(identity.TemplateID)
	if !found {
		t.Fatal("pilot observation requires its declaration")
	}
	at := time.Unix(1700000000, 0).UTC()
	header := runtimepipeline.WorkflowInstance{
		WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), Status: "active",
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, InstanceKey: key,
		ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
		CurrentState: "active", Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	if entity, declared := entityruntime.ResolveForFlow(source, identity.TemplateID); declared {
		header.EntityType = entity.EntityType
	}
	run := runtimerunlifecycle.Snapshot{RunID: owner.RunID, State: runtimerunlifecycle.StateRunning, Origin: runtimerunlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := runtimepipeline.FlowConstructionPublicationEvidence{Identity: identity, InstanceKey: key}
	readiness := runtimepipeline.DynamicFlowRuntimeReadiness{
		Plan:            runtimepipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: owner.RunID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	observed, err := runtimepipeline.AdmitNativeFlowInstanceObservation(lookup, header, run, 1, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	s.observations = append(s.observations, observed)
}

func (s *templateFlowPilotMemoryStore) LookupFlowInstance(ctx context.Context, request runtimepipeline.FlowInstanceLookupRequest) (runtimepipeline.FlowInstanceObservation, bool, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, err
	}
	var selected runtimepipeline.FlowInstanceObservation
	for _, observed := range s.observations {
		identity := observed.Identity()
		if observed.Owner().RunID != request.RunID() || identity.TemplateID != request.FlowID() ||
			(request.ExactPath() != "" && request.ExactPath() != identity.InstancePath) ||
			(request.DeclaredSelection() && (identity.ParentRoute.FlowInstance != request.ParentInstance() || observed.InstanceKey() != request.InstanceKey())) {
			continue
		}
		if err := observed.ValidateSelection(request); err != nil {
			return runtimepipeline.FlowInstanceObservation{}, false, err
		}
		if selected.Valid() {
			return runtimepipeline.FlowInstanceObservation{}, false, &runtimepipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), Cause: fmt.Errorf("duplicate native pilot selector")}
		}
		selected = observed
	}
	return selected, selected.Valid(), nil
}

func (s *templateFlowPilotMemoryStore) ListFlowInstances(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope) ([]runtimepipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var selected []runtimepipeline.FlowInstanceObservation
	for _, observed := range s.observations {
		if observed.Owner().RunID != scope.RunID() {
			continue
		}
		included := slices.Contains(scope.FlowIDs(), observed.Identity().TemplateID)
		for _, coordinate := range scope.Coordinates() {
			included = included || coordinate.Key() == observed.Owner().Key()
		}
		if included {
			selected = append(selected, observed)
		}
	}
	return selected, nil
}

func (s *templateFlowPilotMemoryStore) RequireActiveRun(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if runID != templateInstanceDeliveryRunID {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	return nil
}

func (s *templateFlowPilotMemoryStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	s.commits++
	return s.InMemoryEventStore.CommitPublication(ctx, command)
}

func (s *templateFlowPilotMemoryStore) LoadFlowConstructionPublication(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, entity string) (runtimepipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, err
	}
	for _, instance := range s.constructions {
		if owner.RunID == templateInstanceDeliveryRunID && owner.Route == instance.Route() && entity == instance.EntityID {
			return runtimepipeline.FlowConstructionPublicationEvidence{Identity: instance}, nil
		}
	}
	return runtimepipeline.FlowConstructionPublicationEvidence{}, errors.New("absent exact template pilot fixture receipt")
}

func (s *templateFlowPilotMemoryStore) InsertEventDeliveryRoutes(_ context.Context, eventID string, routes []events.DeliveryRoute) error {
	if s.deliveryRoutes == nil {
		s.deliveryRoutes = map[string][]events.DeliveryRoute{}
	}
	s.deliveryRoutes[eventID] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func loadTemplateFlowPilotInstanceIdentity(t *testing.T, ctx context.Context, db *sql.DB) (string, string) {
	t.Helper()
	var flowInstance string
	var entityID string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_id::text
		FROM entity_state
		WHERE flow_instance LIKE 'account/%'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&flowInstance, &entityID); err != nil {
		t.Fatalf("load account instance identity: %v", err)
	}
	return flowInstance, entityID
}

func templateFlowPilotDeliveryTargetRouteJSON(t *testing.T, target events.RouteIdentity) string {
	t.Helper()
	encoded, err := json.Marshal(events.MustMaterializingEntityTarget(target))
	if err != nil {
		t.Fatalf("marshal template-flow pilot delivery target: %v", err)
	}
	return string(encoded)
}
