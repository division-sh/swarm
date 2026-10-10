package bus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type unscopedConnectIndexTestReader struct {
	constructionIndexTestReader
	requested []pipeline.FlowInstanceLookupScope
}

func TestTargetOwnerProjectionConsumesOnlyScopedIndexObservations(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("index-target-event"), "root.ready", "operator", "", []byte(`{}`), 0,
		busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	ctx := correlation.WithInboundEvent(constructionIndexContext(t, source), event)
	index := &constructionIndexTestReader{}
	resolver := newConnectRoutePlanResolver(source, nil, nil, index, nil)
	bus := &EventBus{semanticSource: source, durable: DurableDependencies{Instances: index}}
	for _, flowID := range []string{".", "consumer"} {
		identity := ConstructedFlowInstanceIdentityFixture(source, flowID, "", event.RunID())
		index.observations = append(index.observations, constructionIndexObservation(t, source, event.RunID(), identity, ""))
	}
	scope, bounded, err := resolver.selectedTargetScope(ctx, event)
	if err != nil || !bounded || len(scope.observations) != 2 {
		t.Fatalf("native target scope lost exact constructed owners: %+v bounded=%t err=%v", scope, bounded, err)
	}
	owners, available, err := bus.activeTargetDescriptors(withSelectedTargetOwnerLookupScope(ctx, scope))
	if err != nil || !available || len(owners) != 2 {
		t.Fatalf("target projection consulted a parallel descriptor reader: %+v available=%t err=%v", owners, available, err)
	}
	candidates := (selectedRunTargetOwnerProjection{descriptors: owners}).targetOwnerCandidates()
	for i, observed := range scope.observations {
		identity := observed.Identity()
		if owners[i].FlowID != identity.TemplateID || owners[i].FlowInstance != identity.InstancePath || owners[i].EntityID != identity.EntityID {
			t.Fatalf("target owner lost its admitted declaration/coordinate: %+v != %+v", owners[i], identity)
		}
		if candidates[i].Route.FlowID != identity.TemplateID || candidates[i].Route.FlowInstance != identity.InstancePath || candidates[i].Route.EntityID != identity.EntityID {
			t.Fatalf("target candidate reinterpreted its stored path as a declaration: %+v != %+v", candidates[i], identity)
		}
	}
	index.observations = nil
	empty, bounded, err := resolver.selectedTargetScope(ctx, event)
	if err != nil || !bounded {
		t.Fatal(err)
	}
	owners, available, err = bus.activeTargetDescriptors(withSelectedTargetOwnerLookupScope(ctx, empty))
	if err != nil || !available || len(owners) != 0 {
		t.Fatalf("source declaration invented existing target: %+v available=%t err=%v", owners, available, err)
	}
	if _, _, err := bus.activeTargetDescriptors(ctx); err == nil {
		t.Fatal("unbounded target inventory substituted for compiled scope")
	}
	if _, _, err := bus.activeTargetDescriptors(withSelectedTargetOwnerLookupScope(correlation.WithInboundEvent(context.Background(), event), scope)); err == nil {
		t.Fatal("target lookup accepted missing admitted source")
	}
}

func TestMaterializedTargetProjectionUsesExactIndexScope(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	ctx := constructionIndexContext(t, source)
	event := eventtest.RuntimeControl(eventtest.UUID("materialized-index-event"), "account.ready", "", "", nil, 0,
		busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	identity := ConstructedFlowInstanceIdentityFixture(source, "account", "one", event.RunID())
	observed := constructionIndexObservation(t, source, event.RunID(), identity, "42")
	reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{observed}}}
	bus := &EventBus{semanticSource: source, durable: DurableDependencies{Instances: reader}}
	bus.rebuildRoutePlanners()
	plan := newRoutePlan(event)
	node := testFlowNode(t, "account", "account-node")
	plan.DeliveryIntents = []RoutePlanDeliveryIntent{{Recipient: events.MustNodeDeliveryRecipient(node), Handler: pipeline.MustDeliveryTargetHandler(node).ForEvent("account.ready"), TargetBlueprint: events.RouteIdentity{
		FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID,
	}}}
	projection, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan)
	if err != nil || len(projection.descriptors) != 1 || projection.descriptors[0].EntityID != identity.EntityID {
		t.Fatalf("materialized receiver lost exact native owner: %+v err=%v", projection.descriptors, err)
	}
	if len(reader.requested) != 1 || len(reader.requested[0].FlowIDs()) != 0 || len(reader.requested[0].Coordinates()) != 1 || reader.requested[0].Coordinates()[0].Key() != observed.Owner().Key() {
		t.Fatalf("materializer escaped exact receiver scope: %+v", reader.requested)
	}
	reader.observations = nil
	projection, err = bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan)
	if err != nil || len(projection.descriptors) != 0 {
		t.Fatalf("materializer substituted declarations for native absence: %+v err=%v", projection.descriptors, err)
	}
	for _, failure := range []error{context.Canceled, errors.New("materializer index failure"), errors.Join(context.Canceled, errors.New("independent materializer failure"))} {
		reader.err = failure
		if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan); !errors.Is(err, failure) {
			t.Fatalf("materializer suppressed index failure: got=%v want=%v", err, failure)
		}
	}
	reader.err = nil
	sibling := ConstructedFlowInstanceIdentityFixture(source, "account", "two", event.RunID())
	reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), sibling, "43")}
	if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan); err == nil {
		t.Fatal("materializer admitted an unselected sibling from the index")
	}
	foreignRun := eventtest.UUID("materialized-foreign-run")
	foreign := ConstructedFlowInstanceIdentityFixture(source, "account", "one", foreignRun)
	reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, foreignRun, foreign, "42")}
	if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan); err == nil {
		t.Fatal("materializer admitted a receiver from another run")
	}
	if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(context.Background(), event, plan); err == nil {
		t.Fatal("materializer accepted missing admitted source evidence")
	}
}

func TestReplyOriginLookupPreservesPreparedConstructionAndIndependentFailures(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	event := admitRunProposalEvent(t, true).Event()
	root := ConstructedFlowInstanceIdentityFixture(source, ".", "", event.RunID())
	child, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	planner := newTestFlowInstanceActivationOwner(nil)
	var plans []pipeline.FlowInstanceActivationPlan
	for _, instance := range []flowidentity.Instance{root, child} {
		plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
			ContractBundle: source, Instance: instance, TriggerEvent: event, OccurredAt: event.CreatedAt(),
		})
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
	}
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: event.RunID(), Route: child.Route()})
	if err != nil {
		t.Fatal(err)
	}
	storeFailure := errors.New("independent reply-origin read failure")
	resolver := connectRoutePlanResolver{source: source, lifecycle: connectInstanceSelector{index: constructionIndexTestReader{err: storeFailure}}}
	previewCtx := withConnectRoutePlanPreview(ctx)
	previewCtx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes).plans = plans
	instance, found, err := resolver.indexedReceiverWorkflowInstance(previewCtx, request)
	if err != nil || !found || instance.StorageRef != child.InstancePath || instance.EntityID != plans[1].Instance.EntityID {
		t.Fatalf("prepared reply origin became native absence/read authority: %+v found=%t err=%v", instance, found, err)
	}
	for _, failure := range []error{context.Canceled, storeFailure, errors.Join(context.Canceled, storeFailure)} {
		resolver.lifecycle.index = constructionIndexTestReader{err: failure}
		if _, found, err := resolver.indexedReceiverWorkflowInstance(ctx, request); found || !errors.Is(err, failure) {
			t.Fatalf("reply read failure became a stale origin: found=%t err=%v want=%v", found, err, failure)
		}
		explicit := eventtest.ExistingRunRootIngress(eventtest.UUID("explicit-receiver"), "work.requested", "test", "", []byte(`{}`), 0, event.RunID(),
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath, EntityID: child.EntityID}), event.CreatedAt())
		if terminal, err := resolver.explicitReceiverTerminated(ctx, explicit); terminal || !errors.Is(err, failure) {
			t.Fatalf("explicit receiver failure became a terminal diagnostic: terminal=%t err=%v want=%v", terminal, err, failure)
		}
	}
}

func TestTerminalTargetDiagnosticPreservesCreateAndReplyAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		source func(t *testing.T) []pinrouting.ConnectRoutePlan
		want   bool
	}{
		{"empty", func(*testing.T) []pinrouting.ConnectRoutePlan { return nil }, false},
		{"ordinary", func(t *testing.T) []pinrouting.ConnectRoutePlan {
			plans, _ := compiledConnectPlans(connectRoutePlanRootProducerSingletonSource(t))
			return plans
		}, true},
		{"create", func(t *testing.T) []pinrouting.ConnectRoutePlan {
			plans, _ := compiledConnectPlans(connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false))
			return plans
		}, false},
		{"select", func(t *testing.T) []pinrouting.ConnectRoutePlan {
			plans, _ := compiledConnectPlans(connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false))
			return plans
		}, false},
		{"select or create", func(t *testing.T) []pinrouting.ConnectRoutePlan {
			plans, _ := compiledConnectPlans(connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false))
			return plans
		}, false},
		{"reply", func(t *testing.T) []pinrouting.ConnectRoutePlan {
			plans, _ := compiledConnectPlans(loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyRootReplyBoundary(t, true, true)))
			return plans
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := terminalDiagnosticPlans(test.source(t)); got != test.want {
				t.Fatalf("terminal diagnostic eligibility=%t want=%t", got, test.want)
			}
		})
	}
}

func (r *unscopedConnectIndexTestReader) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	r.requested = append(r.requested, scope)
	return slices.Clone(r.observations), r.err
}

func TestConnectLookupScopeRejectsForeignInventoryAndIndependentErrors(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := events.NewExternalIngressRoutingSource("account", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithRoutingSource(eventtest.UUID("lookup-event"), "account.ready", "provider", "", []byte(`{"account_id":"42"}`), 0,
		busInternalTestRunID, "", events.EventEnvelope{}, routing, time.Now().UTC())
	ctx := constructionIndexContext(t, source)
	for _, test := range []struct {
		name, runID, flowID, id, key string
	}{
		{"foreign run", eventtest.UUID("other-run"), "account", "one", "42"},
		{"foreign declaration", busInternalTestRunID, "producer", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance := ConstructedFlowInstanceIdentityFixture(source, test.flowID, test.id, test.runID)
			reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{
				constructionIndexObservation(t, source, test.runID, instance, test.key),
			}}}
			resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
			if scope, bounded, err := resolver.selectedTargetScope(ctx, event); err == nil || bounded || len(scope.observations) != 0 {
				t.Fatalf("foreign inventory admitted or became an unbounded fallback: scope=%+v bounded=%v err=%v", scope, bounded, err)
			}
			if len(reader.requested) != 1 || !slices.Equal(reader.requested[0].FlowIDs(), []string{"account"}) || len(reader.requested[0].Coordinates()) != 0 {
				t.Fatalf("lookup escaped its compiled declaration set: %+v", reader.requested)
			}
		})
	}
	storeFailure := errors.New("independent store failure")
	for _, failure := range []error{context.Canceled, storeFailure, errors.Join(context.Canceled, storeFailure)} {
		reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{err: failure}}
		resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
		if _, bounded, err := resolver.selectedTargetScope(ctx, event); bounded || !errors.Is(err, failure) {
			t.Fatalf("inventory failure lost at planning boundary: bounded=%v err=%v want=%v", bounded, err, failure)
		}
	}
}
