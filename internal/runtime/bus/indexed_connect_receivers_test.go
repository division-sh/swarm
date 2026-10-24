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
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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

func TestControlPublicationKeepsEmptyCompiledTargetScope(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	event := eventtest.RuntimeControl(eventtest.UUID("empty-scope-control"), "mailbox.card_deferred", "", "", []byte(`{}`), 0,
		busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	ctx := correlation.WithInboundEvent(constructionIndexContext(t, source), event)
	reader := &unscopedConnectIndexTestReader{}
	resolver := newConnectRoutePlanResolver(source, nil, nil, reader, nil)
	bus := &EventBus{semanticSource: source, durable: DurableDependencies{Instances: reader}}
	scope, bounded, err := resolver.selectedTargetScope(ctx, event)
	if err != nil || !bounded || !scope.lookup.Valid() || len(scope.lookup.FlowIDs()) != 0 || len(scope.lookup.Coordinates()) != 0 {
		t.Fatalf("control publication lost its empty compiled scope: %+v bounded=%t err=%v", scope, bounded, err)
	}
	if len(reader.requested) != 1 || reader.requested[0].RunID() != event.RunID() || !reader.requested[0].SourceFact().Matches(scope.lookup.SourceFact()) {
		t.Fatalf("control lookup lost exact source/run admission: %+v", reader.requested)
	}
	owners, available, err := bus.activeTargetDescriptors(withSelectedTargetOwnerLookupScope(ctx, scope))
	if err != nil || !available || len(owners) != 0 {
		t.Fatalf("empty scope must be known absence, not unbounded target discovery: %+v available=%t err=%v", owners, available, err)
	}
	identity := ConstructedFlowInstanceIdentityFixture(source, "consumer", "", event.RunID())
	reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), identity, "")}
	if _, _, err := resolver.selectedTargetScope(ctx, event); err == nil {
		t.Fatal("empty control scope accepted an unrelated receiver")
	}
	reader.observations = nil
	for _, failure := range []error{context.Canceled, errors.New("empty scope index failure"), errors.Join(context.Canceled, errors.New("independent empty scope failure"))} {
		reader.err = failure
		if _, _, err := resolver.selectedTargetScope(ctx, event); !errors.Is(err, failure) {
			t.Fatalf("empty scope suppressed index error: got=%v want=%v", err, failure)
		}
	}
	reader.err = nil
	if _, _, err := resolver.selectedTargetScope(context.Background(), event); err == nil {
		t.Fatal("empty scope accepted missing source admission")
	}
}

func TestDirectPlanningPropagatesScopedIndexFailures(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	event := eventtest.RuntimeControl(eventtest.UUID("direct-empty-scope-control"), "mailbox.card_deferred", "", "", []byte(`{}`), 0,
		busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	ctx := constructionIndexContext(t, source)
	reader := &unscopedConnectIndexTestReader{}
	bus := &EventBus{semanticSource: source, durable: DurableDependencies{Instances: reader}}
	planner := deliveryPlanner{
		connectPlanner: newConnectRoutePlanResolver(source, nil, nil, reader, nil),
		recipientPolicy: deliveryRecipientPolicy{
			semanticSource: source,
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return nil, false, nil
			},
			loadActiveTargetDescriptors: bus.activeTargetDescriptors,
		},
	}
	for _, failure := range []error{context.Canceled, errors.New("direct index failure"), errors.Join(context.Canceled, errors.New("independent direct index failure"))} {
		reader.err = failure
		if _, err := planner.PlanDirect(ctx, event, []string{"requester"}); !errors.Is(err, failure) {
			t.Fatalf("direct planning bypassed scoped index admission: got=%v want=%v", err, failure)
		}
	}
	if len(reader.requested) != 3 {
		t.Fatalf("direct planning did not query its scoped owner: %+v", reader.requested)
	}
	for _, request := range reader.requested {
		if request.RunID() != event.RunID() || !request.Valid() || len(request.FlowIDs()) != 0 || len(request.Coordinates()) != 0 {
			t.Fatalf("direct control admission broadened its empty scope: %+v", request)
		}
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

func TestMaterializedTargetProjectionKeepsRuntimeTargetlessOutsideConstructionScope(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	ctx := constructionIndexContext(t, source)
	event := eventtest.RuntimeControl(eventtest.UUID("runtime-targetless-index-event"), "mailbox.card_deferred", "", "", nil, 0,
		busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	name, err := agentidentity.RuntimeName("independent-runtime", "runtime-targetless-index")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("runtime-only", "worker", "runtime-only/worker")
	if err != nil {
		t.Fatal(err)
	}
	actor, err := agentidentity.New(event.RunID(), name, route)
	if err != nil {
		t.Fatal(err)
	}
	reader := &unscopedConnectIndexTestReader{}
	bus := &EventBus{semanticSource: source, durable: DurableDependencies{Instances: reader}}
	bus.rebuildRoutePlanners()
	bus.deliveryPlanner.recipientPolicy.loadActiveAgentDescriptors = func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
		return map[agentidentity.Identity]ActiveAgentDescriptor{actor: {Identity: actor}}, true, nil
	}
	plan := newRoutePlan(event)
	plan.DeliveryIntents = []RoutePlanDeliveryIntent{{
		Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor,
		Producer: routeIntentProducerAgentPolicy, Persist: true,
	}}
	projection, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan)
	if err != nil {
		t.Fatalf("runtime-created targetless actor acquired native construction admission: %v", err)
	}
	resolved, err := projection.resolveRoutePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if routes := resolved.DeliveryRoutes(); len(routes) != 1 || routes[0].AgentIdentity != actor || !routes[0].Target.Empty() || !routes[0].Initialization.Empty() {
		t.Fatalf("runtime-created targetless actor changed ownership: %+v", routes)
	}
	if len(reader.requested) != 1 || len(reader.requested[0].Coordinates()) != 0 || len(reader.requested[0].FlowIDs()) != 0 {
		t.Fatalf("runtime-created actor broadened construction scope: %+v", reader.requested)
	}
	for _, failure := range []error{context.Canceled, errors.New("independent targetless index failure"), errors.Join(context.Canceled, errors.New("joined targetless index failure"))} {
		reader.err = failure
		if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, plan); !errors.Is(err, failure) {
			t.Fatalf("targetless classification suppressed scoped index admission: got=%v want=%v", err, failure)
		}
	}
	reader.err = nil
	if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(context.Background(), event, plan); err == nil {
		t.Fatal("targetless actor bypassed admitted source evidence")
	}
	for _, receiver := range []bool{false, true} {
		targeted := plan
		targeted.DeliveryIntents = append([]RoutePlanDeliveryIntent(nil), plan.DeliveryIntents...)
		explicit := events.RouteIdentity{FlowID: route.ScopeKey, FlowInstance: route.InstancePath}
		if receiver {
			targeted.DeliveryIntents[0].ReceiverBlueprint = explicit
		} else {
			targeted.DeliveryIntents[0].TargetBlueprint = explicit
		}
		if _, err := bus.deliveryPlanner.materializedTargetOwnerProjection(ctx, event, targeted); err == nil {
			t.Fatalf("explicit undeclared construction accepted: receiver=%v", receiver)
		}
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

func TestKeylessConnectDistinguishesSelectedUnavailableReceiverFromReadFailure(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	plans, issues := compiledConnectPlans(source)
	if len(plans) != 1 || len(issues) != 0 {
		t.Fatalf("keyless source requires one valid connection: plans=%d issues=%+v", len(plans), issues)
	}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("keyless-unavailable"), "root.ready", "test", "", []byte(`{}`), 0,
		busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	ctx := constructionIndexContext(t, source)
	root := ConstructedFlowInstanceIdentityFixture(source, ".", "", event.RunID())
	child := ConstructedFlowInstanceIdentityFixture(source, "consumer", "", event.RunID())
	reader := constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{
		constructionIndexObservation(t, source, event.RunID(), root, ""),
		TerminatedFlowInstanceObservationFixture(t, source, event.RunID(), child),
	}}
	resolver := newConnectRoutePlanResolver(source, derivedRouteTableFixture(t, source), nil, reader, nil)
	materialized, selection, err := resolver.materializeKeylessConnect(ctx, event, plans[0])
	if err != nil || materialized.Failure != pinrouting.ConnectFailureLifecycleUnavailable || !selection.Empty() || selection.Activation != nil {
		t.Fatalf("selected unavailable keyless receiver escaped its target refusal: %+v %+v err=%v", materialized, selection, err)
	}
	if got := connectRoutePlanTargetFailure(materialized.Failure); got != pinrouting.FailureTargetUnreachableTerminated {
		t.Fatalf("keyless lifecycle refusal lost its canonical target failure: %v", got)
	}
	var unavailable *pipeline.TerminatedReceiverError
	owner, occupied := selection.UnavailableReceiver()
	if !errors.As(selection.unavailableReceiverError, &unavailable) || unavailable.FlowID != "consumer" || !occupied ||
		owner.RunID != event.RunID() || owner.Route != child.Route() || selection.SourceEventID != event.ID() {
		t.Fatalf("keyless planning lost its original exact semantic refusal: owner=%+v err=%v", owner, selection.unavailableReceiverError)
	}
	terminal := &pipeline.TerminatedReceiverError{FlowID: "consumer"}
	corrupt := &pipeline.FlowInstanceConstructionCorruption{RunID: event.RunID(), FlowID: "consumer", Cause: terminal}
	for _, failure := range []error{terminal, corrupt, context.Canceled, errors.Join(terminal, errors.New("independent read failure"))} {
		reader.selectionErr = failure
		resolver.lifecycle.index = reader
		materialized, selection, err := resolver.materializeKeylessConnect(ctx, event, plans[0])
		if !errors.Is(err, failure) || !materialized.Failure.Empty() || !selection.Empty() {
			t.Fatalf("keyless reader failure became semantic unavailability: %+v %+v err=%v want=%v", materialized, selection, err, failure)
		}
	}
}

type unavailableThenFailingConnectReader struct {
	constructionIndexTestReader
	selectionReads int
	failure        error
}

func (r *unavailableThenFailingConnectReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if request.DeclaredSelection() && request.FlowID() == "consumer" {
		r.selectionReads++
		if r.selectionReads == 2 {
			return pipeline.FlowInstanceObservation{}, false, r.failure
		}
	}
	return r.constructionIndexTestReader.LookupFlowInstance(ctx, request)
}

func TestUnavailableConnectionDoesNotHideLaterReadFailure(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyUnavailableConnectionScan(t, "pin_collision"))
	plans, issues := compiledConnectPlans(source)
	if len(plans) != 2 || len(issues) != 0 {
		t.Fatalf("counterexample requires two lawful connections: plans=%d issues=%+v", len(plans), issues)
	}
	producer := ConstructedFlowInstanceIdentityFixture(source, "producer", "", busInternalTestRunID)
	event := connectRoutePlanStaticProducerEvent(eventtest.UUID("terminal-before-read-error"), "producer/work.ready", "test", "", []byte(`{}`), 0,
		busInternalTestRunID, "", events.EventEnvelope{Source: events.RouteIdentity{
			FlowID: producer.TemplateID, FlowInstance: producer.InstancePath, EntityID: producer.EntityID,
		}}, time.Now().UTC())
	root := ConstructedFlowInstanceIdentityFixture(source, ".", "", event.RunID())
	child := ConstructedFlowInstanceIdentityFixture(source, "consumer", "", event.RunID())
	for _, failure := range []error{errors.New("independent later receiver read"), context.Canceled, errors.Join(context.Canceled, errors.New("independent joined reader error"))} {
		reader := &unavailableThenFailingConnectReader{failure: failure, constructionIndexTestReader: constructionIndexTestReader{
			observations: []pipeline.FlowInstanceObservation{
				constructionIndexObservation(t, source, event.RunID(), root, ""),
				constructionIndexObservation(t, source, event.RunID(), producer, ""),
				TerminatedFlowInstanceObservationFixture(t, source, event.RunID(), child),
			},
		}}
		resolver := newConnectRoutePlanResolver(source, derivedRouteTableFixture(t, source), nil, reader, nil)
		result, err := resolver.Plan(constructionIndexContext(t, source), event)
		var terminal *pipeline.TerminatedReceiverError
		if !errors.Is(err, failure) || !errors.As(err, &terminal) || terminal.FlowID != "consumer" || reader.selectionReads != 2 ||
			len(result.DeliveryIntents) != 0 || len(result.ActivationPlans) != 0 || len(result.ReplyCreations) != 0 || len(result.ReplyClaims) != 0 {
			t.Fatalf("earlier terminal selection hid later read error: reads=%d result=%+v err=%v", reader.selectionReads, result, err)
		}
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
