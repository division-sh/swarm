package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

func TestClosedPublicationTargetOwnerEvidenceBoundaries(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	event := admitRunProposalEvent(t, true).Event()
	root := flowidentity.Stored(source, ".", event.RunID(), event.RunID(), event.RunID(), "")
	scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, event.RunID(), nil, []flowidentity.RunScopedFlowInstance{{RunID: event.RunID(), Route: root.Route()}})
	if err != nil {
		t.Fatal(err)
	}
	bounded := withSelectedTargetOwnerLookupScope(correlation.WithInboundEvent(ctx, event), selectedTargetOwnerLookupScope{lookup: scope})
	origin := events.RouteIdentity{FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID}
	record := replycontext.Record{ID: "prepared-reply", RunID: event.RunID(), RequestEventID: event.ID(), Origin: origin}
	base := publicationTargetOwnerEvidence{
		event: event, present: true, origins: map[string]events.RouteIdentity{record.ID: origin},
		projection: selectedRunTargetOwnerProjection{context: bounded, source: source, required: true,
			descriptors: []ActiveTargetDescriptor{{ID: root.InstanceID, FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID}}},
		planner: deliveryPlanner{connectPlanner: connectRoutePlanResolver{source: source}},
	}
	plan := RoutePlan{Event: event}
	if owner, err := base.returnOwner(ctx, event, &plan, record); err != nil || owner.Route() != origin || !owner.ExistingEntity() {
		t.Fatalf("exact prepared return = %+v, %v", owner, err)
	}
	for _, name := range []string{"absent", "wrong_event", "wrong_run", "wrong_source", "missing_scope", "crossed_scope", "logical_instance", "parent", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			candidate, incoming, incomingContext, reply := base, event, ctx, record
			switch name {
			case "absent":
				candidate.present = false
			case "wrong_event":
				candidate.event = events.Event{}
			case "wrong_run":
				reply.RunID = eventtest.UUID("foreign-publication-run")
			case "wrong_source":
				foreign, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
				if err != nil {
					t.Fatal(err)
				}
				incomingContext = correlation.WithSourceArtifactFact(ctx, foreign)
			case "missing_scope":
				candidate.projection.context = ctx
			case "crossed_scope":
				foreign, err := pipeline.NewFlowInstanceLookupScope(source, fact, eventtest.UUID("foreign-scope-run"), nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				candidate.projection.context = withSelectedTargetOwnerLookupScope(bounded, selectedTargetOwnerLookupScope{lookup: foreign})
			case "logical_instance":
				reply.Origin.FlowInstance = "producer/foreign"
			case "parent":
				reply.Origin.FlowInstance = "foreign/producer"
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				incomingContext = cancelled
			}
			before := plan
			_, err := candidate.returnOwner(incomingContext, incoming, &plan, reply)
			if err == nil || name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("crossed publication acquired return authority: %v", err)
			}
			if !reflect.DeepEqual(before, plan) {
				t.Fatal("refused evidence changed the prepared plan")
			}
		})
	}
}

func TestClosedPublicationReturnScopeExtendsOnlyCompiledOrigin(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	event := admitRunProposalEvent(t, true).Event()
	root := flowidentity.Stored(source, ".", event.RunID(), event.RunID(), event.RunID(), "")
	child, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{
		constructionIndexObservation(t, source, event.RunID(), root, ""),
		constructionIndexObservation(t, source, event.RunID(), child, ""),
	}}}
	planner := deliveryPlanner{
		connectPlanner:  connectRoutePlanResolver{source: source, lifecycle: connectInstanceSelector{index: reader}},
		recipientPolicy: testSelectedOwnerPolicy(),
	}
	planner.recipientPolicy.semanticSource = source
	planner.recipientPolicy.loadActiveTargetDescriptors = (&EventBus{semanticSource: source}).activeTargetDescriptors
	bounded := correlation.WithInboundEvent(ctx, event)
	scope, err := planner.connectPlanner.lookupTargetScope(bounded, event.RunID(), map[string]struct{}{"producer": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := planner.recipientPolicy.loadSelectedRunTargetOwnerProjection(withSelectedTargetOwnerLookupScope(bounded, scope))
	if err != nil {
		t.Fatal(err)
	}
	origin := events.RouteIdentity{FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID}
	record := replycontext.Record{ID: "prepared-reply", RunID: event.RunID(), RequestEventID: event.ID(), Origin: origin}
	evidence := publicationTargetOwnerEvidence{event: event, projection: projection, present: true, planner: planner,
		origins: map[string]events.RouteIdentity{record.ID: origin}}
	plan := RoutePlan{Event: event}
	owner, err := evidence.returnOwner(ctx, event, &plan, record)
	if err != nil || owner.Route() != origin || !owner.ExistingEntity() {
		t.Fatalf("extended exact return = %+v, %v", owner, err)
	}
	if len(reader.requested) != 2 || !reflect.DeepEqual(reader.requested[1].FlowIDs(), []string{"producer"}) ||
		!reflect.DeepEqual(reader.requested[1].Coordinates(), []flowidentity.RunScopedFlowInstance{{RunID: event.RunID(), Route: root.Route()}}) {
		t.Fatalf("extension widened the lookup beyond its compiled return: %+v", reader.requested)
	}
	if _, err := evidence.returnOwner(ctx, event, &plan, record); err != nil || len(reader.requested) != 2 {
		t.Fatalf("same publication reloaded its admitted return: reads=%d err=%v", len(reader.requested), err)
	}
}

func TestClosedPublicationOwnersKeepProposedRunAndFinalConstruction(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	admitted := admitRunProposalEvent(t, true)
	event := admitted.Event()
	proposal, err := pipeline.NewFlowInstanceRunProposal(fact, admitted)
	if err != nil {
		t.Fatal(err)
	}
	root := flowidentity.Stored(source, ".", event.RunID(), event.RunID(), event.RunID(), "")
	child, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	constructor := newTestFlowInstanceActivationOwner(nil)
	var activations []pipeline.FlowInstanceActivationPlan
	for _, instance := range []flowidentity.Instance{root, child} {
		plan, err := constructor.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
			ContractBundle: source, Instance: instance, TriggerEvent: event, OccurredAt: event.CreatedAt(),
		})
		if err != nil {
			t.Fatal(err)
		}
		activations = append(activations, plan)
	}
	planner := deliveryPlanner{
		connectPlanner: connectRoutePlanResolver{source: source, lifecycle: connectInstanceSelector{
			index: constructionIndexTestReader{err: errors.New("native evidence read before proposed run commit")}, runProposal: proposal,
		}},
		recipientPolicy: deliveryRecipientPolicy{semanticSource: source, requireTargetOwners: true, runProposal: proposal},
	}
	evidence := &publicationTargetOwnerEvidence{event: event, planner: planner}
	planner.preparedOwners = evidence
	bounded := correlation.WithInboundEvent(ctx, event)
	scope, err := planner.connectPlanner.lookupTargetScope(bounded, event.RunID(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := planner.recipientPolicy.loadSelectedRunTargetOwnerProjection(withSelectedTargetOwnerLookupScope(bounded, scope))
	if err != nil {
		t.Fatal(err)
	}
	origin := events.RouteIdentity{FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID}
	reply := replycontext.Record{ID: "proposed-reply", RunID: event.RunID(), RequestEventID: event.ID(), Origin: origin,
		RequesterFlowID: ".", RequestOutputPin: "work.requested", ReplyInputPin: "work.completed",
		ProviderFlowID: "producer", ProviderInputPin: "work.requested", ProviderOutputPin: "work.completed", RequestCorrelationID: event.ID()}
	plan, err := planner.resolvePreparedRoutePlan(projection, RoutePlan{Event: event, ActivationPlans: activations, ReplyCreations: []replycontext.Record{reply}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := evidence.returnOwner(ctx, event, &plan, reply)
	if err != nil || owner.Route() != origin || owner.ExistingEntity() || len(evidence.projection.activationOwners) != 2 {
		t.Fatalf("proposed return lost exact construction: owner=%+v err=%v activations=%+v", owner, err, evidence.projection.activationOwners)
	}
	if len(evidence.projection.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope).observations) != 0 {
		t.Fatal("proposed construction became native evidence")
	}
}
