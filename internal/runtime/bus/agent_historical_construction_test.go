package bus

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestHistoricalAgentAttachmentCannotBorrowNativeConstruction(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource(t)
	identity := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
	native := constructionIndexObservation(t, source, busInternalTestRunID, identity, "")
	instance, err := native.WorkflowInstance()
	if err != nil {
		t.Fatal(err)
	}
	sourceRun := uuid.NewString()
	origin, err := runlifecycle.ForkMaterializationRunOrigin(sourceRun, runlifecycle.ForkOriginPointDeploymentRevision, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	historical, err := pipeline.AdmitHistoricalFlowInstanceObservation(native.Selection(), instance,
		runlifecycle.Snapshot{RunID: busInternalTestRunID, State: runlifecycle.StateRunning, Origin: origin, BundleHash: native.SourceFact().BundleHash(), StartedAt: instance.CreatedAt},
		1, pipeline.HistoricalFlowInstanceConstruction{Identity: identity, SourceRevision: 1, WorkflowVersion: instance.WorkflowVersion, SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRun, Route: flowidentity.StoredRoute(".", sourceRun, sourceRun)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	name, err := agentidentity.DeclaredName("collector", "swarm://collector")
	if err != nil {
		t.Fatal(err)
	}
	actor := agentidentity.Identity{RunID: busInternalTestRunID, Name: name, Route: agentidentity.Route{Presence: agentidentity.RouteRoot}}
	header := events.RouteIdentity{FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "root.ready", "historical-proof", "", []byte(`{}`), 0, actor.RunID, events.EventEnvelope{}, time.Now().UTC())
	for _, cell := range []struct {
		name   string
		owner  pipeline.FlowInstanceObservation
		valid  bool
		omit   bool
		mutate func(*RoutePlanDeliveryIntent)
	}{
		{name: "admitted_historical", owner: historical, valid: true},
		{name: "native_receipt_omitted", owner: native, omit: true},
		{name: "historical_dependency_omitted", owner: historical, omit: true},
		{name: "no_admitted_history"},
		{name: "foreign_run", owner: historical, mutate: func(intent *RoutePlanDeliveryIntent) { intent.AgentIdentity.RunID = uuid.NewString() }},
		{name: "foreign_header", owner: historical, mutate: func(intent *RoutePlanDeliveryIntent) { intent.ReceiverBlueprint.EntityID = uuid.NewString() }},
		{name: "actor_entity_loan", owner: historical, mutate: func(intent *RoutePlanDeliveryIntent) {
			intent.TargetOwnership = events.MustExistingEntityTarget(header)
			intent.TargetBlueprint = header
		}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			projection := selectedRunTargetOwnerProjection{
				source: source, required: true,
				context: withSelectedTargetOwnerLookupScope(constructionIndexContext(t, source), selectedTargetOwnerLookupScope{observations: []pipeline.FlowInstanceObservation{cell.owner}}),
				agents:  map[agentidentity.Identity]ActiveAgentDescriptor{actor: {Identity: actor}},
			}
			intent := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor,
				TargetBlueprint:   events.RouteIdentity{FlowID: header.FlowID, FlowInstance: header.FlowInstance},
				TargetOwnership:   events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: header.FlowID, FlowInstance: header.FlowInstance}),
				ReceiverBlueprint: header, AgentLifecycle: agentLifecycleAdmissionMaterializingFlow, Persist: true}
			if cell.mutate != nil {
				cell.mutate(&intent)
			}
			admitted, err := projection.admitAgentTarget(event, intent)
			if err != nil {
				if cell.valid {
					t.Fatalf("historical producer rejected exact fixed construction: %v", err)
				}
				return
			}
			if cell.omit {
				admitted.Initialization = events.ReceiverInitialization{}
			}
			plan := newRoutePlan(event)
			plan.DeliveryIntents = []RoutePlanDeliveryIntent{admitted}
			resolved, err := projection.resolveRoutePlan(plan)
			if (err == nil) != cell.valid {
				t.Fatalf("historical/native attachment classification: %v", err)
			}
			if cell.valid {
				dependency, fixed := resolved.DeliveryIntents[0].Initialization.HistoricalConstruction()
				if !fixed || dependency.SourceRunID != sourceRun || dependency.SourceRevision != 1 || !resolved.DeliveryIntents[0].AgentLifecycle.pending() || !resolved.DeliveryIntents[0].TargetOwnership.EntitylessReceiver() {
					t.Fatal("historical attachment lost fixed-cut provenance, pending readiness or exact actor absence")
				}
			}
		})
	}
}
