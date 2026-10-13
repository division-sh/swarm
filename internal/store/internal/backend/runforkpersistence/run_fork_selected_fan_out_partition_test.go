package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func selectedFanOutPartitionFixture(t *testing.T) (*runForkRevisionSnapshot, runfork.RunForkFanOutPendingReplay, events.AdmittedEvent, []events.DeliveryRoute) {
	t.Helper()
	snapshot, sourceEvent, agent := replayCutFixture(t)
	node := identitytest.FlowNode(t, ".", "remaining")
	nodeRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID})}
	nodeID, err := deliverylifecycle.DeliveryID(sourceEvent.ID(), nodeRoute)
	if err != nil {
		t.Fatal(err)
	}
	nodeDelivery := deliverylifecycle.Snapshot{DeliveryID: nodeID, EventID: sourceEvent.ID(), RunID: snapshot.RunID,
		Route: nodeRoute, Status: deliverylifecycle.StatusPending, FinalSelection: deliverylifecycle.AbsentSelection()}
	snapshot.Deliveries = append(snapshot.Deliveries, runForkRevisionDelivery{Snapshot: nodeDelivery,
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 5}})
	childRun := uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(childRun, snapshot.RunID, sourceEvent.ID(), "selected-owner", "", sourceEvent.ExecutionMode())
	if err != nil {
		t.Fatal(err)
	}
	child := eventtest.SelectedForkReplay(activityidentity.ForkLineageEventID(childRun, sourceEvent.ID()), sourceEvent.Type(),
		eventtest.Producer(events.EventProducerPlatform, "selected-owner"), "", sourceEvent.Payload(), 0, lineage,
		events.EventEnvelope{}, time.Now().UTC())
	child, err = eventtest.AdmitPayload(child, ".", string(child.Type()))
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(child, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	replay := runfork.RunForkFanOutPendingReplay{Ordinal: 0, SourceEventID: sourceEvent.ID(),
		SourceDeliveryIDs: []string{agent.DeliveryID, nodeID}, InheritedDisposition: "succeeded"}
	var routes []events.DeliveryRoute
	for _, delivery := range []deliverylifecycle.Snapshot{agent, nodeDelivery} {
		route, err := projectSelectedFanOutRecipient(snapshot, sourceEvent, delivery, childRun)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, route)
	}
	if routes[0].AgentIdentity.RunID != childRun || routes[1].Target.Route().FlowInstance != childRun ||
		routes[1].Target.Route().EntityID != "" || !routes[1].Target.EntitylessReceiver() {
		t.Fatal("projection did not retain separate agent, entityless node and child ownership")
	}
	return snapshot, replay, admitted, routes
}

func TestSelectedFanOutPartitionRetainsPendingNodeAndAgentWithoutGenericReplay(t *testing.T) {
	snapshot, replay, publication, routes := selectedFanOutPartitionFixture(t)
	before := projectionJSON(t, snapshot)
	if err := requireSelectedFanOutRecipientPartition(snapshot, replay, publication, routes); err != nil {
		t.Fatal(err)
	}
	if before != projectionJSON(t, snapshot) {
		t.Fatal("partition validation mutated the source cut")
	}
	if _, err := projectRunForkReplayInitializedReceiver(snapshot, eventtest.ExistingRunRootIngress(replay.SourceEventID,
		publication.Event().Type(), "operator", "", publication.Event().Payload(), 0, snapshot.RunID, events.EventEnvelope{}, time.Now().UTC()),
		snapshot.Deliveries[len(snapshot.Deliveries)-1].Snapshot, publication.Event().RunID()); err == nil {
		t.Fatal("selected fan-out widened the ordinary agent-only receiver path")
	}
	if runfork.RunForkPendingWorkReplayableForHistoricalReplay(runfork.RunForkPendingWork{
		DeliveryID: replay.SourceDeliveryIDs[1], EventID: replay.SourceEventID, SubscriberType: "node", SubscriberID: "remaining",
		Status: "pending", Classification: runfork.RunForkPendingClassificationPending}) {
		t.Fatal("selected fan-out widened generic node replay")
	}
}

func TestSelectedFanOutPartitionRejectsExtraSettledAndSubstitutedRecipients(t *testing.T) {
	for _, fault := range []string{"omitted", "extra_settled", "duplicate", "source_run", "source_event", "source_delivery", "settled_delivery", "source_root_target", "agent_source_run", "receiver_owner"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, replay, publication, routes := selectedFanOutPartitionFixture(t)
			switch fault {
			case "omitted":
				routes = routes[:1]
			case "extra_settled":
				routes = append(routes, snapshot.Deliveries[0].Snapshot.Route)
			case "duplicate":
				routes[1] = routes[0]
			case "source_run":
				snapshot.RunID = uuid.NewString()
			case "source_event":
				replay.SourceEventID = uuid.NewString()
			case "source_delivery":
				replay.SourceDeliveryIDs[1] = uuid.NewString()
			case "settled_delivery":
				replay.SourceDeliveryIDs[1] = snapshot.Deliveries[0].Snapshot.DeliveryID
			case "source_root_target":
				routes[1].Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID})
			case "agent_source_run":
				routes[0].AgentIdentity.RunID = snapshot.RunID
			case "receiver_owner":
				snapshot.EntityMetadata[0].FlowTemplate = "foreign"
			}
			before := projectionJSON(t, snapshot)
			copyRoutes := append([]events.DeliveryRoute(nil), routes...)
			if err := requireSelectedFanOutRecipientPartition(snapshot, replay, publication, routes); err == nil {
				t.Fatal("inexact partition was accepted")
			}
			if before != projectionJSON(t, snapshot) || !reflect.DeepEqual(routes, copyRoutes) {
				t.Fatal("refusal repaired or mutated cut/child evidence")
			}
		})
	}
}

func TestSelectedFanOutPartitionRetainsOnlyCapturedReplyContexts(t *testing.T) {
	snapshot, replay, publication, _ := selectedFanOutPartitionFixture(t)
	_, _, record := runForkReplyPlanFixture(t, replycontext.StateOpen, true, false)
	record.RunID, record.SourceRunID = snapshot.RunID, snapshot.RunID
	record.Origin = events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID}
	record.ReturnJoins = nil
	record.ID = replycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin,
		record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	snapshot.ReplyContexts = []runForkRevisionReplyContext{{runForkHistoricalReplyFact: runForkHistoricalReplyFact{
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 1, Revision: 5}, Record: record,
	}}}
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	sourceEvent, err := cut.event(replay.SourceEventID)
	if err != nil {
		t.Fatal(err)
	}
	var routes []events.DeliveryRoute
	for i := range snapshot.Deliveries {
		delivery := &snapshot.Deliveries[i].Snapshot
		if delivery.Terminal() {
			continue
		}
		oldID := delivery.DeliveryID
		delivery.Route.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}
		delivery.RouteIdentity, err = delivery.Route.Identity()
		if err != nil {
			t.Fatal(err)
		}
		delivery.DeliveryID, err = deliverylifecycle.DeliveryID(sourceEvent.ID(), delivery.Route)
		if err != nil {
			t.Fatal(err)
		}
		for j, id := range replay.SourceDeliveryIDs {
			if id == oldID {
				replay.SourceDeliveryIDs[j] = delivery.DeliveryID
			}
		}
		route, err := projectSelectedFanOutRecipient(snapshot, sourceEvent, *delivery, publication.Event().RunID())
		if err != nil || route.Context.ReplyContextID() != deterministicRunForkReplyContextID(publication.Event().RunID(), record.ID) {
			t.Fatalf("node/agent receiver did not consume its captured child context: route=%+v error=%v", route, err)
		}
		routes = append(routes, route)
	}
	before := projectionJSON(t, snapshot)
	if err := requireSelectedFanOutRecipientPartition(snapshot, replay, publication, routes); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"lost", "source", "sibling", "missing_row", "foreign_row", "duplicate_row"} {
		t.Run(fault, func(t *testing.T) {
			bad := *snapshot
			bad.ReplyContexts = append([]runForkRevisionReplyContext(nil), snapshot.ReplyContexts...)
			changed := make([]events.DeliveryRoute, len(routes))
			for i, route := range routes {
				changed[i] = route.Normalized()
			}
			switch fault {
			case "lost":
				changed[0].Context = events.DeliveryContext{}
			case "source":
				changed[0].Context.Reply.ID = record.ID
			case "sibling":
				changed[0].Context.Reply.ID = deterministicRunForkReplyContextID(uuid.NewString(), record.ID)
			case "missing_row":
				bad.ReplyContexts = nil
			case "foreign_row":
				bad.ReplyContexts[0].Record.RunID = uuid.NewString()
			case "duplicate_row":
				bad.ReplyContexts = append(bad.ReplyContexts, bad.ReplyContexts[0])
			}
			if err := requireSelectedFanOutRecipientPartition(&bad, replay, publication, changed); err == nil {
				t.Fatal("uncaptured receiver reply binding passed the final partition fence")
			}
		})
	}
	if projectionJSON(t, snapshot) != before {
		t.Fatal("receiver projection or refusal changed source evidence")
	}
}
