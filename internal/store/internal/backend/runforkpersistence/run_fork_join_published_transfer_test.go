package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/google/uuid"
)

func publishedArrivalTransferFixture(t *testing.T, root, completion bool, status deliverylifecycle.Status) (*runForkRevisionSnapshot, runfork.RunForkPlan, genericschedule.Activation, events.Event, string) {
	t.Helper()
	snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, root, completion)
	snapshot.Revision = 7
	for i := range snapshot.Timers {
		row := &snapshot.Timers[i]
		row.FirstRevision, row.Revision = 3, 7
		if row.TaskID == join.TimerTaskID() {
			accepted := row.FireAt.Add(time.Hour)
			row.Status = string(genericschedule.StatusFired)
			row.OccurrenceEventID = genericschedule.OccurrenceEventID(row.TimerID, row.FireAt)
			row.OccurrenceAdmittedAt, row.FiredAt, row.AcceptedAt = &accepted, &accepted, &accepted
		}
	}
	arrivals, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
	if err != nil {
		t.Fatal(err)
	}
	var source genericschedule.Activation
	for _, row := range arrivals {
		if row.Command.TaskID == join.TimerTaskID() {
			source = row
		}
	}
	if source.Status != genericschedule.StatusFired {
		t.Fatal("fixture did not retain the fired source arm")
	}
	projectedPayload, err := workflowexpr.ProjectSemanticValue(source.Command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.MarshalPreservingNumberKinds(projectedPayload)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		RunID: source.Command.RunID,
		Facts: events.EventFacts{ID: source.CurrentEventID, Type: events.EventType(source.Command.EventType),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: genericschedule.OccurrenceProducerID()},
			TaskID:   source.Command.TaskID, Payload: payload, RoutingSource: source.Command.RoutingSource,
			Envelope:  events.EventEnvelope{EntityID: source.Command.EntityID, FlowInstance: source.Command.FlowInstance},
			CreatedAt: source.CurrentDueAt, ExecutionMode: source.Command.ExecutionMode},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := join.JoinRef()
	schema, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FlowID: ref.FlowPath(), EventKey: source.Command.EventType,
		SchemaDigest: "sha256:" + strings.Repeat("b", 64), SchemaClass: events.PayloadSchemaPlatform,
	})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadAdmission(payload, schema)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := record.Decode()
	if err != nil {
		t.Fatal(err)
	}
	event = decoded.Event()
	snapshot.Events = []runForkRevisionEvent{genericPublicationRevisionEvent(record, event)}
	entry := ref.StageEntry()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: ref.FlowPath(), FlowInstance: entry.InstancePath, EntityID: entry.EntityID,
	})}
	identity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 7},
		Snapshot: deliverylifecycle.Snapshot{DeliveryID: deliveryID, EventID: event.ID(), RunID: snapshot.RunID,
			Route: route, RouteIdentity: identity, SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: ref.Node().Key(),
			Status: status, FinalSelection: deliverylifecycle.AbsentSelection(), CreatedAt: source.CurrentEventAdmittedAt},
	}}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: arrivals,
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}}
	if _, err := requireRunForkPublishedArrivalDelivery(snapshot, source, event); err != nil {
		t.Fatalf("baseline source delivery: %v", err)
	}
	return snapshot, plan, source, event, uuid.NewString()
}

func publishedArrivalTransferState(t *testing.T, snapshot *runForkRevisionSnapshot, plan runfork.RunForkPlan) string {
	t.Helper()
	var payloads []json.RawMessage
	for _, event := range snapshot.Events {
		payloads = append(payloads, event.Payload)
	}
	var schedules []string
	for _, row := range plan.JoinSchedules {
		digest, err := row.EvidenceDigest()
		if err != nil {
			t.Fatal(err)
		}
		schedules = append(schedules, digest)
	}
	var deliveryIdentities []string
	for _, delivery := range snapshot.Deliveries {
		deliveryIdentities = append(deliveryIdentities, events.EncodeDeliveryRouteIdentity(delivery.Snapshot.RouteIdentity))
	}
	// Event.Payload and Plan.JoinSchedules are intentionally omitted by their
	// JSON views, so include both in the non-mutation observation explicitly.
	return projectionJSON(t, struct {
		Snapshot           *runForkRevisionSnapshot
		Plan               runfork.RunForkPlan
		Payloads           []json.RawMessage
		Schedules          []string
		DeliveryIdentities []string
	}{snapshot, plan, payloads, schedules, deliveryIdentities})
}

func TestPublishedArrivalTransferConsumesExactUnfinishedSourceAtCut(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			for _, status := range []deliverylifecycle.Status{deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress, deliverylifecycle.StatusFailed} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout/", true: "complete/"}[completion]+string(status), func(t *testing.T) {
					snapshot, plan, source, original, childRun := publishedArrivalTransferFixture(t, root, completion, status)
					before := publishedArrivalTransferState(t, snapshot, plan)
					continuation, err := prepareRunForkPublishedArrivalTransfer(snapshot, plan, childRun, original.ID())
					if err != nil || !continuation.Present() || !reflect.DeepEqual(continuation.SourceEvent(), original) {
						t.Fatalf("exact transfer: present=%v err=%v", continuation.Present(), err)
					}
					child := continuation.ChildCommand()
					_, sourceRef, _ := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
					handle, childRef, valid := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
					wantEntity := source.Command.EntityID
					if root {
						wantEntity = childRun
					}
					if !valid || !sourceRef.Declaration().Equal(childRef.Declaration()) || child.RunID != childRun || child.EntityID != wantEntity ||
						child.TaskID != handle.TaskID() || child.TaskID == source.Command.TaskID || child.EventType != source.Command.EventType ||
						!child.Due.Absolute.Equal(source.CurrentDueAt) || child.ExecutionMode != source.Command.ExecutionMode ||
						childRef.StageEntry().OriginRunID != source.Command.RunID || childRef.StageEntry().EventID != sourceRef.StageEntry().EventID {
						t.Fatalf("lost source/child occurrence relation: %+v", child)
					}
					id := activityidentity.ForkLineageEventID(childRun, original.ID())
					event, err := continuation.Event(id, "selection:transfer-test")
					if err != nil || continuation.ValidateEvent(event, "selection:transfer-test") != nil ||
						!event.Producer().Equal(original.Producer()) || !event.CreatedAt().Equal(original.CreatedAt()) {
						t.Fatalf("child publication: %+v err=%v", event, err)
					}
					request := storegenericschedule.ForkJoinRequest{Source: source, Child: child, PointKind: plan.ForkPoint.Kind,
						PointRevision: plan.ForkPoint.Revision, BornAt: source.AcceptedAt.Add(time.Second), Disposition: genericschedule.ForkJoinRetained}
					if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "published") {
						t.Fatalf("published transfer became a fresh schedule request: %v", err)
					}
					if after := publishedArrivalTransferState(t, snapshot, plan); after != before {
						t.Fatal("transfer changed source snapshot, activation, or entity state")
					}
				})
			}
		}
	}
}

func TestPublishedArrivalTransferRefusesUnprovenCutPublicationAndDelivery(t *testing.T) {
	for _, fault := range []string{"wrong_key", "wrong_cut", "foreign_cut", "missing_source", "duplicate_source", "missing_event", "duplicate_event", "foreign_event", "future_event", "unversioned_event", "wrong_task", "wrong_payload", "missing_schema", "prepared_only",
		"missing_delivery", "duplicate_delivery", "foreign_delivery", "future_delivery", "unversioned_delivery", "backwards_delivery", "missing_delivery_id", "wrong_delivery_id", "wrong_node", "wrong_target", "materializing_target", "route_identity", "subscriber_class", "subscriber_id", "invalid_context", "foreign_context_owner", "context_identity", "delivered", "canceled", "dead_letter"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, plan, source, original, childRun := publishedArrivalTransferFixture(t, true, true, deliverylifecycle.StatusPending)
			key := original.ID()
			deliveryFault := false
			switch fault {
			case "wrong_key":
				key = uuid.NewString()
			case "wrong_cut":
				plan.ForkPoint.Revision++
			case "foreign_cut":
				snapshot.RunID = uuid.NewString()
			case "missing_source":
				plan.JoinSchedules = nil
			case "duplicate_source":
				plan.JoinSchedules = append(plan.JoinSchedules, source)
			case "missing_event":
				snapshot.Events = nil
			case "duplicate_event":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "foreign_event":
				snapshot.Events[0].RunID = uuid.NewString()
			case "future_event":
				snapshot.Events[0].Revision = 8
			case "unversioned_event":
				snapshot.Events[0].FirstRevision = 0
			case "wrong_task":
				snapshot.Events[0].TaskID = "unrelated-task"
			case "wrong_payload":
				snapshot.Events[0].Payload = []byte(`{"hostile":true}`)
			case "missing_schema":
				snapshot.Events[0].PayloadSchemaDigest = ""
			case "prepared_only":
				for i := range plan.JoinSchedules {
					if plan.JoinSchedules[i].ID == source.ID {
						plan.JoinSchedules[i].Status = genericschedule.StatusActive
						plan.JoinSchedules[i].FiredAt, plan.JoinSchedules[i].AcceptedAt = time.Time{}, time.Time{}
					}
				}
			default:
				deliveryFault = true
				row := &snapshot.Deliveries[0]
				switch fault {
				case "missing_delivery":
					snapshot.Deliveries = nil
				case "duplicate_delivery":
					snapshot.Deliveries = append(snapshot.Deliveries, *row)
				case "foreign_delivery":
					row.Snapshot.RunID = uuid.NewString()
				case "future_delivery":
					row.Revision = 8
				case "unversioned_delivery":
					row.FirstRevision = 0
				case "backwards_delivery":
					row.Revision = row.FirstRevision - 1
				case "missing_delivery_id":
					row.Snapshot.DeliveryID = ""
				case "wrong_delivery_id":
					row.Snapshot.DeliveryID = uuid.NewString()
				case "wrong_node":
					node, err := runtimeidentity.ParseExecutableNode(".", "unrelated-node")
					if err != nil {
						t.Fatal(err)
					}
					row.Snapshot.Route.Recipient = events.MustNodeDeliveryRecipient(node)
				case "wrong_target":
					target := row.Snapshot.Route.Target.Route()
					target.EntityID = uuid.NewString()
					row.Snapshot.Route.Target = events.MustExistingEntityTarget(target)
				case "materializing_target":
					row.Snapshot.Route.Target = events.MustMaterializingEntityTarget(row.Snapshot.Route.Target.Route())
				case "route_identity":
					identity, err := events.ParseDeliveryRouteIdentity("delivery-route-v2:sha256:" + strings.Repeat("0", 64))
					if err != nil {
						t.Fatal(err)
					}
					row.Snapshot.RouteIdentity = identity
				case "subscriber_class":
					row.Snapshot.SubscriberClass = deliverylifecycle.SubscriberAgent
				case "subscriber_id":
					row.Snapshot.SubscriberID = "unrelated-node"
				case "invalid_context", "foreign_context_owner", "context_identity":
					_, ref, _ := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
					receipt := events.JoinAdmissionReceipt{Ref: ref, Disposition: events.JoinAdmissionBound}
					if fault == "invalid_context" {
						receipt.Disposition = "invalid"
					}
					if fault == "foreign_context_owner" {
						entry := ref.StageEntry()
						entry.EntityID = uuid.NewString()
						foreign, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
						if err != nil {
							t.Fatal(err)
						}
						receipt.Ref = foreign
					}
					row.Snapshot.Route.Context.Joins = []events.JoinAdmissionReceipt{receipt}
				default:
					row.Snapshot.Status = deliverylifecycle.Status(fault)
				}
			}
			before := publishedArrivalTransferState(t, snapshot, plan)
			if deliveryFault {
				if _, err := requireRunForkPublishedArrivalDelivery(snapshot, source, original); err == nil {
					t.Fatal("unproven durable source delivery accepted")
				}
			}
			if got, err := prepareRunForkPublishedArrivalTransfer(snapshot, plan, childRun, key); err == nil || got.Present() {
				t.Fatalf("unproven published transfer accepted: present=%v err=%v", got.Present(), err)
			}
			if after := publishedArrivalTransferState(t, snapshot, plan); after != before {
				t.Fatal("refusal mutated the fixed source evidence")
			}
		})
	}
}

func TestPublishedArrivalTransferRetainsOriginalDeliveryContext(t *testing.T) {
	for _, root := range []bool{false, true} {
		t.Run(map[bool]string{false: "flow", true: "root"}[root], func(t *testing.T) {
			snapshot, plan, source, original, childRun := publishedArrivalTransferFixture(t, root, true, deliverylifecycle.StatusPending)
			_, ref, _ := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
			row := &snapshot.Deliveries[0].Snapshot
			row.Route.Context.Joins = []events.JoinAdmissionReceipt{{Ref: ref, Disposition: events.JoinAdmissionBound}}
			var err error
			row.RouteIdentity, err = row.Route.Identity()
			if err != nil {
				t.Fatal(err)
			}
			row.DeliveryID, err = deliverylifecycle.DeliveryID(original.ID(), row.Route)
			if err != nil {
				t.Fatal(err)
			}
			before := publishedArrivalTransferState(t, snapshot, plan)
			route, err := requireRunForkPublishedArrivalDelivery(snapshot, source, original)
			if err != nil || !reflect.DeepEqual(route, row.Route.Normalized()) || len(route.Context.Joins) != 1 {
				t.Fatalf("original route context replaced or discarded: %+v err=%v", route, err)
			}
			continuation, err := prepareRunForkPublishedArrivalTransfer(snapshot, plan, childRun, original.ID())
			if err != nil {
				t.Fatalf("exact context-bearing transfer refused: %v", err)
			}
			projected, err := projectRunForkPublishedArrivalRoute(plan, continuation.ChildCommand(), route)
			if err != nil || len(projected.Context.Joins) != 1 || projected.Context.Joins[0].Disposition != events.JoinAdmissionBound {
				t.Fatalf("projected route lost its immutable binding: %+v err=%v", projected, err)
			}
			childRef := projected.Context.Joins[0].Ref
			if childRef.StageEntry().RunID != childRun || childRef.StageEntry().OriginRunID != ref.StageEntry().RunID ||
				childRef.StageEntry().EventID != ref.StageEntry().EventID || childRef.Declaration() != ref.Declaration() {
				t.Fatalf("projected route rebound the original return: %+v", childRef)
			}
			if after := publishedArrivalTransferState(t, snapshot, plan); after != before {
				t.Fatal("context readback changed its original source evidence")
			}
		})
	}
}
