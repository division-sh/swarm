package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func publishedBarrierEvidenceFixture(t *testing.T, root bool, mode executionmode.Mode, status deliverylifecycle.Status, summary fanoutbarrier.Summary, generation attemptgeneration.Generation) (*runForkRevisionSnapshot, []runfork.RunForkFanOutObligation) {
	t.Helper()
	snapshot, obligations := barrierScheduleProjectionFixture(t)
	snapshot.Revision = 7
	barrier := obligations[0].Barrier
	registration := &barrier.Registration
	registration.ExecutionMode = mode
	if !root {
		registration.IntentKey.ElementRef.FlowPath = "orders"
		registration.PlanRef.ElementRef = registration.IntentKey.ElementRef
		declaration, err := registration.PlanRef.ElementRef.DeclarationIdentity()
		if err != nil {
			t.Fatal(err)
		}
		node, err := identity.AdmitExecutableNodeDeclaration("orders", "scatter")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", "joined", declaration, registration.PlanRef.BundleHash, registration.PlanRef.SemanticDigest)
		if err != nil {
			t.Fatal(err)
		}
		ref, err = ref.BindFanOutIntent(registration.IntentKey.TriggeringDeliveryID, ref.Generation())
		if err != nil {
			t.Fatal(err)
		}
		registration.Handle, err = timeridentity.JoinCompleteHandle(ref)
		if err != nil {
			t.Fatal(err)
		}
		registration.Route = flowidentity.StoredRoute("orders", "order-1", "orders/order-1")
		registration.EntityID = uuid.NewString()
		registration.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: registration.Route.InstancePath, EntityID: registration.EntityID})
		if err != nil {
			t.Fatal(err)
		}
	}
	ref, _ := registration.Handle.JoinRef()
	ref, err := ref.BindFanOutIntent(registration.IntentKey.TriggeringDeliveryID, generation)
	if err != nil {
		t.Fatal(err)
	}
	registration.Handle, err = timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	barrier.Summary = &summary
	obligations[0].Intent.Request.Key, obligations[0].Intent.Request.PlanRef = registration.IntentKey, registration.PlanRef
	obligations[0].Intent.Request.Cardinality = summary.Total
	timer := &snapshot.Timers[0]
	command, err := genericschedule.FanOutBarrierAdmission(*registration, *barrier.Summary, timer.FireAt)
	if err != nil {
		t.Fatal(err)
	}
	timer.EntityID, timer.FlowInstance, timer.ExecutionMode = command.EntityID, command.FlowInstance, string(mode)
	timer.ScheduleKey, timer.TimerName, timer.TaskID = command.ScheduleKey, command.ScheduleKey, command.TaskID
	timer.ScheduleScope, err = command.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	timer.ImmutableHash, err = command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	timer.FirePayload, err = canonicaljson.Encode(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	timer.RoutingSource, err = json.Marshal(command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	barrier.ScheduleKey = command.ScheduleKey
	at := timer.FireAt.Add(time.Second)
	timer.Status, timer.FiredAt, timer.AcceptedAt, timer.OccurrenceAdmittedAt = "fired", &at, &at, &at
	timer.OccurrenceEventID = genericschedule.OccurrenceEventID(timer.TimerID, timer.FireAt)
	event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{RunID: snapshot.RunID, Facts: events.EventFacts{
		ID: timer.OccurrenceEventID, Type: events.EventType(command.EventType),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: genericschedule.OccurrenceProducerID()},
		TaskID:   command.TaskID, Payload: timer.FirePayload, ExecutionMode: mode, CreatedAt: timer.FireAt,
		Envelope: events.EventEnvelope{EntityID: command.EntityID, FlowInstance: command.FlowInstance}, RoutingSource: command.RoutingSource,
	}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{BundleHash: registration.PlanRef.BundleHash,
		FlowID: registration.IntentKey.ElementRef.FlowPath, EventKey: command.EventType,
		SchemaDigest: "sha256:" + strings.Repeat("b", 64), SchemaClass: events.PayloadSchemaPlatform})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission(timer.FirePayload, schema)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, payload)
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
	snapshot.Events = []runForkRevisionEvent{genericPublicationRevisionEvent(record, event)}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: ref.FlowPath(), FlowInstance: registration.Route.InstancePath, EntityID: registration.EntityID,
	})}.Normalized()
	deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	routeID, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 4}, Snapshot: deliverylifecycle.Snapshot{
		RunID: snapshot.RunID, DeliveryID: deliveryID, EventID: event.ID(), Route: route, RouteIdentity: routeID,
		SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: ref.Node().Key(), Status: status, FinalSelection: deliverylifecycle.AbsentSelection(),
		CreatedAt: at, UpdatedAt: at,
	}}}
	return snapshot, obligations
}

func TestPublishedBarrierAdmissionRequiresExactFixedCutPublicationAndDelivery(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			for _, fault := range []string{"exact", "missing_event", "duplicate_event", "foreign_event", "future_event", "unversioned_event", "missing_schema", "wrong_task", "wrong_payload", "missing_delivery", "duplicate_delivery", "foreign_delivery", "future_delivery", "unversioned_delivery", "wrong_delivery_id", "wrong_recipient", "wrong_target", "materializing_target", "wrong_subscriber", "settled_delivery"} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+string(mode)+"/"+fault, func(t *testing.T) {
					snapshot, obligations := publishedBarrierEvidenceFixture(t, root, mode, deliverylifecycle.StatusPending, fanoutbarrier.Summary{}, attemptgeneration.Generation{})
					switch fault {
					case "missing_event":
						snapshot.Events = nil
					case "duplicate_event":
						snapshot.Events = append(snapshot.Events, snapshot.Events[0])
					case "foreign_event":
						snapshot.Events[0].RunID = uuid.NewString()
					case "future_event":
						snapshot.Events[0].Revision = snapshot.Revision + 1
					case "unversioned_event":
						snapshot.Events[0].FirstRevision = 0
					case "missing_schema":
						snapshot.Events[0].PayloadSchemaDigest = ""
					case "wrong_task":
						snapshot.Events[0].TaskID = "foreign-task"
					case "wrong_payload":
						snapshot.Events[0].Payload = []byte(`{"join":{"total":1}}`)
					case "missing_delivery":
						snapshot.Deliveries = nil
					case "duplicate_delivery":
						snapshot.Deliveries = append(snapshot.Deliveries, snapshot.Deliveries[0])
					case "foreign_delivery":
						snapshot.Deliveries[0].Snapshot.RunID = uuid.NewString()
					case "future_delivery":
						snapshot.Deliveries[0].Revision = snapshot.Revision + 1
					case "unversioned_delivery":
						snapshot.Deliveries[0].FirstRevision = 0
					case "wrong_delivery_id":
						snapshot.Deliveries[0].Snapshot.DeliveryID = uuid.NewString()
					case "wrong_recipient":
						node, err := identity.AdmitExecutableNodeDeclaration(obligations[0].Barrier.Registration.IntentKey.ElementRef.FlowPath, "other")
						if err != nil {
							t.Fatal(err)
						}
						snapshot.Deliveries[0].Snapshot.Route.Recipient = events.MustNodeDeliveryRecipient(node)
					case "wrong_target":
						target := snapshot.Deliveries[0].Snapshot.Route.Target.Route()
						target.EntityID = uuid.NewString()
						snapshot.Deliveries[0].Snapshot.Route.Target = events.MustExistingEntityTarget(target)
					case "materializing_target":
						snapshot.Deliveries[0].Snapshot.Route.Target = events.MustMaterializingEntityTarget(snapshot.Deliveries[0].Snapshot.Route.Target.Route())
					case "wrong_subscriber":
						snapshot.Deliveries[0].Snapshot.SubscriberID = "foreign"
					case "settled_delivery":
						snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
					}
					before, err := json.Marshal(snapshot)
					if err != nil {
						t.Fatal(err)
					}
					owned, err := validateRunForkBarrierSchedules(snapshot, obligations)
					if fault == "exact" {
						if err != nil || len(owned) != 1 {
							t.Fatalf("exact published barrier rejected: owned=%v err=%v", owned, err)
						}
					} else if err == nil {
						t.Fatal("published barrier admitted without exact unfinished publication evidence")
					}
					after, _ := json.Marshal(snapshot)
					if !reflect.DeepEqual(before, after) {
						t.Fatal("publication admission mutated source history")
					}
				})
			}
		}
	}
}

func TestPublishedBarrierEvidenceKeepsObserversSeparateFromOutcomeOwner(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, status := range []deliverylifecycle.Status{deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress, deliverylifecycle.StatusFailed} {
			t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+string(status), func(t *testing.T) {
				snapshot, obligations := publishedBarrierEvidenceFixture(t, root, executionmode.Live, status, fanoutbarrier.Summary{}, attemptgeneration.Generation{})
				original := snapshot.Deliveries[0].Snapshot.Route
				observer := snapshot.Deliveries[0]
				target := original.Target.Route()
				target.EntityID, target.FlowInstance = uuid.NewString(), "orders/observer"
				observer.Snapshot.Route.Target = events.MustExistingEntityTarget(target)
				observer.Snapshot.Status = deliverylifecycle.StatusDelivered
				var err error
				observer.Snapshot.RouteIdentity, err = observer.Snapshot.Route.Identity()
				if err != nil {
					t.Fatal(err)
				}
				observer.Snapshot.DeliveryID, err = deliverylifecycle.DeliveryID(observer.Snapshot.EventID, observer.Snapshot.Route)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Deliveries = append(snapshot.Deliveries, observer)
				activation, err := projectRunForkGenericActivation(snapshot.Timers[0])
				if err != nil {
					t.Fatal(err)
				}
				admitted, route, err := runForkPublishedFanOutBarrierEvidence(snapshot, *obligations[0].Barrier, activation)
				if err != nil || !reflect.DeepEqual(route, original) || admitted.Event().ID() != snapshot.Events[0].EventID {
					t.Fatalf("observer changed exact pending outcome owner: route=%+v err=%v", route, err)
				}
				snapshot.Deliveries = snapshot.Deliveries[1:]
				if _, _, err := runForkPublishedFanOutBarrierEvidence(snapshot, *obligations[0].Barrier, activation); err == nil {
					t.Fatal("observer stood in for the absent exact outcome delivery")
				}
			})
		}
	}
}

func TestPublishedBarrierEvidencePreservesNonemptySummaryAndLoopGeneration(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			for _, summary := range []fanoutbarrier.Summary{{Total: 2, Succeeded: 2}, {Total: 2, DeadLettered: 1, Canceled: 1}, {Total: 3, Succeeded: 1, DeadLettered: 1, SemanticRejected: 1}} {
				for _, generated := range []bool{false, true} {
					t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+string(mode)+"/"+publishedBarrierSummaryName(summary)+"/"+map[bool]string{false: "bare", true: "generation"}[generated], func(t *testing.T) {
						generation := attemptgeneration.Generation{}
						if generated {
							flow := "."
							if !root {
								flow = "orders"
							}
							generation = attemptgeneration.Generation{FlowID: flow, LoopID: "review", ActivationID: uuid.NewString(), RevisionField: "revision", RevisionID: "source-revision", Attempt: 7}
						}
						snapshot, obligations := publishedBarrierEvidenceFixture(t, root, mode, deliverylifecycle.StatusPending, summary, generation)
						activation, err := projectRunForkGenericActivation(snapshot.Timers[0])
						if err != nil {
							t.Fatal(err)
						}
						before, err := json.Marshal(snapshot)
						if err != nil {
							t.Fatal(err)
						}
						admitted, _, err := runForkPublishedFanOutBarrierEvidence(snapshot, *obligations[0].Barrier, activation)
						if err != nil {
							t.Fatal(err)
						}
						payload, err := canonicaljson.Decode(admitted.Event().Payload())
						if err != nil || !payload.Equal(activation.Command.Payload) {
							t.Fatalf("retained summary or generation changed: payload=%s err=%v", admitted.Event().Payload(), err)
						}
						if !admitted.Event().CreatedAt().Equal(activation.CurrentDueAt) || admitted.Event().ExecutionMode() != mode || activation.ValidateForkJoinRestorationSource() == nil {
							t.Fatal("publication evidence changed due/mode or granted rearming")
						}
						after, _ := json.Marshal(snapshot)
						if !reflect.DeepEqual(before, after) {
							t.Fatal("publication evidence changed the source cut")
						}
					})
				}
			}
		}
	}
}

func publishedBarrierSummaryName(summary fanoutbarrier.Summary) string {
	if summary.Succeeded == summary.Total {
		return "success"
	}
	if summary.Canceled != 0 {
		return "dead-letter-canceled"
	}
	return "mixed"
}
