package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func terminalBarrierHistoryFixture(t *testing.T) (*runForkRevisionSnapshot, []runfork.RunForkFanOutObligation, []runfork.RunForkPendingWork) {
	t.Helper()
	snapshot, obligations := barrierScheduleProjectionFixture(t)
	snapshot.Revision = 10
	barrier, timer := obligations[0].Barrier, &snapshot.Timers[0]
	barrier.Status = fanoutbarrier.StatusOutcomeDeadLettered
	at := timer.FireAt.Add(time.Second)
	timer.Status, timer.FiredAt, timer.AcceptedAt, timer.OccurrenceAdmittedAt = "fired", &at, &at, &at
	timer.OccurrenceEventID = genericschedule.OccurrenceEventID(timer.TimerID, timer.FireAt)
	ref, _ := barrier.Registration.Handle.JoinRef()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID})}.Normalized()
	identity, err := route.Identity()
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
	settlementRaw, _ := json.Marshal(settlement)
	snapshot.Events = []runForkRevisionEvent{{RunID: snapshot.RunID, EventID: timer.OccurrenceEventID, EventClass: string(events.EventAdmissionRuntimeControl), EventName: timer.FireEvent,
		ExecutionMode: timer.ExecutionMode, TaskID: timer.TaskID, ProducedBy: genericschedule.OccurrenceProducerID(), ProducedByType: string(events.EventProducerPlatform),
		Payload: timer.FirePayload, RoutingSource: barrier.Registration.RoutingSource, RouteSettlement: settlementRaw, CreatedAt: timer.FireAt}}
	failure := failures.Normalize(failures.New(failures.ClassLifecycleConflict, "barrier_test_failure", "test", "barrier", nil), "test", "barrier")
	delivery := deliverylifecycle.Snapshot{DeliveryID: uuid.NewString(), EventID: timer.OccurrenceEventID, RunID: snapshot.RunID,
		Route: route, RouteIdentity: identity, SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: route.Recipient.ID(),
		Status: deliverylifecycle.StatusDeadLetter, ClaimVersion: 2, Failure: &failure, ReasonCode: "barrier_failed", CreatedAt: at, StartedAt: at, SettledAt: at, UpdatedAt: at}
	snapshot.Deliveries = []runForkRevisionDelivery{{Snapshot: delivery}}
	failureRaw, _ := json.Marshal(failure)
	snapshot.DeadLetters = []runForkRevisionDeadLetter{{DeadLetterID: uuid.NewString(), OriginalEventID: delivery.EventID, DeliveryID: delivery.DeliveryID,
		HandlerNode: delivery.SubscriberID, CreatedAt: at, ClaimVersion: delivery.ClaimVersion, Outcome: "dead_letter", OutcomeReasonCode: delivery.ReasonCode,
		OutcomeFailure: failureRaw, OutcomeSettledAt: &at}}
	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, obligations, pending
}

func TestTerminalBarrierHistoryExactRecipientOnly(t *testing.T) {
	snapshot, obligations, pending := terminalBarrierHistoryFixture(t)
	other := pending[0]
	other.DeliveryID = uuid.NewString()
	other.SubscriberID = "other"
	node, err := identity.AdmitExecutableNodeDeclaration(".", "other")
	if err != nil {
		t.Fatal(err)
	}
	other.DeliveryRoute.Recipient = events.MustNodeDeliveryRecipient(node)
	pending = append(pending, other)
	if err := admitRunForkTerminalBarrierHistory(snapshot, obligations, pending); err != nil {
		t.Fatal(err)
	}
	if !pending[0].RetainsTerminalBarrierHistory() || pending[1].RetainsTerminalBarrierHistory() {
		t.Fatalf("wrong recipient retention: %+v", pending)
	}
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{Pending: pending[:1]})
	if !admission.StateOnlyExecutionReady || admission.ReplayResumeFactsPresent || admission.DeliveryEventReplayReady {
		t.Fatalf("terminal history became replay work: %+v", admission)
	}
	if admission.Dispositions[3].Classification != runfork.RunForkPendingClassificationDeadLetter {
		t.Fatal("failure rewritten as success")
	}
	mixed := runForkReplayResumeAdmission(runForkAdmissionEvidence{Pending: pending})
	if mixed.StateOnlyExecutionReady || !mixed.ReplayResumeFactsPresent || len(mixed.UnsupportedBlockers) != 1 {
		t.Fatalf("sibling failure hidden: %+v", mixed)
	}
	// An exposed readback record is not a factory for admitted history.
	raw, err := json.Marshal(pending[0])
	if err != nil {
		t.Fatal(err)
	}
	var decoded runfork.RunForkPendingWork
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RetainsTerminalBarrierHistory() {
		t.Fatal("wire data manufactured terminal-history admission")
	}
}

func TestTerminalBarrierHistoryRejectsContradictionsWithoutProjectionMutation(t *testing.T) {
	for _, variant := range []string{"activation_missing", "occurrence_missing", "occurrence_run", "occurrence_class", "occurrence_mode", "occurrence_task", "occurrence_producer", "occurrence_parent", "occurrence_payload", "occurrence_time", "occurrence_duplicate", "delivery_missing", "delivery_run", "delivery_route", "delivery_status", "delivery_claim", "delivery_failure", "delivery_retry", "delivery_duplicate", "dead_letter_missing", "dead_letter_event", "dead_letter_claim", "dead_letter_handler", "dead_letter_duplicate", "outcome", "outcome_reason", "outcome_failure", "outcome_time", "pending_status", "pending_claim", "pending_duplicate", "pending_missing"} {
		t.Run(variant, func(t *testing.T) {
			snapshot, obligations, pending := terminalBarrierHistoryFixture(t)
			switch variant {
			case "activation_missing":
				snapshot.Timers = nil
			case "occurrence_missing":
				snapshot.Events = nil
			case "occurrence_run":
				snapshot.Events[0].RunID = uuid.NewString()
			case "occurrence_class":
				snapshot.Events[0].EventClass = "child"
			case "occurrence_mode":
				snapshot.Events[0].ExecutionMode = "mock"
			case "occurrence_task":
				snapshot.Events[0].TaskID = "wrong"
			case "occurrence_producer":
				snapshot.Events[0].ProducedBy = "wrong"
			case "occurrence_parent":
				snapshot.Events[0].SourceEventID = uuid.NewString()
			case "occurrence_payload":
				snapshot.Events[0].Payload = json.RawMessage(`{}`)
			case "occurrence_time":
				snapshot.Events[0].CreatedAt = snapshot.Events[0].CreatedAt.Add(time.Second)
			case "occurrence_duplicate":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "delivery_missing":
				snapshot.Deliveries = nil
			case "delivery_run":
				snapshot.Deliveries[0].Snapshot.RunID = uuid.NewString()
			case "delivery_route":
				snapshot.Deliveries[0].Snapshot.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: uuid.NewString()})
			case "delivery_status":
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusFailed
			case "delivery_claim":
				snapshot.Deliveries[0].Snapshot.ClaimVersion++
			case "delivery_failure":
				snapshot.Deliveries[0].Snapshot.Failure = nil
			case "delivery_retry":
				snapshot.Deliveries[0].Snapshot.NextEligibleAt = time.Now()
			case "delivery_duplicate":
				snapshot.Deliveries = append(snapshot.Deliveries, snapshot.Deliveries[0])
			case "dead_letter_missing":
				snapshot.DeadLetters = nil
			case "dead_letter_event":
				snapshot.DeadLetters[0].OriginalEventID = uuid.NewString()
			case "dead_letter_claim":
				snapshot.DeadLetters[0].ClaimVersion++
			case "dead_letter_handler":
				snapshot.DeadLetters[0].HandlerNode = "wrong"
			case "dead_letter_duplicate":
				snapshot.DeadLetters = append(snapshot.DeadLetters, snapshot.DeadLetters[0])
			case "outcome":
				snapshot.DeadLetters[0].Outcome = "delivered"
			case "outcome_reason":
				snapshot.DeadLetters[0].OutcomeReasonCode = "wrong"
			case "outcome_failure":
				snapshot.DeadLetters[0].OutcomeFailure = json.RawMessage(`{}`)
			case "outcome_time":
				snapshot.DeadLetters[0].OutcomeSettledAt = nil
			case "pending_status":
				pending[0].Status = "delivered"
			case "pending_claim":
				pending[0].ClaimVersion++
			case "pending_duplicate":
				pending = append(pending, pending[0])
			case "pending_missing":
				pending = nil
			}
			before, _ := json.Marshal(snapshot)
			beforePending := append([]runfork.RunForkPendingWork(nil), pending...)
			if err := admitRunForkTerminalBarrierHistory(snapshot, obligations, pending); err == nil {
				t.Fatal("contradiction admitted")
			}
			after, _ := json.Marshal(snapshot)
			if string(before) != string(after) || !reflect.DeepEqual(beforePending, pending) {
				t.Fatal("failed admission mutated its inputs")
			}
		})
	}
}
