package runforkpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func transferredBarrierHistoryFixture(t *testing.T, root bool, mode executionmode.Mode) (*runForkRevisionSnapshot, fanoutbarrier.Barrier) {
	t.Helper()
	snapshot, obligations := publishedBarrierEvidenceFixture(t, root, mode, deliverylifecycle.StatusPending,
		fanoutbarrier.Summary{Total: 2, Succeeded: 1, DeadLettered: 1}, attemptgeneration.Generation{})
	source := *obligations[0].Barrier
	childRun, born := uuid.NewString(), source.UpdatedAt.Add(time.Hour)
	continuation, route, err := prepareRunForkBarrierTransfer(context.Background(), nil, snapshot, source, childRun, source.Registration.PlanRef, nil, born)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := continuation.RetainedPublication(runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := runfork.ProjectFanOutBarrierRegistration(source.Registration, childRun, source.Registration.PlanRef, nil, born)
	if err != nil {
		t.Fatal(err)
	}
	child := fanoutbarrier.Barrier{Registration: registration, Status: fanoutbarrier.StatusClosedPending,
		Summary: source.Summary, ScheduleKey: registration.Handle.TaskID(), UpdatedAt: born,
		TransferredOccurrence: &fanoutbarrier.TransferredOccurrence{Publication: publication, DueAt: continuation.ChildCommand().Due.Absolute},
	}
	occurrence, err := genericschedule.NewTransferredFanOutBarrierOccurrence(child)
	if err != nil {
		t.Fatal(err)
	}
	event, err := occurrence.Event()
	if err != nil {
		t.Fatal(err)
	}
	originalAdmission, _ := continuation.SourceEvent().PayloadAdmission()
	bound, err := events.NewPayloadAdmission(event.Payload(), originalAdmission.Binding())
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, bound)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, _ := events.NewConnectEvaluationLedger(nil)
	settlement, err := events.NewDeliverySettlement(events.EventWriteSelectedForkPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	retained := &runForkRevisionSnapshot{RunID: childRun, Revision: snapshot.Revision,
		Events: []runForkRevisionEvent{genericPublicationRevisionEvent(record, event)}}
	target := occurrence.Command.RoutingSource.Route()
	if root {
		target.FlowID, target.FlowInstance = ".", childRun
	}
	route.Target = events.MustExistingEntityTarget(target)
	identity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	delivery := snapshot.Deliveries[0]
	delivery.Snapshot.RunID, delivery.Snapshot.EventID, delivery.Snapshot.DeliveryID = childRun, event.ID(), id
	delivery.Snapshot.Route, delivery.Snapshot.RouteIdentity = route, identity
	retained.Deliveries = []runForkRevisionDelivery{delivery}
	return retained, child
}

func TestPublishedBarrierTransferRetainsReforkableExactEvidenceWithoutTimer(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			t.Run(map[bool]string{false: "flow", true: "root"}[root]+"/"+string(mode), func(t *testing.T) {
				snapshot, barrier := transferredBarrierHistoryFixture(t, root, mode)
				childRun := uuid.NewString()
				continued, _, err := prepareRunForkBarrierTransfer(context.Background(), nil, snapshot, barrier, childRun, barrier.Registration.PlanRef, nil, barrier.UpdatedAt.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				publication, err := continued.RetainedPublication("exact-next-selected-owner")
				if err != nil {
					t.Fatal(err)
				}
				if publication.SourceRunID != snapshot.RunID || publication.SourceEventID != barrier.TransferredOccurrence.Publication.EventID ||
					publication.CommittedCause != barrier.TransferredOccurrence.Publication.CommittedCause ||
					continued.ChildCommand().ExecutionMode != mode || !continued.ChildCommand().Due.Absolute.Equal(barrier.TransferredOccurrence.DueAt) || len(snapshot.Timers) != 0 {
					t.Fatal("refork invented a timer or replaced immediate source/retained due/mode")
				}
			})
		}
	}
}

func TestUnpublishedBarrierProjectionRetainsCommittedCause(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			t.Run(map[bool]string{false: "flow", true: "root"}[root]+"/"+string(mode), func(t *testing.T) {
				snapshot, barrier := transferredBarrierHistoryFixture(t, root, mode)
				snapshot.Events, snapshot.Deliveries = nil, nil
				source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(barrier)
				if err != nil {
					t.Fatal(err)
				}
				childRun := uuid.NewString()
				registration, err := runfork.ProjectFanOutBarrierRegistration(barrier.Registration, childRun, barrier.Registration.PlanRef, nil, barrier.UpdatedAt.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				command, err := genericschedule.FanOutBarrierAdmission(registration, *barrier.Summary, barrier.TransferredOccurrence.DueAt)
				if err != nil {
					t.Fatal(err)
				}
				projected, err := source.Project(command, "exact-pending-obligation-owner")
				if err != nil {
					t.Fatal(err)
				}
				if projected.Publication.SourceRunID != snapshot.RunID || projected.Publication.SourceEventID != source.Publication.EventID ||
					projected.Publication.CommittedCause != source.Publication.CommittedCause ||
					!projected.Command.Due.Absolute.Equal(source.Command.Due.Absolute) {
					t.Fatal("pending barrier projection skipped the parent or replaced committed cause/due")
				}
				if event, found, err := runForkTransferredJoinEvent(snapshot, source); err != nil || found || event.ID() != "" {
					t.Fatalf("cause-only projection fabricated parent publication: found=%t error=%v", found, err)
				}
				if _, _, err := prepareRunForkBarrierTransfer(context.Background(), nil, snapshot, barrier, childRun, barrier.Registration.PlanRef, nil, barrier.UpdatedAt.Add(time.Hour)); err == nil {
					t.Fatal("pending evidence bypassed native committed-cause admission")
				}
			})
		}
	}
}

func TestPublishedBarrierTransferRefusesMissingOrContradictoryCut(t *testing.T) {
	for _, fault := range []string{"missing_event", "missing_delivery", "duplicate_event", "wrong_entity", "future_event", "settled_delivery", "dual_schedule", "missing_due"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, barrier := transferredBarrierHistoryFixture(t, false, executionmode.Live)
			switch fault {
			case "missing_event":
				snapshot.Events = nil
			case "missing_delivery":
				snapshot.Deliveries = nil
			case "duplicate_event":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "wrong_entity":
				barrier.Registration.EntityID = uuid.NewString()
			case "future_event":
				snapshot.Events[0].Revision = snapshot.Revision + 1
			case "settled_delivery":
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			case "dual_schedule":
				barrier.ScheduleActivationID = uuid.NewString()
			case "missing_due":
				barrier.TransferredOccurrence.DueAt = time.Time{}
			}
			if continuation, _, err := prepareRunForkBarrierTransfer(context.Background(), nil, snapshot, barrier, uuid.NewString(), barrier.Registration.PlanRef, nil, barrier.UpdatedAt.Add(time.Hour)); err == nil || continuation.Present() {
				t.Fatal("invalid cut authorized barrier continuation")
			}
		})
	}
}

func TestTransferredBarrierHistoryDistinguishesIntentPublicationAndSettlement(t *testing.T) {
	for _, name := range []string{"unpublished", "published", "delivered", "dead_lettered", "suppressed_unpublished", "missing_settled_event", "wrong_terminal_delivery", "duplicate_event", "wrong_entity", "wrong_instance", "dual_schedule"} {
		t.Run(name, func(t *testing.T) {
			snapshot, barrier := transferredBarrierHistoryFixture(t, false, executionmode.Live)
			timers := make(map[string]runForkRevisionTimer)
			valid := true
			switch name {
			case "unpublished":
				snapshot.Events, snapshot.Deliveries = nil, nil
			case "published":
			case "delivered":
				barrier.Status = fanoutbarrier.StatusFired
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			case "dead_lettered":
				barrier.Status = fanoutbarrier.StatusOutcomeDeadLettered
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDeadLetter
			case "suppressed_unpublished":
				barrier.Status = fanoutbarrier.StatusSuppressedRunTerminal
				snapshot.Events, snapshot.Deliveries = nil, nil
			case "missing_settled_event":
				barrier.Status = fanoutbarrier.StatusFired
				snapshot.Events = nil
				valid = false
			case "wrong_terminal_delivery":
				barrier.Status = fanoutbarrier.StatusFired
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDeadLetter
				valid = false
			case "duplicate_event":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
				valid = false
			case "wrong_entity":
				barrier.Registration.EntityID = uuid.NewString()
				valid = false
			case "wrong_instance":
				barrier.Registration.Route.InstancePath += ".other"
				valid = false
			case "dual_schedule":
				occurrence, err := genericschedule.NewTransferredFanOutBarrierOccurrence(barrier)
				if err != nil {
					t.Fatal(err)
				}
				scope, err := occurrence.Command.ScopeKey()
				if err != nil {
					t.Fatal(err)
				}
				var timer runForkRevisionTimer
				timer.ScheduleScope, timer.ScheduleKey = scope, occurrence.Command.ScheduleKey
				timers["other-id"] = timer
				valid = false
			}
			if err := requireRunForkTransferredBarrierHistory(snapshot, barrier, timers); (err == nil) != valid {
				t.Fatalf("intent/publication/settlement distinction: valid=%t err=%v", valid, err)
			}
		})
	}
}
