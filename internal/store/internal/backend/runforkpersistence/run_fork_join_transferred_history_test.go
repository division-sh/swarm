package runforkpersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func transferredJoinHistoryFixture(t *testing.T) (*runForkRevisionSnapshot, joinruntime.Activation, genericschedule.TransferredJoinOccurrence) {
	t.Helper()
	snapshot, plan, source, original, childRun := publishedArrivalTransferFixture(t, true, true, deliverylifecycle.StatusPending)
	projected, err := prepareRunForkArrivalJoinSchedules(plan, childRun)
	if err != nil {
		t.Fatalf("source projection: %v", err)
	}
	var command genericschedule.AdmissionCommand
	for _, row := range projected {
		if row.source.ID == source.ID {
			command = row.command
		}
	}
	continuation, err := genericschedule.ProjectPublishedJoinContinuation(source, original, command)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := continuation.RetainedPublication(runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		t.Fatal(err)
	}
	buckets, err := joinruntime.PersistedBuckets(plan.Entities[0].Accumulator)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(buckets)
	if err != nil || len(joins) != 1 {
		t.Fatalf("source join: %v", err)
	}
	_, ref, valid := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !valid {
		t.Fatal("projected handle is invalid")
	}
	join, err := joins[0].WithForkReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	join.TransferredPublication = &publication
	transferred, err := genericschedule.NewTransferredJoinOccurrence(join, publication)
	if err != nil {
		t.Fatal(err)
	}
	event, err := continuation.Event(publication.EventID, publication.AuthorityStamp)
	if err != nil {
		t.Fatal(err)
	}
	admission, _ := original.PayloadAdmission()
	bound, err := events.NewPayloadAdmission(event.Payload(), admission.Binding())
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
	child := &runForkRevisionSnapshot{RunID: childRun, Revision: snapshot.Revision,
		Events: []runForkRevisionEvent{genericPublicationRevisionEvent(record, event)}}
	route := snapshot.Deliveries[0].Snapshot.Route
	route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: ref.StageEntry().InstancePath, EntityID: ref.StageEntry().EntityID})
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
	child.Deliveries = []runForkRevisionDelivery{delivery}
	return child, join, transferred
}

func TestTransferredJoinHistoryDistinguishesPublicationFromArm(t *testing.T) {
	for _, fault := range []string{"published", "prepublication", "settled", "both_variants", "missing_delivery", "duplicate_delivery", "wrong_target", "foreign_event", "future_event", "wrong_source", "duplicate_event", "missing_settled_event", "unowned_inventory"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, join, source := transferredJoinHistoryFixture(t)
			timers := map[runForkJoinScheduleKey]runForkRevisionTimer{}
			switch fault {
			case "prepublication":
				snapshot.Events, snapshot.Deliveries = nil, nil
			case "settled":
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			case "both_variants":
				scope, _ := source.Command.ScopeKey()
				timers[runForkJoinScheduleKey{scope, source.Command.ScheduleKey}] = runForkRevisionTimer{}
			case "missing_delivery":
				snapshot.Deliveries = nil
			case "duplicate_delivery":
				snapshot.Deliveries = append(snapshot.Deliveries, snapshot.Deliveries[0])
			case "wrong_target":
				snapshot.Deliveries[0].Snapshot.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: uuid.NewString(), EntityID: uuid.NewString()})
			case "foreign_event":
				snapshot.Events[0].RunID = uuid.NewString()
			case "future_event":
				snapshot.Events[0].Revision = snapshot.Revision + 1
			case "wrong_source":
				snapshot.Events[0].SelectedForkSourceRunID = uuid.NewString()
			case "duplicate_event":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "missing_settled_event":
				snapshot.Events = nil
				join.OutcomeFired = true
			case "unowned_inventory":
				snapshot.RunID = uuid.NewString()
			}
			err := requireRunForkTransferredJoinHistory(snapshot, join, source, timers)
			positive := fault == "published" || fault == "prepublication" || fault == "settled"
			if positive && err != nil || !positive && err == nil {
				t.Fatalf("historical transferred obligation: %v", err)
			}
		})
	}
}

func TestTransferredJoinInventoryBindsCompleteEvidence(t *testing.T) {
	snapshot, _, source := transferredJoinHistoryFixture(t)
	inventory, err := runForkTimerRecordInventory(snapshot.RunID, nil, nil, source)
	if err != nil || len(inventory.ArrivalScheduleIDs) != 0 || len(inventory.TransferredPublicationIDs) != 1 {
		t.Fatalf("non-executable inventory: %+v err=%v", inventory, err)
	}
	inventory.Complete, inventory.Point = true, runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: snapshot.Revision}
	if _, err := inventory.pendingCertificate(); err != nil {
		t.Fatal(err)
	}
	if _, err := runForkTimerRecordInventory(snapshot.RunID, nil, nil, source, source); err == nil {
		t.Fatal("duplicate transferred obligation admitted")
	}
	if _, err := runForkTimerRecordInventory(uuid.NewString(), nil, nil, source); err == nil {
		t.Fatal("foreign transferred obligation admitted")
	}
	if err := requireRunForkTransferredJoinInventory(snapshot.RunID, []genericschedule.TransferredJoinOccurrence{source}, nil); err == nil {
		t.Fatal("missing native transfer evidence discharged history")
	}
	if err := requireRunForkTransferredJoinInventory(snapshot.RunID, nil, []genericschedule.TransferredJoinOccurrence{source}); err == nil {
		t.Fatal("unexpected native transfer discharged an empty expected inventory")
	}
	if err := requireRunForkTransferredJoinInventory(snapshot.RunID, []genericschedule.TransferredJoinOccurrence{source}, []genericschedule.TransferredJoinOccurrence{source, source}); err == nil {
		t.Fatal("extra native transfer evidence discharged history")
	}
	changed := source
	changed.Publication.AuthorityStamp = "different-stamp"
	if err := requireRunForkTransferredJoinInventory(snapshot.RunID, []genericschedule.TransferredJoinOccurrence{source}, []genericschedule.TransferredJoinOccurrence{changed}); err == nil {
		t.Fatal("changed native transfer evidence discharged history")
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: inventory.Point, TransferredJoins: []genericschedule.TransferredJoinOccurrence{source}}.WithHistoricalEvents(snapshot.Revision, nil)
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: inventory})
	if materializable, err := runForkTimerHistoryMaterializable(plan); err != nil || materializable {
		t.Fatalf("unpublished transfer discharged its capability blocker: allowed=%v err=%v", materializable, err)
	}
	fingerprint := func(plan runfork.RunForkPlan) string {
		t.Helper()
		value, err := runfork.SelectedPreparationPlanFingerprint(plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "declaration")
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	want := fingerprint(plan)
	plan.TransferredJoins = []genericschedule.TransferredJoinOccurrence{changed}
	if fingerprint(plan) == want {
		t.Fatal("preparation omitted transferred lineage evidence")
	}
}

func TestTransferredJoinEmptyInventoryReadsCanonicalOwner(t *testing.T) {
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		rows, err := readRunForkTransferredJoinInventory(ctx, attempt, runfork.RunForkPlan{}, workflowTimerProjectionChildRun)
		if err != nil || len(rows) != 0 {
			t.Fatalf("empty require-only native inventory: rows=%d err=%v", len(rows), err)
		}
	}, 1)
}
