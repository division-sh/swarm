package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func forkOneShotPlanFixture(t *testing.T, absolute bool) runfork.RunForkPlan {
	t.Helper()
	runID, admitted := uuid.NewString(), time.Unix(1700002400, 0).UTC()
	name, err := agentidentity.DeclaredName("worker", "root")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Decode([]byte(`{"business":"unchanged"}`))
	if err != nil {
		t.Fatal(err)
	}
	due := genericschedule.DelayDue(time.Minute)
	if absolute {
		due = genericschedule.AbsoluteDue(admitted.Add(time.Minute))
	}
	command := genericschedule.AdmissionCommand{ScheduleKey: "own-return", RunID: runID, EntityID: runID,
		OwnerKind: genericschedule.OwnerAgent, OwnerID: identity.AgentID(), AgentIdentity: identity,
		EventType: "work.ready", Payload: payload, RoutingSource: eventtest.RootRoutingSource(runID),
		ExecutionMode: executionmode.Mock, Due: due}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
		AdmittedAt: admitted, InitialDueAt: admitted.Add(time.Minute), CurrentDueAt: admitted.Add(time.Minute), Status: genericschedule.StatusActive}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return runfork.RunForkPlan{SourceRunID: runID, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 2, EventID: uuid.NewString()},
		OneShotSchedules: []genericschedule.Activation{activation}}
}

func TestForkOneShotProjectionPreservesCommandAndOriginalDueBasis(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		plan := forkOneShotPlanFixture(t, absolute)
		source := plan.OneShotSchedules[0]
		before, _ := source.EvidenceDigest()
		child, born := uuid.NewString(), source.AdmittedAt.Add(time.Hour)
		requests, err := prepareRunForkOneShotRequests(plan, child, born)
		if err != nil || len(requests) != 1 {
			t.Fatalf("one-shot projection: requests=%+v error=%v", requests, err)
		}
		row, err := requests[0].Expected(uuid.NewString())
		if err != nil || row.Command.RunID != child || row.Command.EntityID != child || row.Command.AgentIdentity.RunID != child ||
			row.Command.ReplyContext != "" || row.Command.Due != source.Command.Due || !row.Command.Payload.Equal(source.Command.Payload) ||
			!row.AdmittedAt.Equal(born) || !row.InitialDueAt.Equal(source.InitialDueAt) || row.ForkOneShotOrigin == nil ||
			!row.ForkOneShotOrigin.DueBasisAt.Equal(source.AdmittedAt) {
			t.Fatalf("one-shot projection changed due/business data or minted reply authority: %+v error=%v", row, err)
		}
		plan.SourceRunID, plan.OneShotSchedules = child, []genericschedule.Activation{row}
		grand, err := prepareRunForkOneShotRequests(plan, uuid.NewString(), born.Add(time.Hour))
		if err != nil || len(grand) != 1 {
			t.Fatalf("one-shot refork: %+v error=%v", grand, err)
		}
		grandRow, err := grand[0].Expected(uuid.NewString())
		if err != nil || grandRow.ForkOneShotOrigin.SourceRunID != child || grandRow.ForkOneShotOrigin.SourceActivationID != row.ID ||
			!grandRow.ForkOneShotOrigin.DueBasisAt.Equal(source.AdmittedAt) || !grandRow.InitialDueAt.Equal(source.InitialDueAt) {
			t.Fatalf("one-shot refork restarted delay or skipped its immediate source: %+v error=%v", grandRow, err)
		}
		after, _ := source.EvidenceDigest()
		if before != after {
			t.Fatal("one-shot projection changed captured source")
		}
	}
}

func TestForkOneShotProjectionRequiresCapturedSourceAndCompleteInventory(t *testing.T) {
	for _, name := range []string{"duplicate", "foreign_run", "same_child", "missing_reply", "recurring", "fired"} {
		t.Run(name, func(t *testing.T) {
			plan := forkOneShotPlanFixture(t, false)
			child := uuid.NewString()
			switch name {
			case "duplicate":
				plan.OneShotSchedules = append(plan.OneShotSchedules, plan.OneShotSchedules[0])
			case "foreign_run":
				plan.SourceRunID = uuid.NewString()
			case "same_child":
				child = plan.SourceRunID
			case "missing_reply":
				plan.OneShotSchedules[0].Command.ReplyContext = uuid.NewString()
			case "recurring":
				plan.OneShotSchedules[0].Command.Due = genericschedule.EveryDue(time.Minute)
			case "fired":
				plan.OneShotSchedules[0].Status = genericschedule.StatusFired
			}
			if name == "missing_reply" || name == "recurring" {
				plan.OneShotSchedules[0].ImmutableHash, _ = plan.OneShotSchedules[0].Command.ImmutableHash()
			}
			before := append([]genericschedule.Activation(nil), plan.OneShotSchedules...)
			if requests, err := prepareRunForkOneShotRequests(plan, child, plan.OneShotSchedules[0].AdmittedAt.Add(time.Hour)); err == nil || requests != nil {
				t.Fatalf("%s admitted contradictory restoration: %+v error=%v", name, requests, err)
			}
			if !reflect.DeepEqual(before, plan.OneShotSchedules) {
				t.Fatal("refusal changed source")
			}
		})
	}
	plan := forkOneShotPlanFixture(t, false)
	inventory, err := runForkPlanTimerRecordInventory(plan)
	if err != nil || len(inventory.OneShotScheduleIDs) != 1 {
		t.Fatalf("complete one-shot inventory: %+v error=%v", inventory, err)
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	original, err := inventory.pendingCertificate()
	if err != nil {
		t.Fatal(err)
	}
	plan.OneShotSchedules[0].Command.Payload, _ = canonicaljson.Decode([]byte(`{"business":"different"}`))
	plan.OneShotSchedules[0].ImmutableHash, _ = plan.OneShotSchedules[0].Command.ImmutableHash()
	changed, err := runForkPlanTimerRecordInventory(plan)
	if err != nil {
		t.Fatal(err)
	}
	changed.Complete, changed.Point = true, plan.ForkPoint
	digest, err := changed.pendingCertificate()
	if err != nil || digest == original {
		t.Fatalf("one-shot inventory ignored changed business evidence: error=%v", err)
	}
}
