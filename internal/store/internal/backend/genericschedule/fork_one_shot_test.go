package genericschedule

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func forkOneShotFixture(t *testing.T, flow, delay, reply bool, status runtimegenericschedule.Status) ForkOneShotRequest {
	t.Helper()
	arm := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	sourceRun, childRun := uuid.NewString(), uuid.NewString()
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey: "agent-one-shot", RunID: sourceRun, EntityID: sourceRun,
		OwnerKind: runtimegenericschedule.OwnerAgent, OwnerID: "agent-a",
		AgentIdentity: agentidentitytest.RootDeclaredForRun(t, sourceRun, "agent-a", "test/agents.yaml"),
		EventType:     "platform.generic_schedule_proof", ExecutionMode: executionmode.Mock,
		Due: runtimegenericschedule.AbsoluteDue(arm.Add(time.Hour)), TaskID: "agent-one-shot",
	}
	var err error
	command.Payload, err = canonicaljson.FromGo(map[string]any{"message": "retained", "count": 3})
	if err != nil {
		t.Fatal(err)
	}
	command.RoutingSource, err = events.NewRootRoutingSource(command.EntityID)
	if flow {
		command.EntityID, command.FlowInstance = uuid.NewString(), "orders/order-1"
		command.AgentIdentity = agentidentitytest.DeclaredForRun(t, sourceRun, "agent-a", "test/agents.yaml", "orders", "order-1", command.FlowInstance)
		command.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
			FlowID: "orders", FlowInstance: command.FlowInstance, EntityID: command.EntityID,
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if delay {
		command.Due = runtimegenericschedule.DelayDue(time.Hour)
	}
	if reply {
		command.ReplyContext = uuid.NewString()
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	source := runtimegenericschedule.Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: arm,
		InitialDueAt: arm.Add(time.Hour), CurrentDueAt: arm.Add(time.Hour), Status: status,
	}
	if status == runtimegenericschedule.StatusCancelled {
		source.CancelCause, source.CancelledAt = "retained_cancel", arm.Add(time.Minute)
	}
	if status == runtimegenericschedule.StatusFailed {
		source.Failure, source.FailedAt = runtimegenericschedule.Failure{Code: "retained_failure", Message: "original failure evidence"}, arm.Add(time.Minute)
	}
	child := forkOneShotProjectChild(t, source.Command, childRun)
	r := ForkOneShotRequest{Source: source, Child: child, PointKind: forkpoint.Event, PointRevision: 7, PointEventID: uuid.NewString(), BornAt: arm.Add(2 * time.Hour)}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func forkOneShotProjectChild(t *testing.T, source runtimegenericschedule.AdmissionCommand, childRun string) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	child := source
	var err error
	child.AgentIdentity, err = agentidentity.New(childRun, source.AgentIdentity.Name, source.AgentIdentity.Route)
	if err != nil {
		t.Fatal(err)
	}
	child.RoutingSource, err = runfork.ProjectProducerOwnership(source.RunID, childRun, source.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	if source.EntityID == source.RunID {
		child.EntityID = childRun
	}
	child.RunID = childRun
	if source.ReplyContext != "" {
		child.ReplyContext = uuid.NewString()
	}
	return child
}

func TestForkOneShotRequestExpectedPreservesOriginalCoordinatesAndCommand(t *testing.T) {
	for _, flow := range []bool{false, true} {
		for _, delay := range []bool{false, true} {
			for _, reply := range []bool{false, true} {
				for _, status := range []runtimegenericschedule.Status{runtimegenericschedule.StatusActive, runtimegenericschedule.StatusCancelled, runtimegenericschedule.StatusFailed} {
					r := forkOneShotFixture(t, flow, delay, reply, status)
					before := r.Source.Canonical()
					child, err := r.Expected(uuid.NewString())
					if err != nil {
						t.Fatal(err)
					}
					if !child.InitialDueAt.Equal(r.Source.InitialDueAt) || !child.CurrentDueAt.Equal(r.Source.CurrentDueAt) ||
						!child.AdmittedAt.Equal(r.BornAt) || !child.ForkOneShotOrigin.DueBasisAt.Equal(r.Source.AdmittedAt) ||
						child.Command.Due != r.Source.Command.Due || !child.Command.Payload.Equal(r.Source.Command.Payload) ||
						child.Command.ExecutionMode != r.Source.Command.ExecutionMode || child.Status != status ||
						child.CurrentEventID != "" || !reflect.DeepEqual(before, r.Source.Canonical()) {
						t.Fatal("child restarted the delay, rewrote author intent, rearmed history, or changed source")
					}
					if status == runtimegenericschedule.StatusCancelled && (child.CancelCause != r.Source.CancelCause || !child.CancelledAt.Equal(r.BornAt)) ||
						status == runtimegenericschedule.StatusFailed && (child.Failure != r.Source.Failure || !child.FailedAt.Equal(r.BornAt)) {
						t.Fatal("child lost retained terminal evidence or backdated execution history")
					}
					hash, _ := r.Child.ImmutableHash()
					scope, _ := r.Child.ScopeKey()
					if replay, err := exactReplay(scope, r.Child.ScheduleKey, hash, child); err != nil || replay.Outcome != runtimegenericschedule.AdmissionExactReplay {
						t.Fatalf("ordinary command retry lost relative intent: %v", err)
					}
				}
			}
		}
	}
}

func TestForkOneShotRequestRefusesUnsupportedOrChangedEvidence(t *testing.T) {
	for _, flow := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			edit func(*ForkOneShotRequest)
		}{
			{"source_hash", func(r *ForkOneShotRequest) { r.Source.ImmutableHash = "bad" }},
			{"source_due", func(r *ForkOneShotRequest) { r.Source.CurrentDueAt = r.Source.CurrentDueAt.Add(time.Second) }},
			{"source_initial", func(r *ForkOneShotRequest) { r.Source.InitialDueAt = r.Source.InitialDueAt.Add(time.Second) }},
			{"source_clock", func(r *ForkOneShotRequest) { r.Source.ClockSuspension = &runtimegenericschedule.ClockSuspension{} }},
			{"source_prepared", func(r *ForkOneShotRequest) {
				r.Source.CurrentEventID, r.Source.CurrentEventAdmittedAt = runtimegenericschedule.OccurrenceEventID(r.Source.ID, r.Source.CurrentDueAt), r.Source.CurrentDueAt
			}},
			{"source_fired", func(r *ForkOneShotRequest) {
				r.Source.Status, r.Source.FiredAt, r.Source.AcceptedAt = runtimegenericschedule.StatusFired, r.Source.CurrentDueAt, r.Source.CurrentDueAt
			}},
			{"source_parked", func(r *ForkOneShotRequest) { r.Source.Status = runtimegenericschedule.StatusParked }},
			{"source_recurring", func(r *ForkOneShotRequest) {
				r.Source.Command.ReplyContext, r.Child.ReplyContext = "", ""
				r.Source.Command.Due, r.Child.Due = runtimegenericschedule.EveryDue(time.Hour), runtimegenericschedule.EveryDue(time.Hour)
				r.Source.ImmutableHash, _ = r.Source.Command.ImmutableHash()
			}},
			{"source_system", func(r *ForkOneShotRequest) {
				r.Source.Command.OwnerKind, r.Child.OwnerKind = runtimegenericschedule.OwnerSystem, runtimegenericschedule.OwnerSystem
				r.Source.Command.AgentIdentity, r.Child.AgentIdentity = agentidentity.Identity{}, agentidentity.Identity{}
				r.Source.ImmutableHash, _ = r.Source.Command.ImmutableHash()
			}},
			{"source_terminal_before_birth", func(r *ForkOneShotRequest) {
				r.Source.Status, r.Source.CancelCause, r.Source.CancelledAt = runtimegenericschedule.StatusCancelled, "cancel", r.BornAt.Add(time.Second)
			}},
			{"source_terminal_before_admission", func(r *ForkOneShotRequest) {
				r.Source.Status, r.Source.Failure, r.Source.FailedAt = runtimegenericschedule.StatusFailed, runtimegenericschedule.Failure{Code: "failed"}, r.Source.AdmittedAt.Add(-time.Second)
			}},
			{"child_run", func(r *ForkOneShotRequest) { r.Child.RunID = r.Source.Command.RunID }},
			{"child_absolute_conversion", func(r *ForkOneShotRequest) { r.Child.Due = runtimegenericschedule.AbsoluteDue(r.Source.InitialDueAt) }},
			{"child_delay", func(r *ForkOneShotRequest) { r.Child.Due = runtimegenericschedule.DelayDue(2 * time.Hour) }},
			{"child_mode", func(r *ForkOneShotRequest) { r.Child.ExecutionMode = executionmode.Live }},
			{"child_payload", func(r *ForkOneShotRequest) {
				r.Child.Payload, _ = canonicaljson.FromGo(map[string]any{"changed": true})
			}},
			{"child_key", func(r *ForkOneShotRequest) { r.Child.ScheduleKey += "-changed" }},
			{"child_task", func(r *ForkOneShotRequest) { r.Child.TaskID += "-changed" }},
			{"child_event", func(r *ForkOneShotRequest) { r.Child.EventType = "platform.other" }},
			{"child_reply_dropped", func(r *ForkOneShotRequest) { r.Child.ReplyContext = "" }},
			{"child_source_reply", func(r *ForkOneShotRequest) { r.Child.ReplyContext = r.Source.Command.ReplyContext }},
			{"child_actor_name", func(r *ForkOneShotRequest) { r.Child.AgentIdentity.Name.Owner = "other.yaml" }},
			{"child_entity_and_route", func(r *ForkOneShotRequest) {
				r.Child.EntityID = uuid.NewString()
				if flow {
					r.Child.RoutingSource, _ = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: r.Child.FlowInstance, EntityID: r.Child.EntityID})
				} else {
					r.Child.RoutingSource, _ = events.NewRootRoutingSource(r.Child.EntityID)
				}
			}},
			{"point_revision", func(r *ForkOneShotRequest) { r.PointRevision = 0 }},
			{"point_event", func(r *ForkOneShotRequest) { r.PointEventID = "" }},
			{"birth", func(r *ForkOneShotRequest) { r.BornAt = r.Source.AdmittedAt.Add(-time.Second) }},
			{"noncanonical_birth", func(r *ForkOneShotRequest) { r.BornAt = r.BornAt.Add(time.Nanosecond) }},
		} {
			t.Run(fmt.Sprintf("flow=%t/%s", flow, tc.name), func(t *testing.T) {
				r := forkOneShotFixture(t, flow, true, true, runtimegenericschedule.StatusActive)
				tc.edit(&r)
				if err := r.Validate(); err == nil {
					t.Fatal("changed request accepted")
				}
				if child, err := r.Expected(uuid.NewString()); err == nil || !reflect.DeepEqual(child, runtimegenericschedule.Activation{}) {
					t.Fatalf("refused request produced child: %+v err=%v", child, err)
				}
			})
		}
	}
	join := forkJoinRequestFixture(t, ".", true, false, false)
	r := ForkOneShotRequest{Source: join.Source, Child: join.Child, PointKind: join.PointKind, PointRevision: join.PointRevision, PointEventID: join.PointEventID, BornAt: join.BornAt}
	if r.Validate() == nil {
		t.Fatal("one-shot leaf weakened arrival admission")
	}
	valid := forkOneShotFixture(t, false, true, false, runtimegenericschedule.StatusActive)
	for _, id := range []string{"", "bad-id", uuid.Nil.String(), "A0000000-0000-4000-8000-000000000001"} {
		if _, err := valid.Expected(id); err == nil {
			t.Fatal("invalid child activation identity accepted", id)
		}
	}
}

func TestForkOneShotRequestRecursiveForkRetainsOriginalBasis(t *testing.T) {
	for _, delay := range []bool{false, true} {
		for _, status := range []runtimegenericschedule.Status{runtimegenericschedule.StatusActive, runtimegenericschedule.StatusCancelled, runtimegenericschedule.StatusFailed} {
			r := forkOneShotFixture(t, false, delay, true, status)
			basis, due := r.Source.AdmittedAt, r.Source.InitialDueAt
			for depth := 0; depth < 3; depth++ {
				before := r.Source.Canonical()
				child, err := r.Expected(uuid.NewString())
				if err != nil || child.ForkOneShotOrigin.SourceActivationID != r.Source.ID || child.ForkOneShotOrigin.SourceRunID != r.Source.Command.RunID ||
					!child.ForkOneShotOrigin.DueBasisAt.Equal(basis) || !child.InitialDueAt.Equal(due) || !child.CurrentDueAt.Equal(due) || !reflect.DeepEqual(before, r.Source.Canonical()) {
					t.Fatalf("recursive fork lost original basis or immediate lineage: child=%+v err=%v", child, err)
				}
				r = ForkOneShotRequest{Source: child, Child: forkOneShotProjectChild(t, child.Command, uuid.NewString()),
					PointKind: forkpoint.DeploymentRevision, PointRevision: int64(8 + depth), BornAt: child.AdmittedAt.Add(time.Hour)}
			}
		}
	}
}

func forkOneShotSnapshot(t *testing.T, a runtimegenericschedule.Activation) runforkrevision.TimerSnapshot {
	t.Helper()
	payload, err := canonicaljson.Encode(a.Command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(a.Command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := a.Command.ScopeKey()
	identity, _ := a.Command.AgentIdentity.StorageFields()
	_, duration, cron := encodeDueBasis(a.Command.Due)
	s := runforkrevision.TimerSnapshot{
		TimerID: a.ID, TimerName: a.Command.ScheduleKey, ScheduleKey: a.Command.ScheduleKey, ScheduleScope: scope,
		ImmutableHash: a.ImmutableHash, RunID: a.Command.RunID, EntityID: a.Command.EntityID, FlowInstance: a.Command.FlowInstance,
		FireEvent: a.Command.EventType, FirePayload: payload, RoutingSource: routing, ExecutionMode: string(a.Command.ExecutionMode),
		FireAt: a.CurrentDueAt, InitialFireAt: &a.InitialDueAt, Recurring: a.Command.Due.Recurring(), OwnerKind: string(a.Command.OwnerKind), OwnerAgent: a.Command.OwnerID,
		AgentNameOwner: identity.NameOwner, AgentNameSource: identity.NameSource, AgentRoutePresence: identity.RoutePresence,
		AgentFlowScopeKey: identity.FlowScopeKey, AgentFlowInstanceID: identity.FlowInstanceID, ReplyContextID: a.Command.ReplyContext, TaskID: a.Command.TaskID,
		DueBasisKind: string(a.Command.Due.Kind), DueBasisDuration: duration, DueBasisCron: cron,
		TaskType: genericTaskType(a.Command), Status: string(a.Status), CreatedAt: a.AdmittedAt,
		CancelCause: a.CancelCause, FailureCode: a.Failure.Code, FailureMessage: a.Failure.Message,
	}
	if a.Command.Due.Kind == runtimegenericschedule.DueAbsolute {
		s.DueBasisAbsolute = &a.Command.Due.Absolute
	}
	if !a.CancelledAt.IsZero() {
		s.CancelledAt = &a.CancelledAt
	}
	if !a.FailedAt.IsZero() {
		s.FailedAt = &a.FailedAt
	}
	if o := a.ForkOneShotOrigin; o != nil {
		s.SourceTimerID, s.ForkedFromRunID, s.ForkedFromPointKind, s.ForkedFromPointRevision = o.SourceActivationID, o.SourceRunID, o.PointKind, o.PointRevision
		s.ForkedFromEventID, s.SourceArmedAt, s.ReconstructionOwner = o.PointEventID, &o.DueBasisAt, o.Owner
	}
	return s
}

func TestForkOneShotDecodeSnapshotUsesCanonicalNativeProjection(t *testing.T) {
	for _, flow := range []bool{false, true} {
		for _, delay := range []bool{false, true} {
			for _, status := range []runtimegenericschedule.Status{runtimegenericschedule.StatusActive, runtimegenericschedule.StatusCancelled, runtimegenericschedule.StatusFailed} {
				r := forkOneShotFixture(t, flow, delay, true, status)
				child, err := r.Expected(uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range []runtimegenericschedule.Activation{r.Source, child} {
					s := forkOneShotSnapshot(t, row)
					for _, null := range []json.RawMessage{nil, json.RawMessage("null")} {
						s.ClockSuspension = null
						actual, err := DecodeSnapshot(s)
						if err != nil || !reflect.DeepEqual(actual.Canonical(), row.Canonical()) {
							t.Fatalf("snapshot changed native command, due or history: actual=%+v err=%v", actual, err)
						}
					}
				}
			}
		}
	}
	r := forkOneShotFixture(t, false, true, true, runtimegenericschedule.StatusActive)
	child, _ := r.Expected(uuid.NewString())
	for _, tc := range []struct {
		name string
		edit func(*runforkrevision.TimerSnapshot)
	}{
		{"hash", func(s *runforkrevision.TimerSnapshot) { s.ImmutableHash = "bad" }},
		{"scope", func(s *runforkrevision.TimerSnapshot) { s.ScheduleScope = "other" }},
		{"name", func(s *runforkrevision.TimerSnapshot) { s.TimerName = "other" }},
		{"key", func(s *runforkrevision.TimerSnapshot) { s.ScheduleKey = "other" }},
		{"task_family", func(s *runforkrevision.TimerSnapshot) { s.TaskType = "workflow_timer" }},
		{"global_family", func(s *runforkrevision.TimerSnapshot) { s.TaskType = "global_recurring" }},
		{"recurrence", func(s *runforkrevision.TimerSnapshot) { s.Recurring = true }},
		{"recurrence_interval", func(s *runforkrevision.TimerSnapshot) { s.RecurrenceInterval = "1h" }},
		{"actor_name", func(s *runforkrevision.TimerSnapshot) { s.AgentNameOwner = "different" }},
		{"noncanonical_actor", func(s *runforkrevision.TimerSnapshot) { s.AgentNameOwner += " " }},
		{"noncanonical_event", func(s *runforkrevision.TimerSnapshot) { s.FireEvent += " " }},
		{"initial_due", func(s *runforkrevision.TimerSnapshot) { s.InitialFireAt = nil }},
		{"current_due", func(s *runforkrevision.TimerSnapshot) { s.FireAt = s.FireAt.Add(time.Second) }},
		{"basis", func(s *runforkrevision.TimerSnapshot) { s.SourceArmedAt = &s.CreatedAt }},
		{"unused_due_absolute", func(s *runforkrevision.TimerSnapshot) { s.DueBasisAbsolute = &s.FireAt }},
		{"unused_due_cron", func(s *runforkrevision.TimerSnapshot) { s.DueBasisCron = "* * * * *" }},
		{"partial_lineage", func(s *runforkrevision.TimerSnapshot) { s.SourceTimerID = "" }},
		{"join_owner", func(s *runforkrevision.TimerSnapshot) {
			s.ReconstructionOwner = runtimegenericschedule.ForkJoinReconstructionOwner
		}},
		{"foreign_owner", func(s *runforkrevision.TimerSnapshot) { s.ReconstructionOwner = "other" }},
		{"noncanonical_time", func(s *runforkrevision.TimerSnapshot) { s.CreatedAt = s.CreatedAt.Add(time.Nanosecond) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := forkOneShotSnapshot(t, child)
			tc.edit(&s)
			if got, err := DecodeSnapshot(s); err == nil || !reflect.DeepEqual(got, runtimegenericschedule.Activation{}) {
				t.Fatalf("corrupt snapshot admitted: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestForkOneShotNativeInsertIncludesCompleteValidatedRestoration(t *testing.T) {
	for _, d := range []dialect{sqliteDialect, postgresDialect} {
		for _, status := range []runtimegenericschedule.Status{runtimegenericschedule.StatusActive, runtimegenericschedule.StatusCancelled, runtimegenericschedule.StatusFailed} {
			t.Run(fmt.Sprintf("dialect=%d/%s", d, status), func(t *testing.T) {
				r := forkOneShotFixture(t, true, true, true, status)
				a, _ := r.Expected(uuid.NewString())
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				scope, _ := a.Command.ScopeKey()
				payload, _ := canonicaljson.Encode(a.Command.Payload)
				routing, _ := json.Marshal(a.Command.RoutingSource)
				i, _ := a.Command.AgentIdentity.StorageFields()
				o := a.ForkOneShotOrigin
				args := []driver.Value{
					a.ID, a.Command.ScheduleKey, scope, a.Command.ScheduleKey, a.ImmutableHash, a.Command.RunID, a.Command.EntityID, a.Command.FlowInstance,
					a.Command.EventType, string(payload), string(routing), string(a.Command.ExecutionMode), a.CurrentDueAt, a.InitialDueAt, false,
					a.Command.OwnerID, string(a.Command.OwnerKind), i.NameOwner, i.NameSource, i.RoutePresence, i.FlowScopeKey, i.FlowInstanceID,
					a.Command.ReplyContext, a.Command.TaskID, string(a.Command.Due.Kind), nil, a.Command.Due.Delay.String(), "", "timer",
					string(a.Status), a.AdmittedAt, a.CancelCause, optionalTime(a.CancelledAt), a.Failure.Code, a.Failure.Message, optionalTime(a.FailedAt),
					o.SourceActivationID, o.SourceRunID, string(o.PointKind), o.PointRevision, o.PointEventID, o.DueBasisAt, o.Owner,
				}
				if d == postgresDialect {
					mock.ExpectQuery("INSERT INTO timers .*source_armed_at, reconstruction_owner.*ON CONFLICT").WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"run_id"}).AddRow(a.Command.RunID))
				} else {
					mock.ExpectExec("INSERT INTO timers .*source_armed_at, reconstruction_owner.*ON CONFLICT").WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectRollback()
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				runID, inserted, err := insertActivationTx(context.Background(), tx, d, scope, a)
				if err != nil || !inserted || runID != a.Command.RunID {
					t.Fatalf("complete native insert: inserted=%t run=%q err=%v", inserted, runID, err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestForkOneShotNativeRestoreRequiresExistingFrame(t *testing.T) {
	r := forkOneShotFixture(t, false, true, false, runtimegenericschedule.StatusActive)
	if _, err := RestoreForkOneShotTx(context.Background(), nil, false, r); err == nil {
		t.Fatal("one-shot restoration minted a mutation frame")
	}
	child, _ := r.Expected(uuid.NewString())
	conflict := r.Child
	conflict.Due = runtimegenericschedule.AbsoluteDue(r.Source.InitialDueAt)
	hash, _ := conflict.ImmutableHash()
	scope, _ := conflict.ScopeKey()
	if _, err := exactReplay(scope, conflict.ScheduleKey, hash, child); !runtimegenericschedule.IsConflict(err) {
		t.Fatal("relative-to-absolute command conversion evaded immutable conflict", err)
	}
}
