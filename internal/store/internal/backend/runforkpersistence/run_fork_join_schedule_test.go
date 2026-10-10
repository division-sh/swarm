package runforkpersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestFixedRevisionArrivalJoinScheduleRetainsExactEvidence(t *testing.T) {
	for _, root := range []bool{true, false} {
		for _, completion := range []bool{false, true} {
			name := map[bool]string{true: "root", false: "flow"}[root] + map[bool]string{true: "/completion", false: "/deadline"}[completion]
			t.Run(name, func(t *testing.T) {
				snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, root, completion)
				before, _ := json.Marshal(struct {
					Snapshot *runForkRevisionSnapshot
					Entities []runfork.RunForkEntityState
				}{snapshot, entities})
				actual, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
				if err != nil || len(actual) != len(snapshot.Timers) {
					t.Fatalf("arrival relation: schedules=%v err=%v", actual, err)
				}
				for _, schedule := range actual {
					if schedule.Command.RunID != snapshot.RunID || schedule.InitialDueAt.IsZero() {
						t.Fatal("lost source or due")
					}
				}
				facts := loadRunForkSourceFactsFromRevision(snapshot, entities)
				inventory, err := loadRunForkTimerHistoryInventory(snapshot, facts, nil)
				if err != nil || len(inventory.UnresolvedTimerIDs) != len(actual) {
					t.Fatalf("relation alone discharged native execution blocker: inventory=%+v err=%v", inventory, err)
				}
				after, _ := json.Marshal(struct {
					Snapshot *runForkRevisionSnapshot
					Entities []runfork.RunForkEntityState
				}{snapshot, entities})
				if string(before) != string(after) {
					t.Fatal("source evidence changed during read-only admission")
				}
			})
		}
	}
}

func TestFixedRevisionArrivalJoinScheduleRejectsSubstitution(t *testing.T) {
	for _, name := range []string{"missing", "duplicate_key", "foreign_run", "metadata", "owner", "wrong_flow", "different_due", "different_task", "malformed_payload", "missing_original_timeout"} {
		t.Run(name, func(t *testing.T) {
			snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, false, name == "missing_original_timeout")
			switch name {
			case "missing":
				snapshot.Timers = nil
			case "duplicate_key":
				snapshot.Timers = append(snapshot.Timers, snapshot.Timers[0])
			case "foreign_run":
				snapshot.Timers[0].RunID = uuid.NewString()
			case "metadata":
				entities[0].MaterializationMetadata = nil
			case "owner":
				entities[0].EntityID = uuid.NewString()
			case "wrong_flow":
				entities[0].MaterializationMetadata.FlowTemplate = "foreign"
			case "different_due":
				command, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Live)
				if err != nil {
					t.Fatal(err)
				}
				command.Due = genericschedule.AbsoluteDue(command.Due.Absolute.Add(time.Second))
				snapshot.Timers[0] = arrivalJoinRevisionTimer(t, command, join.ArmedAt)
				if _, err := projectRunForkGenericActivation(snapshot.Timers[0]); err != nil {
					t.Fatal(err)
				}
			case "different_task":
				snapshot.Timers[0].TaskID = "different"
			case "malformed_payload":
				snapshot.Timers[0].FirePayload = json.RawMessage(`{}`)
			case "missing_original_timeout":
				snapshot.Timers = snapshot.Timers[1:]
			}
			if _, err := loadRunForkArrivalJoinSchedules(snapshot, entities); err == nil {
				t.Fatal("contradictory arrival schedule admitted")
			}
		})
	}
}

func TestSelectedPreparationFingerprintBindsArrivalSchedules(t *testing.T) {
	snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, true, true)
	schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
	if err != nil {
		t.Fatal(err)
	}
	eventID := join.JoinRef().StageEntry().EventID
	plan := (runfork.RunForkPlan{
		SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: eventID, Revision: 1},
		JoinSchedules: schedules,
	}).WithHistoricalEvents(1, []string{eventID})
	fingerprint := func(p runfork.RunForkPlan) string {
		t.Helper()
		hash, err := runfork.SelectedPreparationPlanFingerprint(p, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "declaration-revision")
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	want := fingerprint(plan)
	missing := plan
	missing.JoinSchedules = nil
	if fingerprint(missing) == want {
		t.Fatal("preparation omitted retained arrival evidence")
	}
	changed := plan
	changed.JoinSchedules = append([]genericschedule.Activation(nil), plan.JoinSchedules...)
	changed.JoinSchedules[0].Command.ExecutionMode = executionmode.Mock
	changed.JoinSchedules[0].ImmutableHash, err = changed.JoinSchedules[0].Command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint(changed) == want {
		t.Fatal("preparation failed to bind an exact schedule mode")
	}
}

func TestFixedRevisionSchedulesKeepScopedKeys(t *testing.T) {
	for _, withJoin := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated_only", true: "alongside_join"}[withJoin], func(t *testing.T) {
			snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, true, false)
			command, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			command.ScheduleKey, command.TaskID = "poll", ""
			command.EventType = "poll.tick"
			command.Payload, err = canonicaljson.FromGo(map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			first := arrivalJoinRevisionTimer(t, command, join.ArmedAt)
			command.OwnerID = "another-owner"
			second := arrivalJoinRevisionTimer(t, command, join.ArmedAt)
			if first.ScheduleScope == second.ScheduleScope {
				t.Fatal("fixture does not separate owners")
			}
			for _, row := range []runForkRevisionTimer{first, second} {
				if _, err := projectRunForkGenericActivation(row); err != nil {
					t.Fatal(err)
				}
			}
			want := len(snapshot.Timers)
			if !withJoin {
				entities, snapshot.Timers, want = nil, nil, 0
			}
			snapshot.Timers = append(snapshot.Timers, first, second)
			got, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
			if err != nil || len(got) != want {
				t.Fatalf("scoped schedules: got=%v want=%d err=%v", got, want, err)
			}
			snapshot.Timers = append(snapshot.Timers, first)
			if _, err := loadRunForkArrivalJoinSchedules(snapshot, entities); err == nil {
				t.Fatal("same-scope duplicate schedule admitted")
			}
		})
	}
}

func TestFixedRevisionImmediateEmptyJoinHasNoOriginalTimeout(t *testing.T) {
	for _, withDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "without_deadline", true: "with_deadline"}[withDeadline], func(t *testing.T) {
			snapshot, entities, retained := arrivalJoinScheduleProjectionFixture(t, true, false)
			due := time.Time{}
			if withDeadline {
				due = retained.DeadlineAt
			}
			join, err := joinruntime.NewActivation(retained.JoinRef(), nil, nil, retained.ArmedAt, due)
			if err != nil {
				t.Fatal(err)
			}
			join.Close(joinruntime.CloseReasonComplete, true, false)
			handle, err := timeridentity.JoinCompleteHandle(join.JoinRef())
			if err != nil {
				t.Fatal(err)
			}
			join, err = join.WithTimerHandle(handle, join.ArmedAt)
			if err != nil {
				t.Fatal(err)
			}
			command, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Timers = []runForkRevisionTimer{arrivalJoinRevisionTimer(t, command, join.ArmedAt)}
			buckets := map[string]map[string]any{}
			if err := joinruntime.Store(buckets, join); err != nil {
				t.Fatal(err)
			}
			entities[0].Accumulator = map[string]any{}
			for key, value := range buckets {
				entities[0].Accumulator[key] = value
			}
			got, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
			if err != nil || len(got) != 1 {
				t.Fatalf("immediate empty join: schedules=%v err=%v", got, err)
			}
		})
	}
}

func arrivalJoinScheduleProjectionFixture(t *testing.T, root, completion bool) (*runForkRevisionSnapshot, []runfork.RunForkEntityState, joinruntime.Activation) {
	t.Helper()
	plan, _, reply := runForkReplyPlanFixture(t, replycontext.StateOpen, root, false)
	var ref timeridentity.JoinRef
	for _, receipt := range reply.ReturnJoins {
		if !receipt.Ref.StageEntry().Empty() {
			ref = receipt.Ref
		}
	}
	if !ref.Valid() || ref.StageEntry().Empty() {
		t.Fatal("fixture lacks its bound historical return arm")
	}
	arm := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	join, err := joinruntime.NewActivation(ref, []string{"member"}, nil, arm, arm.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	deadline, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	timers := []runForkRevisionTimer{arrivalJoinRevisionTimer(t, deadline, arm)}
	if completion {
		join.Close(joinruntime.CloseReasonComplete, true, false)
		handle, err := timeridentity.JoinCompleteHandle(ref)
		if err != nil {
			t.Fatal(err)
		}
		join, err = join.WithTimerHandle(handle, arm.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		command, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Live)
		if err != nil {
			t.Fatal(err)
		}
		at := arm.Add(time.Minute)
		timers[0].Status, timers[0].CancelCause, timers[0].CancelledAt = "cancelled", "join_closed", &at
		timers = append(timers, arrivalJoinRevisionTimer(t, command, arm))
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, join); err != nil {
		t.Fatal(err)
	}
	entity := plan.Entities[0]
	entity.Accumulator = map[string]any{}
	for key, value := range buckets {
		entity.Accumulator[key] = value
	}
	snapshot := &runForkRevisionSnapshot{RunID: plan.SourceRunID, Timers: timers}
	return snapshot, []runfork.RunForkEntityState{entity}, join
}

func arrivalJoinRevisionTimer(t *testing.T, command genericschedule.AdmissionCommand, arm time.Time) runForkRevisionTimer {
	t.Helper()
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := command.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Encode(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	source, err := json.Marshal(command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	due := command.Due.Absolute
	return runForkRevisionTimer{TimerSnapshot: runforkrevision.TimerSnapshot{
		TimerID: uuid.NewString(), TimerName: command.ScheduleKey, ScheduleKey: command.ScheduleKey, ScheduleScope: scope, ImmutableHash: hash,
		RunID: command.RunID, EntityID: command.EntityID, FlowInstance: command.FlowInstance, FireEvent: command.EventType,
		FirePayload: payload, RoutingSource: source, ExecutionMode: string(command.ExecutionMode), TaskID: command.TaskID,
		OwnerKind: string(command.OwnerKind), OwnerAgent: command.OwnerID, DueBasisKind: string(genericschedule.DueAbsolute),
		DueBasisAbsolute: &due, InitialFireAt: &due, FireAt: due, CreatedAt: arm, Status: "active", TaskType: "timer",
	}}
}
