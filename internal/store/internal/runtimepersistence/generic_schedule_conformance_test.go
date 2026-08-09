package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func setGenericScheduleClock(t *testing.T, store runtimegenericschedule.Store, now func() time.Time) {
	t.Helper()
	switch selected := store.(type) {
	case *PostgresStore:
		selected.genericSchedulePostgresOwner.SetNowFnForTest(now)
	case *SQLiteRuntimeStore:
		selected.genericScheduleSQLiteOwner.SetNowFnForTest(now)
	default:
		t.Fatalf("unsupported generic schedule store %T", store)
	}
}

func TestGenericScheduleAdmissionReplayConflictAndCancellationOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			base := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
			clock := base
			setGenericScheduleClock(t, store, func() time.Time { return clock })
			command := testAgentGenericScheduleCommand(
				t, runID, "schedule-agent", "schedule/instance", uuid.NewString(), "stable-delay",
				runtimegenericschedule.DelayDue(10*time.Minute),
			)

			created, err := store.AdmitGenericSchedule(ctx, command)
			if err != nil {
				t.Fatalf("create activation: %v", err)
			}
			if created.Outcome != runtimegenericschedule.AdmissionCreated || !created.Activation.AdmittedAt.Equal(base) || !created.Activation.InitialDueAt.Equal(base.Add(10*time.Minute)) {
				t.Fatalf("created activation = %#v", created)
			}

			clock = base.Add(24 * time.Hour)
			replayed, err := store.AdmitGenericSchedule(ctx, command)
			if err != nil {
				t.Fatalf("exact replay: %v", err)
			}
			if replayed.Outcome != runtimegenericschedule.AdmissionExactReplay || replayed.Activation.ID != created.Activation.ID ||
				!replayed.Activation.AdmittedAt.Equal(created.Activation.AdmittedAt) || !replayed.Activation.CurrentDueAt.Equal(created.Activation.CurrentDueAt) {
				t.Fatalf("clock-independent replay = %#v, want %#v", replayed, created)
			}

			conflict := command
			conflict.EventType = "schedule.changed_timer"
			if _, err := store.AdmitGenericSchedule(ctx, conflict); !runtimegenericschedule.IsConflict(err) {
				t.Fatalf("changed-content replay error = %v, want typed conflict", err)
			}

			wakeup, err := created.Activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := store.ClaimGenericScheduleWakeup(ctx, wakeup)
			if err != nil || !claimed {
				t.Fatalf("claim active wakeup = %v, %v", claimed, err)
			}
			cancelled, err := store.CancelGenericSchedule(ctx, runtimegenericschedule.CancelCommand{
				ActivationID: created.Activation.ID, Cause: "operator_cancelled", CancelledAt: clock,
			})
			if err != nil || cancelled.Outcome != runtimegenericschedule.CancelChanged || cancelled.Activation.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("cancel result = %#v, %v", cancelled, err)
			}
			claimed, err = store.ClaimGenericScheduleWakeup(ctx, wakeup)
			if err != nil || claimed {
				t.Fatalf("claim cancelled wakeup = %v, %v", claimed, err)
			}
			replayed, err = store.AdmitGenericSchedule(ctx, command)
			if err != nil || replayed.Outcome != runtimegenericschedule.AdmissionExactReplay || replayed.Activation.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("terminal exact replay = %#v, %v", replayed, err)
			}
		})
	}
}

func TestGenericScheduleConcurrentAdmissionHasOneImmutableWinnerOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			command := testAgentGenericScheduleCommand(
				t, runtimecorrelation.RunIDFromContext(ctx), "race-agent", "race/instance", uuid.NewString(), "race-key",
				runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			)
			type outcome struct {
				result runtimegenericschedule.AdmissionResult
				err    error
			}
			const callers = 8
			results := make(chan outcome, callers)
			var start sync.WaitGroup
			start.Add(1)
			for range callers {
				go func() {
					start.Wait()
					result, err := store.AdmitGenericSchedule(ctx, command)
					results <- outcome{result: result, err: err}
				}()
			}
			start.Done()
			var activationID string
			created := 0
			for range callers {
				out := <-results
				if out.err != nil {
					t.Fatalf("concurrent admission: %v", out.err)
				}
				if activationID == "" {
					activationID = out.result.Activation.ID
				}
				if out.result.Activation.ID != activationID {
					t.Fatalf("concurrent activation IDs differ: %q vs %q", out.result.Activation.ID, activationID)
				}
				if out.result.Outcome == runtimegenericschedule.AdmissionCreated {
					created++
				}
			}
			if created != 1 {
				t.Fatalf("created outcomes = %d, want one", created)
			}
		})
	}
}

func admitGenericScheduleInRollbackTransaction(
	ctx context.Context,
	selected runtimegenericschedule.Store,
	db *sql.DB,
	command runtimegenericschedule.AdmissionCommand,
	now time.Time,
) (runtimegenericschedule.AdmissionResult, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	defer tx.Rollback()
	switch store := selected.(type) {
	case *PostgresStore:
		store.genericSchedulePostgresOwner.SetNowFnForTest(func() time.Time { return now })
		return store.genericSchedulePostgresOwner.AdmitTx(ctx, tx, command)
	case *SQLiteRuntimeStore:
		store.genericScheduleSQLiteOwner.SetNowFnForTest(func() time.Time { return now })
		return store.genericScheduleSQLiteOwner.AdmitTx(ctx, tx, command)
	default:
		return runtimegenericschedule.AdmissionResult{}, fmt.Errorf("unsupported selected store %T", selected)
	}
}

func TestGenericScheduleAdmissionRollbackAckLossAndDueBasisReplayOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			base := time.Date(2026, 8, 9, 12, 7, 0, 0, time.UTC)
			clock := base
			setGenericScheduleClock(t, selected, func() time.Time { return clock })

			rolledBackCommand := testAgentGenericScheduleCommand(
				t, runID, "rollback-agent", "rollback/instance", uuid.NewString(), "rollback-key",
				runtimegenericschedule.DelayDue(5*time.Minute),
			)
			rolledBack, err := admitGenericScheduleInRollbackTransaction(ctx, selected, db, rolledBackCommand, base)
			if err != nil || rolledBack.Outcome != runtimegenericschedule.AdmissionCreated {
				t.Fatalf("rollback transaction admission = %#v, %v", rolledBack, err)
			}
			if _, found, err := selected.LoadGenericScheduleActivation(ctx, rolledBack.Activation.ID); err != nil || found {
				t.Fatalf("rolled-back activation escaped: found=%v err=%v", found, err)
			}

			for _, dueCase := range []struct {
				name string
				due  runtimegenericschedule.DueBasis
			}{
				{name: "delay", due: runtimegenericschedule.DelayDue(17 * time.Minute)},
				{name: "every", due: runtimegenericschedule.EveryDue(23 * time.Minute)},
				{name: "cron", due: runtimegenericschedule.CronDue("13 * * * *")},
			} {
				t.Run(dueCase.name, func(t *testing.T) {
					command := testAgentGenericScheduleCommand(
						t, runID, "basis-agent", "basis/instance", uuid.NewString(), "basis-"+dueCase.name,
						dueCase.due,
					)
					created, err := selected.AdmitGenericSchedule(ctx, command)
					if err != nil {
						t.Fatalf("create %s activation: %v", dueCase.name, err)
					}
					wantDue, err := dueCase.due.FirstDue(base)
					if err != nil {
						t.Fatal(err)
					}
					if !created.Activation.InitialDueAt.Equal(wantDue) {
						t.Fatalf("%s first due = %s, want %s", dueCase.name, created.Activation.InitialDueAt, wantDue)
					}

					// The first response is intentionally discarded to model commit success
					// with acknowledgment loss at the public admission boundary.
					clock = base.Add(72 * time.Hour)
					replayed, err := selected.AdmitGenericSchedule(ctx, command)
					if err != nil || replayed.Outcome != runtimegenericschedule.AdmissionExactReplay {
						t.Fatalf("%s acknowledgment-loss retry = %#v, %v", dueCase.name, replayed, err)
					}
					if replayed.Activation.ID != created.Activation.ID ||
						!replayed.Activation.AdmittedAt.Equal(created.Activation.AdmittedAt) ||
						!replayed.Activation.InitialDueAt.Equal(created.Activation.InitialDueAt) ||
						!replayed.Activation.CurrentDueAt.Equal(created.Activation.CurrentDueAt) {
						t.Fatalf("%s retry reminted activation facts: created=%#v replay=%#v", dueCase.name, created, replayed)
					}
					changed := command
					changed.Due = runtimegenericschedule.DelayDue(99 * time.Minute)
					if _, err := selected.AdmitGenericSchedule(ctx, changed); !runtimegenericschedule.IsConflict(err) {
						t.Fatalf("%s changed due basis error = %v, want conflict", dueCase.name, err)
					}
					clock = base
				})
			}
		})
	}
}

type genericScheduleRunControlStore interface {
	PauseRunControl(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
	StopRunControl(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
}

func transitionGenericScheduleRun(t *testing.T, selected any, runID string, stop bool) {
	t.Helper()
	owner, ok := selected.(genericScheduleRunControlStore)
	if !ok {
		t.Fatalf("generic schedule run control owner = %T, want selected-store owner", selected)
	}
	request := runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "generic schedule conformance", ControlledBy: "test", Now: time.Now().UTC()}
	ctx := authorGenericScheduleConsumerContext(runID)
	var err error
	if stop {
		_, err = owner.StopRunControl(ctx, request)
	} else {
		_, err = owner.PauseRunControl(ctx, request)
	}
	if err != nil {
		t.Fatalf("transition generic schedule run %s stop=%v: %v", runID, stop, err)
	}
}

func TestGenericScheduleRunStateAndGlobalAdmissionOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			dueAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
			activeCommand := testAgentGenericScheduleCommand(
				t, runID, "state-agent", "state/instance", uuid.NewString(), "running",
				runtimegenericschedule.AbsoluteDue(dueAt),
			)
			active, err := selected.AdmitGenericSchedule(ctx, activeCommand)
			if err != nil {
				t.Fatalf("running admission: %v", err)
			}

			transitionGenericScheduleRun(t, selected, runID, false)
			pausedCommand := activeCommand
			pausedCommand.ScheduleKey = "paused"
			pausedCommand.TaskID = "paused"
			if _, err := selected.AdmitGenericSchedule(ctx, pausedCommand); err != nil {
				t.Fatalf("paused admission: %v", err)
			}
			wakeup, err := active.Activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := selected.PrepareGenericScheduleOccurrence(ctx, wakeup)
			if err != nil || prepared.Outcome != runtimegenericschedule.PrepareReady {
				t.Fatalf("paused occurrence = %#v, %v", prepared, err)
			}

			transitionGenericScheduleRun(t, selected, runID, true)
			stale, err := selected.PrepareGenericScheduleOccurrence(ctx, wakeup)
			if err != nil || stale.Outcome != runtimegenericschedule.PrepareStaleCancelled || stale.Activation.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("terminal-run occurrence = %#v, %v", stale, err)
			}
			terminalCommand := activeCommand
			terminalCommand.ScheduleKey = "terminal-rejected"
			terminalCommand.TaskID = "terminal-rejected"
			if _, err := selected.AdmitGenericSchedule(ctx, terminalCommand); err == nil {
				t.Fatal("terminal run admitted a new generic schedule")
			}

			missingCommand := activeCommand
			missingCommand.RunID = uuid.NewString()
			missingCommand.ScheduleKey = "missing-rejected"
			missingCommand.TaskID = "missing-rejected"
			if _, err := selected.AdmitGenericSchedule(ctx, missingCommand); err == nil {
				t.Fatal("missing run admitted a generic schedule")
			}

			global := runtimegenericschedule.AdmissionCommand{
				ScheduleKey: "global-only", OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "runtime",
				EventType: "platform.global_tick", Payload: semanticvalue.EmptyObject(),
				RoutingSource: events.NewPlatformControlRoutingSource(),
				Due:           runtimegenericschedule.EveryDue(time.Hour), TaskID: "global-only",
			}
			if admitted, err := selected.AdmitGenericSchedule(context.Background(), global); err != nil || admitted.Activation.Command.RunID != "" {
				t.Fatalf("global admission = %#v, %v", admitted, err)
			}
		})
	}
}

func TestGenericScheduleReplyContextReplayConflictAndRecurringRejectionOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			command := testAgentGenericScheduleCommand(
				t, runID, "reply-agent", "reply/instance", uuid.NewString(), "reply-once",
				runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			)
			requestEventID := uuid.NewString()
			if err := commitSemanticEventFixture(ctx, selected, eventtest.ExistingRunRootIngress(
				requestEventID, "reply.requested", "test", "", []byte(`{}`), 0, runID,
				events.EventEnvelope{}, time.Now().UTC(),
			)); err != nil {
				t.Fatalf("seed reply-context request event: %v", err)
			}
			now := time.Now().UTC()
			reply := runtimereplycontext.Record{
				RunID: runID, RequestEventID: requestEventID,
				RequesterFlowID: "reply", RequestOutputPin: "requested", ReplyInputPin: "replied",
				ProviderFlowID: "provider", ProviderInputPin: "requested", ProviderOutputPin: "replied",
				Origin:               events.RouteIdentity{FlowID: "reply", FlowInstance: "reply/instance", EntityID: command.EntityID},
				RequestCorrelationID: requestEventID, State: runtimereplycontext.StateOpen,
				CreatedAt: now, UpdatedAt: now,
			}
			reply.ID = runtimereplycontext.DeterministicID(
				reply.RequestEventID, reply.RequesterFlowID, reply.RequestOutputPin,
				reply.ReplyInputPin, reply.ProviderFlowID, reply.Origin,
			)
			replyStore, ok := selected.(runtimereplycontext.Store)
			if !ok {
				t.Fatalf("selected store %T lacks reply-context owner", selected)
			}
			if err := replyStore.CreateReplyContext(ctx, reply); err != nil {
				t.Fatalf("seed reply context: %v", err)
			}
			command.ReplyContext = reply.ID
			created, err := selected.AdmitGenericSchedule(ctx, command)
			if err != nil {
				t.Fatalf("reply-context admission: %v", err)
			}
			replayed, err := selected.AdmitGenericSchedule(ctx, command)
			if err != nil || replayed.Outcome != runtimegenericschedule.AdmissionExactReplay ||
				replayed.Activation.Command.ReplyContext != command.ReplyContext {
				t.Fatalf("reply-context replay = %#v, %v", replayed, err)
			}
			conflict := command
			conflict.ReplyContext = uuid.NewString()
			if _, err := selected.AdmitGenericSchedule(ctx, conflict); !runtimegenericschedule.IsConflict(err) {
				t.Fatalf("changed reply context error = %v, want conflict", err)
			}
			recurring := command
			recurring.ScheduleKey = "reply-recurring"
			recurring.TaskID = "reply-recurring"
			recurring.Due = runtimegenericschedule.EveryDue(time.Hour)
			if _, err := selected.AdmitGenericSchedule(ctx, recurring); err == nil {
				t.Fatal("recurring reply-context schedule was admitted")
			}
			active, err := selected.ListActiveGenericScheduleActivations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, activation := range active {
				if activation.Command.ScheduleKey == recurring.ScheduleKey {
					t.Fatalf("rejected recurring reply-context schedule mutated storage: %#v", activation)
				}
			}
			if loaded, found, err := selected.LoadGenericScheduleActivation(ctx, created.Activation.ID); err != nil || !found || loaded.Command.ReplyContext != command.ReplyContext {
				t.Fatalf("reply-context readback = %#v found=%v err=%v", loaded, found, err)
			}
		})
	}
}

func TestMalformedGenericScheduleTerminalizesLoudlyOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			command := testAgentGenericScheduleCommand(
				t, runtimecorrelation.RunIDFromContext(ctx), "malformed-agent", "malformed/instance", uuid.NewString(), "malformed-key",
				runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			)
			created, err := store.AdmitGenericSchedule(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			wakeup, err := created.Activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			query := `UPDATE timers SET immutable_hash = ? WHERE timer_id = ?`
			args := []any{"corrupt", created.Activation.ID}
			if _, ok := store.(*PostgresStore); ok {
				query = `UPDATE timers SET immutable_hash = $1 WHERE timer_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("corrupt activation: %v", err)
			}
			prepared, err := store.PrepareGenericScheduleOccurrence(ctx, wakeup)
			if err != nil || prepared.Outcome != runtimegenericschedule.PrepareTerminal {
				t.Fatalf("prepare malformed activation = %#v, %v", prepared, err)
			}
			statusQuery := `SELECT status, failure_code, failure_message FROM timers WHERE timer_id = ?`
			if _, ok := store.(*PostgresStore); ok {
				statusQuery = `SELECT status, failure_code, failure_message FROM timers WHERE timer_id = $1::uuid`
			}
			var status, code, message string
			if err := db.QueryRowContext(ctx, statusQuery, created.Activation.ID).Scan(&status, &code, &message); err != nil {
				t.Fatal(err)
			}
			if status != "failed" || code != "malformed_persisted_activation" || message == "" {
				t.Fatalf("terminal malformed fact = status:%q code:%q message:%q", status, code, message)
			}
		})
	}
}

func TestGenericScheduleOwnerDoesNotInterpretWorkflowTimerRowsOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			row := newWorkflowTimerDDLProofRow(runtimecorrelation.RunIDFromContext(ctx))
			if err := insertWorkflowTimerDDLProofRow(ctx, db, store, row); err != nil {
				t.Fatalf("insert workflow timer: %v", err)
			}
			if activation, found, err := store.LoadGenericScheduleActivation(ctx, row.timerID); err != nil || found {
				t.Fatalf("generic load interpreted workflow timer: found=%v activation=%#v err=%v", found, activation, err)
			}
			cancelled, err := store.CancelGenericSchedule(ctx, runtimegenericschedule.CancelCommand{
				ActivationID: row.timerID, Cause: "generic_probe", CancelledAt: time.Now(),
			})
			if err != nil || cancelled.Outcome != runtimegenericschedule.CancelMissing {
				t.Fatalf("generic cancel interpreted workflow timer: %#v, %v", cancelled, err)
			}
			active, err := store.ListActiveGenericScheduleActivations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, activation := range active {
				if activation.ID == row.timerID {
					t.Fatalf("generic list interpreted workflow timer: %#v", activation)
				}
			}
			var status string
			statusQuery := `SELECT status FROM timers WHERE timer_id = ?`
			if _, ok := store.(*PostgresStore); ok {
				statusQuery = `SELECT status FROM timers WHERE timer_id = $1::uuid`
			}
			if err := db.QueryRowContext(ctx, statusQuery, row.timerID).Scan(&status); err != nil || status != "active" {
				t.Fatalf("workflow timer changed through generic owner: status=%q err=%v", status, err)
			}
		})
	}
}

func TestGenericScheduleTestMatrixNamesBothSelectedStores(t *testing.T) {
	got := make(map[string]bool)
	for _, tc := range selectedScheduleStoreCases() {
		got[tc.name] = true
	}
	for _, name := range []string{"sqlite", "postgres"} {
		if !got[name] {
			t.Fatal(fmt.Sprintf("generic schedule matrix is missing %s", name))
		}
	}
}
