package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedStoreLifecycleScheduler struct {
	callback   func(context.Context, runtimegenericschedule.Wakeup)
	registered []runtimegenericschedule.Wakeup
	retired    []runtimegenericschedule.Wakeup
}

func (s *selectedStoreLifecycleScheduler) BindGenericScheduleLifecycle(callback func(context.Context, runtimegenericschedule.Wakeup)) error {
	if callback == nil || s.callback != nil {
		return errors.New("selected-store lifecycle scheduler requires one callback")
	}
	s.callback = callback
	return nil
}

func (s *selectedStoreLifecycleScheduler) RegisterGenericScheduleWakeup(_ context.Context, wakeup runtimegenericschedule.Wakeup) error {
	s.registered = append(s.registered, wakeup)
	return nil
}

func (s *selectedStoreLifecycleScheduler) RetireGenericScheduleWakeup(wakeup runtimegenericschedule.Wakeup) error {
	s.retired = append(s.retired, wakeup)
	return nil
}

func (*selectedStoreLifecycleScheduler) StopGenericScheduleWakeups(context.Context) error { return nil }

type terminalSchedulePlannerProbe struct{ prepareCalls int }

func (p *terminalSchedulePlannerProbe) PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	p.prepareCalls++
	return nil, errors.New("terminal generic schedule reached event publication")
}

func (*terminalSchedulePlannerProbe) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	return nil
}

func (*terminalSchedulePlannerProbe) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	return nil
}

type terminalScheduleDispatcherProbe struct{ calls int }

func (d *terminalScheduleDispatcherProbe) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	d.calls++
	return nil
}

func TestPostgresGenericScheduleEmptyTerminalPreparationTransfersExactClaim(t *testing.T) {
	for _, terminalCase := range []string{"missing", "malformed"} {
		t.Run(terminalCase, func(t *testing.T) {
			dsn, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected := admitTestPostgresStore(t, db)
			t.Cleanup(func() { _ = selected.ReleaseGenericScheduleClaims(context.Background()) })
			runID := uuid.NewString()
			ctx := selectedScheduleTestContext(t, runID)
			requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())

			scheduler := &selectedStoreLifecycleScheduler{}
			planner := &terminalSchedulePlannerProbe{}
			dispatcher := &terminalScheduleDispatcherProbe{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(selected, scheduler, planner, dispatcher, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := lifecycle.Stop(stopCtx); err != nil {
					t.Errorf("stop generic schedule lifecycle: %v", err)
				}
			})

			command := testAgentGenericScheduleCommand(
				t, runID, "claim-agent", "claim/instance", uuid.NewString(), "terminal-"+terminalCase,
				runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			)
			admitted, err := lifecycle.Admit(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			if len(scheduler.registered) != 1 {
				t.Fatalf("registered wakeups = %#v", scheduler.registered)
			}
			wakeup := scheduler.registered[0]
			lockKey := "swarm:generic_schedule:" + admitted.Activation.ID

			switch terminalCase {
			case "missing":
				if _, err := db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id = $1::uuid`, admitted.Activation.ID); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if _, err := db.ExecContext(ctx, `UPDATE timers SET immutable_hash = 'corrupt' WHERE timer_id = $1::uuid`, admitted.Activation.ID); err != nil {
					t.Fatal(err)
				}
			}

			if scheduler.callback == nil {
				t.Fatal("generic schedule lifecycle callback was not bound")
			}
			scheduler.callback(ctx, wakeup)
			if len(scheduler.retired) != 1 || scheduler.retired[0] != wakeup {
				t.Fatalf("terminal wakeup retirement = %#v, want %#v", scheduler.retired, wakeup)
			}
			if planner.prepareCalls != 0 || dispatcher.calls != 0 {
				t.Fatalf("terminal preparation emitted event: prepare=%d dispatch=%d", planner.prepareCalls, dispatcher.calls)
			}
			if terminalCase == "malformed" {
				var status, code string
				if err := db.QueryRowContext(ctx, `SELECT status, failure_code FROM timers WHERE timer_id = $1::uuid`, admitted.Activation.ID).Scan(&status, &code); err != nil {
					t.Fatal(err)
				}
				if status != "failed" || code != "malformed_persisted_activation" {
					t.Fatalf("malformed terminal fact = status:%q code:%q", status, code)
				}
			}
			assertIndependentAdvisoryLockAvailable(t, dsn, lockKey)
		})
	}
}

func TestPostgresGenericScheduleOccurrenceUsesDatabaseClockAcrossPrepareAndCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	ctx := selectedScheduleTestContext(t, runID)
	requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
	selected.genericSchedulePostgresOwner.SetNowFnForTest(func() time.Time {
		return time.Now().UTC().Add(24 * time.Hour)
	})

	var databaseNow time.Time
	if err := db.QueryRowContext(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	entityID := uuid.NewString()
	command := testRootGenericScheduleCommand(
		t, runID, entityID, "postgres-clock-domain", runtimegenericschedule.AbsoluteDue(databaseNow.UTC()),
	)
	command.EventType = "test.node_emitted"
	admitted, err := selected.AdmitGenericSchedule(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	wakeup, err := admitted.Activation.Wakeup()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := selected.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || prepared.Outcome != runtimegenericschedule.PrepareReady {
		t.Fatalf("prepare skewed occurrence = %#v, %v", prepared, err)
	}
	if !prepared.Occurrence.AdmittedAt.Before(admitted.Activation.AdmittedAt) {
		t.Fatalf("occurrence admission %s used process clock %s", prepared.Occurrence.AdmittedAt, admitted.Activation.AdmittedAt)
	}
	payload, err := json.Marshal(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RuntimeControlWithRoutingSource(
		prepared.Occurrence.EventID,
		events.EventType(command.EventType),
		runtimegenericschedule.OccurrenceProducerID(),
		command.TaskID,
		payload,
		0,
		runID,
		"",
		events.EventEnvelope{EntityID: entityID},
		command.RoutingSource,
		prepared.Occurrence.DueAt,
	)
	bus, err := newStoreTestEventBus(t, selected)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := bus.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{{Event: event}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("prepare occurrence publication plans=%d err=%v", len(plans), err)
	}
	committed, err := selected.CommitGenericScheduleOccurrence(ctx, runtimegenericschedule.CommitCommand{
		Activation: prepared.Activation, Occurrence: prepared.Occurrence, Publication: plans[0],
	})
	if err != nil || committed.Outcome != runtimegenericschedule.CommitCommitted || committed.Next.Status != runtimegenericschedule.StatusFired {
		t.Fatalf("commit skewed occurrence = %#v, %v", committed, err)
	}
	if committed.Next.AcceptedAt.Before(prepared.Occurrence.AdmittedAt) {
		t.Fatalf("accepted_at %s precedes database occurrence admission %s", committed.Next.AcceptedAt, prepared.Occurrence.AdmittedAt)
	}
}

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
			if created.Activation.Command.ExecutionMode != executionmode.Live {
				t.Fatalf("created execution mode = %q, want live", created.Activation.Command.ExecutionMode)
			}
			loaded, found, err := store.LoadGenericScheduleActivation(ctx, created.Activation.ID)
			if err != nil || !found || loaded.Command.ExecutionMode != executionmode.Live {
				t.Fatalf("execution mode readback = found:%v activation:%#v err:%v", found, loaded, err)
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
			modeConflict := command
			modeConflict.ExecutionMode = executionmode.Mock
			if _, err := store.AdmitGenericSchedule(ctx, modeConflict); !runtimegenericschedule.IsConflict(err) {
				t.Fatalf("changed-mode replay error = %v, want typed conflict", err)
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

func TestGenericScheduleExecutionModeSurvivesReplayAndStoreReconstructionOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				t.Run(string(mode), func(t *testing.T) {
					command := testAgentGenericScheduleCommand(
						t, runID, "mode-agent", "mode/instance", uuid.NewString(), "mode-"+string(mode),
						runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
					)
					command.ExecutionMode = mode

					created, err := store.AdmitGenericSchedule(ctx, command)
					if err != nil || created.Outcome != runtimegenericschedule.AdmissionCreated {
						t.Fatalf("create %s activation = %#v, %v", mode, created, err)
					}
					replayed, err := store.AdmitGenericSchedule(ctx, command)
					if err != nil || replayed.Outcome != runtimegenericschedule.AdmissionExactReplay || replayed.Activation.ID != created.Activation.ID {
						t.Fatalf("exact %s replay = %#v, %v", mode, replayed, err)
					}

					var reconstructed runtimegenericschedule.Store
					switch store.(type) {
					case *SQLiteRuntimeStore:
						restarted := NewSQLiteRuntimeStoreForTest(db)
						if err := restarted.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
							t.Fatalf("bootstrap reconstructed SQLite store: %v", err)
						}
						reconstructed = restarted
					case *PostgresStore:
						restarted := NewPostgresStoreForTest(db)
						if err := restarted.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
							t.Fatalf("bootstrap reconstructed PostgreSQL store: %v", err)
						}
						reconstructed = restarted
					default:
						t.Fatalf("unsupported selected store %T", store)
					}
					loaded, found, err := reconstructed.LoadGenericScheduleActivation(ctx, created.Activation.ID)
					if err != nil || !found || loaded.Command.ExecutionMode != mode {
						t.Fatalf("reconstructed %s readback = found:%v activation:%#v err:%v", mode, found, loaded, err)
					}

					changedMode := command
					if mode == executionmode.Live {
						changedMode.ExecutionMode = executionmode.Mock
					} else {
						changedMode.ExecutionMode = executionmode.Live
					}
					if _, err := reconstructed.AdmitGenericSchedule(ctx, changedMode); !runtimegenericschedule.IsConflict(err) {
						t.Fatalf("%s-to-%s replay error = %v, want typed conflict", mode, changedMode.ExecutionMode, err)
					}
				})
			}
		})
	}
}

func TestTimerExecutionModeFreshSchemaIsRequiredWithoutDefaultOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			var nullable string
			var defaultValue sql.NullString
			switch store.(type) {
			case *SQLiteRuntimeStore:
				rows, err := db.QueryContext(ctx, `PRAGMA table_info(timers)`)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				found := false
				for rows.Next() {
					var ordinal, notNull, primaryKey int
					var name, dataType string
					if err := rows.Scan(&ordinal, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
						t.Fatal(err)
					}
					if name == "execution_mode" {
						found = true
						if notNull != 1 || defaultValue.Valid {
							t.Fatalf("SQLite execution_mode schema not_null=%d default=%q", notNull, defaultValue.String)
						}
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if !found {
					t.Fatal("SQLite timers.execution_mode column is missing")
				}
			case *PostgresStore:
				if err := db.QueryRowContext(ctx, `
					SELECT is_nullable, column_default
					FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = 'timers' AND column_name = 'execution_mode'
				`).Scan(&nullable, &defaultValue); err != nil {
					t.Fatal(err)
				}
				if nullable != "NO" || defaultValue.Valid {
					t.Fatalf("PostgreSQL execution_mode nullable=%q default=%q", nullable, defaultValue.String)
				}
			default:
				t.Fatalf("unsupported selected store %T", store)
			}

			invalid := newWorkflowTimerDDLProofRow(runtimecorrelation.RunIDFromContext(ctx))
			invalid.executionMode = executionmode.Mode("invalid")
			if err := insertWorkflowTimerDDLProofRow(ctx, db, store, invalid); err == nil {
				t.Fatal("fresh schema accepted invalid timer execution_mode")
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
		return store.genericSchedulePostgresOwner.AdmitTx(ctx, tx, privaterunforkrevision.NewEffects(), command)
	case *SQLiteRuntimeStore:
		store.genericScheduleSQLiteOwner.SetNowFnForTest(func() time.Time { return now })
		return store.genericScheduleSQLiteOwner.AdmitTx(ctx, tx, privaterunforkrevision.NewEffects(), command)
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
				ExecutionMode: executionmode.Live,
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

func TestGenericScheduleFreshSchemaRejectsIllegalStateEvidenceOnBothStores(t *testing.T) {
	type mutation struct {
		name        string
		sqliteSQL   string
		postgresSQL string
		args        func(string, time.Time) []any
		recurring   bool
	}
	mutations := []mutation{
		{
			name:        "occurrence identity without admission",
			sqliteSQL:   `UPDATE timers SET occurrence_event_id = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET occurrence_event_id = $1::uuid WHERE timer_id = $2::uuid`,
			args:        func(id string, _ time.Time) []any { return []any{uuid.NewString(), id} },
		},
		{
			name:        "occurrence admission without identity",
			sqliteSQL:   `UPDATE timers SET occurrence_admitted_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET occurrence_admitted_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "fired without accepted",
			sqliteSQL:   `UPDATE timers SET fired_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET fired_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "accepted without fired",
			sqliteSQL:   `UPDATE timers SET accepted_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET accepted_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "accepted before fired",
			sqliteSQL:   `UPDATE timers SET fired_at = ?, accepted_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET fired_at = $1, accepted_at = $2 WHERE timer_id = $3::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, now.Add(-time.Second), id} },
		},
		{
			name:        "one shot active accepted history",
			sqliteSQL:   `UPDATE timers SET fired_at = ?, accepted_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET fired_at = $1, accepted_at = $2 WHERE timer_id = $3::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, now, id} },
		},
		{
			name:        "recurring active half accepted history",
			sqliteSQL:   `UPDATE timers SET fired_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET fired_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
			recurring:   true,
		},
		{
			name:        "cancel cause without time",
			sqliteSQL:   `UPDATE timers SET status = 'cancelled', cancel_cause = 'run_stopped' WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'cancelled', cancel_cause = 'run_stopped' WHERE timer_id = $1::uuid`,
			args:        func(id string, _ time.Time) []any { return []any{id} },
		},
		{
			name:        "cancel time without cause",
			sqliteSQL:   `UPDATE timers SET status = 'cancelled', cancelled_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'cancelled', cancelled_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "failure code without time",
			sqliteSQL:   `UPDATE timers SET status = 'failed', failure_code = 'broken' WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'failed', failure_code = 'broken' WHERE timer_id = $1::uuid`,
			args:        func(id string, _ time.Time) []any { return []any{id} },
		},
		{
			name:        "failure time without code",
			sqliteSQL:   `UPDATE timers SET status = 'failed', failed_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'failed', failed_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "failure message without code",
			sqliteSQL:   `UPDATE timers SET status = 'failed', failure_message = 'broken', failed_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'failed', failure_message = 'broken', failed_at = $1 WHERE timer_id = $2::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, id} },
		},
		{
			name:        "mixed cancellation and failure facts",
			sqliteSQL:   `UPDATE timers SET status = 'cancelled', cancel_cause = 'run_stopped', cancelled_at = ?, failure_code = 'broken', failed_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'cancelled', cancel_cause = 'run_stopped', cancelled_at = $1, failure_code = 'broken', failed_at = $2 WHERE timer_id = $3::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, now, id} },
		},
		{
			name:        "cancelled without facts",
			sqliteSQL:   `UPDATE timers SET status = 'cancelled' WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid`,
			args:        func(id string, _ time.Time) []any { return []any{id} },
		},
		{
			name:        "failed without facts",
			sqliteSQL:   `UPDATE timers SET status = 'failed' WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'failed' WHERE timer_id = $1::uuid`,
			args:        func(id string, _ time.Time) []any { return []any{id} },
		},
		{
			name:        "fired with cancellation facts",
			sqliteSQL:   `UPDATE timers SET status = 'fired', fired_at = ?, accepted_at = ?, cancel_cause = 'run_stopped', cancelled_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'fired', fired_at = $1, accepted_at = $2, cancel_cause = 'run_stopped', cancelled_at = $3 WHERE timer_id = $4::uuid`,
			args:        func(id string, now time.Time) []any { return []any{now, now, now, id} },
		},
		{
			name:        "fired acceptance before occurrence admission",
			sqliteSQL:   `UPDATE timers SET status = 'fired', occurrence_event_id = ?, occurrence_admitted_at = ?, fired_at = ?, accepted_at = ? WHERE timer_id = ?`,
			postgresSQL: `UPDATE timers SET status = 'fired', occurrence_event_id = $1::uuid, occurrence_admitted_at = $2, fired_at = $3, accepted_at = $4 WHERE timer_id = $5::uuid`,
			args: func(id string, now time.Time) []any {
				return []any{uuid.NewString(), now.Add(time.Hour), now, now, id}
			},
		},
	}
	for _, selectedCase := range selectedScheduleStoreCases() {
		t.Run(selectedCase.name, func(t *testing.T) {
			selected, db, ctx := selectedCase.open(t)
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					now := time.Now().UTC().Truncate(time.Microsecond)
					due := runtimegenericschedule.AbsoluteDue(now.Add(time.Hour))
					if mutation.recurring {
						due = runtimegenericschedule.EveryDue(time.Hour)
					}
					activation := admitGenericScheduleFixture(t, ctx, selected, testAgentGenericScheduleCommand(
						t, runtimecorrelation.RunIDFromContext(ctx), "matrix-agent", "matrix/instance", uuid.NewString(), uuid.NewString(), due,
					))
					query := mutation.sqliteSQL
					if _, ok := selected.(*PostgresStore); ok {
						query = mutation.postgresSQL
					}
					if _, err := db.ExecContext(ctx, query, mutation.args(activation.ID, now.Add(2*time.Hour))...); err == nil {
						t.Fatal("fresh selected-store schema admitted illegal generic schedule state")
					}
					loaded, found, err := selected.LoadGenericScheduleActivation(ctx, activation.ID)
					if err != nil || !found || loaded.Status != runtimegenericschedule.StatusActive {
						t.Fatalf("rejected corruption changed readback: found=%v activation=%#v err=%v", found, loaded, err)
					}
				})
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
