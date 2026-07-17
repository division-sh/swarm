package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedScheduleStoreCase struct {
	name string
	open func(*testing.T) (runtimepipeline.SchedulePersistence, *sql.DB, context.Context)
}

func selectedScheduleStoreCases() []selectedScheduleStoreCase {
	return []selectedScheduleStoreCase{
		{name: "sqlite", open: func(t *testing.T) (runtimepipeline.SchedulePersistence, *sql.DB, context.Context) {
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(context.Background(), runID)
			seedSQLiteScheduleRun(t, store, ctx, runID)
			return store, store.DB, ctx
		}},
		{name: "postgres", open: func(t *testing.T) (runtimepipeline.SchedulePersistence, *sql.DB, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(context.Background(), runID)
			if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', now())`, runID); err != nil {
				t.Fatalf("seed PostgreSQL run: %v", err)
			}
			store := &PostgresStore{DB: db}
			t.Cleanup(func() { _ = store.ReleaseScheduleClaims(context.Background()) })
			return store, db, ctx
		}},
	}
}

func TestGenericScheduleStoreCannotInterpretWorkflowTimerRows(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := uuid.NewString()
			activationID := uuid.NewString()
			ref := timeridentity.WorkflowTimerActivationRef{
				ActivationID: activationID,
				Declaration:  "waiting.timeout",
			}.Normalize()
			fireAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
			genericTaskID := "workflowXtimer:v1:payload-key-collision"
			payload := json.RawMessage(`{"__schedule_task_id":"workflowXtimer:v1:payload-key-collision","business":true}`)

			switch store.(type) {
			case *SQLiteRuntimeStore:
				_, err := db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, recurring, owner_agent, task_type, status, created_at
					) VALUES (?, ?, ?, ?, 'timer-proof', 'timer.timeout', ?, ?, false, 'runtime', 'timer', 'active', ?)
				`, activationID, runID, ref.TaskID(), entityID, string(payload), fireAt, fireAt.Add(-time.Hour))
				if err != nil {
					t.Fatalf("insert SQLite workflow activation: %v", err)
				}
			case *PostgresStore:
				_, err := db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						fire_at, recurring, owner_agent, task_type, status, created_at
					) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'timer-proof', 'timer.timeout', $5::jsonb,
					          $6, false, 'runtime', 'timer', 'active', $7)
				`, activationID, runID, ref.TaskID(), entityID, string(payload), fireAt, fireAt.Add(-time.Hour))
				if err != nil {
					t.Fatalf("insert PostgreSQL workflow activation: %v", err)
				}
			default:
				t.Fatalf("unsupported schedule store %T", store)
			}

			generic := runtimepipeline.Schedule{
				RunID: runID, AgentID: "runtime", EventType: "timer.timeout", Mode: "once", At: fireAt,
				EntityID: entityID, FlowInstance: "timer-proof", TaskID: genericTaskID, Payload: json.RawMessage(`{"business":true}`),
			}
			active, err := store.LoadActiveSchedules(ctx)
			if err != nil || len(active) != 0 {
				t.Fatalf("generic load before insert = %#v, err=%v; want no workflow rows", active, err)
			}
			claimed, err := store.ClaimSchedule(ctx, generic)
			if err != nil || claimed {
				t.Fatalf("generic claim before insert = %v, err=%v; want false", claimed, err)
			}
			if err := store.CancelScheduleExact(ctx, generic); err != nil {
				t.Fatalf("generic exact cancel: %v", err)
			}
			if err := store.MarkScheduleFiredExact(ctx, generic); err != nil {
				t.Fatalf("generic exact completion: %v", err)
			}
			if postgres, ok := store.(*PostgresStore); ok {
				if err := postgres.CancelSchedule(ctx, generic.AgentID, generic.EventType); err != nil {
					t.Fatalf("generic broad cancel: %v", err)
				}
				if err := postgres.MarkScheduleFired(ctx, generic); err != nil {
					t.Fatalf("generic broad completion: %v", err)
				}
			}
			assertWorkflowTimerRowStatus(t, db, store, activationID, "active")

			if err := store.UpsertSchedule(ctx, generic); err != nil {
				t.Fatalf("insert generic schedule beside workflow activation: %v", err)
			}
			active, err = store.LoadActiveSchedules(ctx)
			if err != nil || len(active) != 1 || active[0].TaskID != genericTaskID {
				t.Fatalf("generic load after insert = %#v, err=%v; want one generic row", active, err)
			}
			claimed, err = store.ClaimSchedule(ctx, generic)
			if err != nil || !claimed {
				t.Fatalf("generic claim after insert = %v, err=%v; want true", claimed, err)
			}
			if err := store.CancelScheduleExactTerminal(ctx, generic); err != nil {
				t.Fatalf("terminalize generic schedule: %v", err)
			}
			active, err = store.LoadActiveSchedules(ctx)
			if err != nil || len(active) != 0 {
				t.Fatalf("generic load after exact cancellation = %#v, err=%v; want no active rows", active, err)
			}
			assertWorkflowTimerRowStatus(t, db, store, activationID, "active")
		})
	}
}

func TestGenericScheduleStoreRejectsReservedWorkflowTimerIdentityOnBothStores(t *testing.T) {
	reserved := timeridentity.WorkflowTimerActivationTaskPrefix() + "malformed"
	activation := timeridentity.WorkflowTimerActivationRef{
		ActivationID: uuid.NewString(),
		Declaration:  "waiting.timeout",
	}.Normalize()
	occurrence := timeridentity.WorkflowTimerOccurrenceRef{
		Activation: activation,
		DueAt:      time.Now().UTC().Truncate(time.Microsecond),
	}.Normalize()
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			for _, test := range []struct {
				name      string
				taskID    string
				eventType string
				wantError string
			}{
				{name: "reserved_task_id", taskID: reserved, eventType: "generic.tick", wantError: "reserved workflow timer prefix"},
				{name: "reserved_event_type_fallback", eventType: reserved, wantError: "reserved workflow timer prefix"},
				{name: "trimmed_activation_task_id", taskID: "  " + activation.TaskID() + "  ", eventType: "generic.tick", wantError: "reserved workflow timer prefix"},
				{name: "trimmed_occurrence_task_id", taskID: "\t" + occurrence.TaskID() + "\n", eventType: "generic.tick", wantError: "workflow timer occurrences must be persisted"},
			} {
				t.Run(test.name, func(t *testing.T) {
					err := store.UpsertSchedule(ctx, runtimepipeline.Schedule{
						AgentID: "generic", EventType: test.eventType, TaskID: test.taskID,
						Mode: "once", At: time.Now().UTC().Add(time.Hour),
					})
					if err == nil || !strings.Contains(err.Error(), test.wantError) {
						t.Fatalf("UpsertSchedule error = %v, want %q refusal", err, test.wantError)
					}
					var rows int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers`).Scan(&rows); err != nil {
						t.Fatalf("count timers after refused insert: %v", err)
					}
					if rows != 0 {
						t.Fatalf("persisted timers after refused insert = %d, want 0", rows)
					}
				})
			}
		})
	}
}

func TestGenericRecurringScheduleFiresRestoresAndCancelsOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			schedule := runtimepipeline.Schedule{
				RunID: runtimecorrelation.RunIDFromContext(ctx), AgentID: "generic-scheduler",
				EventType: "generic.tick", Mode: "cron", Cron: "@every 200ms",
				TaskID: "generic-recurring-proof", Payload: json.RawMessage(`{"business":true}`),
			}
			if err := store.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatalf("persist generic recurring schedule: %v", err)
			}

			firstResults := make(chan error, 8)
			firstScheduler := runtimepipeline.NewScheduler(func(fired runtimepipeline.Schedule) {
				firstResults <- store.CompleteScheduleFireExact(ctx, fired)
			})
			claimed, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, firstScheduler, schedule)
			if err != nil || !claimed {
				firstScheduler.Stop()
				t.Fatalf("claim/register first generic recurring schedule claimed=%v err=%v", claimed, err)
			}
			waitGenericScheduleResults(t, firstResults, 2)
			firstScheduler.Stop()
			waitCtx, cancelWait := context.WithTimeout(ctx, time.Second)
			if err := firstScheduler.Wait(waitCtx); err != nil {
				cancelWait()
				t.Fatalf("wait first generic scheduler: %v", err)
			}
			cancelWait()
			if err := store.ReleaseScheduleClaims(ctx); err != nil {
				t.Fatalf("release generic schedule claims for restart: %v", err)
			}

			restored, err := store.LoadActiveSchedules(ctx)
			if err != nil || len(restored) != 1 || restored[0].TaskID != schedule.TaskID || restored[0].EffectiveTimerID() != "" {
				t.Fatalf("restored generic recurring schedules = %#v, err=%v", restored, err)
			}
			secondResults := make(chan error, 8)
			releaseCallback := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
			t.Cleanup(release)
			secondScheduler := runtimepipeline.NewScheduler(func(fired runtimepipeline.Schedule) {
				secondResults <- store.CompleteScheduleFireExact(ctx, fired)
				<-releaseCallback
			})
			t.Cleanup(secondScheduler.Stop)
			claimed, err = runtimepipeline.ClaimAndRegisterSchedule(ctx, store, secondScheduler, restored[0])
			if err != nil || !claimed {
				t.Fatalf("claim/register restored generic recurring schedule claimed=%v err=%v", claimed, err)
			}
			waitGenericScheduleResults(t, secondResults, 1)
			if err := store.CancelScheduleExactTerminal(ctx, restored[0]); err != nil {
				t.Fatalf("cancel restored generic recurring schedule: %v", err)
			}
			if err := secondScheduler.CancelExact(restored[0]); err != nil {
				t.Fatalf("cancel restored generic scheduler task: %v", err)
			}
			release()
			select {
			case err := <-secondResults:
				t.Fatalf("generic recurring schedule fired after cancellation: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
			active, err := store.LoadActiveSchedules(ctx)
			if err != nil || len(active) != 0 {
				t.Fatalf("active generic schedules after cancellation = %#v, err=%v", active, err)
			}
		})
	}
}

func waitGenericScheduleResults(t *testing.T, results <-chan error, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("generic recurring fire %d: %v", i+1, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for generic recurring fire %d", i+1)
		}
	}
}

func assertWorkflowTimerRowStatus(t *testing.T, db *sql.DB, store runtimepipeline.SchedulePersistence, activationID, want string) {
	t.Helper()
	query := `SELECT status FROM timers WHERE timer_id = ?`
	if _, ok := store.(*PostgresStore); ok {
		query = `SELECT status FROM timers WHERE timer_id = $1::uuid`
	}
	var got string
	if err := db.QueryRowContext(context.Background(), query, activationID).Scan(&got); err != nil {
		t.Fatalf("load workflow timer status: %v", err)
	}
	if got != want {
		t.Fatalf("workflow timer status = %q, want %q", got, want)
	}
}
