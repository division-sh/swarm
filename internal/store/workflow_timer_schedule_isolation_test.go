package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
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
			ctx := selectedScheduleTestContext(t, runID)
			seedSQLiteScheduleRun(t, store, ctx, runID)
			return store, store.DB, ctx
		}},
		{name: "postgres", open: func(t *testing.T) (runtimepipeline.SchedulePersistence, *sql.DB, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			ctx := selectedScheduleTestContext(t, runID)
			store := admitTestPostgresStore(t, db)
			requireRunningRunForTest(t, ctx, store, runID, time.Now().UTC())
			t.Cleanup(func() { _ = store.ReleaseScheduleClaims(context.Background()) })
			return store, db, ctx
		}},
	}
}

func selectedScheduleTestContext(t *testing.T, runID string) context.Context {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		t.Fatalf("create selected schedule bundle source fact: %v", err)
	}
	return runtimecorrelation.WithBundleSourceFact(
		runtimecorrelation.WithRunID(context.Background(), runID),
		fact,
	)
}

func TestGenericScheduleAPIsCannotInterpretWorkflowTimerFamilyOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := uuid.NewString()
			activationID := uuid.NewString()
			ref := timeridentity.WorkflowTimerActivationRef{
				ActivationID:        activationID,
				Declaration:         "waiting.timeout",
				DeclarationRevision: "sha256:waiting-timeout",
				Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
			}.Normalize()
			fireAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
			genericTaskID := "workflowXtimer:v1:payload-key-collision"
			payload := json.RawMessage(`{"__schedule_task_id":"workflowXtimer:v1:payload-key-collision","business":true}`)
			routingSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
				FlowID: "timer-proof", FlowInstance: "timer-proof", EntityID: entityID,
			})
			if err != nil {
				t.Fatalf("construct workflow timer routing source: %v", err)
			}
			routingSourceJSON, err := json.Marshal(routingSource)
			if err != nil {
				t.Fatalf("marshal workflow timer routing source: %v", err)
			}

			switch store.(type) {
			case *SQLiteRuntimeStore:
				_, err := db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						routing_source, fire_at, recurring, owner_agent, owner_kind, task_type, status, created_at
					) VALUES (?, ?, ?, ?, 'timer-proof', 'timer.timeout', ?, ?, ?, false, 'runtime', 'system', 'workflow_timer', 'active', ?)
				`, activationID, runID, ref.TaskID(), entityID, string(payload), string(routingSourceJSON), fireAt, fireAt.Add(-time.Hour))
				if err != nil {
					t.Fatalf("insert SQLite workflow activation: %v", err)
				}
			case *PostgresStore:
				_, err := db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
						routing_source, fire_at, recurring, owner_agent, owner_kind, task_type, status, created_at
					) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'timer-proof', 'timer.timeout', $5::jsonb,
					          $6::jsonb, $7, false, 'runtime', 'system', 'workflow_timer', 'active', $8)
				`, activationID, runID, ref.TaskID(), entityID, string(payload), string(routingSourceJSON), fireAt, fireAt.Add(-time.Hour))
				if err != nil {
					t.Fatalf("insert PostgreSQL workflow activation: %v", err)
				}
			default:
				t.Fatalf("unsupported schedule store %T", store)
			}

			generic := runtimepipeline.Schedule{
				RunID: runID, AgentID: "runtime", OwnerKind: runtimepipeline.ScheduleOwnerSystem, EventType: "timer.timeout", Mode: "once", At: fireAt,
				EntityID: entityID, FlowInstance: "timer-proof", TaskID: genericTaskID, Payload: json.RawMessage(`{"business":true}`),
			}
			generic = testAgentOwnedSchedule(t, generic)
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

func TestGenericScheduleIdentityDoesNotInferWorkflowTimerFamilyOnBothStores(t *testing.T) {
	reserved := timeridentity.WorkflowTimerActivationTaskPrefix() + "malformed"
	activation := timeridentity.WorkflowTimerActivationRef{
		ActivationID:        uuid.NewString(),
		Declaration:         "waiting.timeout",
		DeclarationRevision: "sha256:waiting-timeout",
		Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
	}.Normalize()
	occurrence := timeridentity.WorkflowTimerOccurrenceRef{
		Activation: activation,
		DueAt:      time.Now().UTC().Truncate(time.Microsecond),
	}.Normalize()
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			tests := []struct {
				name      string
				taskID    string
				eventType string
			}{
				{name: "former_reserved_task_id", taskID: reserved, eventType: "generic.tick"},
				{name: "former_reserved_event_type", taskID: "generic-event-prefix", eventType: reserved},
				{name: "activation_shaped_task_id", taskID: activation.TaskID(), eventType: "generic.tick"},
				{name: "occurrence_shaped_task_id", taskID: occurrence.TaskID(), eventType: "generic.tick"},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					schedule := testAgentOwnedSchedule(t, runtimepipeline.Schedule{
						AgentID: "generic-" + test.name, EventType: test.eventType, TaskID: test.taskID,
						Mode: "once", At: time.Now().UTC().Add(time.Hour),
					})
					if err := store.UpsertSchedule(ctx, schedule); err != nil {
						t.Fatalf("UpsertSchedule inferred workflow ownership from an opaque identity: %v", err)
					}
				})
			}
			var workflowRows int
			switch store.(type) {
			case *SQLiteRuntimeStore:
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE task_type = 'workflow_timer'`).Scan(&workflowRows); err != nil {
					t.Fatalf("count SQLite workflow timer rows: %v", err)
				}
			case *PostgresStore:
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE task_type = 'workflow_timer'`).Scan(&workflowRows); err != nil {
					t.Fatalf("count PostgreSQL workflow timer rows: %v", err)
				}
			default:
				t.Fatalf("unsupported schedule store %T", store)
			}
			if workflowRows != 0 {
				t.Fatalf("generic identity-shaped schedules persisted as workflow_timer rows = %d, want 0", workflowRows)
			}
			active, err := store.LoadActiveSchedules(ctx)
			if err != nil {
				t.Fatalf("load generic schedules: %v", err)
			}
			if len(active) != len(tests) {
				t.Fatalf("generic schedules loaded = %d, want %d", len(active), len(tests))
			}
		})
	}
}

func TestWorkflowTimerFreshSchemaOneInvalidFactMatrixOnBothStores(t *testing.T) {
	type invalidFact struct {
		name   string
		mutate func(*workflowTimerDDLProofRow)
	}
	invalidFacts := []invalidFact{
		{name: "missing run", mutate: func(row *workflowTimerDDLProofRow) { row.runID = nil }},
		{name: "missing entity", mutate: func(row *workflowTimerDDLProofRow) { row.entityID = nil }},
		{name: "blank flow instance", mutate: func(row *workflowTimerDDLProofRow) { row.flowInstance = " " }},
		{name: "blank activation identity", mutate: func(row *workflowTimerDDLProofRow) { row.timerName = " " }},
		{name: "blank fire event", mutate: func(row *workflowTimerDDLProofRow) { row.fireEvent = " " }},
		{name: "node owner", mutate: func(row *workflowTimerDDLProofRow) { row.ownerNode = "workflow-node" }},
		{name: "blank agent owner", mutate: func(row *workflowTimerDDLProofRow) { row.ownerAgent = " " }},
		{name: "cron recurrence", mutate: func(row *workflowTimerDDLProofRow) { row.recurrenceCron = "@daily" }},
		{name: "non-object payload", mutate: func(row *workflowTimerDDLProofRow) { row.payload = "[]" }},
		{name: "due before creation", mutate: func(row *workflowTimerDDLProofRow) { row.fireAt = row.createdAt.Add(-time.Second) }},
		{name: "partial fork lineage", mutate: func(row *workflowTimerDDLProofRow) { row.reconstructionOwner = "selected-fork" }},
		{name: "one-shot interval", mutate: func(row *workflowTimerDDLProofRow) { row.recurrenceInterval = "1h" }},
		{name: "recurring without interval", mutate: func(row *workflowTimerDDLProofRow) { row.recurring = true }},
		{name: "active one-shot with fired time", mutate: func(row *workflowTimerDDLProofRow) { row.firedAt = row.fireAt }},
		{name: "fired one-shot without fired time", mutate: func(row *workflowTimerDDLProofRow) { row.status = "fired" }},
		{name: "fired one-shot before due", mutate: func(row *workflowTimerDDLProofRow) {
			row.status = "fired"
			row.firedAt = row.createdAt
		}},
		{name: "fired recurring row", mutate: func(row *workflowTimerDDLProofRow) {
			row.recurring = true
			row.recurrenceInterval = "1h"
			row.status = "fired"
			row.firedAt = row.fireAt
		}},
		{name: "expired workflow row", mutate: func(row *workflowTimerDDLProofRow) { row.status = "expired" }},
	}

	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			valid := newWorkflowTimerDDLProofRow(runID)
			if err := insertWorkflowTimerDDLProofRow(ctx, db, store, valid); err != nil {
				t.Fatalf("insert canonical workflow timer row: %v", err)
			}
			for _, test := range invalidFacts {
				test := test
				t.Run(test.name, func(t *testing.T) {
					row := newWorkflowTimerDDLProofRow(runID)
					test.mutate(&row)
					if err := insertWorkflowTimerDDLProofRow(ctx, db, store, row); err == nil {
						t.Fatal("fresh schema accepted an invalid workflow timer fact")
					}
					var rows int
					query := `SELECT COUNT(*) FROM timers WHERE timer_id = ?`
					if _, ok := store.(*PostgresStore); ok {
						query = `SELECT COUNT(*) FROM timers WHERE timer_id = $1::uuid`
					}
					if err := db.QueryRowContext(ctx, query, row.timerID).Scan(&rows); err != nil {
						t.Fatalf("count rejected workflow timer row: %v", err)
					}
					if rows != 0 {
						t.Fatalf("invalid workflow timer rows = %d, want 0", rows)
					}
				})
			}
		})
	}
}

type workflowTimerDDLProofRow struct {
	timerID             string
	runID               any
	timerName           string
	entityID            any
	flowInstance        string
	fireEvent           string
	payload             string
	routingSource       string
	fireAt              time.Time
	recurring           bool
	recurrenceCron      any
	recurrenceInterval  any
	ownerNode           any
	ownerAgent          string
	status              string
	firedAt             any
	createdAt           time.Time
	sourceTimerID       any
	forkedFromRunID     any
	forkedFromEventID   any
	reconstructionOwner any
}

func newWorkflowTimerDDLProofRow(runID string) workflowTimerDDLProofRow {
	timerID := uuid.NewString()
	entityID := uuid.NewString()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID:        timerID,
		Declaration:         "waiting.timeout",
		DeclarationRevision: "sha256:waiting-timeout",
		Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
	}
	routingSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
		FlowID: "root", FlowInstance: "root", EntityID: entityID,
	})
	if err != nil {
		panic(err)
	}
	routingSourceJSON, err := json.Marshal(routingSource)
	if err != nil {
		panic(err)
	}
	return workflowTimerDDLProofRow{
		timerID:       timerID,
		runID:         runID,
		timerName:     ref.TaskID(),
		entityID:      entityID,
		flowInstance:  "root",
		fireEvent:     "timer.timeout",
		payload:       `{"business":true}`,
		routingSource: string(routingSourceJSON),
		fireAt:        createdAt.Add(time.Hour),
		ownerAgent:    "workflow-runtime",
		status:        "active",
		createdAt:     createdAt,
	}
}

func insertWorkflowTimerDDLProofRow(
	ctx context.Context,
	db *sql.DB,
	store runtimepipeline.SchedulePersistence,
	row workflowTimerDDLProofRow,
) error {
	args := []any{
		row.timerID, row.runID, row.timerName, row.entityID, row.flowInstance, row.fireEvent,
		row.payload, row.routingSource, row.fireAt, row.recurring, row.recurrenceCron, row.recurrenceInterval,
		row.ownerNode, row.ownerAgent, row.status, row.firedAt, row.createdAt, row.sourceTimerID,
		row.forkedFromRunID, row.forkedFromEventID, row.reconstructionOwner,
	}
	if _, ok := store.(*PostgresStore); ok {
		_, err := db.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				routing_source, fire_at, recurring, recurrence_cron, recurrence_interval, owner_node, owner_agent,
				owner_kind, task_type, status, fired_at, created_at, source_timer_id, forked_from_run_id,
				forked_from_event_id, reconstruction_owner
			) VALUES (
				$1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7::jsonb,
				$8::jsonb, $9, $10, $11, $12, $13, $14, 'system', 'workflow_timer', $15, $16, $17,
				$18::uuid, $19::uuid, $20::uuid, $21
			)
		`, args...)
		return err
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			routing_source, fire_at, recurring, recurrence_cron, recurrence_interval, owner_node, owner_agent,
			owner_kind, task_type, status, fired_at, created_at, source_timer_id, forked_from_run_id,
			forked_from_event_id, reconstruction_owner
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'system', 'workflow_timer', ?, ?, ?, ?, ?, ?, ?
		)
	`, args...)
	return err
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
			schedule = testAgentOwnedSchedule(t, schedule)
			if err := store.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatalf("persist generic recurring schedule: %v", err)
			}

			firstResults := make(chan error, 8)
			firstScheduler := runtimepipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t), func(_ context.Context, fired runtimepipeline.Schedule) {
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
			if err != nil || len(restored) != 1 || restored[0].TaskID != schedule.TaskID {
				t.Fatalf("restored generic recurring schedules = %#v, err=%v", restored, err)
			}
			secondResults := make(chan error, 8)
			releaseCallback := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
			t.Cleanup(release)
			secondScheduler := runtimepipeline.NewSchedulerWithWorkOwner(storeTestWorkOwner(t), func(_ context.Context, fired runtimepipeline.Schedule) {
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
