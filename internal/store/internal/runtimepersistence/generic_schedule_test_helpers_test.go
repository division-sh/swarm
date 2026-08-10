package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedScheduleStoreCase struct {
	name string
	open func(*testing.T) (runtimegenericschedule.Store, *sql.DB, context.Context)
}

func selectedScheduleStoreCases() []selectedScheduleStoreCase {
	return []selectedScheduleStoreCase{
		{name: "sqlite", open: func(t *testing.T) (runtimegenericschedule.Store, *sql.DB, context.Context) {
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			runID := uuid.NewString()
			ctx := selectedScheduleTestContext(t, runID)
			seedSQLiteScheduleRun(t, store, ctx, runID)
			return store, store.backend.ConstructionHandle(), ctx
		}},
		{name: "postgres", open: func(t *testing.T) (runtimegenericschedule.Store, *sql.DB, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			ctx := selectedScheduleTestContext(t, runID)
			store := admitTestPostgresStore(t, db)
			requireRunningRunForTest(t, ctx, store, runID, time.Now().UTC())
			t.Cleanup(func() { _ = store.ReleaseGenericScheduleClaims(context.Background()) })
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
	return runtimecorrelation.WithBundleSourceFact(runtimecorrelation.WithRunID(context.Background(), runID), fact)
}

func testAgentGenericScheduleCommand(t testing.TB, runID, agentID, flowInstance, entityID, key string, due runtimegenericschedule.DueBasis) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	identity := testAgentIdentity(t, agentID, flowInstance)
	flowID, _, path, present := identity.Route.Fields()
	if !present {
		t.Fatal("generic schedule fixture requires a flow-owned agent identity")
	}
	routing, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID})
	if err != nil {
		t.Fatalf("construct generic schedule routing source: %v", err)
	}
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey: key, RunID: runID, EntityID: entityID, FlowInstance: path,
		OwnerKind: runtimegenericschedule.OwnerAgent, OwnerID: identity.AgentID(), AgentIdentity: identity,
		EventType: flowID + ".timer_fired", Payload: semanticvalue.EmptyObject(), RoutingSource: routing,
		ExecutionMode: executionmode.Live,
		Due:           due, TaskID: key,
	}
}

func testRootGenericScheduleCommand(t testing.TB, runID, entityID, key string, due runtimegenericschedule.DueBasis) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	routing, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatalf("construct root generic schedule routing source: %v", err)
	}
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey: key, RunID: runID, EntityID: entityID,
		OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "runtime-test",
		EventType: "test.timer_fired", Payload: semanticvalue.EmptyObject(), RoutingSource: routing,
		ExecutionMode: executionmode.Live,
		Due:           due, TaskID: key,
	}
}

func testGlobalGenericScheduleCommand(key string, due runtimegenericschedule.DueBasis) runtimegenericschedule.AdmissionCommand {
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey: key, OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "runtime-test",
		EventType: "platform.test_timer_fired", Payload: semanticvalue.EmptyObject(), RoutingSource: events.NewPlatformControlRoutingSource(),
		ExecutionMode: executionmode.Live,
		Due:           due, TaskID: key,
	}
}

func admitGenericScheduleFixture(t testing.TB, ctx context.Context, store runtimegenericschedule.Store, command runtimegenericschedule.AdmissionCommand) runtimegenericschedule.Activation {
	t.Helper()
	result, err := store.AdmitGenericSchedule(ctx, command)
	if err != nil {
		t.Fatalf("admit generic schedule fixture: %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("validate admitted generic schedule fixture: %v", err)
	}
	return result.Activation
}

func cancelGenericScheduleFixture(t testing.TB, ctx context.Context, store runtimegenericschedule.Store, activation runtimegenericschedule.Activation, cause string, at time.Time) runtimegenericschedule.Activation {
	t.Helper()
	result, err := store.CancelGenericSchedule(ctx, runtimegenericschedule.CancelCommand{
		ActivationID: activation.ID,
		Cause:        cause,
		CancelledAt:  at,
	})
	if err != nil {
		t.Fatalf("cancel generic schedule fixture: %v", err)
	}
	if result.Outcome != runtimegenericschedule.CancelChanged {
		t.Fatalf("cancel generic schedule fixture outcome = %q, want cancelled", result.Outcome)
	}
	if err := result.Activation.Validate(); err != nil {
		t.Fatalf("validate cancelled generic schedule activation: %v", err)
	}
	return result.Activation
}

type workflowTimerDDLProofRow struct {
	timerID, runID, timerName, entityID, flowScopeKey, flowInstanceID, flowInstance string
	fireEvent, payload, routingSource, recurrenceInterval                           string
	executionMode                                                                   executionmode.Mode
	ownerNode, ownerAgent, status                                                   string
	fireAt, firedAt, createdAt                                                      time.Time
	recurring                                                                       bool
	sourceTimerID, forkedFromRunID, forkedFromEventID, reconstructionOwner          any
}

func newWorkflowTimerDDLProofRow(runID string) workflowTimerDDLProofRow {
	timerID := uuid.NewString()
	entityID := uuid.NewString()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID: timerID, Declaration: "waiting.timeout", DeclarationRevision: "sha256:waiting-timeout",
		Cause: timeridentity.WorkflowTimerActivationCauseInitial,
	}
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		panic(err)
	}
	routingSourceJSON, err := json.Marshal(routingSource)
	if err != nil {
		panic(err)
	}
	return workflowTimerDDLProofRow{
		timerID: timerID, runID: runID, timerName: ref.TaskID(), entityID: entityID,
		flowScopeKey: "root", flowInstanceID: "root", flowInstance: "root", fireEvent: "timer.timeout",
		payload: `{"business":true}`, routingSource: string(routingSourceJSON), fireAt: createdAt.Add(time.Hour),
		executionMode: executionmode.Live, ownerAgent: "workflow-runtime", status: "active", createdAt: createdAt,
	}
}

func insertWorkflowTimerDDLProofRow(ctx context.Context, db *sql.DB, store any, row workflowTimerDDLProofRow) error {
	args := []any{
		row.timerID, row.runID, row.timerName, row.entityID, row.flowScopeKey, row.flowInstanceID, row.flowInstance, row.fireEvent,
		row.payload, row.routingSource, row.executionMode, row.fireAt, row.recurring, nullableScheduleFixtureText(row.recurrenceInterval),
		nullableScheduleFixtureText(row.ownerNode), row.ownerAgent, row.status, nullableScheduleFixtureTime(row.firedAt), row.createdAt, row.sourceTimerID,
		row.forkedFromRunID, row.forkedFromEventID, row.reconstructionOwner,
	}
	if _, ok := store.(*PostgresStore); ok {
		_, err := db.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event, fire_payload,
				routing_source, execution_mode, fire_at, recurring, recurrence_interval, owner_node, owner_agent,
				owner_kind, task_type, status, fired_at, created_at, source_timer_id, forked_from_run_id,
				forked_from_event_id, reconstruction_owner
			) VALUES (
				$1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9::jsonb,
				$10::jsonb, $11, $12, $13, $14, $15, $16, 'system', 'workflow_timer', $17, $18, $19,
				$20::uuid, $21::uuid, $22::uuid, $23
			)
		`, args...)
		return err
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event, fire_payload,
			routing_source, execution_mode, fire_at, recurring, recurrence_interval, owner_node, owner_agent,
			owner_kind, task_type, status, fired_at, created_at, source_timer_id, forked_from_run_id,
			forked_from_event_id, reconstruction_owner
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'system', 'workflow_timer', ?, ?, ?, ?, ?, ?, ?
		)
	`, args...)
	return err
}

func nullableScheduleFixtureText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableScheduleFixtureTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
