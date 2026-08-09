package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// insertGenericSchedulePersistenceFixture creates a hostile sibling-family row
// for pipeline isolation tests from the closed semantic command.
func insertGenericSchedulePersistenceFixture(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	postgres bool,
	command runtimegenericschedule.AdmissionCommand,
) runtimegenericschedule.Activation {
	t.Helper()
	activation, err := pipelineTestGenericScheduleActivation(command)
	if err != nil {
		t.Fatalf("construct generic schedule persistence fixture: %v", err)
	}
	scope, err := command.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Encode(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	due := command.Due.Canonical()
	var dueAbsolute any
	var dueDuration, dueCron string
	switch due.Kind {
	case runtimegenericschedule.DueAbsolute:
		dueAbsolute = due.Absolute
	case runtimegenericschedule.DueDelay:
		dueDuration = due.Delay.String()
	case runtimegenericschedule.DueCron:
		dueCron = due.Cron
	case runtimegenericschedule.DueEvery:
		dueDuration = due.Every.String()
	}
	taskType := "timer"
	if due.Recurring() {
		taskType = "scheduled_task"
		if command.RunID == "" {
			taskType = "global_recurring"
		}
	}
	query := `INSERT INTO timers (
		timer_id, timer_name, schedule_scope, schedule_key, immutable_hash, run_id, entity_id, flow_instance,
		fire_event, fire_payload, routing_source, fire_at, initial_fire_at, recurring,
		owner_agent, owner_kind, reply_context_id, task_id, due_basis_kind, due_basis_absolute,
		due_basis_duration, due_basis_cron, task_type, status, created_at
	) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?,
		NULLIF(?, ''), NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, 'active', ?)`
	args := []any{
		activation.ID, command.ScheduleKey, scope, command.ScheduleKey, activation.ImmutableHash,
		command.RunID, command.EntityID, command.FlowInstance, command.EventType, string(payload), string(routing),
		activation.CurrentDueAt, activation.InitialDueAt, command.Due.Recurring(), command.OwnerID, command.OwnerKind,
		command.ReplyContext, command.TaskID, command.Due.Kind, dueAbsolute, dueDuration, dueCron, taskType, activation.AdmittedAt,
	}
	if postgres {
		query = `INSERT INTO timers (
			timer_id, timer_name, schedule_scope, schedule_key, immutable_hash, run_id, entity_id, flow_instance,
			fire_event, fire_payload, routing_source, fire_at, initial_fire_at, recurring,
			owner_agent, owner_kind, reply_context_id, task_id, due_basis_kind, due_basis_absolute,
			due_basis_duration, due_basis_cron, task_type, status, created_at
		) VALUES ($1::uuid, $2, $3, $4, $5, NULLIF($6, '')::uuid, NULLIF($7, '')::uuid, NULLIF($8, ''),
			$9, $10::jsonb, $11::jsonb, $12, $13, $14, $15, $16, NULLIF($17, ''), NULLIF($18, ''),
			$19, $20, NULLIF($21, ''), NULLIF($22, ''), $23, 'active', $24)`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert generic schedule persistence fixture: %v", err)
	}
	return activation
}
