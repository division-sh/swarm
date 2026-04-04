package store

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresStore_BindSchemaCapabilities_CanonicalOptionalVariants(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name", "column_name"})
	addColumns := func(table string, columns ...string) {
		for _, column := range columns {
			rows.AddRow(table, column)
		}
	}

	addColumns("runs", "run_id", "status")
	addColumns("agents",
		"agent_id", "flow_instance", "role", "model_tier", "llm_backend", "conversation_mode",
		"parent_agent_id", "entity_id", "config", "subscriptions", "emit_events", "tools",
		"permissions", "status", "turn_count", "last_active_at", "created_at",
	)
	addColumns("events",
		"event_id", "run_id", "event_name", "entity_id", "flow_instance", "scope", "payload",
		"chain_depth", "produced_by", "produced_by_type", "source_event_id", "idempotency_key", "created_at",
	)
	addColumns("event_deliveries",
		"run_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count",
		"reason_code", "last_error", "active_session_id", "started_at", "delivered_at", "created_at",
	)
	addColumns("event_receipts",
		"event_id", "subscriber_type", "subscriber_id", "entity_id", "flow_instance",
		"outcome", "reason_code", "side_effects", "processed_at",
	)
	addColumns("agent_sessions",
		"session_id", "run_id", "agent_id", "entity_id", "flow_instance", "scope_key", "scope",
		"conversation", "turn_count", "runtime_mode", "runtime_state", "lease_holder",
		"lease_expires_at", "status", "created_at", "updated_at",
	)
	addColumns("agent_conversation_audits",
		"session_id", "run_id", "agent_id", "entity_id", "flow_instance", "scope_key", "scope",
		"conversation", "turn_count", "runtime_mode", "runtime_state", "status", "created_at", "updated_at",
	)
	addColumns("agent_turns",
		"turn_id", "run_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id",
		"trigger_event_id", "trigger_event_type", "task_id", "available_tools", "tool_calls",
		"emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
		"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms",
		"retry_count", "error", "created_at",
	)
	addColumns("mailbox",
		"item_id", "entity_id", "flow_instance", "scope", "item_type", "source_event_id",
		"from_agent", "severity", "summary", "payload", "status", "decision", "decision_notes",
		"decided_by", "decided_at", "notified", "expires_at", "created_at",
	)
	addColumns("timers",
		"timer_id", "timer_name", "entity_id", "flow_instance", "fire_event", "fire_payload",
		"fire_at", "recurring", "recurrence_cron", "recurrence_interval", "owner_node",
		"owner_agent", "task_type", "status", "fired_at", "created_at",
	)

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)

	pg := &PostgresStore{DB: db}
	caps, err := pg.BindSchemaCapabilities(context.Background())
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Agents != SchemaFlavorCanonical {
		t.Fatalf("agents flavor = %s", caps.Agents)
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID || !caps.Events.LogIdempotencyKey {
		t.Fatalf("events caps = %+v", caps.Events)
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID {
		t.Fatalf("delivery caps = %+v", caps.Events)
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical || !caps.Conversations.SessionRunID {
		t.Fatalf("session caps = %+v", caps.Conversations)
	}
	if caps.Conversations.Audits != SchemaFlavorCanonical || !caps.Conversations.AuditRunID {
		t.Fatalf("audit caps = %+v", caps.Conversations)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical || !caps.Conversations.TurnRunID || !caps.Conversations.TurnBlocks {
		t.Fatalf("turn caps = %+v", caps.Conversations)
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		t.Fatalf("mailbox flavor = %s", caps.Mailbox)
	}
	if caps.Schedules != SchemaFlavorCanonical {
		t.Fatalf("schedules flavor = %s", caps.Schedules)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestPostgresStore_BindSchemaCapabilities_DetectsLegacyShapes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name", "column_name"})
	addColumns := func(table string, columns ...string) {
		for _, column := range columns {
			rows.AddRow(table, column)
		}
	}

	addColumns("agents",
		"id", "type", "role", "mode", "entity_id", "parent_agent_id", "status",
		"coordinator_id", "config", "budget_envelope", "hired_by", "template_version",
		"started_at", "last_active_at",
	)
	addColumns("events", "id", "type", "source_agent", "task_id", "entity_id", "payload", "created_at")
	addColumns("event_deliveries", "event_id", "agent_id", "status", "created_at")
	addColumns("event_receipts", "event_id", "agent_id", "processed_at", "status", "retry_count", "error")
	addColumns("mailbox",
		"id", "event_id", "entity_id", "from_agent", "type", "priority", "status",
		"context", "summary", "timeout_at", "decision", "decision_notes", "notified", "created_at",
	)
	addColumns("schedules",
		"agent_id", "entity_id", "event_type", "mode", "cron_expr", "at_time",
		"next_fire_at", "payload", "active", "cancelled_at", "last_fired_at", "created_at",
	)

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)

	pg := &PostgresStore{DB: db}
	caps, err := pg.BindSchemaCapabilities(context.Background())
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Agents != SchemaFlavorLegacy {
		t.Fatalf("agents flavor = %s", caps.Agents)
	}
	if caps.Events.Log != SchemaFlavorLegacy || caps.Events.Deliveries != SchemaFlavorLegacy || caps.Events.Receipts != SchemaFlavorLegacy {
		t.Fatalf("events caps = %+v", caps.Events)
	}
	if caps.Mailbox != SchemaFlavorLegacy {
		t.Fatalf("mailbox flavor = %s", caps.Mailbox)
	}
	if caps.Schedules != SchemaFlavorLegacy {
		t.Fatalf("schedules flavor = %s", caps.Schedules)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
