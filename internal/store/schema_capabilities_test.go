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

	addColumns("runs", "run_id", "status", "bundle_hash", "bundle_source", "bundle_fingerprint")
	addColumns("agents",
		"agent_id", "flow_instance", "role", "model", "llm_backend", "memory_enabled", "memory_source",
		"parent_agent_id", "entity_id", "config", "subscriptions", "emit_events", "tools",
		"permissions", "runtime_descriptor", "lifecycle_phase", "lifecycle_generation",
		"lifecycle_runtime_epoch", "lifecycle_config_revision", "lifecycle_run_mode",
		"lifecycle_last_transition_id", "status", "turn_count", "last_active_at", "created_at",
	)
	addColumns("events",
		"event_id", "run_id", "event_name", "entity_id", "flow_instance", "scope", "payload",
		"execution_mode", "chain_depth", "produced_by", "produced_by_type", "source_event_id", "idempotency_key", "created_at",
	)
	addColumns("event_deliveries",
		"run_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count",
		"reason_code", "failure", "active_session_id", "started_at", "delivered_at", "created_at",
		"delivery_target_route", "delivery_context",
		"delivery_payload_projection",
	)
	addColumns("event_receipts",
		"receipt_id", "event_id", "subscriber_type", "subscriber_id", "entity_id", "flow_instance",
		"outcome", "reason_code", "state_before", "state_after", "side_effects", "failure",
		"duration_ms", "idempotency_key", "processed_at",
	)
	addColumns("activity_attempts",
		"request_event_id", "run_id", "execution_mode", "source_event_id", "parent_event_id", "entity_id", "flow_instance",
		"node_id", "handler_event_key", "activity_id", "tool", "effect_class", "attempt", "status",
		"success_event", "failure_event", "result_event_id", "result_event_type", "result_payload",
		"failure", "input_hash", "loop_generation", "loop_stage", "started_at", "completed_at", "updated_at",
	)
	addColumns("entity_state",
		"run_id", "entity_id", "flow_instance", "entity_type", "slug", "name",
		"current_state", "gates", "fields", "accumulator", "revision",
		"entered_state_at", "created_at", "updated_at",
	)
	addColumns("agent_sessions",
		"session_id", "run_id", "agent_id", "flow_instance", "memory_enabled", "memory_source",
		"conversation", "turn_count", "runtime_state", "lease_holder",
		"lease_expires_at", "status", "termination_reason", "termination_detail",
		"successor_session_id", "terminated_at", "created_at", "updated_at",
	)
	addColumns("agent_conversation_audits",
		"session_id", "run_id", "agent_id", "entity_id", "flow_instance", "memory_enabled", "memory_source",
		"conversation", "turn_count", "runtime_state", "status", "created_at", "updated_at",
	)
	addColumns("managed_agent_capability_surfaces",
		"surface_id", "integrity_hash", "authority_kind", "authority_id", "execution_kind",
		"execution_authority_id", "run_id", "actor_id", "provider", "transport", "surface", "created_at",
	)
	addColumns("agent_turns",
		"turn_id", "run_id", "agent_id", "session_id", "flow_instance", "memory_enabled", "memory_source", "entity_id",
		"trigger_event_id", "trigger_event_type", "task_id", "capability_surface_id", "tool_calls",
		"emitted_events",
		"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms",
		"retry_count", "execution_mode", "failure", "created_at",
	)
	addColumns("mailbox",
		"item_id", "entity_id", "flow_instance", "scope", "item_type", "source_event_id",
		"from_agent", "severity", "summary", "payload", "status", "decision", "decision_notes",
		"decided_by", "decided_at", "notified", "expires_at", "deferred_until", "created_at",
	)
	addColumns("timers",
		"timer_id", "run_id", "source_timer_id", "forked_from_run_id", "forked_from_event_id",
		"reconstruction_owner", "timer_name", "entity_id", "flow_instance", "fire_event", "fire_payload",
		"fire_at", "recurring", "recurrence_cron", "recurrence_interval", "owner_node",
		"owner_agent", "task_type", "status", "fired_at", "created_at",
	)

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)
	expectEventReceiptsTypedIdentityKey(mock, true)

	pg := &PostgresStore{DB: db}
	caps, err := pg.BindSchemaCapabilities(context.Background())
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Agents != SchemaFlavorCanonical {
		t.Fatalf("agents flavor = %s", caps.Agents)
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID || !caps.Events.LogIdempotencyKey || !caps.Events.RunBundleHash || !caps.Events.RunBundleSource || !caps.Events.RunBundleFingerprint {
		t.Fatalf("events caps = %+v", caps.Events)
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical ||
		!caps.Events.DeliveryRunID ||
		!caps.Events.DeliveryTargetRoute ||
		!caps.Events.DeliveryContext ||
		!caps.Events.DeliveryPayloadProjection {
		t.Fatalf("delivery caps = %+v", caps.Events)
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || !caps.Events.ReceiptTypedIdentity {
		t.Fatalf("receipt caps = %+v", caps.Events)
	}
	if caps.EntityState != SchemaFlavorCanonical || !caps.EntityRunID {
		t.Fatalf("entity_state caps = %+v run_id=%v", caps.EntityState, caps.EntityRunID)
	}
	if caps.Activity.Attempts != SchemaFlavorCanonical {
		t.Fatalf("activity attempts flavor = %s", caps.Activity.Attempts)
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
	if caps.Agents != SchemaFlavorUnsupported {
		t.Fatalf("agents flavor = %s, want legacy agent schema rejected", caps.Agents)
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

func TestPostgresStore_BindSchemaCapabilities_FailsClosedOnPartialCanonicalShapes(t *testing.T) {
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

	addColumns("events",
		"event_id", "event_name", "entity_id", "flow_instance", "scope", "payload",
		"chain_depth", "produced_by", "produced_by_type", "created_at",
	)
	addColumns("agent_sessions",
		"session_id", "agent_id", "scope_key", "scope", "conversation",
		"turn_count", "runtime_mode", "status", "created_at", "updated_at",
	)
	addColumns("agent_turns",
		"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key",
		"request_payload", "response_payload", "parse_ok", "latency_ms", "created_at",
	)
	addColumns("timers", "timer_id")

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)

	pg := &PostgresStore{DB: db}
	caps, err := pg.BindSchemaCapabilities(context.Background())
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Events.Log != SchemaFlavorUnsupported {
		t.Fatalf("events log flavor = %s, want %s", caps.Events.Log, SchemaFlavorUnsupported)
	}
	if caps.Conversations.Sessions != SchemaFlavorUnsupported {
		t.Fatalf("sessions flavor = %s, want %s", caps.Conversations.Sessions, SchemaFlavorUnsupported)
	}
	if caps.Conversations.Turns != SchemaFlavorUnsupported {
		t.Fatalf("turns flavor = %s, want %s", caps.Conversations.Turns, SchemaFlavorUnsupported)
	}
	if caps.Schedules != SchemaFlavorUnsupported {
		t.Fatalf("schedules flavor = %s, want %s", caps.Schedules, SchemaFlavorUnsupported)
	}
	if caps.Conversations.Audits != SchemaFlavorUnavailable {
		t.Fatalf("audits flavor = %s, want %s", caps.Conversations.Audits, SchemaFlavorUnavailable)
	}
	if caps.Mailbox != SchemaFlavorUnavailable {
		t.Fatalf("mailbox flavor = %s, want %s", caps.Mailbox, SchemaFlavorUnavailable)
	}
	if caps.Events.LogIdempotencyKey {
		t.Fatalf("expected partial canonical events table to report missing idempotency_key: %+v", caps.Events)
	}
	if caps.Conversations.TurnBlocks {
		t.Fatalf("expected partial canonical turns table to report missing turn_blocks: %+v", caps.Conversations)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestPostgresStore_CanonicalCapabilityReaders(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name", "column_name"}).
		AddRow("events", "event_id").
		AddRow("events", "run_id").
		AddRow("events", "event_name").
		AddRow("events", "entity_id").
		AddRow("events", "flow_instance").
		AddRow("events", "scope").
		AddRow("events", "payload").
		AddRow("events", "execution_mode").
		AddRow("events", "chain_depth").
		AddRow("events", "produced_by").
		AddRow("events", "produced_by_type").
		AddRow("events", "source_event_id").
		AddRow("events", "created_at").
		AddRow("event_receipts", "receipt_id").
		AddRow("event_receipts", "event_id").
		AddRow("event_receipts", "subscriber_type").
		AddRow("event_receipts", "subscriber_id").
		AddRow("event_receipts", "entity_id").
		AddRow("event_receipts", "flow_instance").
		AddRow("event_receipts", "outcome").
		AddRow("event_receipts", "reason_code").
		AddRow("event_receipts", "state_before").
		AddRow("event_receipts", "state_after").
		AddRow("event_receipts", "side_effects").
		AddRow("event_receipts", "failure").
		AddRow("event_receipts", "duration_ms").
		AddRow("event_receipts", "idempotency_key").
		AddRow("event_receipts", "processed_at")
	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)
	expectEventReceiptsTypedIdentityKey(mock, true)

	pg := &PostgresStore{DB: db}
	logEnabled, logRunID, err := pg.CanonicalRuntimeLogCapability(context.Background())
	if err != nil {
		t.Fatalf("CanonicalRuntimeLogCapability: %v", err)
	}
	if !logEnabled || !logRunID {
		t.Fatalf("runtime log capability = enabled:%v run_id:%v, want true/true", logEnabled, logRunID)
	}
	receiptsEnabled, err := pg.CanonicalEventReceiptsCapability(context.Background())
	if err != nil {
		t.Fatalf("CanonicalEventReceiptsCapability: %v", err)
	}
	if !receiptsEnabled {
		t.Fatal("CanonicalEventReceiptsCapability = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestPostgresStore_CanonicalEventReceiptsCapability_FailsClosedWithoutCanonicalEventsLog(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name", "column_name"}).
		AddRow("events", "id").
		AddRow("events", "type").
		AddRow("events", "source_agent").
		AddRow("events", "task_id").
		AddRow("events", "entity_id").
		AddRow("events", "payload").
		AddRow("events", "created_at").
		AddRow("event_receipts", "receipt_id").
		AddRow("event_receipts", "event_id").
		AddRow("event_receipts", "subscriber_type").
		AddRow("event_receipts", "subscriber_id").
		AddRow("event_receipts", "entity_id").
		AddRow("event_receipts", "flow_instance").
		AddRow("event_receipts", "outcome").
		AddRow("event_receipts", "reason_code").
		AddRow("event_receipts", "state_before").
		AddRow("event_receipts", "state_after").
		AddRow("event_receipts", "side_effects").
		AddRow("event_receipts", "failure").
		AddRow("event_receipts", "duration_ms").
		AddRow("event_receipts", "idempotency_key").
		AddRow("event_receipts", "processed_at")
	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)
	expectEventReceiptsTypedIdentityKey(mock, true)

	pg := &PostgresStore{DB: db}
	receiptsEnabled, err := pg.CanonicalEventReceiptsCapability(context.Background())
	if err != nil {
		t.Fatalf("CanonicalEventReceiptsCapability: %v", err)
	}
	if receiptsEnabled {
		t.Fatal("CanonicalEventReceiptsCapability = true, want false when events log is not canonical")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestPostgresStore_CanonicalEventReceiptsCapability_FailsClosedWithoutTypedIdentityKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name", "column_name"}).
		AddRow("events", "event_id").
		AddRow("events", "run_id").
		AddRow("events", "event_name").
		AddRow("events", "entity_id").
		AddRow("events", "flow_instance").
		AddRow("events", "scope").
		AddRow("events", "payload").
		AddRow("events", "chain_depth").
		AddRow("events", "produced_by").
		AddRow("events", "produced_by_type").
		AddRow("events", "source_event_id").
		AddRow("events", "created_at").
		AddRow("event_receipts", "receipt_id").
		AddRow("event_receipts", "event_id").
		AddRow("event_receipts", "subscriber_type").
		AddRow("event_receipts", "subscriber_id").
		AddRow("event_receipts", "entity_id").
		AddRow("event_receipts", "flow_instance").
		AddRow("event_receipts", "outcome").
		AddRow("event_receipts", "reason_code").
		AddRow("event_receipts", "state_before").
		AddRow("event_receipts", "state_after").
		AddRow("event_receipts", "side_effects").
		AddRow("event_receipts", "failure").
		AddRow("event_receipts", "duration_ms").
		AddRow("event_receipts", "idempotency_key").
		AddRow("event_receipts", "processed_at")
	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(rows)
	expectEventReceiptsTypedIdentityKey(mock, false)

	pg := &PostgresStore{DB: db}
	caps, err := pg.BindSchemaCapabilities(context.Background())
	if err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	if caps.Events.Receipts != SchemaFlavorUnsupported || caps.Events.ReceiptTypedIdentity {
		t.Fatalf("event_receipts caps = %+v, want unsupported without typed identity key", caps.Events)
	}
	receiptsEnabled, err := pg.CanonicalEventReceiptsCapability(context.Background())
	if err != nil {
		t.Fatalf("CanonicalEventReceiptsCapability: %v", err)
	}
	if receiptsEnabled {
		t.Fatal("CanonicalEventReceiptsCapability = true, want false without typed identity key")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func expectEventReceiptsTypedIdentityKey(mock sqlmock.Sqlmock, exists bool) {
	mock.ExpectQuery("FROM pg_index").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
}
