package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SchemaFlavor string

const (
	SchemaFlavorUnavailable SchemaFlavor = "unavailable"
	SchemaFlavorUnsupported SchemaFlavor = "unsupported"
	SchemaFlavorLegacy      SchemaFlavor = "legacy"
	SchemaFlavorCanonical   SchemaFlavor = "canonical"
)

type EventSchemaCapabilities struct {
	Log                  SchemaFlavor
	Deliveries           SchemaFlavor
	Receipts             SchemaFlavor
	HasRuns              bool
	RunStartedAt         bool
	RunTriggerColumns    bool
	RunCounterColumns    bool
	RunTerminalFields    bool
	RunBundleHash        bool
	RunBundleSource      bool
	RunBundleFingerprint bool
	LogRunID             bool
	DeliveryRunID        bool
	LogIdempotencyKey    bool
	LogRouteIdentity     bool
	DeliveryTargetRoute  bool
	DeliveryContext      bool
	ReceiptTypedIdentity bool
}

type ConversationSchemaCapabilities struct {
	Sessions      SchemaFlavor
	Audits        SchemaFlavor
	Turns         SchemaFlavor
	Forks         SchemaFlavor
	ForkSnapshots SchemaFlavor
	ForkTurns     SchemaFlavor
	SessionRunID  bool
	AuditRunID    bool
	TurnRunID     bool
	TurnBlocks    bool
}

type StoreSchemaCapabilities struct {
	Agents        SchemaFlavor
	Activity      ActivitySchemaCapabilities
	EntityState   SchemaFlavor
	Schedules     SchemaFlavor
	Mailbox       SchemaFlavor
	Events        EventSchemaCapabilities
	Conversations ConversationSchemaCapabilities
	EntityRunID   bool
}

type ActivitySchemaCapabilities struct {
	Attempts SchemaFlavor
}

type schemaColumnCatalog struct {
	tables map[string]map[string]struct{}
}

func (c schemaColumnCatalog) hasTable(tableName string) bool {
	_, ok := c.tables[strings.TrimSpace(tableName)]
	return ok
}

func (c schemaColumnCatalog) hasColumns(tableName string, columns ...string) bool {
	table, ok := c.tables[strings.TrimSpace(tableName)]
	if !ok {
		return false
	}
	for _, column := range columns {
		if _, ok := table[strings.TrimSpace(column)]; !ok {
			return false
		}
	}
	return true
}

func loadSchemaColumnCatalog(ctx context.Context, db *sql.DB) (schemaColumnCatalog, error) {
	catalog := schemaColumnCatalog{tables: map[string]map[string]struct{}{}}
	if db == nil {
		return catalog, fmt.Errorf("postgres store is required")
	}
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
	`)
	if err != nil {
		return schemaColumnCatalog{}, fmt.Errorf("inspect store schema: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return schemaColumnCatalog{}, fmt.Errorf("scan store schema column: %w", err)
		}
		tableName = strings.TrimSpace(tableName)
		columnName = strings.TrimSpace(columnName)
		if tableName == "" || columnName == "" {
			continue
		}
		if catalog.tables[tableName] == nil {
			catalog.tables[tableName] = map[string]struct{}{}
		}
		catalog.tables[tableName][columnName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return schemaColumnCatalog{}, fmt.Errorf("read store schema columns: %w", err)
	}
	return catalog, nil
}

func eventReceiptsTypedSubscriberIdentityKeyExists(ctx context.Context, db *sql.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_index i
			JOIN pg_class tbl ON tbl.oid = i.indrelid
			JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
			WHERE ns.nspname = 'public'
			  AND tbl.relname = 'event_receipts'
			  AND i.indisunique
			  AND replace(pg_get_indexdef(i.indexrelid), '"', '') LIKE '% USING btree (event_id, subscriber_type, subscriber_id)%'
		)
	`).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect event_receipts typed subscriber identity key: %w", err)
	}
	return exists, nil
}

func detectSchemaFlavor(catalog schemaColumnCatalog, tableName string, canonicalColumns, legacyColumns []string) SchemaFlavor {
	switch {
	case len(canonicalColumns) > 0 && catalog.hasColumns(tableName, canonicalColumns...):
		return SchemaFlavorCanonical
	case len(legacyColumns) > 0 && catalog.hasColumns(tableName, legacyColumns...):
		return SchemaFlavorLegacy
	case catalog.hasTable(tableName):
		return SchemaFlavorUnsupported
	default:
		return SchemaFlavorUnavailable
	}
}

func detectStoreSchemaCapabilities(catalog schemaColumnCatalog) StoreSchemaCapabilities {
	caps := StoreSchemaCapabilities{}
	caps.Agents = detectSchemaFlavor(catalog, "agents",
		[]string{
			"agent_id", "flow_instance", "role", "model", "llm_backend", "conversation_mode",
			"parent_agent_id", "entity_id", "config", "subscriptions", "emit_events", "tools",
			"permissions", "runtime_descriptor", "status", "turn_count", "last_active_at", "created_at",
		},
		[]string{
			"id", "type", "role", "mode", "entity_id", "parent_agent_id", "status",
			"coordinator_id", "config", "budget_envelope", "hired_by", "template_version",
			"started_at", "last_active_at",
		},
	)
	caps.EntityState = detectSchemaFlavor(catalog, "entity_state",
		[]string{
			"run_id", "entity_id", "flow_instance", "entity_type", "slug", "name",
			"current_state", "gates", "fields", "accumulator", "revision",
			"entered_state_at", "created_at", "updated_at",
		},
		[]string{
			"entity_id", "flow_instance", "entity_type", "slug", "name",
			"current_state", "gates", "fields", "accumulator", "revision",
			"entered_state_at", "created_at", "updated_at",
		},
	)
	caps.EntityRunID = catalog.hasColumns("entity_state", "run_id")
	caps.Activity = ActivitySchemaCapabilities{
		Attempts: detectSchemaFlavor(catalog, "activity_attempts",
			[]string{
				"request_event_id", "run_id", "activity_id", "tool", "effect_class",
				"attempt", "status", "success_event", "failure_event", "result_event_id",
				"result_event_type", "result_payload", "error", "input_hash",
				"started_at", "completed_at", "updated_at",
			},
			nil,
		),
	}

	caps.Events = EventSchemaCapabilities{
		Log: detectSchemaFlavor(catalog, "events",
			[]string{
				"event_id", "event_name", "entity_id", "flow_instance", "scope", "payload",
				"chain_depth", "produced_by", "produced_by_type", "source_event_id", "created_at",
			},
			[]string{"id", "type", "source_agent", "task_id", "entity_id", "payload", "created_at"},
		),
		Deliveries: detectSchemaFlavor(catalog, "event_deliveries",
			[]string{
				"event_id", "subscriber_type", "subscriber_id", "status", "retry_count",
				"reason_code", "last_error", "active_session_id", "started_at", "delivered_at", "created_at",
			},
			[]string{"event_id", "agent_id", "status", "created_at"},
		),
		Receipts: detectSchemaFlavor(catalog, "event_receipts",
			[]string{
				"receipt_id", "event_id", "subscriber_type", "subscriber_id", "entity_id", "flow_instance",
				"outcome", "reason_code", "state_before", "state_after", "side_effects",
				"duration_ms", "idempotency_key", "processed_at",
			},
			[]string{"event_id", "agent_id", "processed_at", "status", "retry_count", "error"},
		),
		HasRuns:              catalog.hasColumns("runs", "run_id", "status"),
		RunStartedAt:         catalog.hasColumns("runs", "started_at"),
		RunTriggerColumns:    catalog.hasColumns("runs", "trigger_event_id", "trigger_event_type"),
		RunCounterColumns:    catalog.hasColumns("runs", "event_count", "entity_count"),
		RunTerminalFields:    catalog.hasColumns("runs", "error_summary", "ended_at"),
		RunBundleHash:        catalog.hasColumns("runs", "bundle_hash"),
		RunBundleSource:      catalog.hasColumns("runs", "bundle_source"),
		RunBundleFingerprint: catalog.hasColumns("runs", "bundle_fingerprint"),
		LogRunID:             catalog.hasColumns("events", "run_id"),
		DeliveryRunID:        catalog.hasColumns("event_deliveries", "run_id"),
		LogIdempotencyKey:    catalog.hasColumns("events", "idempotency_key"),
		LogRouteIdentity:     catalog.hasColumns("events", "source_route", "target_route", "target_set"),
		DeliveryTargetRoute:  catalog.hasColumns("event_deliveries", "delivery_target_route"),
		DeliveryContext:      catalog.hasColumns("event_deliveries", "delivery_context"),
	}

	caps.Conversations = ConversationSchemaCapabilities{
		Sessions: detectSchemaFlavor(catalog, "agent_sessions",
			[]string{
				"session_id", "agent_id", "entity_id", "flow_instance", "scope_key", "scope",
				"conversation", "turn_count", "runtime_mode", "runtime_state", "lease_holder",
				"lease_expires_at", "status", "termination_reason", "termination_detail",
				"successor_session_id", "terminated_at", "created_at", "updated_at",
			},
			nil,
		),
		Audits: detectSchemaFlavor(catalog, "agent_conversation_audits",
			[]string{
				"session_id", "agent_id", "entity_id", "flow_instance", "scope_key", "scope",
				"conversation", "turn_count", "runtime_mode", "runtime_state", "status",
				"created_at", "updated_at",
			},
			nil,
		),
		Turns: detectSchemaFlavor(catalog, "agent_turns",
			[]string{
				"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id",
				"trigger_event_id", "trigger_event_type", "task_id", "available_tools", "tool_calls",
				"emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
				"request_payload", "response_payload", "parse_ok", "latency_ms", "retry_count",
				"error", "created_at",
			},
			nil,
		),
		Forks: detectSchemaFlavor(catalog, "conversation_forks",
			[]string{
				"fork_id", "source_session_id", "source_run_id", "source_agent_id",
				"fork_point_kind", "fork_point_turn_index", "fork_point_turn_id",
				"fork_point_event_id", "fork_point_at", "fork_point_selected_at",
				"created_by", "created_at", "expires_at", "deleted_at",
			},
			nil,
		),
		ForkSnapshots: detectSchemaFlavor(catalog, "conversation_fork_snapshots",
			[]string{
				"fork_id", "source_session_id", "source_run_id", "source_agent_id",
				"fork_point_turn_id", "fork_point_turn_index", "fork_point_selected_at",
				"source_turn", "entity_snapshot", "snapshot_owner", "created_at",
			},
			nil,
		),
		ForkTurns: detectSchemaFlavor(catalog, "conversation_fork_turns",
			[]string{
				"fork_turn_id", "fork_id", "turn_index", "actor_token_id", "message",
				"assistant_message", "request_payload", "response_payload", "tool_calls",
				"sandbox_policy", "snapshot_owner", "created_at",
			},
			nil,
		),
		SessionRunID: catalog.hasColumns("agent_sessions", "run_id"),
		AuditRunID:   catalog.hasColumns("agent_conversation_audits", "run_id"),
		TurnRunID:    catalog.hasColumns("agent_turns", "run_id"),
		TurnBlocks:   catalog.hasColumns("agent_turns", "turn_blocks"),
	}

	caps.Mailbox = detectSchemaFlavor(catalog, "mailbox",
		[]string{
			"item_id", "entity_id", "flow_instance", "scope", "item_type", "source_event_id",
			"from_agent", "severity", "summary", "payload", "status", "decision", "decision_notes",
			"decided_by", "decided_at", "notified", "expires_at", "deferred_until", "created_at",
		},
		[]string{
			"id", "event_id", "entity_id", "from_agent", "type", "priority", "status",
			"context", "summary", "timeout_at", "decision", "decision_notes", "notified", "created_at",
		},
	)

	caps.Schedules = detectSchemaFlavor(catalog, "timers",
		[]string{
			"timer_id", "run_id", "source_timer_id", "forked_from_run_id", "forked_from_event_id",
			"reconstruction_owner", "timer_name", "entity_id", "flow_instance", "fire_event", "fire_payload",
			"fire_at", "recurring", "recurrence_cron", "recurrence_interval", "owner_node",
			"owner_agent", "task_type", "status", "fired_at", "created_at",
		},
		nil,
	)
	if caps.Schedules == SchemaFlavorUnavailable {
		caps.Schedules = detectSchemaFlavor(catalog, "schedules",
			nil,
			[]string{
				"agent_id", "entity_id", "event_type", "mode", "cron_expr", "at_time",
				"next_fire_at", "payload", "active", "cancelled_at", "last_fired_at", "created_at",
			},
		)
	}

	return caps
}

func (s *PostgresStore) BindSchemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if s == nil || s.DB == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("postgres store is required")
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return StoreSchemaCapabilities{}, err
	}
	caps := detectStoreSchemaCapabilities(catalog)
	if caps.Events.Receipts == SchemaFlavorCanonical {
		hasTypedIdentity, err := eventReceiptsTypedSubscriberIdentityKeyExists(ctx, s.DB)
		if err != nil {
			return StoreSchemaCapabilities{}, err
		}
		caps.Events.ReceiptTypedIdentity = hasTypedIdentity
		if !hasTypedIdentity {
			caps.Events.Receipts = SchemaFlavorUnsupported
		}
	}
	s.schemaCapsMu.Lock()
	s.schemaCaps = caps
	s.schemaCapsBound = true
	s.schemaCapsMu.Unlock()
	return caps, nil
}

func (s *PostgresStore) schemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if s == nil || s.DB == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("postgres store is required")
	}
	s.schemaCapsMu.RLock()
	if s.schemaCapsBound {
		caps := s.schemaCaps
		s.schemaCapsMu.RUnlock()
		return caps, nil
	}
	s.schemaCapsMu.RUnlock()
	return s.BindSchemaCapabilities(ctx)
}

func (s *PostgresStore) SchemaCapabilities() StoreSchemaCapabilities {
	if s == nil {
		return StoreSchemaCapabilities{}
	}
	s.schemaCapsMu.RLock()
	defer s.schemaCapsMu.RUnlock()
	return s.schemaCaps
}

func (s *PostgresStore) ResolveSchemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	return s.schemaCapabilities(ctx)
}

func (s *PostgresStore) CanonicalRuntimeLogCapability(ctx context.Context) (bool, bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, false, err
	}
	if caps.Events.Log != SchemaFlavorCanonical {
		return false, false, nil
	}
	return true, caps.Events.LogRunID, nil
}

func (s *PostgresStore) CanonicalEventReceiptsCapability(ctx context.Context) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	return caps.Events.Log == SchemaFlavorCanonical && caps.Events.Receipts == SchemaFlavorCanonical, nil
}

func unsupportedSchemaCapability(subject string, flavor SchemaFlavor) error {
	switch flavor {
	case SchemaFlavorUnavailable:
		return fmt.Errorf("store: %s schema is unavailable", strings.TrimSpace(subject))
	case SchemaFlavorUnsupported, SchemaFlavorLegacy:
		return fmt.Errorf("store: %s schema is unsupported by the explicit capability boundary", strings.TrimSpace(subject))
	default:
		return fmt.Errorf("store: %s schema capability is invalid (%s)", strings.TrimSpace(subject), strings.TrimSpace(string(flavor)))
	}
}
