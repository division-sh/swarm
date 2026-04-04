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
	Log               SchemaFlavor
	Deliveries        SchemaFlavor
	Receipts          SchemaFlavor
	HasRuns           bool
	LogRunID          bool
	DeliveryRunID     bool
	LogIdempotencyKey bool
}

type ConversationSchemaCapabilities struct {
	Sessions     SchemaFlavor
	Turns        SchemaFlavor
	SessionRunID bool
	TurnRunID    bool
	TurnBlocks   bool
}

type StoreSchemaCapabilities struct {
	Agents        SchemaFlavor
	Schedules     SchemaFlavor
	Mailbox       SchemaFlavor
	Events        EventSchemaCapabilities
	Conversations ConversationSchemaCapabilities
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
			"agent_id", "flow_instance", "role", "model_tier", "llm_backend", "conversation_mode",
			"parent_agent_id", "entity_id", "config", "subscriptions", "emit_events", "tools",
			"permissions", "status", "turn_count", "last_active_at", "created_at",
		},
		[]string{
			"id", "type", "role", "mode", "entity_id", "parent_agent_id", "status",
			"coordinator_id", "config", "budget_envelope", "hired_by", "template_version",
			"started_at", "last_active_at",
		},
	)

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
				"event_id", "subscriber_type", "subscriber_id", "entity_id", "flow_instance",
				"outcome", "reason_code", "side_effects", "processed_at",
			},
			[]string{"event_id", "agent_id", "processed_at", "status", "retry_count", "error"},
		),
		HasRuns:           catalog.hasColumns("runs", "run_id", "status"),
		LogRunID:          catalog.hasColumns("events", "run_id"),
		DeliveryRunID:     catalog.hasColumns("event_deliveries", "run_id"),
		LogIdempotencyKey: catalog.hasColumns("events", "idempotency_key"),
	}

	caps.Conversations = ConversationSchemaCapabilities{
		Sessions: detectSchemaFlavor(catalog, "agent_sessions",
			[]string{
				"session_id", "agent_id", "entity_id", "flow_instance", "scope_key", "scope",
				"conversation", "turn_count", "runtime_mode", "runtime_state", "lease_holder",
				"lease_expires_at", "status", "created_at", "updated_at",
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
		SessionRunID: catalog.hasColumns("agent_sessions", "run_id"),
		TurnRunID:    catalog.hasColumns("agent_turns", "run_id"),
		TurnBlocks:   catalog.hasColumns("agent_turns", "turn_blocks"),
	}

	caps.Mailbox = detectSchemaFlavor(catalog, "mailbox",
		[]string{
			"item_id", "entity_id", "flow_instance", "scope", "item_type", "source_event_id",
			"from_agent", "severity", "summary", "payload", "status", "decision", "decision_notes",
			"decided_by", "decided_at", "notified", "expires_at", "created_at",
		},
		[]string{
			"id", "event_id", "entity_id", "from_agent", "type", "priority", "status",
			"context", "summary", "timeout_at", "decision", "decision_notes", "notified", "created_at",
		},
	)

	caps.Schedules = detectSchemaFlavor(catalog, "timers",
		[]string{
			"timer_id", "timer_name", "entity_id", "flow_instance", "fire_event", "fire_payload",
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
