package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type mailboxMaterializationExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *PostgresStore) MaterializeMailboxWrite(ctx context.Context, item runtimepipeline.MailboxWriteMaterialization) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("mailbox", caps.Mailbox)
	}
	item, err = normalizeMailboxWriteMaterialization(item)
	if err != nil {
		return err
	}
	exec := mailboxMaterializationDBExecutor(ctx, s.DB)
	_, err = exec.ExecContext(ctx, `
		INSERT INTO mailbox (
			item_id, entity_id, flow_instance, scope, item_type, source_event_id,
			from_agent, severity, summary, payload, status, notified, created_at
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, NULLIF($3,''), $4, $5, $6::uuid,
			$7, $8, NULLIF($9,''), $10::jsonb, 'pending', false, now()
		)
		ON CONFLICT (item_id) DO NOTHING
	`, item.ItemID, item.EntityID, item.FlowInstance, item.Scope, item.ItemType, item.SourceEventID, item.FromAgent, item.Severity, item.Summary, string(item.Payload))
	if err != nil {
		return fmt.Errorf("materialize postgres mailbox_write: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) MaterializeMailboxWrite(ctx context.Context, item runtimepipeline.MailboxWriteMaterialization) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("mailbox", caps.Mailbox)
	}
	item, err = normalizeMailboxWriteMaterialization(item)
	if err != nil {
		return err
	}
	exec := mailboxMaterializationDBExecutor(ctx, s.DB)
	_, err = exec.ExecContext(ctx, `
		INSERT INTO mailbox (
			item_id, entity_id, flow_instance, scope, item_type, source_event_id,
			from_agent, severity, summary, payload, status, notified, created_at
		)
		VALUES (?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, NULLIF(?, ''), ?, 'pending', 0, ?)
		ON CONFLICT(item_id) DO NOTHING
	`, item.ItemID, item.EntityID, item.FlowInstance, item.Scope, item.ItemType, item.SourceEventID, item.FromAgent, item.Severity, item.Summary, string(item.Payload), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("materialize sqlite mailbox_write: %w", err)
	}
	return nil
}

func mailboxMaterializationDBExecutor(ctx context.Context, db *sql.DB) mailboxMaterializationExecutor {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return tx
	}
	return db
}

func normalizeMailboxWriteMaterialization(item runtimepipeline.MailboxWriteMaterialization) (runtimepipeline.MailboxWriteMaterialization, error) {
	item.ItemID = strings.TrimSpace(item.ItemID)
	item.EntityID = strings.TrimSpace(item.EntityID)
	item.FlowInstance = strings.Trim(strings.TrimSpace(item.FlowInstance), "/")
	item.Scope = strings.ToLower(strings.TrimSpace(item.Scope))
	item.ItemType = strings.TrimSpace(item.ItemType)
	item.SourceEventID = strings.TrimSpace(item.SourceEventID)
	item.FromAgent = strings.TrimSpace(item.FromAgent)
	item.Severity = normalizeMailboxSeverity(item.Severity)
	item.Summary = strings.TrimSpace(item.Summary)
	if item.ItemID == "" {
		return item, fmt.Errorf("mailbox_write item_id is required")
	}
	if _, err := uuid.Parse(item.ItemID); err != nil {
		return item, fmt.Errorf("mailbox_write item_id: %w", err)
	}
	if item.SourceEventID == "" {
		return item, fmt.Errorf("mailbox_write source_event_id is required")
	}
	if _, err := uuid.Parse(item.SourceEventID); err != nil {
		return item, fmt.Errorf("mailbox_write source_event_id: %w", err)
	}
	if item.EntityID != "" {
		if _, err := uuid.Parse(item.EntityID); err != nil {
			return item, fmt.Errorf("mailbox_write entity_id: %w", err)
		}
	}
	if item.ItemType == "" {
		return item, fmt.Errorf("mailbox_write item_type is required")
	}
	if item.FromAgent == "" {
		return item, fmt.Errorf("mailbox_write from_agent is required")
	}
	if item.Summary == "" {
		return item, fmt.Errorf("mailbox_write summary is required")
	}
	if len(item.Payload) == 0 || string(item.Payload) == "null" {
		item.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(item.Payload) {
		return item, fmt.Errorf("mailbox_write payload must be valid JSON")
	}
	derivedScope := "global"
	switch {
	case item.EntityID != "":
		derivedScope = "entity"
	case item.FlowInstance != "":
		derivedScope = "flow"
	}
	if item.Scope == "" {
		item.Scope = derivedScope
	}
	if item.Scope != derivedScope {
		return item, fmt.Errorf("mailbox_write scope %q does not match materialization fields %q", item.Scope, derivedScope)
	}
	return item, nil
}
