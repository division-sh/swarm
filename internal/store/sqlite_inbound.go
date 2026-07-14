package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

var _ runtime.InboundPersistence = (*SQLiteRuntimeStore)(nil)

func (s *SQLiteRuntimeStore) RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
	inserted := false
	err := s.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
		inbound, ok := mutation.(runtimebus.InboundDeliveryMutation)
		if !ok {
			return fmt.Errorf("selected-store event mutation does not support inbound delivery claims")
		}
		var err error
		inserted, err = inbound.ClaimInboundEvent(mutation.Context(), providerEventID, entityID, provider)
		return err
	})
	return inserted, err
}

func (s *SQLiteRuntimeStore) ClaimInboundEventTx(ctx context.Context, tx *sql.Tx, providerEventID, entityID, provider string) (bool, error) {
	if strings.TrimSpace(providerEventID) == "" {
		return false, fmt.Errorf("provider_event_id is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return false, fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(provider) == "" {
		return false, fmt.Errorf("provider is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogIdempotencyKey {
		if caps.Events.Log != SchemaFlavorCanonical {
			return false, unsupportedSchemaCapability("events", caps.Events.Log)
		}
		return false, fmt.Errorf("store: inbound event recording requires canonical events.idempotency_key support")
	}
	if tx == nil {
		return false, fmt.Errorf("inbound delivery claim transaction is required")
	}
	idempotencyKey := inboundEventIdempotencyKey(providerEventID, entityID, provider)
	var exists bool
	if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1
				FROM events
				WHERE idempotency_key = ?
				  AND event_name = 'platform.inbound_recorded'
				  AND entity_id IS ?
			)
		`, idempotencyKey, sqliteNullUUID(entityID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("check sqlite inbound event dedupe: %w", err)
	}
	if exists {
		return false, nil
	}

	payload, err := json.Marshal(map[string]any{
		"provider":          provider,
		"provider_event_id": providerEventID,
		"entity_id":         entityID,
	})
	if err != nil {
		return false, fmt.Errorf("marshal inbound event payload: %w", err)
	}
	if err := s.validateEventPayload("platform.inbound_recorded", payload); err != nil {
		return false, err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	evt, err := events.AdmitForPersistence(
		events.NewDiagnosticDirectEvent(
			uuid.NewString(),
			events.EventType("platform.inbound_recorded"),
			provider,
			"",
			payload,
			0,
			runID,
			"",
			events.EventEnvelope{EntityID: entityID},
			time.Time{},
		),
		events.AdmissionOptions{},
	)
	if err != nil {
		return false, err
	}
	if caps.Events.LogRunID {
		if err := sqliteEnsureRunRow(ctx, tx, evt.RunID(), "", "", evt.CreatedAt()); err != nil {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
				scope, payload, chain_depth, produced_by, produced_by_type, idempotency_key, created_at
			)
			VALUES (?, ?, 'platform.inbound_recorded', ?, NULL, '{}', '{}', '[]', 'entity', ?, 0, ?, 'external', ?, ?)
		`, evt.ID(), sqliteNullUUID(evt.RunID()), sqliteNullUUID(entityID), string(evt.Payload()), provider, idempotencyKey, evt.CreatedAt().UTC())
	if err != nil {
		return false, fmt.Errorf("record sqlite inbound event in events: %w", err)
	}
	if err := recordInboundAuthorActivity(ctx, evt, provider); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLiteRuntimeStore) PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return 0, err
	}
	if limit <= 0 {
		limit = 1000
	}
	if caps.Events.Log != SchemaFlavorCanonical {
		return 0, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	deleted := 0
	err = s.runRuntimeMutation(ctx, "sqlite purge inbound events", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `
			DELETE FROM events
			WHERE event_id IN (
				SELECT event_id
				FROM events
				WHERE event_name = 'platform.inbound_recorded'
				  AND produced_by_type = 'external'
				  AND run_id IS NULL
				  AND created_at < ?
				ORDER BY created_at ASC
				LIMIT ?
			)
		`, before.UTC(), limit)
		if err != nil {
			return fmt.Errorf("purge sqlite inbound events from events: %w", err)
		}
		n, _ := res.RowsAffected()
		deleted = int(n)
		return nil
	})
	return deleted, err
}
