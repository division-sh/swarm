package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func (s *PostgresStore) RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
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

func (s *PostgresStore) PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error) {
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
	return s.purgeInboundEventsSpec(ctx, before, limit)
}

func (s *PostgresStore) ClaimInboundEventTx(ctx context.Context, tx *sql.Tx, providerEventID, entityID, provider string) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	if tx == nil {
		return false, fmt.Errorf("inbound delivery claim transaction is required")
	}
	if strings.TrimSpace(providerEventID) == "" {
		return false, fmt.Errorf("provider_event_id is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return false, fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(provider) == "" {
		return false, fmt.Errorf("provider is required")
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogIdempotencyKey {
		if caps.Events.Log != SchemaFlavorCanonical {
			return false, unsupportedSchemaCapability("events", caps.Events.Log)
		}
		return false, fmt.Errorf("store: inbound event recording requires canonical events.idempotency_key support")
	}

	idempotencyKey := inboundEventIdempotencyKey(providerEventID, entityID, provider)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, idempotencyKey); err != nil {
		return false, fmt.Errorf("lock inbound event key: %w", err)
	}

	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM events
			WHERE idempotency_key = $1
			  AND event_name = 'platform.inbound_recorded'
			  AND entity_id IS NOT DISTINCT FROM NULLIF($2,'')::uuid
		)
	`, idempotencyKey, entityID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check inbound event dedupe: %w", err)
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
			"",
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
	insertQ := `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, idempotency_key, created_at
		)
		VALUES (
			$1::uuid, 'platform.inbound_recorded', $2::uuid, NULL, 'entity', $3::jsonb,
			0, $4, 'external', $5, $6
		)
	`
	args := []any{evt.ID(), entityID, string(evt.Payload()), provider, idempotencyKey, evt.CreatedAt()}
	if caps.Events.LogRunID {
		if err := s.ensureRunRow(ctx, caps, tx, evt.RunID(), "", "", true); err != nil {
			return false, err
		}
		insertQ = `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, idempotency_key, created_at
			)
			VALUES (
				$1::uuid, NULLIF($2,'')::uuid, 'platform.inbound_recorded', $3::uuid, NULL, 'entity', $4::jsonb,
				0, $5, 'external', $6, $7
			)
		`
		args = []any{evt.ID(), evt.RunID(), entityID, string(evt.Payload()), provider, idempotencyKey, evt.CreatedAt()}
	}
	if _, err := tx.ExecContext(ctx, insertQ, args...); err != nil {
		return false, fmt.Errorf("record inbound event in events: %w", err)
	}
	return true, nil
}

func (s *PostgresStore) purgeInboundEventsSpec(ctx context.Context, before time.Time, limit int) (int, error) {
	res, err := s.DB.ExecContext(ctx, `
		WITH doomed AS (
			SELECT event_id
			FROM events
			WHERE event_name = 'platform.inbound_recorded'
			  AND produced_by_type = 'external'
			  AND run_id IS NULL
			  AND created_at < $1
			ORDER BY created_at ASC
			LIMIT $2
		)
		DELETE FROM events e
		USING doomed d
		WHERE e.event_id = d.event_id
	`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge inbound events from events: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func inboundEventIdempotencyKey(providerEventID, entityID, provider string) string {
	return strings.Join([]string{
		"platform.inbound_recorded",
		strings.TrimSpace(strings.ToLower(provider)),
		strings.TrimSpace(entityID),
		strings.TrimSpace(providerEventID),
	}, ":")
}
