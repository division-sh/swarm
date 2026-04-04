package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"swarm/internal/runtime"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

func (s *PostgresStore) RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
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
	return s.recordInboundEventSpec(ctx, providerEventID, entityID, provider)
}

func (s *PostgresStore) ResolveInboundTarget(ctx context.Context, entityKey, provider string) (runtime.InboundTarget, error) {
	entityKey = strings.TrimSpace(entityKey)
	provider = strings.TrimSpace(strings.ToLower(provider))
	if entityKey == "" {
		return runtime.InboundTarget{}, fmt.Errorf("entity key is required")
	}
	if provider == "" {
		return runtime.InboundTarget{}, fmt.Errorf("provider is required")
	}

	var target runtime.InboundTarget
	const q = `
		SELECT
			entity_id::text,
			COALESCE(NULLIF(slug, ''), '')
		FROM entity_state
		WHERE slug = $1
		   OR entity_id::text = $1
		ORDER BY CASE WHEN slug = $1 THEN 0 ELSE 1 END, created_at DESC, updated_at DESC
		LIMIT 1
	`
	if err := s.DB.QueryRowContext(ctx, q, entityKey).Scan(&target.EntityID, &target.EntitySlug); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no rows") {
			return runtime.InboundTarget{}, fmt.Errorf("entity not found for key: %s", entityKey)
		}
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: %w", err)
	}
	if target.EntitySlug == "" {
		target.EntitySlug = entityKey
	}
	return target, nil
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

func (s *PostgresStore) DeleteInboundEvent(ctx context.Context, providerEventID, entityID, provider string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(providerEventID) == "" {
		return fmt.Errorf("provider_event_id is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(provider) == "" {
		return fmt.Errorf("provider is required")
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogIdempotencyKey {
		if caps.Events.Log != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("events", caps.Events.Log)
		}
		return fmt.Errorf("store: inbound event deletion requires canonical events.idempotency_key support")
	}
	idempotencyKey := inboundEventIdempotencyKey(providerEventID, entityID, provider)
	_, err = s.DB.ExecContext(ctx, `
		DELETE FROM events
		WHERE idempotency_key = $1
		  AND event_name = 'platform.inbound_recorded'
		  AND entity_id IS NOT DISTINCT FROM NULLIF($2,'')::uuid
	`, idempotencyKey, entityID)
	if err != nil {
		return fmt.Errorf("delete inbound event marker: %w", err)
	}
	return nil
}

func (s *PostgresStore) recordInboundEventSpec(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin inbound event tx: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

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
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit inbound event dedupe tx: %w", err)
		}
		committed = true
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
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	insertQ := `
		INSERT INTO events (
			event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, idempotency_key, created_at
		)
		VALUES (
			'platform.inbound_recorded', $1::uuid, NULL, 'entity', $2::jsonb,
			0, $3, 'external', $4, now()
		)
	`
	args := []any{entityID, string(payload), provider, idempotencyKey}
	if caps.Events.LogRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID); err != nil {
			return false, err
		}
		insertQ = `
			INSERT INTO events (
				run_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, idempotency_key, created_at
			)
			VALUES (
				NULLIF($1,'')::uuid, 'platform.inbound_recorded', $2::uuid, NULL, 'entity', $3::jsonb,
				0, $4, 'external', $5, now()
			)
		`
		args = []any{runID, entityID, string(payload), provider, idempotencyKey}
	}
	if _, err := tx.ExecContext(ctx, insertQ, args...); err != nil {
		return false, fmt.Errorf("record inbound event in events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit inbound event tx: %w", err)
	}
	committed = true
	return true, nil
}

func (s *PostgresStore) purgeInboundEventsSpec(ctx context.Context, before time.Time, limit int) (int, error) {
	res, err := s.DB.ExecContext(ctx, `
		WITH doomed AS (
			SELECT event_id
			FROM events
			WHERE event_name = 'platform.inbound_recorded'
			  AND produced_by_type = 'external'
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
