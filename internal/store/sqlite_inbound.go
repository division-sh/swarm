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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/google/uuid"
)

var _ runtime.InboundPersistence = (*SQLiteRuntimeStore)(nil)

func (s *SQLiteRuntimeStore) RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
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
	inserted := false
	err = s.runRuntimeMutation(ctx, "sqlite record inbound event", func(txctx context.Context, tx *sql.Tx) error {
		idempotencyKey := inboundEventIdempotencyKey(providerEventID, entityID, provider)
		var exists bool
		if err := tx.QueryRowContext(txctx, `
			SELECT EXISTS(
				SELECT 1
				FROM events
				WHERE idempotency_key = ?
				  AND event_name = 'platform.inbound_recorded'
				  AND entity_id IS ?
			)
		`, idempotencyKey, sqliteNullUUID(entityID)).Scan(&exists); err != nil {
			return fmt.Errorf("check sqlite inbound event dedupe: %w", err)
		}
		if exists {
			inserted = false
			return nil
		}

		payload, err := json.Marshal(map[string]any{
			"provider":          provider,
			"provider_event_id": providerEventID,
			"entity_id":         entityID,
		})
		if err != nil {
			return fmt.Errorf("marshal inbound event payload: %w", err)
		}
		if err := s.validateEventPayload("platform.inbound_recorded", payload); err != nil {
			return err
		}
		runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(txctx))
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
			return err
		}
		if caps.Events.LogRunID {
			if err := sqliteEnsureRunRow(txctx, tx, evt.RunID(), "", "", evt.CreatedAt()); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(txctx, `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
				scope, payload, chain_depth, produced_by, produced_by_type, idempotency_key, created_at
			)
			VALUES (?, ?, 'platform.inbound_recorded', ?, NULL, '{}', '{}', '[]', 'entity', ?, 0, ?, 'external', ?, ?)
		`, evt.ID(), sqliteNullUUID(evt.RunID()), sqliteNullUUID(entityID), string(evt.Payload()), provider, idempotencyKey, evt.CreatedAt().UTC())
		if err != nil {
			return fmt.Errorf("record sqlite inbound event in events: %w", err)
		}
		inserted = true
		return nil
	})
	return inserted, err
}

func (s *SQLiteRuntimeStore) ResolveInboundTarget(ctx context.Context, entityKey, provider string) (runtime.InboundTarget, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtime.InboundTarget{}, err
	}
	entityKey = strings.TrimSpace(entityKey)
	provider = normalizeInboundProviderKey(provider)
	if entityKey == "" {
		return runtime.InboundTarget{}, fmt.Errorf("entity key is required")
	}
	if provider == "" {
		return runtime.InboundTarget{}, fmt.Errorf("provider is required")
	}
	runID, ok, err := runtimecurrentstate.RunIDFromContext(ctx)
	if err != nil {
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: %w", err)
	}
	if ok {
		return s.resolveSQLiteInboundTargetForRun(ctx, entityKey, provider, runID)
	}
	if caps.EntityState != SchemaFlavorCanonical || !caps.EntityRunID {
		if caps.EntityState != SchemaFlavorCanonical {
			return runtime.InboundTarget{}, unsupportedSchemaCapability("entity_state", caps.EntityState)
		}
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: entity_state.run_id schema capability is required")
	}
	return s.resolveSQLiteInboundTargetUnambiguous(ctx, entityKey, provider)
}

func (s *SQLiteRuntimeStore) resolveSQLiteInboundTargetForRun(ctx context.Context, entityKey, provider, runID string) (runtime.InboundTarget, error) {
	var target runtime.InboundTarget
	err := s.DB.QueryRowContext(ctx, `
		SELECT
			es.entity_id,
			COALESCE(NULLIF(es.slug, ''), ''),
			COALESCE(json_extract(fi.config, ?), '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
		  AND (es.slug = ? OR es.entity_id = ?)
		ORDER BY CASE WHEN es.slug = ? THEN 0 ELSE 1 END, es.created_at DESC, es.updated_at DESC
		LIMIT 1
	`, inboundWebhookSecretPath(provider), runID, entityKey, entityKey, entityKey).Scan(&target.EntityID, &target.EntitySlug, &target.WebhookSecret)
	if err != nil {
		if err == sql.ErrNoRows {
			return runtime.InboundTarget{}, fmt.Errorf("entity not found for key: %s", entityKey)
		}
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: %w", err)
	}
	if target.EntitySlug == "" {
		target.EntitySlug = entityKey
	}
	return target, nil
}

func (s *SQLiteRuntimeStore) resolveSQLiteInboundTargetUnambiguous(ctx context.Context, entityKey, provider string) (runtime.InboundTarget, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			es.entity_id,
			COALESCE(NULLIF(es.slug, ''), ''),
			COALESCE(json_extract(fi.config, ?), ''),
			COALESCE(es.run_id, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.slug = ? OR es.entity_id = ?
		ORDER BY CASE WHEN es.slug = ? THEN 0 ELSE 1 END, es.created_at DESC, es.updated_at DESC
		LIMIT 2
	`, inboundWebhookSecretPath(provider), entityKey, entityKey, entityKey)
	if err != nil {
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: %w", err)
	}
	defer rows.Close()

	var matches []runtime.InboundTarget
	for rows.Next() {
		var target runtime.InboundTarget
		var runID string
		if err := rows.Scan(&target.EntityID, &target.EntitySlug, &target.WebhookSecret, &runID); err != nil {
			return runtime.InboundTarget{}, fmt.Errorf("scan inbound target: %w", err)
		}
		if target.EntitySlug == "" {
			target.EntitySlug = entityKey
		}
		matches = append(matches, target)
	}
	if err := rows.Err(); err != nil {
		return runtime.InboundTarget{}, fmt.Errorf("iterate inbound targets: %w", err)
	}
	switch len(matches) {
	case 0:
		return runtime.InboundTarget{}, fmt.Errorf("entity not found for key: %s", entityKey)
	case 1:
		return matches[0], nil
	default:
		return runtime.InboundTarget{}, fmt.Errorf("entity key %s is ambiguous across runs; run_id is required", entityKey)
	}
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

func (s *SQLiteRuntimeStore) DeleteInboundEvent(ctx context.Context, providerEventID, entityID, provider string) error {
	if strings.TrimSpace(providerEventID) == "" {
		return fmt.Errorf("provider_event_id is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(provider) == "" {
		return fmt.Errorf("provider is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogIdempotencyKey {
		if caps.Events.Log != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("events", caps.Events.Log)
		}
		return fmt.Errorf("store: inbound event deletion requires canonical events.idempotency_key support")
	}
	return s.runRuntimeMutation(ctx, "sqlite delete inbound event", func(txctx context.Context, tx *sql.Tx) error {
		idempotencyKey := inboundEventIdempotencyKey(providerEventID, entityID, provider)
		if _, err := tx.ExecContext(txctx, `
			DELETE FROM events
			WHERE idempotency_key = ?
			  AND event_name = 'platform.inbound_recorded'
			  AND entity_id IS ?
		`, idempotencyKey, sqliteNullUUID(entityID)); err != nil {
			return fmt.Errorf("delete sqlite inbound event marker: %w", err)
		}
		return nil
	})
}

func inboundWebhookSecretPath(provider string) string {
	return "$.secrets.webhook_signing." + normalizeInboundProviderKey(provider)
}
