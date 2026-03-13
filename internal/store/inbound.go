package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"empireai/internal/runtime"
)

func (s *PostgresStore) RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error) {
	if strings.TrimSpace(providerEventID) == "" {
		return false, fmt.Errorf("provider_event_id is required")
	}
	if strings.TrimSpace(entityID) == "" {
		return false, fmt.Errorf("entity_id is required")
	}
	if strings.TrimSpace(provider) == "" {
		return false, fmt.Errorf("provider is required")
	}
	const q = `
		INSERT INTO inbound_events (provider_event_id, vertical_id, provider, received_at)
		VALUES ($1, $2::uuid, $3, now())
		ON CONFLICT (provider_event_id, vertical_id) DO NOTHING
	`
	res, err := s.DB.ExecContext(ctx, q, providerEventID, entityID, provider)
	if err != nil {
		return false, fmt.Errorf("record inbound event: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
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
	var credentialsRaw []byte
	const q = `
		SELECT
			instance_id::text,
			COALESCE(NULLIF(metadata->>'slug', ''), ''),
			COALESCE(metadata->'credentials', '{}'::jsonb)
		FROM workflow_instances
		WHERE metadata->>'slug' = $1
		   OR instance_id::text = $1
		ORDER BY CASE WHEN metadata->>'slug' = $1 THEN 0 ELSE 1 END, created_at DESC, updated_at DESC
		LIMIT 1
	`
	if err := s.DB.QueryRowContext(ctx, q, entityKey).Scan(&target.EntityID, &target.EntitySlug, &credentialsRaw); err != nil {
		if err == sql.ErrNoRows {
			return runtime.InboundTarget{}, fmt.Errorf("entity not found for key: %s", entityKey)
		}
		return runtime.InboundTarget{}, fmt.Errorf("resolve inbound target: %w", err)
	}
	if target.EntitySlug == "" {
		target.EntitySlug = entityKey
	}
	target.WebhookSecret = s.extractWebhookSecret(ctx, credentialsRaw, provider)
	return target, nil
}

func (s *PostgresStore) PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 1000
	}
	const q = `
		WITH doomed AS (
			SELECT provider_event_id, vertical_id
			FROM inbound_events
			WHERE received_at < $1
			ORDER BY received_at ASC
			LIMIT $2
		)
		DELETE FROM inbound_events i
		USING doomed d
		WHERE i.provider_event_id = d.provider_event_id
		  AND i.vertical_id = d.vertical_id
	`
	res, err := s.DB.ExecContext(ctx, q, before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge inbound events: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *PostgresStore) extractWebhookSecret(ctx context.Context, credentialsRaw []byte, provider string) string {
	if len(credentialsRaw) == 0 || !json.Valid(credentialsRaw) {
		return ""
	}
	var creds map[string]any
	if err := json.Unmarshal(credentialsRaw, &creds); err != nil {
		return ""
	}
	creds = s.decryptCredentialMap(ctx, creds)
	// Preferred: credentials.webhooks.<provider>.secret
	if webhooks, ok := creds["webhooks"].(map[string]any); ok {
		if p, ok := webhooks[provider].(map[string]any); ok {
			if v := pickSecretKeys(p, "secret", "webhook_secret", "token"); v != "" {
				return v
			}
		}
	}
	// Legacy: credentials.<provider>.secret
	if p, ok := creds[provider].(map[string]any); ok {
		if v := pickSecretKeys(p, "secret", "webhook_secret", "token"); v != "" {
			return v
		}
	}
	// Flat fallback: credentials.<provider>_webhook_secret
	if v, ok := creds[provider+"_webhook_secret"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func (s *PostgresStore) decryptCredentialMap(ctx context.Context, in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = s.decryptCredentialValue(ctx, v)
	}
	return out
}

func (s *PostgresStore) decryptCredentialValue(ctx context.Context, v any) any {
	switch t := v.(type) {
	case map[string]any:
		return s.decryptCredentialMap(ctx, t)
	case []any:
		arr := make([]any, len(t))
		for i := range t {
			arr[i] = s.decryptCredentialValue(ctx, t[i])
		}
		return arr
	case string:
		const prefix = "enc::"
		if !strings.HasPrefix(t, prefix) {
			return t
		}
		key := strings.TrimSpace(os.Getenv("MAS_CREDENTIALS_KEY"))
		if key == "" {
			return t
		}
		encoded := strings.TrimSpace(strings.TrimPrefix(t, prefix))
		if encoded == "" {
			return ""
		}
		var plain string
		if err := s.DB.QueryRowContext(ctx, `
			SELECT pgp_sym_decrypt(decode($1, 'base64'), $2::text)
		`, encoded, key).Scan(&plain); err != nil {
			return t
		}
		return plain
	default:
		return v
	}
}

func pickSecretKeys(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok {
			if vv := strings.TrimSpace(v); vv != "" {
				return vv
			}
		}
	}
	return ""
}
