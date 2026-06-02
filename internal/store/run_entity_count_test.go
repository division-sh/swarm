package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func seedPostgresEntityStateRows(t *testing.T, db *sql.DB, ctx context.Context, runID string, entityIDs ...string) {
	t.Helper()
	for idx, entityID := range entityIDs {
		slug := fmt.Sprintf("entity-%d-%s", idx, entityID[:8])
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, 'test-flow', 'default', $3, $4, 'ready',
				'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $5, $5, $5
			)
		`, runID, entityID, slug, slug, time.Now().UTC()); err != nil {
			t.Fatalf("seed postgres entity_state row %s: %v", entityID, err)
		}
	}
}

func seedSQLiteEntityStateRows(t *testing.T, db *sql.DB, ctx context.Context, runID string, entityIDs ...string) {
	t.Helper()
	for idx, entityID := range entityIDs {
		slug := fmt.Sprintf("entity-%d-%s", idx, entityID[:8])
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			) VALUES (
				?, ?, 'test-flow', 'default', ?, ?, 'ready',
				'{}', '{}', '{}', 1, ?, ?, ?
			)
		`, runID, entityID, slug, slug, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
			t.Fatalf("seed sqlite entity_state row %s: %v", entityID, err)
		}
	}
}
