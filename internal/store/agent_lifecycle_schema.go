package store

import (
	"context"
	"fmt"
)

func (s *PostgresStore) ensurePostgresAgentLifecycleColumns(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT to_regclass('public.agents') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("inspect agents lifecycle table: %w", err)
	}
	if !exists {
		return nil
	}
	statements := []string{
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_phase TEXT NOT NULL DEFAULT 'registered'`,
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_generation BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_runtime_epoch BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_config_revision TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_run_mode TEXT NOT NULL DEFAULT 'stopped'`,
		`ALTER TABLE agents ADD COLUMN IF NOT EXISTS lifecycle_last_transition_id UUID`,
	}
	for _, statement := range statements {
		if _, err := s.DB.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure postgres agent lifecycle column: %w", err)
		}
	}
	_, err := s.BindSchemaCapabilities(ctx)
	return err
}

func (s *SQLiteSchemaStore) ensureSQLiteAgentLifecycleColumns(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite schema store is required")
	}
	catalog, err := loadSQLiteSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasTable("agents") {
		return nil
	}
	columns := []struct {
		name       string
		definition string
	}{
		{"lifecycle_phase", `TEXT NOT NULL DEFAULT 'registered'`},
		{"lifecycle_generation", `INTEGER NOT NULL DEFAULT 0`},
		{"lifecycle_runtime_epoch", `INTEGER NOT NULL DEFAULT 0`},
		{"lifecycle_config_revision", `TEXT NOT NULL DEFAULT ''`},
		{"lifecycle_run_mode", `TEXT NOT NULL DEFAULT 'stopped'`},
		{"lifecycle_last_transition_id", `TEXT`},
	}
	for _, column := range columns {
		if catalog.hasColumns("agents", column.name) {
			continue
		}
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agents ADD COLUMN `+column.name+` `+column.definition); err != nil {
			return fmt.Errorf("ensure sqlite agents.%s: %w", column.name, err)
		}
	}
	return nil
}
