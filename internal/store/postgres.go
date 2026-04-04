package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"swarm/internal/config"
)

type PostgresStore struct {
	DB *sql.DB

	schemaCapsMu    sync.RWMutex
	schemaCaps      StoreSchemaCapabilities
	schemaCapsBound bool
}

func DSNFromConfig(cfg config.DatabaseConfig) string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	parts := []string{
		fmt.Sprintf("host=%s", cfg.Host),
		fmt.Sprintf("port=%d", cfg.Port),
		fmt.Sprintf("dbname=%s", cfg.Name),
		fmt.Sprintf("sslmode=%s", sslMode),
	}
	if cfg.User != "" {
		parts = append(parts, fmt.Sprintf("user=%s", cfg.User))
	}
	if cfg.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", cfg.Password))
	}
	return strings.Join(parts, " ")
}

func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	// Safe defaults; callers can still override pool settings afterward.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &PostgresStore{DB: db}, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.DB.PingContext(ctx)
}

func (s *PostgresStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for schema ddl")
	}
	if len(plans) == 0 {
		return nil
	}
	if _, err := s.DB.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("ensure pgcrypto extension: %w", err)
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin schema ddl tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("ensure %s table %s: %w", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName), err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema ddl tx: %w", err)
	}
	committed = true
	if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) ensureSchemaCompatibilityColumns(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasTable("agent_turns") {
		goto ensureEntityState
	}
	if !catalog.hasColumns("agent_turns", "turn_blocks") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agent_turns ADD COLUMN IF NOT EXISTS turn_blocks JSONB NOT NULL DEFAULT '[]'::jsonb`); err != nil {
			return fmt.Errorf("ensure agent_turns.turn_blocks column: %w", err)
		}
	}
	if err := s.ensureConversationAuditTable(ctx); err != nil {
		return err
	}
ensureEntityState:
	if !catalog.hasTable("entity_state") {
		return nil
	}
	if !catalog.hasColumns("entity_state", "subject_id") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE entity_state ADD COLUMN IF NOT EXISTS subject_id UUID`); err != nil {
			return fmt.Errorf("ensure entity_state.subject_id column: %w", err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_entity_subject ON entity_state(subject_id) WHERE subject_id IS NOT NULL`); err != nil {
		return fmt.Errorf("ensure entity_state.subject_id index: %w", err)
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func (s *PostgresStore) ensureConversationAuditTable(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	hasAuditTable := catalog.hasTable("agent_conversation_audits")
	hasAuditRunID := catalog.hasColumns("agent_conversation_audits", "run_id")
	hasRunsTable := catalog.hasColumns("runs", "run_id")
	if !hasAuditTable {
		if _, err := s.DB.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS agent_conversation_audits (
				session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				agent_id TEXT NOT NULL,
				entity_id UUID,
				flow_instance TEXT,
				scope_key TEXT,
				scope TEXT NOT NULL DEFAULT 'global',
				conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
				turn_count INTEGER NOT NULL DEFAULT 0,
				runtime_mode TEXT NOT NULL DEFAULT 'task',
				runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
				status TEXT NOT NULL DEFAULT 'active',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				CHECK (runtime_mode = 'task')
			)
		`); err != nil {
			return fmt.Errorf("ensure agent_conversation_audits table: %w", err)
		}
		hasAuditTable = true
	}
	if hasRunsTable && !hasAuditRunID {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agent_conversation_audits ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES runs(run_id)`); err != nil {
			return fmt.Errorf("ensure agent_conversation_audits.run_id column: %w", err)
		}
		hasAuditRunID = true
	}
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_agent_conversation_audits_agent ON agent_conversation_audits(agent_id, updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_conversation_audits_status ON agent_conversation_audits(status, updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_conversation_audits_entity ON agent_conversation_audits(entity_id) WHERE entity_id IS NOT NULL`,
	} {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure agent_conversation_audits indexes: %w", err)
		}
	}
	if hasAuditRunID {
		if _, err := s.DB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_agent_conversation_audits_run ON agent_conversation_audits(run_id, updated_at) WHERE run_id IS NOT NULL`); err != nil {
			return fmt.Errorf("ensure agent_conversation_audits.run_id index: %w", err)
		}
	}
	if err := s.migrateLegacyTaskConversationAudits(ctx); err != nil {
		return err
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func (s *PostgresStore) migrateLegacyTaskConversationAudits(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasTable("agent_sessions") || !catalog.hasTable("agent_conversation_audits") {
		return nil
	}
	sessionHasRunID := catalog.hasColumns("agent_sessions", "run_id")
	auditHasRunID := catalog.hasColumns("agent_conversation_audits", "run_id")
	insert := `
		INSERT INTO agent_conversation_audits (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		)
		SELECT
			session_id, agent_id, entity_id, flow_instance, NULLIF(scope_key, ''), scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		FROM agent_sessions
		WHERE runtime_mode = 'task'
	`
	if sessionHasRunID && auditHasRunID {
		insert = `
			INSERT INTO agent_conversation_audits (
				session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			SELECT
				session_id, run_id, agent_id, entity_id, flow_instance, NULLIF(scope_key, ''), scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			FROM agent_sessions
			WHERE runtime_mode = 'task'
		`
	}
	insert += `
		ON CONFLICT (session_id) DO UPDATE SET
			agent_id = EXCLUDED.agent_id,
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			scope_key = EXCLUDED.scope_key,
			scope = EXCLUDED.scope,
			conversation = EXCLUDED.conversation,
			turn_count = EXCLUDED.turn_count,
			runtime_mode = EXCLUDED.runtime_mode,
			runtime_state = EXCLUDED.runtime_state,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at
	`
	if auditHasRunID {
		insert += `,
			run_id = COALESCE(EXCLUDED.run_id, agent_conversation_audits.run_id)
		`
	}
	if _, err := s.DB.ExecContext(ctx, insert); err != nil {
		return fmt.Errorf("migrate task conversation audits: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM agent_sessions WHERE runtime_mode = 'task'`); err != nil {
		return fmt.Errorf("delete legacy task sessions: %w", err)
	}
	return nil
}
