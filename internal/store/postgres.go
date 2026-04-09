package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
)

type PostgresStore struct {
	DB *sql.DB

	schemaCapsMu    sync.RWMutex
	schemaCaps      StoreSchemaCapabilities
	schemaCapsBound bool

	eventPayloadValidator EventPayloadValidator

	scheduleClaimMu   sync.Mutex
	scheduleClaimConn *sql.Conn
	scheduleClaimKeys map[string]struct{}
}

type EventPayloadValidator func(eventType string, payload []byte) error

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

func (*PostgresStore) SupportsPersistedReplay() bool { return true }

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
	if schemaDDLIncludesPlatformTables(plans) {
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return fmt.Errorf("ensure platform-table compatibility prerequisites: %w", err)
		}
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
	if schemaDDLIncludesPlatformTables(plans) {
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return fmt.Errorf("ensure platform-table compatibility aftermath: %w", err)
		}
	}
	return nil
}

func schemaDDLIncludesPlatformTables(plans []SchemaTableDDL) bool {
	for _, plan := range plans {
		if strings.TrimSpace(plan.SchemaKind) == "platform_spec" {
			return true
		}
	}
	return false
}

func (s *PostgresStore) ensureSchemaCompatibilityColumns(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if catalog.hasTable("agent_turns") {
		if !catalog.hasColumns("agent_turns", "turn_blocks") {
			if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agent_turns ADD COLUMN IF NOT EXISTS turn_blocks JSONB NOT NULL DEFAULT '[]'::jsonb`); err != nil {
				return fmt.Errorf("ensure agent_turns.turn_blocks column: %w", err)
			}
		}
	}
	if catalog.hasTable("agent_conversation_audits") || catalog.hasTable("agent_turns") {
		if err := s.ensureConversationAuditTable(ctx); err != nil {
			return err
		}
	}
	if err := s.ensureAgentSessionTerminationMetadata(ctx); err != nil {
		return err
	}
	if err := s.ensureAgentRuntimeDescriptorColumn(ctx); err != nil {
		return err
	}
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

func (s *PostgresStore) ensureAgentSessionTerminationMetadata(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasTable("agent_sessions") {
		return nil
	}
	for _, stmt := range []string{
		`ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS termination_reason TEXT`,
		`ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS termination_detail TEXT`,
		`ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS successor_session_id UUID REFERENCES agent_sessions(session_id)`,
		`ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS terminated_at TIMESTAMPTZ`,
	} {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure agent_sessions termination metadata columns: %w", err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE agent_sessions
		SET termination_reason = COALESCE(NULLIF(termination_reason, ''), 'legacy'),
		    terminated_at = COALESCE(terminated_at, updated_at, created_at)
		WHERE status = 'terminated'
		  AND (NULLIF(termination_reason, '') IS NULL OR terminated_at IS NULL)
	`); err != nil {
		return fmt.Errorf("backfill agent_sessions termination metadata: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		DO $$
		DECLARE rec RECORD;
		BEGIN
			FOR rec IN
				SELECT c.conname
				FROM pg_constraint c
				WHERE c.conrelid = 'agent_sessions'::regclass
				  AND c.contype = 'u'
				  AND c.conkey = ARRAY[
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'agent_sessions'::regclass AND attname = 'agent_id'),
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'agent_sessions'::regclass AND attname = 'scope_key')
				  ]
			LOOP
				EXECUTE format('ALTER TABLE agent_sessions DROP CONSTRAINT %I', rec.conname);
			END LOOP;
		END
		$$;
	`); err != nil {
		return fmt.Errorf("drop legacy agent_sessions uniqueness constraint: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS agent_sessions_nonterminated_unique
		ON agent_sessions (agent_id, scope_key)
		WHERE status <> 'terminated'
	`); err != nil {
		return fmt.Errorf("ensure agent_sessions nonterminated unique index: %w", err)
	}
	for name, statement := range map[string]string{
		"agent_sessions_termination_reason_enum_check": `
			ALTER TABLE agent_sessions
			ADD CONSTRAINT agent_sessions_termination_reason_enum_check
			CHECK (
				termination_reason IS NULL OR
				termination_reason IN ('normal', 'cancelled', 'failed', 'orphaned', 'contaminated', 'legacy')
			)
		`,
		"agent_sessions_status_termination_check": `
			ALTER TABLE agent_sessions
			ADD CONSTRAINT agent_sessions_status_termination_check
			CHECK (
				(status IN ('active', 'suspended') AND termination_reason IS NULL AND termination_detail IS NULL AND successor_session_id IS NULL AND terminated_at IS NULL)
				OR
				(status = 'terminated' AND termination_reason IS NOT NULL AND terminated_at IS NOT NULL)
			)
		`,
		"agent_sessions_termination_detail_requires_reason_check": `
			ALTER TABLE agent_sessions
			ADD CONSTRAINT agent_sessions_termination_detail_requires_reason_check
			CHECK (termination_detail IS NULL OR termination_reason IS NOT NULL)
		`,
		"agent_sessions_successor_requires_terminated_check": `
			ALTER TABLE agent_sessions
			ADD CONSTRAINT agent_sessions_successor_requires_terminated_check
			CHECK (successor_session_id IS NULL OR status = 'terminated')
		`,
		"agent_sessions_successor_not_self_check": `
			ALTER TABLE agent_sessions
			ADD CONSTRAINT agent_sessions_successor_not_self_check
			CHECK (successor_session_id IS NULL OR successor_session_id <> session_id)
		`,
	} {
		exists, err := namedTableConstraintExists(ctx, s.DB, "agent_sessions", name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := s.DB.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure %s: %w", name, err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION enforce_agent_session_termination_invariants()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		DECLARE
			succ_agent TEXT;
			succ_scope TEXT;
			succ_scope_key TEXT;
			creates_cycle BOOLEAN;
		BEGIN
			IF TG_OP = 'UPDATE' THEN
				IF NEW.agent_id IS DISTINCT FROM OLD.agent_id OR NEW.scope IS DISTINCT FROM OLD.scope OR NEW.scope_key IS DISTINCT FROM OLD.scope_key THEN
					RAISE EXCEPTION 'agent_sessions identity fields are immutable';
				END IF;
				IF OLD.termination_reason IS NOT NULL AND NEW.termination_reason IS DISTINCT FROM OLD.termination_reason THEN
					RAISE EXCEPTION 'agent_sessions termination_reason is immutable once set';
				END IF;
				IF OLD.terminated_at IS NOT NULL AND NEW.terminated_at IS DISTINCT FROM OLD.terminated_at THEN
					RAISE EXCEPTION 'agent_sessions terminated_at is immutable once set';
				END IF;
			END IF;
			IF NEW.termination_reason = 'legacy' AND (TG_OP = 'INSERT' OR COALESCE(OLD.termination_reason, '') <> 'legacy') THEN
				RAISE EXCEPTION 'agent_sessions termination_reason legacy is reserved for migration backfill';
			END IF;
			IF NEW.successor_session_id IS NOT NULL THEN
				SELECT agent_id, scope, scope_key
				INTO succ_agent, succ_scope, succ_scope_key
				FROM agent_sessions
				WHERE session_id = NEW.successor_session_id;
				IF NOT FOUND THEN
					RAISE EXCEPTION 'agent_sessions successor_session_id must reference an existing session';
				END IF;
				IF succ_agent IS DISTINCT FROM NEW.agent_id OR succ_scope IS DISTINCT FROM NEW.scope OR succ_scope_key IS DISTINCT FROM NEW.scope_key THEN
					RAISE EXCEPTION 'agent_sessions successor_session_id must reference the same agent/scope/scope_key lineage';
				END IF;
				WITH RECURSIVE lineage AS (
					SELECT session_id, successor_session_id
					FROM agent_sessions
					WHERE session_id = NEW.successor_session_id
					UNION ALL
					SELECT s.session_id, s.successor_session_id
					FROM agent_sessions s
					JOIN lineage l ON s.session_id = l.successor_session_id
					WHERE l.successor_session_id IS NOT NULL
				)
				SELECT EXISTS (
					SELECT 1 FROM lineage WHERE session_id = NEW.session_id OR successor_session_id = NEW.session_id
				)
				INTO creates_cycle;
				IF creates_cycle THEN
					RAISE EXCEPTION 'agent_sessions successor_session_id must be acyclic';
				END IF;
			END IF;
			RETURN NEW;
		END
		$$;
	`); err != nil {
		return fmt.Errorf("ensure agent_sessions invariant function: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		DROP TRIGGER IF EXISTS agent_sessions_invariant_write_guard ON agent_sessions;
		CREATE TRIGGER agent_sessions_invariant_write_guard
		BEFORE INSERT OR UPDATE ON agent_sessions
		FOR EACH ROW
		EXECUTE FUNCTION enforce_agent_session_termination_invariants();
	`); err != nil {
		return fmt.Errorf("ensure agent_sessions invariant trigger: %w", err)
	}
	return nil
}

func namedTableConstraintExists(ctx context.Context, db *sql.DB, tableName, constraintName string) (bool, error) {
	if db == nil {
		return false, nil
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_constraint
			WHERE conrelid = to_regclass($1)
			  AND conname = $2
		)
	`, strings.TrimSpace(tableName), strings.TrimSpace(constraintName)).Scan(&exists); err != nil {
		return false, fmt.Errorf("query constraint %s on %s: %w", strings.TrimSpace(constraintName), strings.TrimSpace(tableName), err)
	}
	return exists, nil
}

func (s *PostgresStore) ensureAgentRuntimeDescriptorColumn(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasTable("agents") {
		return nil
	}
	if !catalog.hasColumns("agents", "runtime_descriptor") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agents ADD COLUMN IF NOT EXISTS runtime_descriptor JSONB NOT NULL DEFAULT '{}'::jsonb`); err != nil {
			return fmt.Errorf("ensure agents.runtime_descriptor column: %w", err)
		}
	}
	if err := s.migrateLegacyAgentRuntimeDescriptors(ctx); err != nil {
		return err
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func (s *PostgresStore) migrateLegacyAgentRuntimeDescriptors(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT agent_id, COALESCE(model_tier, ''), COALESCE(config, '{}'::jsonb), COALESCE(runtime_descriptor, '{}'::jsonb)
		FROM agents
	`)
	if err != nil {
		return fmt.Errorf("query agent runtime descriptor migration rows: %w", err)
	}
	defer rows.Close()

	type rowUpdate struct {
		agentID           string
		config            []byte
		runtimeDescriptor []byte
	}
	updates := make([]rowUpdate, 0)
	for rows.Next() {
		var (
			agentID           string
			modelTier         string
			configRaw         []byte
			runtimeDescriptor []byte
		)
		if err := rows.Scan(&agentID, &modelTier, &configRaw, &runtimeDescriptor); err != nil {
			return fmt.Errorf("scan agent runtime descriptor migration row: %w", err)
		}
		desc, ok := decodePersistedAgentRuntimeDescriptorLoose(runtimeDescriptor)
		if !ok {
			// Malformed canonical descriptors must fail closed during hydration rather than
			// being silently rewritten into a new shape by the legacy migration pass.
			continue
		}
		legacy := decodeLegacyAgentRuntimeConfig(configRaw)
		if desc.Type == "" {
			desc.Type = coalesce(legacy.Type, strings.TrimSpace(modelTier))
		}
		if desc.Mode == "" {
			desc.Mode = legacy.Mode
		}
		if desc.SessionScope == "" {
			desc.SessionScope = legacy.SessionScope
		}
		if desc.MaxTurnsPerTask == 0 {
			desc.MaxTurnsPerTask = legacy.MaxTurnsPerTask
		}
		if !desc.NativeTools.Any() {
			desc.NativeTools = legacy.NativeTools
		}
		if desc.WorkspaceClass == "" {
			desc.WorkspaceClass = legacy.WorkspaceClass
		}
		if desc.ManagerFallback == "" {
			desc.ManagerFallback = legacy.ManagerFallback
		}
		sanitizedConfig, err := sanitizeOpaqueAgentConfig(configRaw)
		if err != nil {
			return fmt.Errorf("sanitize legacy agent config for %s: %w", strings.TrimSpace(agentID), err)
		}
		nextDescriptor, err := json.Marshal(desc)
		if err != nil {
			return fmt.Errorf("marshal runtime descriptor for %s: %w", strings.TrimSpace(agentID), err)
		}
		updates = append(updates, rowUpdate{
			agentID:           agentID,
			config:            sanitizedConfig,
			runtimeDescriptor: nextDescriptor,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read agent runtime descriptor migration rows: %w", err)
	}
	if len(updates) == 0 {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin agent runtime descriptor migration tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, update := range updates {
		if _, err := tx.ExecContext(ctx, `
			UPDATE agents
			SET config = $2::jsonb,
			    runtime_descriptor = $3::jsonb
			WHERE agent_id = $1
		`, update.agentID, string(update.config), string(update.runtimeDescriptor)); err != nil {
			return fmt.Errorf("update agent runtime descriptor for %s: %w", strings.TrimSpace(update.agentID), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit agent runtime descriptor migration tx: %w", err)
	}
	committed = true
	return nil
}

func decodeLegacyAgentRuntimeConfig(raw []byte) persistedAgentRuntimeDescriptor {
	if len(raw) == 0 || !json.Valid(raw) {
		return persistedAgentRuntimeDescriptor{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return persistedAgentRuntimeDescriptor{}
	}
	desc := persistedAgentRuntimeDescriptor{
		Type:            strings.TrimSpace(stringValue(payload["type"])),
		Mode:            strings.TrimSpace(stringValue(payload["mode"])),
		SessionScope:    strings.TrimSpace(stringValue(payload["session_scope"])),
		MaxTurnsPerTask: intValue(payload["max_turns_per_task"]),
		NativeTools:     nativeToolConfigValue(payload["native_tools"]),
		WorkspaceClass:  strings.TrimSpace(stringValue(payload["workspace_class"])),
		ManagerFallback: strings.TrimSpace(stringValue(payload["manager_fallback"])),
	}
	if desc.MaxTurnsPerTask == 0 {
		if constraints, ok := payload["constraints"].(map[string]any); ok {
			desc.MaxTurnsPerTask = intValue(constraints["max_turns_per_task"])
		}
	}
	return desc
}

func stringValue(v any) string {
	if value, ok := v.(string); ok {
		return value
	}
	return ""
}

func intValue(v any) int {
	switch typed := v.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	case json.Number:
		out, _ := typed.Int64()
		return int(out)
	default:
		return 0
	}
}

func nativeToolConfigValue(v any) runtimeactors.NativeToolConfig {
	items, ok := v.(map[string]any)
	if !ok {
		return runtimeactors.NativeToolConfig{}
	}
	return runtimeactors.NativeToolConfig{
		Bash:      boolValue(items["bash"]),
		WebSearch: boolValue(items["web_search"]),
		FileIO:    boolValue(items["file_io"]),
	}
}

func boolValue(v any) bool {
	value, _ := v.(bool)
	return value
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
