package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/store/platformschema"
	_ "github.com/lib/pq"
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

func DSNFromConfig(cfg config.DatabaseConfig, password string) string {
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = "swarm"
	}
	sslMode := strings.TrimSpace(cfg.SSLMode)
	if sslMode == "" {
		sslMode = "disable"
	}
	user := strings.TrimSpace(cfg.User)
	if user == "" {
		user = "postgres"
	}
	parts := []string{
		postgresKeywordParam("host", host),
		fmt.Sprintf("port=%d", port),
		postgresKeywordParam("dbname", name),
		postgresKeywordParam("sslmode", sslMode),
		postgresKeywordParam("user", user),
	}
	if password != "" {
		parts = append(parts, postgresKeywordParam("password", password))
	}
	return strings.Join(parts, " ")
}

func postgresKeywordParam(key, value string) string {
	if value == "" {
		return key + "="
	}
	return fmt.Sprintf("%s='%s'", key, escapePostgresKeywordValue(value))
}

func escapePostgresKeywordValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
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
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
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
	if schemaDDLIncludesPlatformTables(plans) {
		if err := ensurePostgresCanonicalFailureSchema(ctx, s.DB); err != nil {
			return fmt.Errorf("migrate canonical runtime failures: %w", err)
		}
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return fmt.Errorf("ensure platform-table compatibility prerequisites: %w", err)
		}
	}
	if err := platformschema.EnsurePostgresTables(ctx, s.DB, plans, func(plan SchemaTableDDL, cause error) error {
		return s.outdatedSchemaErrorForPlan(ctx, plan, cause)
	}); err != nil {
		return err
	}
	if schemaDDLIncludesPlatformTables(plans) {
		if err := ensurePostgresCanonicalFailureSchema(ctx, s.DB); err != nil {
			return fmt.Errorf("validate canonical runtime failures: %w", err)
		}
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return fmt.Errorf("ensure platform-table compatibility aftermath: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) outdatedSchemaErrorForPlan(ctx context.Context, plan SchemaTableDDL, cause error) error {
	if s == nil || s.DB == nil {
		return nil
	}
	tableName := strings.TrimSpace(plan.TableName)
	if tableName == "" {
		return nil
	}
	expectedColumns := schemaDDLPlanColumnNames(plan)
	if len(expectedColumns) == 0 {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil || !catalog.hasTable(tableName) {
		return nil
	}
	missingColumns := make([]string, 0)
	for _, columnName := range expectedColumns {
		if !catalog.hasColumns(tableName, columnName) {
			missingColumns = append(missingColumns, columnName)
		}
	}
	if len(missingColumns) == 0 {
		return nil
	}
	return &OutdatedSchemaError{
		SchemaKind:     strings.TrimSpace(plan.SchemaKind),
		TableName:      tableName,
		MissingColumns: missingColumns,
		Cause:          cause,
	}
}

func schemaDDLIncludesPlatformTables(plans []SchemaTableDDL) bool {
	return platformschema.IncludesPlatformTables(plans)
}

func (s *PostgresStore) ensureSchemaCompatibilityColumns(ctx context.Context) error {
	if err := s.ensurePostgresAgentLifecycleColumns(ctx); err != nil {
		return err
	}
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
	if err := s.ensureAgentSessionTerminationMetadata(ctx); err != nil {
		return err
	}
	if catalog.hasTable("agent_conversation_audits") || catalog.hasTable("agent_turns") {
		if err := s.ensureConversationAuditTable(ctx); err != nil {
			return err
		}
	}
	if err := s.ensureAgentModelAliasColumn(ctx); err != nil {
		return err
	}
	if err := s.ensureAgentRuntimeDescriptorColumn(ctx); err != nil {
		return err
	}
	if catalog.hasTable("agents") && catalog.hasColumns("agents", "llm_backend") {
		if err := s.ensureAgentLLMBackendProfiles(ctx); err != nil {
			return err
		}
	}
	if catalog.hasTable("runs") {
		if err := s.ensureRunBundleSourceSchema(ctx, catalog); err != nil {
			return err
		}
		catalog, err = loadSchemaColumnCatalog(ctx, s.DB)
		if err != nil {
			return err
		}
	}
	if err := s.ensureRunForkBundleHashSelectionSchema(ctx, catalog); err != nil {
		return err
	}
	if err := s.ensureMailboxDeferredUntilSchema(ctx, catalog); err != nil {
		return err
	}
	if catalog.hasTable("events") {
		for _, stmt := range []struct {
			column string
			sql    string
		}{
			{"source_route", `ALTER TABLE events ADD COLUMN IF NOT EXISTS source_route JSONB NOT NULL DEFAULT '{}'::jsonb`},
			{"target_route", `ALTER TABLE events ADD COLUMN IF NOT EXISTS target_route JSONB NOT NULL DEFAULT '{}'::jsonb`},
			{"target_set", `ALTER TABLE events ADD COLUMN IF NOT EXISTS target_set JSONB NOT NULL DEFAULT '[]'::jsonb`},
		} {
			if !catalog.hasColumns("events", stmt.column) {
				if _, err := s.DB.ExecContext(ctx, stmt.sql); err != nil {
					return fmt.Errorf("ensure events.%s column: %w", stmt.column, err)
				}
			}
		}
	}
	if catalog.hasTable("event_deliveries") && !catalog.hasColumns("event_deliveries", "delivery_target_route") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE event_deliveries ADD COLUMN IF NOT EXISTS delivery_target_route JSONB NOT NULL DEFAULT '{}'::jsonb`); err != nil {
			return fmt.Errorf("ensure event_deliveries.delivery_target_route column: %w", err)
		}
	}
	if catalog.hasTable("event_deliveries") && !catalog.hasColumns("event_deliveries", "delivery_context") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE event_deliveries ADD COLUMN IF NOT EXISTS delivery_context JSONB NOT NULL DEFAULT '{}'::jsonb`); err != nil {
			return fmt.Errorf("ensure event_deliveries.delivery_context column: %w", err)
		}
	}
	if catalog.hasTable("reply_contexts") {
		for _, column := range []struct {
			table string
			name  string
		}{
			{table: "activity_attempts", name: "reply_context_id"},
			{table: "timers", name: "reply_context_id"},
			{table: "mailbox", name: "reply_context_id"},
		} {
			if !catalog.hasTable(column.table) || catalog.hasColumns(column.table, column.name) {
				continue
			}
			if _, err := s.DB.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT REFERENCES reply_contexts(reply_context_id)`, column.table, column.name)); err != nil {
				return fmt.Errorf("ensure %s.%s column: %w", column.table, column.name, err)
			}
		}
	}
	if catalog.hasTable("activity_attempts") {
		for _, column := range []struct {
			name string
			sql  string
		}{
			{name: "loop_generation", sql: `ALTER TABLE activity_attempts ADD COLUMN IF NOT EXISTS loop_generation JSONB NOT NULL DEFAULT '{}'::jsonb`},
			{name: "loop_stage", sql: `ALTER TABLE activity_attempts ADD COLUMN IF NOT EXISTS loop_stage TEXT`},
		} {
			if catalog.hasColumns("activity_attempts", column.name) {
				continue
			}
			if _, err := s.DB.ExecContext(ctx, column.sql); err != nil {
				return fmt.Errorf("ensure activity_attempts.%s column: %w", column.name, err)
			}
		}
	}
	if catalog.hasTable("event_receipts") && catalog.hasColumns("event_receipts", "event_id", "subscriber_type", "subscriber_id") {
		if err := s.ensureEventReceiptsTypedSubscriberIdentity(ctx); err != nil {
			return err
		}
	}
	if catalog.hasTable("spend_ledger") && !catalog.hasColumns("spend_ledger", "usage_accounting") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS usage_accounting TEXT NOT NULL DEFAULT 'estimated' CHECK (usage_accounting IN ('exact', 'estimated'))`); err != nil {
			return fmt.Errorf("ensure spend_ledger.usage_accounting column: %w", err)
		}
	}
	if catalog.hasTable("spend_ledger") {
		for _, stmt := range []struct {
			column string
			sql    string
		}{
			{"model_alias", `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS model_alias TEXT NOT NULL DEFAULT 'unknown'`},
			{"backend_profile", `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS backend_profile TEXT NOT NULL DEFAULT 'unknown'`},
			{"provider", `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'unknown'`},
			{"transport", `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS transport TEXT NOT NULL DEFAULT 'unknown'`},
			{"resolved_model", `ALTER TABLE spend_ledger ADD COLUMN IF NOT EXISTS resolved_model TEXT NOT NULL DEFAULT 'unknown'`},
		} {
			if !catalog.hasColumns("spend_ledger", stmt.column) {
				if _, err := s.DB.ExecContext(ctx, stmt.sql); err != nil {
					return fmt.Errorf("ensure spend_ledger.%s column: %w", stmt.column, err)
				}
			}
		}
		if _, err := s.DB.ExecContext(ctx, `
			UPDATE spend_ledger
			SET resolved_model = model
			WHERE BTRIM(COALESCE(resolved_model, '')) = ''
			   OR resolved_model = 'unknown';
		`); err != nil {
			return fmt.Errorf("backfill spend_ledger.resolved_model: %w", err)
		}
	}
	if !catalog.hasTable("entity_state") {
		return nil
	}
	if _, err := s.DB.ExecContext(ctx, `DROP INDEX IF EXISTS idx_entity_subject`); err != nil {
		return fmt.Errorf("drop deprecated entity_state.subject_id index: %w", err)
	}
	if catalog.hasColumns("entity_state", "subject_id") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE entity_state DROP COLUMN IF EXISTS subject_id`); err != nil {
			return fmt.Errorf("drop deprecated entity_state.subject_id column: %w", err)
		}
	}
	if !catalog.hasColumns("entity_state", "run_id") {
		var rows int
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state`).Scan(&rows); err != nil {
			return fmt.Errorf("inspect entity_state rows before run_id migration: %w", err)
		}
		if rows > 0 {
			return fmt.Errorf("entity_state.run_id migration requires explicit run ownership; refusing to infer run_id for %d existing rows", rows)
		}
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE entity_state ADD COLUMN run_id UUID NOT NULL REFERENCES runs(run_id)`); err != nil {
			return fmt.Errorf("add entity_state.run_id column: %w", err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, `
		DO $$
		DECLARE pk_name TEXT;
		BEGIN
			SELECT c.conname
			INTO pk_name
			FROM pg_constraint c
			WHERE c.conrelid = 'entity_state'::regclass
			  AND c.contype = 'p'
			  AND NOT (
				c.conkey = ARRAY[
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'entity_state'::regclass AND attname = 'run_id'),
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'entity_state'::regclass AND attname = 'entity_id')
				]
			  );
			IF pk_name IS NOT NULL THEN
				EXECUTE format('ALTER TABLE entity_state DROP CONSTRAINT %I', pk_name);
			END IF;
			IF NOT EXISTS (
				SELECT 1
				FROM pg_constraint c
				WHERE c.conrelid = 'entity_state'::regclass
				  AND c.contype = 'p'
				  AND c.conkey = ARRAY[
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'entity_state'::regclass AND attname = 'run_id'),
					(SELECT attnum FROM pg_attribute WHERE attrelid = 'entity_state'::regclass AND attname = 'entity_id')
				  ]
			) THEN
				ALTER TABLE entity_state ADD PRIMARY KEY (run_id, entity_id);
			END IF;
		END
		$$;
	`); err != nil {
		return fmt.Errorf("ensure entity_state run-scoped primary key: %w", err)
	}
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS idx_entity_flow`,
		`DROP INDEX IF EXISTS idx_entity_state`,
		`DROP INDEX IF EXISTS idx_entity_type`,
		`DROP INDEX IF EXISTS idx_entity_slug`,
	} {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure entity_state run-scoped indexes: %w", err)
		}
	}
	catalog, err = loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	indexes := []struct {
		stmt    string
		columns []string
	}{
		{`CREATE INDEX IF NOT EXISTS idx_entity_flow ON entity_state(run_id, flow_instance, current_state)`, []string{"run_id", "flow_instance", "current_state"}},
		{`CREATE INDEX IF NOT EXISTS idx_entity_state ON entity_state(run_id, current_state)`, []string{"run_id", "current_state"}},
		{`CREATE INDEX IF NOT EXISTS idx_entity_type ON entity_state(run_id, entity_type)`, []string{"run_id", "entity_type"}},
		{`CREATE INDEX IF NOT EXISTS idx_entity_slug ON entity_state(run_id, slug) WHERE slug IS NOT NULL`, []string{"run_id", "slug"}},
		{`CREATE INDEX IF NOT EXISTS idx_entity_cross_run ON entity_state(entity_id)`, []string{"entity_id"}},
	}
	for _, index := range indexes {
		if !catalog.hasColumns("entity_state", index.columns...) {
			continue
		}
		if _, err := s.DB.ExecContext(ctx, index.stmt); err != nil {
			return fmt.Errorf("ensure entity_state run-scoped indexes: %w", err)
		}
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func (s *PostgresStore) ensureMailboxDeferredUntilSchema(ctx context.Context, catalog schemaColumnCatalog) error {
	if s == nil || s.DB == nil || !catalog.hasTable("mailbox") {
		return nil
	}
	if !catalog.hasColumns("mailbox", "deferred_until") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE mailbox ADD COLUMN IF NOT EXISTS deferred_until TIMESTAMPTZ`); err != nil {
			return fmt.Errorf("ensure mailbox.deferred_until column: %w", err)
		}
	}
	if !catalog.hasColumns("mailbox", "status", "decision") {
		return nil
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE mailbox
		SET status = 'pending',
		    decision = NULL,
		    deferred_until = NULL
		WHERE status = 'decided'
		  AND COALESCE(decision, '') = 'deferred'
	`)
	if err != nil {
		return fmt.Errorf("normalize postgres legacy deferred mailbox rows: %w", err)
	}
	if count, err := res.RowsAffected(); err == nil && count > 0 {
		log.Printf("normalized %d postgres legacy deferred mailbox rows to pending with deferred_until unset", count)
	}
	return nil
}

func (s *PostgresStore) ensureAgentLLMBackendProfiles(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	if _, err := s.DB.ExecContext(ctx, `
		DO $$
		DECLARE check_name TEXT;
		BEGIN
			FOR check_name IN
				SELECT c.conname
				FROM pg_constraint c
				WHERE c.conrelid = 'agents'::regclass
				  AND c.contype = 'c'
				  AND pg_get_constraintdef(c.oid) LIKE '%llm_backend%'
			LOOP
				EXECUTE format('ALTER TABLE agents DROP CONSTRAINT %I', check_name);
			END LOOP;
		END
		$$;
	`); err != nil {
		return fmt.Errorf("drop legacy agents.llm_backend constraint: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		ALTER TABLE agents ALTER COLUMN llm_backend SET DEFAULT 'anthropic';
		UPDATE agents
		SET llm_backend = CASE BTRIM(llm_backend)
			WHEN 'api' THEN 'anthropic'
			WHEN 'cli_test' THEN 'claude_cli'
			ELSE BTRIM(llm_backend)
		END
		WHERE llm_backend IS NOT NULL;
		ALTER TABLE agents
			ADD CONSTRAINT agents_llm_backend_check
			CHECK (llm_backend IN ('anthropic', 'claude_cli', 'openai_compatible', 'openai_responses', 'mock', 'local'));
	`); err != nil {
		return fmt.Errorf("migrate agents.llm_backend profiles: %w", err)
	}
	return nil
}

func (s *PostgresStore) ensureAgentModelAliasColumn(ctx context.Context) error {
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
	if !catalog.hasColumns("agents", "model") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agents ADD COLUMN IF NOT EXISTS model TEXT`); err != nil {
			return fmt.Errorf("ensure agents.model column: %w", err)
		}
	}
	catalog, err = loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if catalog.hasColumns("agents", "model_tier") {
		var unmappable int
		if err := s.DB.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM agents
			WHERE (model IS NULL OR BTRIM(model) = '')
			  AND model_tier IS NOT NULL
			  AND BTRIM(model_tier) <> ''
			  AND LOWER(BTRIM(model_tier)) NOT IN ('haiku', 'low_cost', 'sonnet', 'general', 'generic')
		`).Scan(&unmappable); err != nil {
			return fmt.Errorf("inspect legacy agents.model_tier: %w", err)
		}
		if unmappable > 0 {
			return fmt.Errorf("agents.model migration cannot map %d legacy model_tier rows; use model alias cheap, regular, or frontier", unmappable)
		}
		if _, err := s.DB.ExecContext(ctx, `
			UPDATE agents
			SET model = CASE LOWER(BTRIM(model_tier))
				WHEN 'haiku' THEN 'cheap'
				WHEN 'low_cost' THEN 'cheap'
				WHEN 'sonnet' THEN 'regular'
				WHEN 'general' THEN 'regular'
				WHEN 'generic' THEN 'regular'
				ELSE NULL
			END
			WHERE (model IS NULL OR BTRIM(model) = '')
			  AND model_tier IS NOT NULL
			  AND BTRIM(model_tier) <> '';
		`); err != nil {
			return fmt.Errorf("backfill agents.model from legacy model_tier: %w", err)
		}
	}
	var missing int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE model IS NULL OR BTRIM(model) = ''`).Scan(&missing); err != nil {
		return fmt.Errorf("inspect agents.model backfill: %w", err)
	}
	if missing > 0 {
		return fmt.Errorf("agents.model migration requires explicit model alias for %d existing rows", missing)
	}
	if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agents ALTER COLUMN model SET NOT NULL`); err != nil {
		return fmt.Errorf("ensure agents.model not null: %w", err)
	}
	return nil
}

func (s *PostgresStore) ensureRunBundleSourceSchema(ctx context.Context, catalog schemaColumnCatalog) error {
	if s == nil || s.DB == nil || !catalog.hasTable("runs") {
		return nil
	}
	if !catalog.hasColumns("runs", "bundle_hash") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN IF NOT EXISTS bundle_hash TEXT`); err != nil {
			return fmt.Errorf("ensure runs.bundle_hash column: %w", err)
		}
	}
	if !catalog.hasColumns("runs", "bundle_source") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN IF NOT EXISTS bundle_source TEXT NOT NULL DEFAULT 'legacy'`); err != nil {
			return fmt.Errorf("ensure runs.bundle_source column: %w", err)
		}
	}
	if !catalog.hasColumns("runs", "bundle_fingerprint") {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN IF NOT EXISTS bundle_fingerprint TEXT`); err != nil {
			return fmt.Errorf("ensure legacy runs.bundle_fingerprint compatibility column: %w", err)
		}
	}
	for _, stmt := range []string{
		`UPDATE runs SET bundle_source = 'legacy' WHERE bundle_source IS NULL OR BTRIM(bundle_source) = ''`,
		`ALTER TABLE runs ALTER COLUMN bundle_source SET DEFAULT 'legacy'`,
		`ALTER TABLE runs ALTER COLUMN bundle_source SET NOT NULL`,
		`ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_bundle_hash_format_check`,
		`ALTER TABLE runs ADD CONSTRAINT runs_bundle_hash_format_check CHECK (bundle_hash IS NULL OR bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$')`,
		`ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_bundle_source_check`,
		`ALTER TABLE runs ADD CONSTRAINT runs_bundle_source_check CHECK (bundle_source IN ('persisted', 'ephemeral', 'deleted', 'legacy'))`,
	} {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure runs bundle source schema: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) ensureRunForkBundleHashSelectionSchema(ctx context.Context, catalog schemaColumnCatalog) error {
	if s == nil || s.DB == nil {
		return nil
	}
	for _, tableName := range []string{
		runForkSelectedContractBindingTable,
		runForkSelectedContractRouteRecoveryTable,
	} {
		if !catalog.hasTable(tableName) {
			continue
		}
		for _, stmt := range []struct {
			name string
			sql  string
		}{
			{"add bundle_hash", fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS bundle_hash TEXT`, tableName)},
			{"drop contracts_root not null", fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN contracts_root DROP NOT NULL`, tableName)},
			{"drop legacy selection checks", fmt.Sprintf(`
				DO $$
				DECLARE rec RECORD;
				BEGIN
					FOR rec IN
						SELECT c.conname
						FROM pg_constraint c
						JOIN pg_class tbl ON tbl.oid = c.conrelid
						JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
						WHERE ns.nspname = 'public'
						  AND tbl.relname = '%s'
						  AND c.contype = 'c'
						  AND pg_get_constraintdef(c.oid) LIKE '%%selected_contracts%%'
					LOOP
						EXECUTE format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I', 'public', '%s', rec.conname);
					END LOOP;
				END
				$$;
			`, tableName, tableName)},
			{"add mode check", fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s_mode_check CHECK (mode IN ('selected_contracts', 'bundle_hash'))`, tableName, tableName)},
			{"add selection shape check", fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s_selection_shape_check CHECK (
				(
					mode = 'selected_contracts'
					AND NULLIF(BTRIM(COALESCE(contracts_root, '')), '') IS NOT NULL
					AND NULLIF(BTRIM(COALESCE(bundle_hash, '')), '') IS NULL
				)
				OR (
					mode = 'bundle_hash'
					AND NULLIF(BTRIM(COALESCE(contracts_root, '')), '') IS NULL
					AND bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$'
				)
			)`, tableName, tableName)},
		} {
			if _, err := s.DB.ExecContext(ctx, stmt.sql); err != nil {
				return fmt.Errorf("ensure %s %s: %w", tableName, stmt.name, err)
			}
		}
	}
	return nil
}

func (s *PostgresStore) ensureEventReceiptsTypedSubscriberIdentity(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return nil
	}
	var duplicateTypedRows int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT event_id, subscriber_type, subscriber_id
			FROM event_receipts
			GROUP BY event_id, subscriber_type, subscriber_id
			HAVING COUNT(*) > 1
		) dup
	`).Scan(&duplicateTypedRows); err != nil {
		return fmt.Errorf("inspect event_receipts typed identity duplicates: %w", err)
	}
	if duplicateTypedRows > 0 {
		return fmt.Errorf("event_receipts typed subscriber identity migration found %d duplicate typed identities", duplicateTypedRows)
	}

	var nodePipelineConflicts int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts legacy
		WHERE legacy.subscriber_type = 'node'
		  AND legacy.subscriber_id = 'node:pipeline'
		  AND COALESCE(legacy.idempotency_key, '') = 'pipeline:' || legacy.event_id::text
		  AND EXISTS (
			SELECT 1
			FROM event_receipts canonical
			WHERE canonical.event_id = legacy.event_id
			  AND canonical.subscriber_type = 'node'
			  AND canonical.subscriber_id = 'pipeline'
		  )
	`).Scan(&nodePipelineConflicts); err != nil {
		return fmt.Errorf("inspect event_receipts node:pipeline migration conflicts: %w", err)
	}
	if nodePipelineConflicts > 0 {
		return fmt.Errorf("event_receipts typed subscriber identity migration found %d node:pipeline rows colliding with canonical node pipeline receipts", nodePipelineConflicts)
	}
	var ambiguousNodePipelineRows int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE subscriber_type = 'node'
		  AND subscriber_id = 'node:pipeline'
		  AND COALESCE(idempotency_key, '') NOT IN (
			'pipeline:' || event_id::text,
			'node:pipeline:' || event_id::text
		  )
	`).Scan(&ambiguousNodePipelineRows); err != nil {
		return fmt.Errorf("inspect ambiguous event_receipts node:pipeline rows: %w", err)
	}
	if ambiguousNodePipelineRows > 0 {
		return fmt.Errorf("event_receipts typed subscriber identity migration found %d ambiguous node:pipeline rows without canonical system-node idempotency proof", ambiguousNodePipelineRows)
	}

	for _, stmt := range []struct {
		name string
		sql  string
	}{
		{
			name: "drop legacy event_receipts untyped unique constraints",
			sql: `
				DO $$
				DECLARE rec RECORD;
				BEGIN
					FOR rec IN
						SELECT c.conname
						FROM pg_constraint c
						JOIN pg_class tbl ON tbl.oid = c.conrelid
						JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
						WHERE ns.nspname = 'public'
						  AND tbl.relname = 'event_receipts'
						  AND c.contype = 'u'
						  AND replace(pg_get_constraintdef(c.oid), '"', '') = 'UNIQUE (event_id, subscriber_id)'
					LOOP
						EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', 'public', 'event_receipts', rec.conname);
					END LOOP;
				END
				$$;
			`,
		},
		{
			name: "drop legacy event_receipts untyped unique indexes",
			sql: `
				DO $$
				DECLARE rec RECORD;
				BEGIN
					FOR rec IN
						SELECT idx.relname AS index_name
						FROM pg_index i
						JOIN pg_class tbl ON tbl.oid = i.indrelid
						JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
						JOIN pg_class idx ON idx.oid = i.indexrelid
						LEFT JOIN pg_constraint c ON c.conindid = i.indexrelid
						WHERE ns.nspname = 'public'
						  AND tbl.relname = 'event_receipts'
						  AND i.indisunique
						  AND c.oid IS NULL
						  AND replace(pg_get_indexdef(i.indexrelid), '"', '') LIKE '% USING btree (event_id, subscriber_id)%'
					LOOP
						EXECUTE format('DROP INDEX IF EXISTS %I.%I', 'public', rec.index_name);
					END LOOP;
				END
				$$;
			`,
		},
		{
			name: "normalize event_receipts node:pipeline subscriber ids",
			sql: `
				UPDATE event_receipts
				SET subscriber_id = 'pipeline'
				WHERE subscriber_type = 'node'
				  AND subscriber_id = 'node:pipeline'
				  AND COALESCE(idempotency_key, '') = 'pipeline:' || event_id::text
			`,
		},
	} {
		if _, err := s.DB.ExecContext(ctx, stmt.sql); err != nil {
			return fmt.Errorf("%s: %w", stmt.name, err)
		}
	}

	var postMigrationDuplicates int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT event_id, subscriber_type, subscriber_id
			FROM event_receipts
			GROUP BY event_id, subscriber_type, subscriber_id
			HAVING COUNT(*) > 1
		) dup
	`).Scan(&postMigrationDuplicates); err != nil {
		return fmt.Errorf("inspect event_receipts typed identity duplicates after migration: %w", err)
	}
	if postMigrationDuplicates > 0 {
		return fmt.Errorf("event_receipts typed subscriber identity migration left %d duplicate typed identities", postMigrationDuplicates)
	}
	hasTypedIdentity, err := eventReceiptsTypedSubscriberIdentityKeyExists(ctx, s.DB)
	if err != nil {
		return err
	}
	if !hasTypedIdentity {
		if _, err := s.DB.ExecContext(ctx, `
			CREATE UNIQUE INDEX event_receipts_event_subscriber_identity_unique
			ON event_receipts (event_id, subscriber_type, subscriber_id)
		`); err != nil {
			return fmt.Errorf("ensure event_receipts typed subscriber identity unique index: %w", err)
		}
	}
	return nil
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
	if err := s.neutralizeLegacyTaskConversationSessions(ctx); err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) neutralizeLegacyTaskConversationSessions(ctx context.Context) error {
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
	if !catalog.hasColumns("agent_sessions", "termination_reason", "terminated_at") {
		return nil
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = COALESCE(NULLIF(termination_reason, ''), 'orphaned'),
		    terminated_at = COALESCE(terminated_at, updated_at, created_at, now()),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE runtime_mode = 'task'
		  AND status <> 'terminated'
	`); err != nil {
		return fmt.Errorf("neutralize legacy task conversation sessions: %w", err)
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
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE agents
		SET runtime_descriptor = jsonb_set(
			CASE
				WHEN runtime_descriptor IS NULL THEN '{}'::jsonb
				WHEN jsonb_typeof(runtime_descriptor) = 'object' THEN runtime_descriptor
				ELSE '{}'::jsonb
			END,
			'{type}',
			to_jsonb(BTRIM(model)),
			true
		)
		WHERE NULLIF(BTRIM(model), '') IS NOT NULL
		  AND (
			runtime_descriptor IS NULL
			OR (
				jsonb_typeof(runtime_descriptor) = 'object'
				AND NOT (runtime_descriptor ? 'type')
			)
		  )
	`); err != nil {
		return fmt.Errorf("backfill agents.runtime_descriptor.type from model: %w", err)
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
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}
