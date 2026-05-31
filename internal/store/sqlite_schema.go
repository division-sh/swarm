package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

type SQLiteSchemaStore struct {
	DB   *sql.DB
	path string

	schemaCapsMu    sync.RWMutex
	schemaCaps      StoreSchemaCapabilities
	schemaCapsBound bool
}

func NewSQLiteSchemaStore(path string) (*SQLiteSchemaStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("sqlite schema store path is required")
	}
	if sqlitePathIsInMemory(path) {
		return nil, fmt.Errorf("sqlite schema store must be file-backed; in-memory paths are not allowed")
	}
	cleanPath := filepath.Clean(path)
	parent := filepath.Dir(cleanPath)
	if parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite schema store parent directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", cleanPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite schema store: %w", err)
	}
	store := &SQLiteSchemaStore{DB: db, path: cleanPath}
	if err := store.configure(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqlitePathIsInMemory(path string) bool {
	value := strings.ToLower(strings.TrimSpace(path))
	return value == ":memory:" || strings.Contains(value, "mode=memory") || strings.HasPrefix(value, "file::memory:")
}

func (s *SQLiteSchemaStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *SQLiteSchemaStore) Close() error {
	if s == nil || s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

func (s *SQLiteSchemaStore) Ping(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite schema store is required")
	}
	return s.DB.PingContext(ctx)
}

func (s *SQLiteSchemaStore) configure(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite schema store is required")
	}
	if _, err := s.DB.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	return s.DB.PingContext(ctx)
}

func (s *SQLiteSchemaStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite schema store is required for schema ddl")
	}
	if len(plans) == 0 {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin sqlite schema ddl tx: %w", err)
	}
	committed := false
	agentStatements := []string{}
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, plan := range plans {
		statements, err := SQLiteStatementsForPlan(plan)
		if err != nil {
			return err
		}
		if strings.TrimSpace(plan.TableName) == "agents" {
			agentStatements = append(agentStatements, statements...)
		}
		for _, statement := range statements {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("ensure sqlite %s table %s: %w", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName), err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite schema ddl tx: %w", err)
	}
	committed = true
	if len(agentStatements) > 0 {
		if err := s.ensureSQLiteAgentLLMBackendProfiles(ctx, agentStatements); err != nil {
			return err
		}
		if err := s.ensureSQLiteAgentModelAliases(ctx); err != nil {
			return err
		}
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func (s *SQLiteSchemaStore) ensureSQLiteAgentLLMBackendProfiles(ctx context.Context, agentStatements []string) error {
	var sqlText string
	err := s.DB.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='agents'`).Scan(&sqlText)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect sqlite agents table: %w", err)
	}
	if !strings.Contains(sqlText, "llm_backend") {
		return nil
	}
	if sqliteAgentLLMBackendSchemaNeedsRebuild(sqlText) {
		return s.rebuildSQLiteAgentsLLMBackendSchema(ctx, agentStatements)
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE agents
		SET llm_backend = CASE TRIM(llm_backend)
			WHEN 'api' THEN 'anthropic'
			WHEN 'cli_test' THEN 'claude_cli'
			ELSE TRIM(llm_backend)
		END
		WHERE llm_backend IS NOT NULL;
	`); err != nil {
		return fmt.Errorf("backfill sqlite agents.llm_backend profiles: %w", err)
	}
	return nil
}

func sqliteAgentLLMBackendSchemaNeedsRebuild(sqlText string) bool {
	lower := strings.ToLower(sqlText)
	return strings.Contains(lower, "llm_backend") &&
		(strings.Contains(lower, "'api'") || strings.Contains(lower, "'cli_test'")) &&
		!strings.Contains(lower, "'anthropic'") &&
		!strings.Contains(lower, "'claude_cli'")
}

func (s *SQLiteSchemaStore) rebuildSQLiteAgentsLLMBackendSchema(ctx context.Context, agentStatements []string) error {
	createStatement := ""
	indexStatements := []string{}
	for _, statement := range agentStatements {
		statement = strings.TrimSpace(statement)
		if schemaDDLExtractTableName(statement) == "agents" {
			createStatement = statement
			continue
		}
		if strings.Contains(strings.ToLower(statement), " on agents(") || strings.Contains(strings.ToLower(statement), " on \"agents\"(") {
			indexStatements = append(indexStatements, statement)
		}
	}
	if createStatement == "" {
		return fmt.Errorf("sqlite agents llm_backend migration missing canonical agents DDL")
	}
	oldColumns, err := sqliteTableColumnList(ctx, s.DB, "agents")
	if err != nil {
		return err
	}
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve sqlite agents llm_backend migration connection: %w", err)
	}
	defer conn.Close()
	legacyAlterTableEnabled, err := sqliteConnBoolPragma(ctx, conn, "legacy_alter_table")
	if err != nil {
		return err
	}
	restorePragmas := func() error {
		_, legacyErr := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA legacy_alter_table = %s`, sqlitePragmaBoolValue(legacyAlterTableEnabled)))
		_, fkErr := conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`)
		if legacyErr != nil {
			legacyErr = fmt.Errorf("restore sqlite legacy_alter_table after agents llm_backend migration: %w", legacyErr)
		}
		if fkErr != nil {
			fkErr = fmt.Errorf("restore sqlite foreign_keys after agents llm_backend migration: %w", fkErr)
		}
		return errors.Join(legacyErr, fkErr)
	}
	pragmasRestored := false
	defer func() {
		if !pragmasRestored {
			_ = restorePragmas()
		}
	}()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("disable sqlite foreign keys for agents llm_backend migration: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA legacy_alter_table = ON`); err != nil {
		return fmt.Errorf("preserve sqlite child foreign keys during agents llm_backend migration: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin sqlite agents llm_backend migration: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agents RENAME TO agents__llm_backend_migration`); err != nil {
		return fmt.Errorf("rename legacy sqlite agents table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, createStatement); err != nil {
		return fmt.Errorf("create canonical sqlite agents table: %w", err)
	}
	newColumns, err := sqliteTableColumnListTx(ctx, tx, "agents")
	if err != nil {
		return err
	}
	copyColumns, selectExpressions := sqliteAgentLLMBackendCopyExpressions(oldColumns, newColumns)
	if len(copyColumns) == 0 {
		return fmt.Errorf("sqlite agents llm_backend migration found no copyable columns")
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO agents (%s) SELECT %s FROM agents__llm_backend_migration`,
		strings.Join(copyColumns, ", "),
		strings.Join(selectExpressions, ", "),
	)); err != nil {
		return fmt.Errorf("copy sqlite agents rows with canonical llm_backend: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE agents__llm_backend_migration`); err != nil {
		return fmt.Errorf("drop legacy sqlite agents table: %w", err)
	}
	for _, statement := range indexStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create canonical sqlite agents index: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite agents llm_backend migration: %w", err)
	}
	committed = true
	if err := restorePragmas(); err != nil {
		return err
	}
	pragmasRestored = true
	if err := sqliteForeignKeyCheck(ctx, conn); err != nil {
		return err
	}
	return nil
}

func sqliteConnBoolPragma(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	if conn == nil {
		return false, fmt.Errorf("sqlite connection is required")
	}
	var value int
	if err := conn.QueryRowContext(ctx, fmt.Sprintf(`PRAGMA %s`, name)).Scan(&value); err != nil {
		return false, fmt.Errorf("inspect sqlite %s pragma: %w", name, err)
	}
	return value != 0, nil
}

func sqlitePragmaBoolValue(enabled bool) string {
	if enabled {
		return "ON"
	}
	return "OFF"
}

func sqliteForeignKeyCheck(ctx context.Context, conn *sql.Conn) error {
	if conn == nil {
		return fmt.Errorf("sqlite connection is required")
	}
	rows, err := conn.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("check sqlite foreign keys after agents llm_backend migration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		var rowid int64
		var parent string
		var fkid int
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			return fmt.Errorf("scan sqlite foreign key check result: %w", err)
		}
		return fmt.Errorf("sqlite foreign_key_check failed after agents llm_backend migration: table=%s rowid=%d parent=%s fkid=%d", table, rowid, parent, fkid)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read sqlite foreign key check result: %w", err)
	}
	return nil
}

func sqliteTableColumnList(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quoteIdent(tableName)))
	if err != nil {
		return nil, fmt.Errorf("inspect sqlite table %s columns: %w", tableName, err)
	}
	defer rows.Close()
	return scanSQLiteTableColumns(rows, tableName)
}

func sqliteTableColumnListTx(ctx context.Context, tx *sql.Tx, tableName string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quoteIdent(tableName)))
	if err != nil {
		return nil, fmt.Errorf("inspect sqlite table %s columns: %w", tableName, err)
	}
	defer rows.Close()
	return scanSQLiteTableColumns(rows, tableName)
}

func scanSQLiteTableColumns(rows *sql.Rows, tableName string) ([]string, error) {
	columns := []string{}
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan sqlite table %s columns: %w", tableName, err)
		}
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan sqlite table %s columns: %w", tableName, err)
	}
	return columns, nil
}

func sqliteAgentLLMBackendCopyExpressions(oldColumns, newColumns []string) ([]string, []string) {
	oldSet := make(map[string]struct{}, len(oldColumns))
	for _, column := range oldColumns {
		oldSet[strings.TrimSpace(column)] = struct{}{}
	}
	copyColumns := []string{}
	selectExpressions := []string{}
	for _, column := range newColumns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		copyColumns = append(copyColumns, quoteIdent(column))
		if column == "model" {
			if _, ok := oldSet["model"]; ok {
				selectExpressions = append(selectExpressions, quoteIdent(column))
				continue
			}
			if _, ok := oldSet["model_tier"]; ok {
				selectExpressions = append(selectExpressions, `CASE LOWER(TRIM("model_tier")) WHEN 'haiku' THEN 'cheap' WHEN 'low_cost' THEN 'cheap' WHEN 'sonnet' THEN 'regular' WHEN 'general' THEN 'regular' WHEN 'generic' THEN 'regular' ELSE NULL END`)
				continue
			}
			copyColumns = copyColumns[:len(copyColumns)-1]
			continue
		}
		if _, ok := oldSet[column]; !ok {
			copyColumns = copyColumns[:len(copyColumns)-1]
			continue
		}
		if column == "llm_backend" {
			selectExpressions = append(selectExpressions, `CASE TRIM("llm_backend") WHEN 'api' THEN 'anthropic' WHEN 'cli_test' THEN 'claude_cli' ELSE TRIM("llm_backend") END`)
			continue
		}
		selectExpressions = append(selectExpressions, quoteIdent(column))
	}
	return copyColumns, selectExpressions
}

func (s *SQLiteSchemaStore) ensureSQLiteAgentModelAliases(ctx context.Context) error {
	columns, err := sqliteTableColumnList(ctx, s.DB, "agents")
	if err != nil {
		return err
	}
	hasModel := false
	hasModelTier := false
	for _, column := range columns {
		switch strings.TrimSpace(column) {
		case "model":
			hasModel = true
		case "model_tier":
			hasModelTier = true
		}
	}
	if !hasModel {
		if _, err := s.DB.ExecContext(ctx, `ALTER TABLE agents ADD COLUMN model TEXT`); err != nil {
			return fmt.Errorf("ensure sqlite agents.model column: %w", err)
		}
	}
	if hasModelTier {
		var unmappable int
		if err := s.DB.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM agents
			WHERE (model IS NULL OR TRIM(model) = '')
			  AND model_tier IS NOT NULL
			  AND TRIM(model_tier) <> ''
			  AND LOWER(TRIM(model_tier)) NOT IN ('haiku', 'low_cost', 'sonnet', 'general', 'generic');
		`).Scan(&unmappable); err != nil {
			return fmt.Errorf("inspect sqlite legacy agents.model_tier: %w", err)
		}
		if unmappable > 0 {
			return fmt.Errorf("sqlite agents.model migration cannot map %d legacy model_tier rows; use model alias cheap, regular, or frontier", unmappable)
		}
		if _, err := s.DB.ExecContext(ctx, `
			UPDATE agents
			SET model = CASE LOWER(TRIM(model_tier))
				WHEN 'haiku' THEN 'cheap'
				WHEN 'low_cost' THEN 'cheap'
				WHEN 'sonnet' THEN 'regular'
				WHEN 'general' THEN 'regular'
				WHEN 'generic' THEN 'regular'
				ELSE NULL
			END
			WHERE (model IS NULL OR TRIM(model) = '')
			  AND model_tier IS NOT NULL
			  AND TRIM(model_tier) <> '';
		`); err != nil {
			return fmt.Errorf("backfill sqlite agents.model from legacy model_tier: %w", err)
		}
	}
	var missing int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE model IS NULL OR TRIM(model) = ''`).Scan(&missing); err != nil {
		return fmt.Errorf("inspect sqlite agents.model backfill: %w", err)
	}
	if missing > 0 {
		return fmt.Errorf("sqlite agents.model migration requires explicit model alias for %d existing rows", missing)
	}
	return nil
}

func (s *SQLiteSchemaStore) BindSchemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if s == nil || s.DB == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("sqlite schema store is required")
	}
	catalog, err := loadSQLiteSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return StoreSchemaCapabilities{}, err
	}
	caps := detectStoreSchemaCapabilities(catalog)
	if caps.Events.Receipts == SchemaFlavorCanonical {
		hasTypedIdentity, err := sqliteEventReceiptsTypedSubscriberIdentityKeyExists(ctx, s.DB)
		if err != nil {
			return StoreSchemaCapabilities{}, err
		}
		caps.Events.ReceiptTypedIdentity = hasTypedIdentity
		if !hasTypedIdentity {
			caps.Events.Receipts = SchemaFlavorUnsupported
		}
	}
	s.schemaCapsMu.Lock()
	s.schemaCaps = caps
	s.schemaCapsBound = true
	s.schemaCapsMu.Unlock()
	return caps, nil
}

func (s *SQLiteSchemaStore) ResolveSchemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if s == nil || s.DB == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("sqlite schema store is required")
	}
	s.schemaCapsMu.RLock()
	if s.schemaCapsBound {
		caps := s.schemaCaps
		s.schemaCapsMu.RUnlock()
		return caps, nil
	}
	s.schemaCapsMu.RUnlock()
	return s.BindSchemaCapabilities(ctx)
}

func (s *SQLiteSchemaStore) SchemaCapabilities() StoreSchemaCapabilities {
	if s == nil {
		return StoreSchemaCapabilities{}
	}
	s.schemaCapsMu.RLock()
	defer s.schemaCapsMu.RUnlock()
	return s.schemaCaps
}

func loadSQLiteSchemaColumnCatalog(ctx context.Context, db *sql.DB) (schemaColumnCatalog, error) {
	catalog := schemaColumnCatalog{tables: map[string]map[string]struct{}{}}
	if db == nil {
		return catalog, fmt.Errorf("sqlite schema store is required")
	}
	rows, err := db.QueryContext(ctx, `
		SELECT name
		FROM sqlite_master
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return schemaColumnCatalog{}, fmt.Errorf("inspect sqlite schema tables: %w", err)
	}
	defer rows.Close()

	tableNames := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return schemaColumnCatalog{}, fmt.Errorf("scan sqlite schema table: %w", err)
		}
		tableName = strings.TrimSpace(tableName)
		if tableName != "" {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := rows.Err(); err != nil {
		return schemaColumnCatalog{}, fmt.Errorf("read sqlite schema tables: %w", err)
	}

	for _, tableName := range tableNames {
		columns, err := sqliteTableColumns(ctx, db, tableName)
		if err != nil {
			return schemaColumnCatalog{}, err
		}
		catalog.tables[tableName] = columns
	}
	return catalog, nil
}

func sqliteTableColumns(ctx context.Context, db *sql.DB, tableName string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(tableName)))
	if err != nil {
		return nil, fmt.Errorf("inspect sqlite table %s columns: %w", strings.TrimSpace(tableName), err)
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var columnName string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan sqlite table %s column: %w", strings.TrimSpace(tableName), err)
		}
		columnName = strings.TrimSpace(columnName)
		if columnName != "" {
			columns[columnName] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite table %s columns: %w", strings.TrimSpace(tableName), err)
	}
	return columns, nil
}

func sqliteEventReceiptsTypedSubscriberIdentityKeyExists(ctx context.Context, db *sql.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("sqlite schema store is required")
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quoteIdent("event_receipts")))
	if err != nil {
		return false, fmt.Errorf("inspect sqlite event_receipts indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var indexName string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &indexName, &unique, &origin, &partial); err != nil {
			return false, fmt.Errorf("scan sqlite event_receipts index: %w", err)
		}
		if unique != 1 {
			continue
		}
		columns, err := sqliteIndexColumns(ctx, db, indexName)
		if err != nil {
			return false, err
		}
		if sameStringSlice(columns, []string{"event_id", "subscriber_type", "subscriber_id"}) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read sqlite event_receipts indexes: %w", err)
	}
	return false, nil
}

func sqliteIndexColumns(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info(%s)", quoteIdent(indexName)))
	if err != nil {
		return nil, fmt.Errorf("inspect sqlite index %s columns: %w", strings.TrimSpace(indexName), err)
	}
	defer rows.Close()
	columns := make([]string, 0)
	for rows.Next() {
		var seqno int
		var cid int
		var columnName string
		if err := rows.Scan(&seqno, &cid, &columnName); err != nil {
			return nil, fmt.Errorf("scan sqlite index %s column: %w", strings.TrimSpace(indexName), err)
		}
		columns = append(columns, strings.TrimSpace(columnName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite index %s columns: %w", strings.TrimSpace(indexName), err)
	}
	return columns, nil
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
