package store

import (
	"context"
	"database/sql"
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
	_, err = s.BindSchemaCapabilities(ctx)
	return err
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
