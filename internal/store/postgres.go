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
