package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"empireai/internal/config"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	DB *sql.DB
}

type MigrationSpec struct {
	Version int
	Name    string
	Path    string
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

func (s *PostgresStore) ApplyMigrationFile(ctx context.Context, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration file: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, string(b)); err != nil {
		return fmt.Errorf("apply migration file: %w", err)
	}
	return nil
}

func (s *PostgresStore) ApplyManagedMigrations(ctx context.Context, migrations []MigrationSpec) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for migrations")
	}
	const lockKey int64 = 937221
	if _, err := s.DB.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = s.DB.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)
	}()

	if _, err := s.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_version (
			version     INT PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_version table: %w", err)
	}

	sorted := make([]MigrationSpec, 0, len(migrations))
	for _, m := range migrations {
		if m.Version <= 0 || strings.TrimSpace(m.Path) == "" {
			continue
		}
		sorted = append(sorted, m)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })

	for _, m := range sorted {
		tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin migration tx v%d: %w", m.Version, err)
		}

		var exists bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM schema_version WHERE version = $1)
		`, m.Version).Scan(&exists); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("check schema version %d: %w", m.Version, err)
		}
		if exists {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit skipped migration %d: %w", m.Version, err)
			}
			continue
		}

		b, err := os.ReadFile(m.Path)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("read migration file %s: %w", m.Path, err)
		}
		if _, err := tx.ExecContext(ctx, string(b)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Path, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_version (version, name, applied_at)
			VALUES ($1, $2, now())
			ON CONFLICT (version) DO NOTHING
		`, m.Version, strings.TrimSpace(m.Name)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record schema version %d: %w", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}

func (s *PostgresStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for schema ddl")
	}
	if len(plans) == 0 {
		return nil
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
	return nil
}
