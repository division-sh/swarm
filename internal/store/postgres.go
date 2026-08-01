package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	backend *postgresRuntimeBackend

	schemaAdmission schemaAdmission

	eventPayloadValidator   EventPayloadValidator
	authorActivityCatalogMu sync.Mutex
	authorActivityCatalog   *runtimeauthoractivity.EventCatalogRegistry
	sessionLockTTL          time.Duration
	runLifecycleSinks       runLifecycleCandidateSinkRegistry
}

// postgresRuntimeBackend is the private owner of PostgreSQL runtime
// capabilities. The selected-store facade deliberately contains no raw SQL
// handle; runtime consumers can only reach the named operations on
// PostgresStore.
type postgresRuntimeBackend struct {
	db *sql.DB

	scheduleClaimMu   sync.Mutex
	scheduleClaimConn *sql.Conn
	scheduleClaimKeys map[string]struct{}
}

type EventPayloadValidator func(ctx context.Context, eventType string, payload []byte) error

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
	store, _, err := OpenPostgresStore(dsn)
	return store, err
}

// OpenPostgresStore returns the selected runtime store and the separately
// owned process-construction database handle. Only CLI/serve construction may
// retain the latter; runtime consumers receive the typed store facade.
func OpenPostgresStore(dsn string) (*PostgresStore, *sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	// Safe defaults; callers can still override pool settings afterward.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	store := &PostgresStore{
		backend:        &postgresRuntimeBackend{db: db},
		sessionLockTTL: 120 * time.Second,
	}
	return store, db, nil
}

func (s *PostgresStore) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.sessionLockTTL = ttl
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return fmt.Errorf("postgres store is required")
	}
	return s.backend.db.PingContext(ctx)
}

func (s *PostgresStore) Close() error {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return nil
	}
	return s.backend.db.Close()
}
