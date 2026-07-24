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
	DB *sql.DB

	schemaAdmission schemaAdmission

	eventPayloadValidator   EventPayloadValidator
	authorActivityCatalogMu sync.Mutex
	authorActivityCatalog   *runtimeauthoractivity.EventCatalogRegistry
	sessionLockTTL          time.Duration

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
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	// Safe defaults; callers can still override pool settings afterward.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &PostgresStore{
		DB:             db,
		sessionLockTTL: 120 * time.Second,
	}, nil
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
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	return s.DB.PingContext(ctx)
}
