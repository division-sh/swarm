package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

type SQLiteSchemaStore struct {
	backend *sqlitebackend.Backend
	path    string

	schemaAdmission schemaAdmission
}

const sqliteDriverBusyTimeoutMillis = 50

var sqliteSchemaBootstrapLocks sync.Map

func sqliteSchemaBootstrapMutex(path string) *sync.Mutex {
	value, _ := sqliteSchemaBootstrapLocks.LoadOrStore(filepath.Clean(path), &sync.Mutex{})
	return value.(*sync.Mutex)
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
	db, err := sql.Open("sqlite", sqliteFileDSN(cleanPath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite schema store: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	backend, err := sqlitebackend.New(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &SQLiteSchemaStore{backend: backend, path: cleanPath}
	if err := store.configure(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteFileDSN(path string) string {
	u := url.URL{Scheme: "file", Opaque: path}
	q := u.Query()
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", sqliteDriverBusyTimeoutMillis))
	u.RawQuery = q.Encode()
	return u.String()
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
	if s == nil || !s.backend.Valid() {
		return nil
	}
	return s.backend.Close()
}

func (s *SQLiteSchemaStore) Ping(ctx context.Context) error {
	if s == nil || !s.backend.Valid() {
		return fmt.Errorf("sqlite schema store is required")
	}
	return s.backend.Ping(ctx)
}

func (s *SQLiteSchemaStore) configure(ctx context.Context) error {
	if s == nil || !s.backend.Valid() {
		return fmt.Errorf("sqlite schema store is required")
	}
	if _, err := s.backend.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	if _, err := s.backend.ExecContext(ctx, fmt.Sprintf(`PRAGMA busy_timeout = %d`, sqliteDriverBusyTimeoutMillis)); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	return s.backend.Ping(ctx)
}
