package schemastore

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

type SQLite struct {
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

func NewSQLite(path string) (*SQLite, error) {
	store, _, err := OpenSQLiteForConstruction(path)
	return store, err
}

// OpenSQLiteForConstruction returns the private backend only to the
// store-internal process construction path. Runtime consumers receive the
// composed typed facade instead.
func OpenSQLiteForConstruction(path string) (*SQLite, *sqlitebackend.Backend, error) {
	return openSQLite(path, false)
}

func OpenSQLiteReadOnlyForInspection(path string) (*SQLite, *sqlitebackend.Backend, error) {
	return openSQLite(path, true)
}

func openSQLite(path string, readOnly bool) (*SQLite, *sqlitebackend.Backend, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil, fmt.Errorf("sqlite schema store path is required")
	}
	if sqlitePathIsInMemory(path) {
		return nil, nil, fmt.Errorf("sqlite schema store must be file-backed; in-memory paths are not allowed")
	}
	cleanPath := filepath.Clean(path)
	parent := filepath.Dir(cleanPath)
	if !readOnly && parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return nil, nil, fmt.Errorf("create sqlite schema store parent directory: %w", err)
		}
	}
	dsn := sqliteFileDSN(cleanPath)
	if readOnly {
		dsn = sqliteReadOnlyFileDSN(cleanPath)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open sqlite schema store: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	backend, err := sqlitebackend.New(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	store := &SQLite{backend: backend, path: cleanPath}
	if err := store.configure(context.Background()); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, backend, nil
}

func sqliteReadOnlyFileDSN(path string) string {
	u := url.URL{Scheme: "file", Opaque: path}
	q := u.Query()
	q.Add("mode", "ro")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", sqliteDriverBusyTimeoutMillis))
	u.RawQuery = q.Encode()
	return u.String()
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

func NewSQLiteWithBackend(backend *sqlitebackend.Backend, path string) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return &SQLite{backend: backend, path: strings.TrimSpace(path)}, nil
}

func (s *SQLite) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *SQLite) Close() error {
	if s == nil || !s.backend.Valid() {
		return nil
	}
	return s.backend.Close()
}

func (s *SQLite) Ping(ctx context.Context) error {
	if s == nil || !s.backend.Valid() {
		return fmt.Errorf("sqlite schema store is required")
	}
	return s.backend.Ping(ctx)
}

func (s *SQLite) CatalogEmpty(ctx context.Context) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite schema store is required")
	}
	tables, err := sqliteUserTables(ctx, s.backend)
	if err != nil {
		return false, err
	}
	return len(tables) == 0, nil
}

func (s *SQLite) configure(ctx context.Context) error {
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
