package bundlecatalog

import (
	"fmt"
	"strings"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type Postgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("bundle catalog postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("bundle catalog postgres schema guard is required")
	}
	return &Postgres{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *Postgres) requireCurrentSchema() error { return s.schemaGuard() }

type SQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("bundle catalog sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("bundle catalog sqlite schema guard is required")
	}
	return &SQLite{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *SQLite) requireCurrentSchema() error { return s.schemaGuard() }

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid sqlite time %q", raw)
}
