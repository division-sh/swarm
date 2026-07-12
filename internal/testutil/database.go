package testutil

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

type DatabaseBackend uint8

const (
	databaseBackendInvalid DatabaseBackend = iota
	DatabaseBackendSQLite
	DatabaseBackendPostgreSQL
)

type DatabaseIsolation uint8

const (
	databaseIsolationInvalid DatabaseIsolation = iota
	databaseIsolationSQLiteTemp
	databaseIsolationSQLiteFile
	databaseIsolationSQLiteSharedFile
	databaseIsolationPostgresRowState
	databaseIsolationPostgresFreshPhysical
	databaseIsolationPostgresEmptyPhysical
)

type DatabaseRequirement struct {
	backend   DatabaseBackend
	isolation DatabaseIsolation
}

func SQLiteDefaultTemp() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendSQLite, isolation: databaseIsolationSQLiteTemp}
}

func SQLiteFreshFile() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendSQLite, isolation: databaseIsolationSQLiteFile}
}

func SQLiteSharedFile() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendSQLite, isolation: databaseIsolationSQLiteSharedFile}
}

func PostgresRowState() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendPostgreSQL, isolation: databaseIsolationPostgresRowState}
}

func PostgresFreshPhysical() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendPostgreSQL, isolation: databaseIsolationPostgresFreshPhysical}
}

func PostgresEmptyPhysical() DatabaseRequirement {
	return DatabaseRequirement{backend: DatabaseBackendPostgreSQL, isolation: databaseIsolationPostgresEmptyPhysical}
}

func (r DatabaseRequirement) validate() error {
	switch {
	case r.backend == DatabaseBackendSQLite && (r.isolation == databaseIsolationSQLiteTemp || r.isolation == databaseIsolationSQLiteFile || r.isolation == databaseIsolationSQLiteSharedFile):
		return nil
	case r.backend == DatabaseBackendPostgreSQL && (r.isolation == databaseIsolationPostgresRowState || r.isolation == databaseIsolationPostgresFreshPhysical || r.isolation == databaseIsolationPostgresEmptyPhysical):
		return nil
	default:
		return fmt.Errorf("unsupported or omitted test database backend/isolation requirement (%d/%d)", r.backend, r.isolation)
	}
}

func SQLitePath(t testing.TB, requirement DatabaseRequirement) string {
	t.Helper()
	if err := requirement.validate(); err != nil {
		t.Fatal(err)
	}
	if requirement.backend != DatabaseBackendSQLite {
		t.Fatalf("SQLite path requires SQLite backend, got %d", requirement.backend)
	}
	switch requirement.isolation {
	case databaseIsolationSQLiteTemp:
		return filepath.Join(t.TempDir(), ".swarm", "dev.db")
	case databaseIsolationSQLiteFile:
		return filepath.Join(t.TempDir(), "database.db")
	case databaseIsolationSQLiteSharedFile:
		return filepath.Join(t.TempDir(), "shared.db")
	default:
		t.Fatalf("unsupported SQLite isolation %d", requirement.isolation)
		return ""
	}
}

func SQLiteDeclaredPath(t testing.TB, requirement DatabaseRequirement, path string) string {
	t.Helper()
	declared, err := DeclareSQLitePath(requirement, path)
	if err != nil {
		t.Fatal(err)
	}
	return declared
}

func DeclareSQLitePath(requirement DatabaseRequirement, path string) (string, error) {
	if err := requirement.validate(); err != nil {
		return "", err
	}
	if requirement.backend != DatabaseBackendSQLite {
		return "", fmt.Errorf("explicit SQLite path requires SQLite backend, got %d/%d", requirement.backend, requirement.isolation)
	}
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("explicit SQLite path is required")
	}
	return path, nil
}

func SQLiteDeclaredDSN(t testing.TB, requirement DatabaseRequirement, dsn string) string {
	t.Helper()
	if err := requirement.validate(); err != nil {
		t.Fatal(err)
	}
	if requirement.backend != DatabaseBackendSQLite {
		t.Fatalf("SQLite DSN requires SQLite backend, got %d", requirement.backend)
	}
	if strings.TrimSpace(dsn) == "" {
		t.Fatal("SQLite DSN is required")
	}
	return dsn
}
