package testutil

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestStartPostgres_Smoke(t *testing.T) {
	_, db, cleanup := StartPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var one int
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("query: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected 1, got %d", one)
	}
}

func loadPlatformSpec() (runtimecontracts.PlatformSpecDocument, error) {
	path, err := platformSpecPath()
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, err
	}
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, err
	}
	var spec runtimecontracts.PlatformSpecDocument
	err = source.Decode(&spec)
	return spec, err
}

func TestStartPostgres_CanonicalSchemaTemplatePreservesIsolation(t *testing.T) {
	spec, err := loadPlatformSpec()
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	if spec.Platform.Version == "" {
		t.Fatal("platform spec version is required")
	}

	_, firstDB, cleanupFirst := StartPostgres(t)
	defer cleanupFirst()
	assertStartPostgresSchema(t, firstDB, spec.Platform.Version)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := firstDB.ExecContext(ctx, `CREATE TABLE start_postgres_isolation_probe (id BIGINT PRIMARY KEY)`); err != nil {
		t.Fatalf("create isolation probe in first database: %v", err)
	}

	_, secondDB, cleanupSecond := StartPostgres(t)
	defer cleanupSecond()
	assertStartPostgresSchema(t, secondDB, spec.Platform.Version)

	var leaked bool
	if err := secondDB.QueryRowContext(ctx, `SELECT to_regclass('public.start_postgres_isolation_probe') IS NOT NULL`).Scan(&leaked); err != nil {
		t.Fatalf("check isolation probe in second database: %v", err)
	}
	if leaked {
		t.Fatal("table created in one StartPostgres database leaked into another")
	}
}

func assertStartPostgresSchema(t *testing.T, db *sql.DB, wantVersion string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, table := range []string{"runtime_store_metadata", "bundles", "runs", "events", "event_receipts", "timers"} {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("expected platform table %s to exist", table)
		}
	}

	var gotVersion string
	if err := db.QueryRowContext(ctx, `SELECT platform_version FROM runtime_store_metadata WHERE id = 1`).Scan(&gotVersion); err != nil {
		t.Fatalf("read runtime_store_metadata: %v", err)
	}
	if gotVersion != wantVersion {
		t.Fatalf("runtime_store_metadata platform_version = %q, want %q", gotVersion, wantVersion)
	}
}
