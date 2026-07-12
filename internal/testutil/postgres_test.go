package testutil

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestAcquirePostgresFreshPhysicalSmoke(t *testing.T) {
	_, db, cleanup := AcquirePostgres(t, PostgresFreshPhysical())
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

func TestAcquirePostgresFreshPhysicalPreservesIsolation(t *testing.T) {
	spec, err := loadPlatformSpec()
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	if spec.Platform.Version == "" {
		t.Fatal("platform spec version is required")
	}

	_, firstDB, cleanupFirst := AcquirePostgres(t, PostgresFreshPhysical())
	defer cleanupFirst()
	assertPostgresSchema(t, firstDB, spec.Platform.Version)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := firstDB.ExecContext(ctx, `CREATE TABLE start_postgres_isolation_probe (id BIGINT PRIMARY KEY)`); err != nil {
		t.Fatalf("create isolation probe in first database: %v", err)
	}

	_, secondDB, cleanupSecond := AcquirePostgres(t, PostgresFreshPhysical())
	defer cleanupSecond()
	assertPostgresSchema(t, secondDB, spec.Platform.Version)

	var leaked bool
	if err := secondDB.QueryRowContext(ctx, `SELECT to_regclass('public.start_postgres_isolation_probe') IS NOT NULL`).Scan(&leaked); err != nil {
		t.Fatalf("check isolation probe in second database: %v", err)
	}
	if leaked {
		t.Fatal("table created in one fresh physical database leaked into another")
	}
}

func TestAcquirePostgresRowStateReusesResetSlotAndFencesLease(t *testing.T) {
	spec, err := loadPlatformSpec()
	if err != nil {
		t.Fatal(err)
	}
	dsnA, dbA, releaseA := AcquirePostgres(t, PostgresRowState())
	var slotA string
	if err := dbA.QueryRow(`SELECT current_database()`).Scan(&slotA); err != nil {
		t.Fatal(err)
	}
	if _, err := dbA.Exec(`UPDATE runtime_store_metadata SET platform_version='leaked' WHERE id=1`); err != nil {
		t.Fatalf("lease DML authority: %v", err)
	}
	if _, err := dbA.Exec(`CREATE TABLE forbidden_lease_ddl (id bigint)`); err == nil {
		t.Fatal("row-state lease unexpectedly acquired schema creation authority")
	}
	connectionA, err := testpostgres.ParseConnection(dsnA)
	if err != nil {
		t.Fatal(err)
	}
	other, err := connectionA.WithDatabase("postgres")
	if err != nil {
		t.Fatal(err)
	}
	otherDB, err := other.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := otherDB.Ping(); err == nil {
		_ = otherDB.Close()
		t.Fatal("lease role unexpectedly connected to another database")
	}
	_ = otherDB.Close()
	dormant, err := connectionA.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := dormant.Ping(); err != nil {
		t.Fatalf("open dormant lease client: %v", err)
	}
	releaseA()
	if err := dormant.Ping(); err == nil {
		_ = dormant.Close()
		t.Fatal("release did not fence a dormant external lease client")
	}
	_ = dormant.Close()

	stale, err := connectionA.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Ping(); err == nil {
		_ = stale.Close()
		t.Fatal("released lease DSN remained usable")
	}
	_ = stale.Close()

	_, dbB, releaseB := AcquirePostgres(t, PostgresRowState())
	defer releaseB()
	var slotB, version string
	if err := dbB.QueryRow(`SELECT current_database()`).Scan(&slotB); err != nil {
		t.Fatal(err)
	}
	if slotB != slotA {
		t.Fatalf("row-state slot was not reused: first=%s second=%s", slotA, slotB)
	}
	if err := dbB.QueryRow(`SELECT platform_version FROM runtime_store_metadata WHERE id=1`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != spec.Platform.Version {
		t.Fatalf("row-state reset retained prior rows: platform_version=%q want=%q", version, spec.Platform.Version)
	}
}

func assertPostgresSchema(t *testing.T, db *sql.DB, wantVersion string) {
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
