package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testpostgres"
)

var postgresManagers = struct {
	sync.Mutex
	bySource map[string]*testpostgres.Manager
}{bySource: make(map[string]*testpostgres.Manager)}

func AcquirePostgres(t *testing.T, requirement DatabaseRequirement) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	if err := requirement.validate(); err != nil {
		t.Fatal(err)
	}
	if requirement.backend != DatabaseBackendPostgreSQL {
		t.Fatalf("PostgreSQL acquisition requires PostgreSQL backend, got %d", requirement.backend)
	}
	return acquirePostgresDatabase(t, requirement)
}

func acquirePostgresDatabase(t *testing.T, requirement DatabaseRequirement) (string, *sql.DB, func()) {
	t.Helper()
	connection, err := testpostgres.ConnectionFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	cacheKey, err := connection.String()
	if err != nil {
		t.Fatalf("serialize canonical Postgres test connection: %v", err)
	}

	postgresManagers.Lock()
	manager := postgresManagers.bySource[cacheKey]
	if manager == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		manager, err = testpostgres.NewManager(ctx, connection)
		cancel()
		if err != nil {
			postgresManagers.Unlock()
			t.Fatalf("initialize Postgres test manager: %v", err)
		}
		postgresManagers.bySource[cacheKey] = manager
	}
	postgresManagers.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if requirement.isolation == databaseIsolationPostgresRowState {
		lease, err := manager.AcquireRowState(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire reusable Postgres row-state lease: %v", err)
		}
		dsn, err := lease.Connection.String()
		if err != nil {
			_ = lease.Release(context.Background())
			t.Fatalf("serialize Postgres row-state lease: %v", err)
		}
		release := func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := lease.Release(ctx); err != nil {
				t.Errorf("release Postgres row-state lease %q: %v", lease.Name, err)
			}
		}
		t.Cleanup(release)
		return dsn, lease.DB, release
	}
	useTemplate := requirement.isolation == databaseIsolationPostgresFreshPhysical
	if !useTemplate && requirement.isolation != databaseIsolationPostgresEmptyPhysical {
		cancel()
		t.Fatalf("unsupported PostgreSQL isolation %d", requirement.isolation)
	}
	sandbox, err := manager.Acquire(ctx, useTemplate)
	cancel()
	if err != nil {
		t.Fatalf("acquire Postgres test sandbox: %v", err)
	}
	dsn, err := sandbox.Connection.String()
	if err != nil {
		_ = sandbox.Release(context.Background())
		t.Fatalf("serialize Postgres test sandbox: %v", err)
	}
	release := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := sandbox.Release(ctx); err != nil {
			t.Errorf("release Postgres test sandbox %q: %v", sandbox.Name, err)
		}
	}
	t.Cleanup(release)
	return dsn, sandbox.DB, release
}

func platformSpecPath() (string, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	path := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("platform spec not found: %w", err)
	}
	return path, nil
}
