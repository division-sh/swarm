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

// StartPostgres returns a database-per-test sandbox cloned from the canonical
// content-addressed platform template.
func StartPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	dsn, db, cleanup, _ = startPostgresDatabase(t, true)
	return dsn, db, cleanup
}

// StartPostgresDSN provides a sandbox location for native store construction
// without granting its pool to semantic fixture callers.
func StartPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn, _, _, _ := startPostgresDatabase(t, true)
	return dsn
}

// StartEmptyPostgres returns a database-per-test sandbox without platform
// schema bootstrap.
func StartEmptyPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	dsn, db, cleanup, _ = startPostgresDatabase(t, false)
	return dsn, db, cleanup
}

// StartEmptyPostgresDSN preserves empty-store admission without exporting its pool.
func StartEmptyPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn, _, _, _ := startPostgresDatabase(t, false)
	return dsn
}

// StartEmptyPostgresProcessLeaseProof retains the observer inside the native
// harness and exposes only connection data and the exact process-wait witness.
func StartEmptyPostgresProcessLeaseProof(t *testing.T) (string, testpostgres.Parameters, func(context.Context) (bool, error)) {
	t.Helper()
	dsn, observer, _, parameters := startPostgresDatabase(t, false)
	return dsn, parameters, func(ctx context.Context) (bool, error) {
		var waiting bool
		err := observer.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
			AND classid::bigint=CASE WHEN hashtext('swarm:runtime:shared-store-owner')<0 THEN 4294967295::bigint ELSE 0::bigint END
			AND objid::bigint=(hashtext('swarm:runtime:shared-store-owner')::bigint & 4294967295::bigint) AND objsubid=1)`).Scan(&waiting)
		return waiting, err
	}
}

func startPostgresDatabase(t *testing.T, useTemplate bool) (string, *sql.DB, func(), testpostgres.Parameters) {
	t.Helper()
	sandbox, release := startPostgresSandbox(t, useTemplate)
	dsn, err := sandbox.Connection.String()
	if err != nil {
		t.Fatalf("serialize Postgres test sandbox: %v", err)
	}
	return dsn, sandbox.DB, release, sandbox.Connection.Parameters()
}

func startPostgresSandbox(t *testing.T, useTemplate bool) (*testpostgres.Sandbox, func()) {
	t.Helper()
	connection, err := testpostgres.ConnectionFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	manager, err := postgresManagerForConnection(ctx, connection)
	cancel()
	if err != nil {
		t.Fatalf("initialize Postgres test manager: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	sandbox, err := manager.Acquire(ctx, useTemplate)
	cancel()
	if err != nil {
		t.Fatalf("acquire Postgres test sandbox: %v", err)
	}
	release := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := sandbox.Release(ctx); err != nil {
			t.Errorf("release Postgres test sandbox %q: %v", sandbox.Name, err)
		}
	}
	t.Cleanup(release)
	return sandbox, release
}

func postgresManagerForConnection(ctx context.Context, connection testpostgres.Connection) (*testpostgres.Manager, error) {
	cacheKey, err := connection.String()
	if err != nil {
		return nil, fmt.Errorf("serialize canonical Postgres test connection: %w", err)
	}
	postgresManagers.Lock()
	defer postgresManagers.Unlock()
	if manager := postgresManagers.bySource[cacheKey]; manager != nil {
		return manager, nil
	}
	manager, err := testpostgres.NewManager(ctx, connection)
	if err != nil {
		return nil, err
	}
	postgresManagers.bySource[cacheKey] = manager
	return manager, nil
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
