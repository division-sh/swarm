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
	return startPostgresDatabase(t, true)
}

// StartPostgresDSN provides a sandbox location for native store construction
// without granting its pool to semantic fixture callers.
func StartPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn, _, _ := startPostgresDatabase(t, true)
	return dsn
}

// StartEmptyPostgres returns a database-per-test sandbox without platform
// schema bootstrap.
func StartEmptyPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	return startPostgresDatabase(t, false)
}

func startPostgresDatabase(t *testing.T, useTemplate bool) (string, *sql.DB, func()) {
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
