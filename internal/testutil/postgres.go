package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

// StartEmptyPostgres returns a database-per-test sandbox without platform
// schema bootstrap.
func StartEmptyPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	return startPostgresDatabase(t, false)
}

func startPostgresDatabase(t *testing.T, useTemplate bool) (string, *sql.DB, func()) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(testpostgres.SourceEnv))
	if raw == "" {
		t.Fatalf("%s is not set; run tests with `go run ./cmd/swarm-test-postgres -- go test ...` or configure host Postgres using internal/testutil/POSTGRES.md", testpostgres.SourceEnv)
	}

	postgresManagers.Lock()
	manager := postgresManagers.bySource[raw]
	if manager == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		var err error
		connection, parseErr := testpostgres.ParseConnection(raw)
		if parseErr != nil {
			postgresManagers.Unlock()
			cancel()
			t.Fatalf("parse %s: %v", testpostgres.SourceEnv, parseErr)
		}
		manager, err = testpostgres.NewManager(ctx, connection)
		cancel()
		if err != nil {
			postgresManagers.Unlock()
			t.Fatalf("initialize Postgres test manager: %v", err)
		}
		postgresManagers.bySource[raw] = manager
	}
	postgresManagers.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
