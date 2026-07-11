package testpostgres

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestManagerLifecycleSupportedRepresentations(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv(SourceEnv))
	if raw == "" {
		t.Skip(SourceEnv + " is not set")
	}
	base, err := ParseConnection(raw)
	if err != nil {
		t.Fatal(err)
	}
	params := base.Parameters()
	u := &url.URL{Scheme: "postgres", Host: params.Host + ":" + strconv.Itoa(int(params.Port)), Path: "/" + params.Database}
	u.User = url.UserPassword(params.User, params.Password)
	query := u.Query()
	query.Set("sslmode", params.SSLMode)
	u.RawQuery = query.Encode()
	keyword, err := base.String()
	if err != nil {
		t.Fatal(err)
	}

	for _, source := range []struct{ name, dsn string }{{"keyword", keyword}, {"url", u.String()}} {
		t.Run(source.name, func(t *testing.T) {
			connection, err := ParseConnection(source.dsn)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			manager, err := NewManager(ctx, connection)
			if err != nil {
				t.Fatal(err)
			}
			sandbox, err := manager.Acquire(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			var version string
			if err := sandbox.DB.QueryRowContext(ctx, `SELECT platform_version FROM schema_version WHERE id=1`).Scan(&version); err != nil {
				t.Fatalf("canonical schema missing: %v", err)
			}
			if err := sandbox.Release(ctx); err != nil {
				t.Fatal(err)
			}
			assertDatabaseAbsent(t, connection, sandbox.Name)

			empty, err := manager.Acquire(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			var table *string
			err = empty.DB.QueryRowContext(ctx, `SELECT to_regclass('public.schema_version')::text`).Scan(&table)
			if err != nil || table != nil {
				t.Fatalf("empty sandbox schema_version = %v, err=%v", table, err)
			}
			if err := empty.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagerReconcilesSandboxAfterLeaseOwnerDies(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	name := sandbox.Name
	_ = sandbox.DB.Close()
	_ = sandbox.leaseConn.Close()
	sandbox.leaseConn = nil
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseAbsent(t, manager.admin, name)
}

func TestManagerLeavesUnprovableSandboxUntouched(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	name := sandboxNamePrefix + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(context.Background(), db, name)
	if err := manager.Reconcile(ctx); err == nil || !strings.Contains(err.Error(), "unprovable") {
		t.Fatalf("Reconcile() error = %v, want unprovable blocker", err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil || !exists {
		t.Fatalf("sentinel exists=%v err=%v", exists, err)
	}
}

func integrationManager(t *testing.T) *Manager {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(SourceEnv))
	if raw == "" {
		t.Skip(SourceEnv + " is not set")
	}
	connection, err := ParseConnection(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	manager, err := NewManager(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func assertDatabaseAbsent(t *testing.T, connection Connection, name string) {
	t.Helper()
	db, err := connection.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("database %q still exists", name)
	}
}
