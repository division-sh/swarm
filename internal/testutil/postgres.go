package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	_ "github.com/lib/pq"
)

// StartPostgres spins up a disposable Postgres container via the local docker CLI,
// waits for readiness, applies the bootstrap migration, and returns a DSN + cleanup.
//
// This is intentionally docker-first: most of the repo is Postgres-specific (UUID/JSONB),
// so sqlmock would not provide meaningful execution coverage.
func StartPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()

	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		t.Fatalf("docker not found in PATH: %v", err)
	}

	name := fmt.Sprintf("empireai-test-pg-%d-%d", time.Now().UnixNano(), time.Now().UnixNano()%1000)

	// Publish to an ephemeral host port bound to localhost only.
	runArgs := []string{
		"run", "-d", "--rm",
		"--name", name,
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=empireai",
		"-p", "127.0.0.1::5432",
		"postgres:16",
	}
	if out, err := exec.Command(dockerBin, runArgs...).CombinedOutput(); err != nil {
		t.Fatalf("docker run postgres failed: %v output=%s", err, strings.TrimSpace(string(out)))
	}
	t.Cleanup(func() {
		_ = exec.Command(dockerBin, "stop", name).Run()
	})

	portOut, err := exec.Command(dockerBin, "port", name, "5432/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port failed: %v output=%s", err, strings.TrimSpace(string(portOut)))
	}
	portLine := strings.TrimSpace(string(portOut))
	// expected: 127.0.0.1:12345
	hostPort := portLine
	if idx := strings.LastIndex(hostPort, "\n"); idx >= 0 {
		hostPort = strings.TrimSpace(hostPort[idx+1:])
	}
	parts := strings.Split(hostPort, ":")
	if len(parts) < 2 {
		t.Fatalf("unexpected docker port output: %q", portLine)
	}
	port := strings.TrimSpace(parts[len(parts)-1])
	if port == "" {
		t.Fatalf("empty host port from docker port: %q", portLine)
	}

	dsn = fmt.Sprintf("host=127.0.0.1 port=%s user=postgres password=postgres dbname=empireai sslmode=disable", port)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Wait for readiness.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		pingErr := db.PingContext(ctx)
		cancel()
		if pingErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres not ready in time: %v", pingErr)
		}
		time.Sleep(150 * time.Millisecond)
	}

	// Apply migrations.
	_, thisFile, _, _ := runtime.Caller(0)
	migrationPath := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "001_initial.sql"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(b)); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	cleanup = func() {
		_ = db.Close()
		_ = exec.Command(dockerBin, "stop", name).Run()
	}
	return dsn, db, cleanup
}
