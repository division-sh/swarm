package testutil

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type sharedPostgresState struct {
	mu        sync.Mutex
	started   bool
	dockerBin string
	name      string
	dsn       string
}

var sharedPostgres sharedPostgresState

// StartPostgres provides an isolated database on a shared Postgres container.
// The container is started once per test process; each call resets the schema
// before returning and holds an exclusive lease until the test cleanup runs.
func StartPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()

	sharedPostgres.mu.Lock()

	var err error
	if !sharedPostgres.started {
		if err = sharedPostgres.startLocked(); err != nil {
			sharedPostgres.mu.Unlock()
			t.Fatalf("start shared postgres: %v", err)
		}
	}

	dsn = sharedPostgres.dsn
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		sharedPostgres.mu.Unlock()
		t.Fatalf("open postgres: %v", err)
	}

	if err := sharedPostgres.resetLocked(db); err != nil {
		_ = db.Close()
		sharedPostgres.mu.Unlock()
		t.Fatalf("reset postgres schema: %v", err)
	}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = db.Close()
		sharedPostgres.mu.Unlock()
	}

	t.Cleanup(release)
	return dsn, db, release
}

func (s *sharedPostgresState) startLocked() error {
	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}

	name := fmt.Sprintf("empireai-test-pg-%d", os.Getpid())
	runArgs := []string{
		"run", "-d", "--rm",
		"--name", name,
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=empireai",
		"-p", "127.0.0.1::5432",
		"postgres:16",
		"-c", "max_connections=300",
	}
	if out, err := exec.Command(dockerBin, runArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("docker run postgres failed: %v output=%s", err, strings.TrimSpace(string(out)))
	}

	portOut, err := exec.Command(dockerBin, "port", name, "5432/tcp").CombinedOutput()
	if err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("docker port failed: %v output=%s", err, strings.TrimSpace(string(portOut)))
	}
	portLine := strings.TrimSpace(string(portOut))
	hostPort := portLine
	if idx := strings.LastIndex(hostPort, "\n"); idx >= 0 {
		hostPort = strings.TrimSpace(hostPort[idx+1:])
	}
	parts := strings.Split(hostPort, ":")
	if len(parts) < 2 {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("unexpected docker port output: %q", portLine)
	}
	port := strings.TrimSpace(parts[len(parts)-1])
	if port == "" {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("empty host port from docker port: %q", portLine)
	}

	dsn := fmt.Sprintf("host=127.0.0.1 port=%s user=postgres password=postgres dbname=empireai sslmode=disable", port)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		pingErr := db.PingContext(ctx)
		cancel()
		if pingErr == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = exec.Command(dockerBin, "stop", name).Run()
			return fmt.Errorf("postgres not ready in time: %w", pingErr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := startContainerWatcher(dockerBin, name); err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return err
	}

	s.started = true
	s.dockerBin = dockerBin
	s.name = name
	s.dsn = dsn
	return nil
}

func (s *sharedPostgresState) resetLocked(db *sql.DB) error {
	migrationPath, err := canonicalMigrationPath()
	if err != nil {
		return err
	}
	b, err := os.ReadFile(migrationPath)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resetSQL := []string{
		`DROP SCHEMA IF EXISTS public CASCADE`,
		`CREATE SCHEMA public`,
		`GRANT ALL ON SCHEMA public TO postgres`,
		`GRANT ALL ON SCHEMA public TO public`,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
	}
	for _, stmt := range resetSQL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("reset stmt %q: %w", stmt, err)
		}
	}
	if _, err := db.ExecContext(ctx, string(b)); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schema_version (version, name, applied_at)
		VALUES (1, 'ddl-canonical', now())
		ON CONFLICT (version) DO NOTHING
	`); err != nil {
		return fmt.Errorf("seed schema_version: %w", err)
	}
	return nil
}

func canonicalMigrationPath() (string, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	migrationPath := filepath.Join(repoRoot, "contracts", "ddl-canonical.sql")
	if _, statErr := os.Stat(migrationPath); statErr == nil {
		return migrationPath, nil
	}
	migrationPath = filepath.Join(repoRoot, "migrations", "001_initial.sql")
	if _, statErr := os.Stat(migrationPath); statErr != nil {
		return "", fmt.Errorf("migration file not found: %w", statErr)
	}
	return migrationPath, nil
}

func startContainerWatcher(dockerBin, containerName string) error {
	pid := strconv.Itoa(os.Getpid())
	cmd := exec.Command("sh", "-c", `
pid="$1"
docker_bin="$2"
container="$3"
while kill -0 "$pid" 2>/dev/null; do
  sleep 1
done
"$docker_bin" stop "$container" >/dev/null 2>&1 || true
`, "watch", pid, dockerBin, containerName)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start postgres container watcher: %w", err)
	}
	return nil
}
