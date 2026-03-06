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
	"sync/atomic"
	"testing"
	"time"
)

type sharedPostgresState struct {
	mu        sync.Mutex
	started   bool
	dockerBin string
	name      string
	adminDSN  string
	nextDBID  uint64
}

var sharedPostgres sharedPostgresState

// StartPostgres provides an isolated database on a shared Postgres container.
// The container is started once per test process; each call creates a fresh
// database, applies the canonical schema, and drops that database on cleanup.
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
	adminDSN := sharedPostgres.adminDSN
	dbName := sharedPostgres.nextDatabaseName()
	sharedPostgres.mu.Unlock()

	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatalf("open postgres admin: %v", err)
	}
	defer adminDB.Close()

	if err := createIsolatedDatabase(adminDB, dbName); err != nil {
		t.Fatalf("create isolated postgres database %q: %v", dbName, err)
	}

	dsn = withDBName(adminDSN, dbName)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		t.Fatalf("open postgres %q: %v", dbName, err)
	}
	if err := initializeDatabase(db); err != nil {
		_ = db.Close()
		_ = dropIsolatedDatabase(adminDB, dbName)
		t.Fatalf("initialize postgres %q: %v", dbName, err)
	}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = db.Close()
		adminCleanupDB, err := sql.Open("postgres", adminDSN)
		if err != nil {
			t.Fatalf("reopen postgres admin for cleanup: %v", err)
		}
		defer adminCleanupDB.Close()
		if err := dropIsolatedDatabase(adminCleanupDB, dbName); err != nil {
			t.Fatalf("drop isolated postgres database %q: %v", dbName, err)
		}
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

	adminDSN := fmt.Sprintf("host=127.0.0.1 port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port)
	db, err := sql.Open("postgres", adminDSN)
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
	s.adminDSN = adminDSN
	return nil
}

func (s *sharedPostgresState) nextDatabaseName() string {
	id := atomic.AddUint64(&s.nextDBID, 1)
	return fmt.Sprintf("empireai_test_%d_%d", os.Getpid(), id)
}

func initializeDatabase(db *sql.DB) error {
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

	initSQL := []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`GRANT ALL ON SCHEMA public TO postgres`,
		`GRANT ALL ON SCHEMA public TO public`,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
	}
	for _, stmt := range initSQL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init stmt %q: %w", stmt, err)
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

func createIsolatedDatabase(adminDB *sql.DB, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		return err
	}
	return nil
}

func dropIsolatedDatabase(adminDB *sql.DB, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1
		  AND pid <> pg_backend_pid()
	`, dbName); err != nil {
		return fmt.Errorf("terminate lingering sessions for %s: %w", dbName, err)
	}
	if _, err := adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)); err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

func withDBName(dsn, dbName string) string {
	parts := strings.Fields(dsn)
	for i, part := range parts {
		if strings.HasPrefix(part, "dbname=") {
			parts[i] = "dbname=" + dbName
			return strings.Join(parts, " ")
		}
	}
	return dsn + " dbname=" + dbName
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
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
