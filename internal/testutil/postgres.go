package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"gopkg.in/yaml.v3"
)

const (
	postgresTestSetupDoc      = "internal/testutil/POSTGRES.md"
	dockerPostgresFallbackLog = "using Docker Postgres (set SWARM_TEST_POSTGRES_DSN for host Postgres - faster)"
)

type sharedPostgresState struct {
	mu                sync.Mutex
	lifecycle         sync.Mutex
	started           bool
	dockerBin         string
	name              string
	admin             testPostgresDSN
	authenticatedRole string
	template          string
	templated         bool
	cleanupStarted    bool
	nextDBID          uint64
	reportWriter      io.Writer
	reportFallback    sync.Once
}

var sharedPostgres sharedPostgresState

func init() {
	if os.Getenv("SWARM_TEST_POSTGRES_TEMPLATE_CLEANUP") != "1" {
		return
	}
	runPostgresTemplateCleanupWatcherFromEnv()
	os.Exit(0)
}

// StartPostgres provides an isolated database on a shared Postgres container.
// The container is started once per test process; each call creates a fresh
// database cloned from one canonical schema template, and drops that database on cleanup.
func StartPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	return startPostgresDatabase(t, true)
}

// StartEmptyPostgres provides an isolated database without the canonical schema
// template. Use it for bootstrapping tests that must prove schema creation from
// a fresh Postgres database.
func StartEmptyPostgres(t *testing.T) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	return startPostgresDatabase(t, false)
}

func startPostgresDatabase(t *testing.T, useTemplate bool) (dsn string, db *sql.DB, cleanup func()) {
	t.Helper()
	sharedPostgres.mu.Lock()
	var err error
	if !sharedPostgres.started {
		if err = sharedPostgres.startLocked(); err != nil {
			sharedPostgres.mu.Unlock()
			t.Fatalf("start shared postgres: %v", err)
		}
	}
	admin := sharedPostgres.admin
	dbName := sharedPostgres.nextDatabaseName()
	sharedPostgres.mu.Unlock()

	adminDB, err := admin.open()
	if err != nil {
		t.Fatalf("open postgres admin: %v", err)
	}
	defer adminDB.Close()

	sharedPostgres.lifecycle.Lock()
	if useTemplate {
		if err := sharedPostgres.ensureTemplateDatabase(adminDB); err != nil {
			sharedPostgres.lifecycle.Unlock()
			t.Fatalf("initialize postgres template: %v", err)
		}
		if err := createIsolatedDatabaseFromTemplate(adminDB, dbName, sharedPostgres.template); err != nil {
			sharedPostgres.lifecycle.Unlock()
			t.Fatalf("create isolated postgres database %q: %v", dbName, err)
		}
	} else if err := createIsolatedDatabase(adminDB, dbName); err != nil {
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("create empty postgres database %q: %v", dbName, err)
	}

	projected, err := admin.withDatabase(dbName)
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("project postgres database %q: %v", dbName, err)
	}
	dsn, err = projected.string()
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("serialize postgres database %q: %v", dbName, err)
	}
	db, err = projected.open()
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("reopen postgres %q: %v", dbName, err)
	}
	if err := waitForTestDatabase(context.Background(), db, 30*time.Second); err != nil {
		_ = db.Close()
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("reopen ping postgres %q: %v", dbName, err)
	}
	sharedPostgres.lifecycle.Unlock()

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = db.Close()
		adminCleanupDB, err := admin.open()
		if err != nil {
			t.Fatalf("reopen postgres admin for cleanup: %v", err)
		}
		defer adminCleanupDB.Close()
		sharedPostgres.lifecycle.Lock()
		defer sharedPostgres.lifecycle.Unlock()
		if err := dropIsolatedDatabase(adminCleanupDB, dbName); err != nil {
			t.Fatalf("drop isolated postgres database %q: %v", dbName, err)
		}
	}

	t.Cleanup(release)
	return dsn, db, release
}

func waitForTestDatabase(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 25; attempt++ {
		lastErr = db.PingContext(ctx)
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(200 * time.Millisecond):
		}
	}
	return lastErr
}

func (s *sharedPostgresState) startLocked() error {
	return s.startLockedWithDSN(strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_DSN")))
}

func (s *sharedPostgresState) startLockedWithDSN(raw string) error {
	if raw != "" {
		if err := s.startExternalLocked(raw); err != nil {
			return fmt.Errorf("SWARM_TEST_POSTGRES_DSN is set but unusable; Docker fallback is disabled; see %s: %w", postgresTestSetupDoc, err)
		}
		return nil
	}
	s.reportDockerFallback()
	if err := s.startDockerLocked(); err != nil {
		return dockerPostgresSetupError(err)
	}
	return nil
}

func dockerPostgresSetupError(cause error) error {
	return fmt.Errorf("SWARM_TEST_POSTGRES_DSN is not set and Docker fallback failed; configure host Postgres using %s: %w", postgresTestSetupDoc, cause)
}

func (s *sharedPostgresState) startExternalLocked(raw string) error {
	admin, err := parseTestPostgresDSN(raw)
	if err != nil {
		return err
	}
	db, err := admin.open()
	if err != nil {
		return fmt.Errorf("open external postgres: %w", err)
	}
	defer db.Close()
	if err := waitForTestDatabase(context.Background(), db, 180*time.Second); err != nil {
		return fmt.Errorf("external postgres not ready: %w", err)
	}
	role, err := inspectTestPostgresSession(db)
	if err != nil {
		return err
	}
	s.started = true
	s.admin = admin
	s.authenticatedRole = role
	return nil
}

func (s *sharedPostgresState) startDockerLocked() error {

	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}
	if err := cleanupStaleTestContainers(dockerBin); err != nil {
		return err
	}

	name := fmt.Sprintf("swarm-test-pg-%d-%d", os.Getpid(), time.Now().UnixNano())
	runArgs := dockerPostgresRunArgs(name)
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

	admin, err := parseTestPostgresDSN(fmt.Sprintf("host=127.0.0.1 port=%s user=postgres password=postgres dbname=postgres sslmode=disable", port))
	if err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("build owned postgres DSN: %w", err)
	}
	db, err := admin.open()
	if err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	deadline := time.Now().Add(180 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr := db.PingContext(ctx)
		cancel()
		if pingErr == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = exec.Command(dockerBin, "stop", name).Run()
			return fmt.Errorf("postgres not ready in time: %w", pingErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
	role, err := inspectTestPostgresSession(db)
	if err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return err
	}

	if err := startContainerWatcher(dockerBin, name); err != nil {
		_ = exec.Command(dockerBin, "stop", name).Run()
		return err
	}

	s.started = true
	s.dockerBin = dockerBin
	s.name = name
	s.admin = admin
	s.authenticatedRole = role
	return nil
}

func dockerPostgresRunArgs(name string) []string {
	return []string{
		"run", "-d", "--rm",
		"--name", name,
		"--tmpfs", "/var/lib/postgresql/data:rw",
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=postgres",
		"-p", "127.0.0.1::5432",
		"postgres:16",
		"-c", "max_connections=300",
		"-c", "fsync=off",
		"-c", "synchronous_commit=off",
		"-c", "full_page_writes=off",
	}
}

func (s *sharedPostgresState) reportDockerFallback() {
	s.reportFallback.Do(func() {
		writer := s.reportWriter
		if writer == nil {
			writer = os.Stderr
		}
		_, _ = fmt.Fprintln(writer, dockerPostgresFallbackLog)
	})
}

func inspectTestPostgresSession(db *sql.DB) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var (
		role             string
		gssAuthenticated bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT current_user,
		       COALESCE((
		           SELECT gss_authenticated
		           FROM pg_catalog.pg_stat_gssapi
		           WHERE pid = pg_backend_pid()
		       ), false)
	`).Scan(&role, &gssAuthenticated); err != nil {
		return "", fmt.Errorf("verify postgres test authentication: %w", err)
	}
	if strings.TrimSpace(role) == "" {
		return "", fmt.Errorf("verify postgres test authentication: current_user is empty")
	}
	if gssAuthenticated {
		return "", fmt.Errorf("postgres test DSN used GSS authentication, which cannot be reproduced by the cleanup process; use password authentication")
	}
	return role, nil
}

func cleanupStaleTestContainers(dockerBin string) error {
	out, err := exec.Command(dockerBin, "ps", "--format", "{{.Names}}").CombinedOutput()
	if err != nil {
		return fmt.Errorf("list docker containers: %v output=%s", err, strings.TrimSpace(string(out)))
	}
	for _, name := range strings.Fields(string(out)) {
		if !strings.HasPrefix(name, "swarm-test-pg-") {
			continue
		}
		rest := strings.TrimPrefix(name, "swarm-test-pg-")
		pidPart := rest
		if idx := strings.Index(pidPart, "-"); idx >= 0 {
			pidPart = pidPart[:idx]
		}
		pid, convErr := strconv.Atoi(strings.TrimSpace(pidPart))
		if convErr != nil || pid <= 0 {
			continue
		}
		proc, findErr := os.FindProcess(pid)
		if findErr == nil && proc != nil && proc.Signal(syscall.Signal(0)) == nil {
			continue
		}
		_ = exec.Command(dockerBin, "stop", name).Run()
	}
	return nil
}

func (s *sharedPostgresState) nextDatabaseName() string {
	id := atomic.AddUint64(&s.nextDBID, 1)
	return fmt.Sprintf("mas_test_%d_%d", os.Getpid(), id)
}

func (s *sharedPostgresState) templateDatabaseName() string {
	if s.template == "" {
		s.template = fmt.Sprintf("mas_template_%d", os.Getpid())
	}
	return s.template
}

func (s *sharedPostgresState) ensureTemplateDatabase(adminDB *sql.DB) error {
	if s.templated {
		return nil
	}
	templateName := s.templateDatabaseName()
	if err := dropIsolatedDatabase(adminDB, templateName); err != nil {
		return fmt.Errorf("reset template database %q: %w", templateName, err)
	}
	if err := createIsolatedDatabase(adminDB, templateName); err != nil {
		return fmt.Errorf("create template database %q: %w", templateName, err)
	}
	templateDSN, err := s.admin.withDatabase(templateName)
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, templateName)
		return fmt.Errorf("project template database %q: %w", templateName, err)
	}
	templateDB, err := templateDSN.open()
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, templateName)
		return fmt.Errorf("open template database %q: %w", templateName, err)
	}
	if err := initializeDatabase(templateDB, s.authenticatedRole); err != nil {
		_ = templateDB.Close()
		_ = dropIsolatedDatabase(adminDB, templateName)
		return fmt.Errorf("initialize template database %q: %w", templateName, err)
	}
	if err := templateDB.Close(); err != nil {
		_ = dropIsolatedDatabase(adminDB, templateName)
		return fmt.Errorf("close template database %q: %w", templateName, err)
	}
	if err := s.startTemplateCleanupWatcher(templateName); err != nil {
		_ = dropIsolatedDatabase(adminDB, templateName)
		return err
	}
	s.templated = true
	return nil
}

func (s *sharedPostgresState) startTemplateCleanupWatcher(templateName string) error {
	if s.cleanupStarted || s.dockerBin != "" {
		return nil
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve postgres template cleanup binary: %w", err)
	}
	adminDSN, err := s.admin.string()
	if err != nil {
		return fmt.Errorf("serialize postgres cleanup admin DSN: %w", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(withoutPostgresConnectionEnv(os.Environ()),
		"SWARM_TEST_POSTGRES_TEMPLATE_CLEANUP=1",
		"SWARM_TEST_POSTGRES_TEMPLATE_PARENT_PID="+strconv.Itoa(os.Getpid()),
		"SWARM_TEST_POSTGRES_TEMPLATE_ADMIN_DSN="+adminDSN,
		"SWARM_TEST_POSTGRES_TEMPLATE_NAME="+templateName,
	)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start postgres template cleanup watcher: %w", err)
	}
	if err := cmd.Process.Release(); err != nil {
		return fmt.Errorf("release postgres template cleanup watcher: %w", err)
	}
	s.cleanupStarted = true
	return nil
}

func withoutPostgresConnectionEnv(env []string) []string {
	result := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "PG") || strings.HasPrefix(key, "SWARM_TEST_POSTGRES_TEMPLATE_") {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func runPostgresTemplateCleanupWatcherFromEnv() {
	parentPID, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_TEMPLATE_PARENT_PID")))
	if err != nil || parentPID <= 0 {
		return
	}
	adminDSN := strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_TEMPLATE_ADMIN_DSN"))
	templateName := strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_TEMPLATE_NAME"))
	if adminDSN == "" || templateName == "" {
		return
	}
	waitForProcessExit(parentPID)
	admin, err := parseTestPostgresDSN(adminDSN)
	if err != nil {
		return
	}
	db, err := admin.open()
	if err != nil {
		return
	}
	defer db.Close()
	_ = dropIsolatedDatabase(db, templateName)
}

func waitForProcessExit(pid int) {
	proc, err := os.FindProcess(pid)
	if err != nil || proc == nil {
		return
	}
	for {
		if proc.Signal(syscall.Signal(0)) != nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func initializeDatabase(db *sql.DB, authenticatedRole string) error {
	spec, err := loadPlatformSpec()
	if err != nil {
		return err
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return fmt.Errorf("generate platform tables ddl: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := ensurePublicSchema(ctx, db, authenticatedRole); err != nil {
		return err
	}
	if err := platformschema.EnsurePostgresTables(ctx, db, plans, nil); err != nil {
		return fmt.Errorf("bootstrap platform tables: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schema_version (id, platform_version, applied_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET
			platform_version = EXCLUDED.platform_version,
			applied_at = EXCLUDED.applied_at
	`, spec.Platform.Version); err != nil {
		return fmt.Errorf("seed schema_version: %w", err)
	}
	return nil
}

func loadPlatformSpec() (runtimecontracts.PlatformSpecDocument, error) {
	specPath, err := platformSpecPath()
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, err
	}
	b, err := os.ReadFile(specPath)
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, nil
}

func ensurePublicSchema(ctx context.Context, db *sql.DB, authenticatedRole string) error {
	if strings.TrimSpace(authenticatedRole) == "" {
		return fmt.Errorf("authenticated postgres role is required")
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`GRANT ALL ON SCHEMA public TO ` + quoteIdent(authenticatedRole),
		`GRANT ALL ON SCHEMA public TO public`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init stmt %q: %w", stmt, err)
		}
	}
	return nil
}

func createIsolatedDatabase(adminDB *sql.DB, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		return err
	}
	return nil
}

func createIsolatedDatabaseFromTemplate(adminDB *sql.DB, dbName, templateName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(dbName)+" WITH TEMPLATE "+quoteIdent(templateName)); err != nil {
		return err
	}
	return nil
}

func dropIsolatedDatabase(adminDB *sql.DB, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func platformSpecPath() (string, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	specPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	if _, statErr := os.Stat(specPath); statErr != nil {
		return "", fmt.Errorf("platform spec not found: %w", statErr)
	}
	return specPath, nil
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
