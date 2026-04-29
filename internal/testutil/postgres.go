package testutil

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	runtimecontracts "swarm/internal/runtime/contracts"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type sharedPostgresState struct {
	mu        sync.Mutex
	lifecycle sync.Mutex
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

	sharedPostgres.lifecycle.Lock()
	if err := createIsolatedDatabase(adminDB, dbName); err != nil {
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("create isolated postgres database %q: %v", dbName, err)
	}

	dsn = withDBName(adminDSN, dbName)
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("open postgres %q: %v", dbName, err)
	}
	if err := initializeDatabase(db); err != nil {
		_ = db.Close()
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("initialize postgres %q: %v", dbName, err)
	}
	if err := db.Close(); err != nil {
		_ = dropIsolatedDatabase(adminDB, dbName)
		sharedPostgres.lifecycle.Unlock()
		t.Fatalf("close bootstrap postgres %q: %v", dbName, err)
	}
	db, err = sql.Open("postgres", dsn)
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
		adminCleanupDB, err := sql.Open("postgres", adminDSN)
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
	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}
	if err := cleanupStaleTestContainers(dockerBin); err != nil {
		return err
	}

	name := fmt.Sprintf("swarm-test-pg-%d-%d", os.Getpid(), time.Now().UnixNano())
	runArgs := []string{
		"run", "-d", "--rm",
		"--name", name,
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=swarm",
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

func initializeDatabase(db *sql.DB) error {
	specPath, err := platformSpecPath()
	if err != nil {
		return err
	}
	b, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return fmt.Errorf("unmarshal platform spec: %w", err)
	}
	statements, err := bootstrapPlatformTableStatements(spec)
	if err != nil {
		return fmt.Errorf("generate platform tables ddl: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	if err := execBootstrapStatements(ctx, db, statements); err != nil {
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

func createIsolatedDatabase(adminDB *sql.DB, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
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

func platformSpecPath() (string, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	specPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	if _, statErr := os.Stat(specPath); statErr != nil {
		return "", fmt.Errorf("platform spec not found: %w", statErr)
	}
	return specPath, nil
}

var (
	testutilCreateTableName = regexp.MustCompile(`(?is)^create\s+table(?:\s+if\s+not\s+exists)?\s+"?([a-z_][a-z0-9_]*)"?`)
	testutilInlineIndexLine = regexp.MustCompile(`(?i)^(unique\s+)?index\s+([a-z_][a-z0-9_]*)\s*\((.+?)\)\s*(where\s+.+)?$`)
)

func bootstrapPlatformTableStatements(spec runtimecontracts.PlatformSpecDocument) ([]string, error) {
	tableNames := make([]string, 0, len(spec.PlatformTables.Tables))
	for tableName := range spec.PlatformTables.Tables {
		tableNames = append(tableNames, strings.TrimSpace(tableName))
	}
	sort.Slice(tableNames, func(i, j int) bool {
		left := bootstrapPlatformTableOrder(tableNames[i])
		right := bootstrapPlatformTableOrder(tableNames[j])
		if left != right {
			return left < right
		}
		return tableNames[i] < tableNames[j]
	})
	statements := make([]string, 0, len(tableNames)*2)
	for _, tableName := range tableNames {
		normalized, err := bootstrapNormalizePlatformDDL(spec.PlatformTables.Tables[tableName].DDL)
		if err != nil {
			return nil, fmt.Errorf("platform table %s: %w", tableName, err)
		}
		statements = append(statements, normalized...)
	}
	return statements, nil
}

func execBootstrapStatements(ctx context.Context, db *sql.DB, statements []string) error {
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func bootstrapNormalizePlatformDDL(rawDDL string) ([]string, error) {
	chunks := strings.Split(rawDDL, ";")
	statements := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		statement := strings.TrimSpace(chunk)
		if statement == "" {
			continue
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE "):
			tableStmt, indexStatements, err := bootstrapNormalizeCreateTable(statement)
			if err != nil {
				return nil, err
			}
			statements = append(statements, tableStmt)
			statements = append(statements, indexStatements...)
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE INDEX "):
			statements = append(statements, bootstrapEnsureIfNotExists(statement, "CREATE INDEX"))
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE UNIQUE INDEX "):
			statements = append(statements, bootstrapEnsureIfNotExists(statement, "CREATE UNIQUE INDEX"))
		default:
			return nil, fmt.Errorf("unsupported platform ddl statement %q", statement)
		}
	}
	if len(statements) == 0 {
		return nil, fmt.Errorf("no executable platform ddl statements found")
	}
	return statements, nil
}

func bootstrapNormalizeCreateTable(statement string) (string, []string, error) {
	statement = bootstrapEnsureIfNotExists(statement, "CREATE TABLE")
	tableName := bootstrapExtractTableName(statement)
	if tableName == "" {
		return "", nil, fmt.Errorf("unable to extract table name from %q", statement)
	}
	start := strings.Index(statement, "(")
	end := strings.LastIndex(statement, ")")
	if start < 0 || end <= start {
		return statement, nil, nil
	}
	body := statement[start+1 : end]
	lines := strings.Split(body, "\n")
	kept := make([]string, 0, len(lines))
	indexStatements := make([]string, 0, 2)
	for _, rawLine := range lines {
		trimmed := strings.TrimSpace(strings.TrimSuffix(rawLine, ","))
		if trimmed == "" {
			continue
		}
		if matches := testutilInlineIndexLine.FindStringSubmatch(trimmed); len(matches) >= 3 {
			uniquePrefix := strings.TrimSpace(matches[1])
			indexName := strings.TrimSpace(matches[2])
			indexCols := strings.TrimSpace(matches[3])
			whereClause := ""
			if len(matches) >= 5 {
				whereClause = strings.TrimSpace(matches[4])
			}
			indexStmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), indexCols)
			if uniquePrefix != "" {
				indexStmt = fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), indexCols)
			}
			if whereClause != "" {
				indexStmt += " " + whereClause
			}
			indexStatements = append(indexStatements, indexStmt)
			continue
		}
		kept = append(kept, trimmed)
	}
	return fmt.Sprintf("%s (\n    %s\n)", statement[:start], strings.Join(kept, ",\n    ")), indexStatements, nil
}

func bootstrapEnsureIfNotExists(statement, prefix string) string {
	statement = strings.TrimSpace(statement)
	upper := strings.ToUpper(statement)
	if strings.HasPrefix(upper, prefix+" IF NOT EXISTS ") {
		return statement
	}
	return prefix + " IF NOT EXISTS " + strings.TrimSpace(statement[len(prefix):])
}

func bootstrapExtractTableName(statement string) string {
	statement = strings.TrimSpace(statement)
	matches := testutilCreateTableName.FindStringSubmatch(statement)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func bootstrapPlatformTableOrder(name string) int {
	switch strings.TrimSpace(name) {
	case "schema_version":
		return 0
	case "runs":
		return 5
	case "events":
		return 10
	case "run_fork_selected_contract_bindings":
		return 15
	case "dead_letters":
		return 20
	case "agents":
		return 30
	case "flow_instances":
		return 40
	case "entity_state":
		return 50
	case "agent_sessions":
		return 60
	case "agent_turns":
		return 65
	case "routing_rules":
		return 70
	case "event_deliveries":
		return 80
	case "event_receipts":
		return 90
	case "entity_mutations":
		return 95
	case "mailbox":
		return 100
	case "spend_ledger":
		return 110
	case "timers":
		return 120
	default:
		return 1000
	}
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
