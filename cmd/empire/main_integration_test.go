package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"

	"empireai/internal/config"
	"empireai/internal/mailbox"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func repoRootFromCmd(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	// cmd/empire -> repo root
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func mustDSNField(t *testing.T, dsn, key string) string {
	t.Helper()
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, key+"=") {
			return strings.TrimPrefix(part, key+"=")
		}
	}
	t.Fatalf("%s not found in dsn: %q", key, dsn)
	return ""
}

func mustPortFromDSN(t *testing.T, dsn string) int {
	t.Helper()
	n, err := strconv.Atoi(mustDSNField(t, dsn, "port"))
	if err != nil {
		t.Fatalf("parse port from dsn %q: %v", dsn, err)
	}
	return n
}

func mustDBNameFromDSN(t *testing.T, dsn string) string {
	t.Helper()
	return mustDSNField(t, dsn, "dbname")
}

func writeTempConfig(t *testing.T, dsn string) string {
	t.Helper()
	port := mustPortFromDSN(t, dsn)
	dbName := mustDBNameFromDSN(t, dsn)
	cfg := strings.Join([]string{
		"runtime:",
		"  max_concurrent_agents: 10",
		"  event_poll_interval: 1s",
		"  recovery_on_startup: false",
		"database:",
		"  host: 127.0.0.1",
		fmt.Sprintf("  port: %d", port),
		fmt.Sprintf("  name: %s", dbName),
		"  user: postgres",
		"  password: postgres",
		"  sslmode: disable",
		"  pool_size: 5",
		"llm:",
		"  runtime_mode: cli_test",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"  claude_cli:",
		"    command: true",
		"    timeout: 2s",
		"    output_format: json",
		"    retries: 1",
		"    no_session_persistence: false",
		"    use_tmux: false",
		"budget:",
		"  factory_monthly_cap: 50000",
		"  per_vertical_monthly_cap: 20000",
		"  portfolio_monthly_cap: 100000",
		"  auto_approve_spend_below: 1500",
		"  human_tasks:",
		"    max_tasks_per_week: 3",
		"    budget_reset: monday",
		"    auto_expire_hours: 168",
		"    categories_enabled: [verification, partnership]",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "empire.yaml")
	if err := os.WriteFile(p, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

func writeTempEmpireConfig(t *testing.T, dsn string) string {
	t.Helper()
	port := mustPortFromDSN(t, dsn)
	dbName := mustDBNameFromDSN(t, dsn)
	dir := t.TempDir()
	path := filepath.Join(dir, "empire.yaml")
	raw := `
runtime:
  max_concurrent_agents: 2
llm:
  runtime_mode: cli_test
  session:
    lock_ttl: 1s
    rotate_after_turns: 40
    rotate_on_parse_failures: 3
  claude_cli:
    command: "true"
    output_format: "json"
    timeout: 1s
    retries: 1
database:
  host: 127.0.0.1
  port: ` + strconv.Itoa(port) + `
  name: ` + dbName + `
  user: postgres
  password: postgres
  sslmode: disable
`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestCLI_PrintTasksAndMailboxAndDigest(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, db, _ := testutil.StartPostgres(t)
	cfgPath := writeTempConfig(t, dsn)

	// Seed minimal org template + agents so some operator actions have data.
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES ('2.0.1', '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, 'test', 'test', now())
	`); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Seed a mailbox item and a human task.
	mbID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES ($1::uuid, $2::uuid, 'empire-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'test', now())
	`, mbID, verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	taskID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'pending_review', now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed human task: %v", err)
	}

	// Exercise printHumanTasks/printHumanTask (main.go) directly.
	if err := printHumanTasks(context.Background(), db, "all", "", "", 10); err != nil {
		t.Fatalf("printHumanTasks: %v", err)
	}
	if err := printHumanTask(context.Background(), db, taskID); err != nil {
		t.Fatalf("printHumanTask: %v", err)
	}

	// Exercise mailbox subcommand parsing and operator action wiring.
	if err := runMailboxSubcommand([]string{"list", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("mailbox list: %v", err)
	}
	if err := runMailboxSubcommand([]string{"view", "--config", cfgPath, "--store", "postgres", mbID}); err != nil {
		t.Fatalf("mailbox view: %v", err)
	}
	if err := runMailboxSubcommand([]string{"decide", "--config", cfgPath, "--store", "postgres", "--action", "approve", mbID}); err != nil {
		t.Fatalf("mailbox decide: %v", err)
	}

	// Digest command should run against postgres store.
	if err := runDigestSubcommand([]string{"--config", cfgPath, "--store", "postgres", "--top", "3"}); err != nil {
		t.Fatalf("digest: %v", err)
	}

	// Template init helpers should operate without starting a full runtime.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, filepath.Join(root, "contracts", "ddl-canonical.sql"))
	if err := ensureInitialTemplateCLI(context.Background(), stores.SQLDB, stores.MailboxStore, "2.0.1",
		filepath.Join(root, "configs", "agents", "templates"),
		filepath.Join(root, "configs", "agents", "templates", "routes.yaml"),
	); err != nil {
		t.Fatalf("ensureInitialTemplateCLI: %v", err)
	}
	if err := persistRuntimeConfig(context.Background(), stores.SQLDB, cfgPath); err != nil {
		t.Fatalf("persistRuntimeConfig: %v", err)
	}
}

func TestTelegramHelpers_RenderAndDeliver(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	taskID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, expected_value, priority, status, created_at, talking_points)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'high', 'high', 'approved', now(), $3::jsonb)
	`, taskID, verticalID, `["a","b"]`); err != nil {
		t.Fatalf("seed human task: %v", err)
	}

	// Local fake telegram endpoint.
	var gotText string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		gotText = string(b)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer ts.Close()

	tg := &mailbox.TelegramNotifier{
		BotToken: "x",
		ChatID:   "1",
		BaseURL:  ts.URL,
		Client:   ts.Client(),
	}

	if err := deliverTaskByID(context.Background(), db, tg, taskID); err != nil {
		t.Fatalf("deliverTaskByID: %v", err)
	}
	if gotText == "" {
		t.Fatalf("expected telegram request body")
	}

	// Render helpers.
	msg := renderTaskTelegramMessage(humanTaskRow{ID: taskID, Category: "verification", Description: "call", Priority: "high"})
	if !strings.Contains(msg, "/claim") {
		t.Fatalf("expected commands in message, got %q", msg)
	}
	if tp := renderTalkingPoints([]byte(`["x","y"]`)); !strings.Contains(tp, "- x") {
		t.Fatalf("unexpected talking points: %q", tp)
	}
}

type stubScheduleStore struct {
	last runtimepipeline.Schedule
}

func (s *stubScheduleStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	_ = ctx
	s.last = sc
	return nil
}
func (s *stubScheduleStore) CancelSchedule(context.Context, string, string) error { return nil }
func (s *stubScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return nil, nil
}
func (s *stubScheduleStore) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func TestOpsMonitors_EnsureDigestScheduleAndCompactRender(t *testing.T) {
	st := &stubScheduleStore{}
	if err := ensurePortfolioDigestSchedule(context.Background(), st); err != nil {
		t.Fatalf("ensure schedule: %v", err)
	}
	if st.last.AgentID != "empire-coordinator" || st.last.EventType != "timer.portfolio_digest" {
		t.Fatalf("unexpected schedule: %+v", st.last)
	}
}
