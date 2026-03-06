package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestCLI_Subcommands_EndToEnd_More(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	cfgPath := writeTempConfig(t, dsn)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}

	t.Setenv("EMPIREAI_ENABLE_DOCKER_WORKSPACES", "false")
	migrationFile := filepath.Join(root, "contracts", "ddl-canonical.sql")
	ctx := context.Background()
	stores := buildStores(ctx, "postgres", cfg, true, migrationFile)
	db := stores.SQLDB
	if db == nil {
		t.Fatalf("expected postgres db")
	}

	// Seed baseline agents needed for targeted deliveries.
	for _, id := range []string{"empire-coordinator", "spec-auditor"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{"system_prompt":"x","subscriptions":["*"]}'::jsonb, now(), now())
			ON CONFLICT (id) DO NOTHING
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
	}

	// Operating vertical for most commands.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', NULL, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed operating vertical: %v", err)
	}
	// Team agent for team listing and routing FK usage.
	seedAgentID := "seed-agent-" + verticalID
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, coordinator_id, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'seed-role', 'operating', $2::uuid, 'active', 'empire-coordinator', '{"system_prompt":"x","subscriptions":["board.*"]}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, seedAgentID, verticalID); err != nil {
		t.Fatalf("seed team agent: %v", err)
	}

	// Vertical metrics + event rows.
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO vertical_metrics (id, vertical_id, period_start, period_end, users_total, users_new, users_churned, mrr_cents, api_cost_cents, infra_cost_cents, csat_avg, created_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, 10, 2, 0, 1234, 10, 20, 4.2, now())
	`, uuid.NewString(), verticalID, now.Add(-24*time.Hour), now); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	evtID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'board.directive', 'human', $2::uuid, '{}'::jsonb, now())
	`, evtID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	// Deployments.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO deployments (id, vertical_id, status, url, environment, version, health_status, created_at)
		VALUES ($1::uuid, $2::uuid, 'deployed', 'https://example.com', 'production', 1, 'healthy', now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed deployment: %v", err)
	}

	// Status and verticals.
	if err := runStatusSubcommand([]string{"--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("status: %v", err)
	}
	if err := runStatusSubcommand([]string{"--config", cfgPath, "--store", "postgres", "--vertical", "testco"}); err != nil {
		t.Fatalf("status vertical: %v", err)
	}
	if err := runVerticalsSubcommand([]string{"list", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("verticals list: %v", err)
	}
	if err := runVerticalsSubcommand([]string{"operating", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("verticals operating: %v", err)
	}

	// Vertical subcommands.
	if err := runVerticalSubcommand([]string{"testco", "metrics", "--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("vertical metrics: %v", err)
	}
	if err := runVerticalSubcommand([]string{"testco", "team", "--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("vertical team: %v", err)
	}
	if err := runVerticalSubcommand([]string{"testco", "logs", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("vertical logs: %v", err)
	}
	if err := runVerticalSubcommand([]string{"testco", "logs", "--config", cfgPath, "--store", "postgres", "--agent", "human", "--limit", "5"}); err != nil {
		t.Fatalf("vertical logs filtered: %v", err)
	}
	if err := runVerticalSubcommand([]string{"testco", "kill", "--config", cfgPath, "--store", "postgres", "--notes", "test"}); err != nil {
		t.Fatalf("vertical kill: %v", err)
	}

	// Deployments.
	if err := runDeploymentsSubcommand([]string{"list", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("deployments list: %v", err)
	}
	if err := runDeploymentsSubcommand([]string{"health", "--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("deployments health: %v", err)
	}

	// Secrets (exercise pgcrypto encryption path).
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "unit-test-key")
	if err := runSecretsSubcommand([]string{"set", "--config", cfgPath, "--store", "postgres", "testco", "webhooks.whatsapp.secret", "s3cr3t"}); err != nil {
		t.Fatalf("secrets set: %v", err)
	}
	if err := runSecretsSubcommand([]string{"list", "--config", cfgPath, "--store", "postgres", "testco"}); err != nil {
		t.Fatalf("secrets list: %v", err)
	}
	if err := runSecretsSubcommand([]string{"rotate", "--config", cfgPath, "--store", "postgres", "--value", "new", "testco", "whatsapp"}); err != nil {
		t.Fatalf("secrets rotate: %v", err)
	}
	var credRaw []byte
	_ = db.QueryRowContext(ctx, `SELECT COALESCE(credentials,'{}'::jsonb) FROM verticals WHERE id=$1::uuid`, verticalID).Scan(&credRaw)
	if !strings.Contains(string(credRaw), "enc::") {
		t.Fatalf("expected encrypted credential marker, got %s", string(credRaw))
	}

	// Config file manipulation (standalone yaml set/get).
	cfgFile := filepath.Join(t.TempDir(), "x.yaml")
	if err := os.WriteFile(cfgFile, []byte("a:\n  b: 1\n"), 0o644); err != nil {
		t.Fatalf("write cfg file: %v", err)
	}
	if err := runConfigSubcommand([]string{"set", "--file", cfgFile, "a.c", "2"}); err != nil {
		t.Fatalf("config set: %v", err)
	}
	if err := runConfigSubcommand([]string{"get", "--file", cfgFile, "a.c"}); err != nil {
		t.Fatalf("config get: %v", err)
	}

	// Scan (local synthetic scanners).
	if err := runScanSubcommand([]string{"--config", cfgPath, "--store", "postgres", "--geography", "us", "--depth", "discovery", "--count", "2"}); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Factory run: seed one pending factory vertical.
	factoryID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'FactoryCo', 'factoryco', 'us', 'discovered', 'factory', now(), now())
	`, factoryID); err != nil {
		t.Fatalf("seed factory vertical: %v", err)
	}
	if err := runFactorySubcommand([]string{"run", "--config", cfgPath, "--store", "postgres", "--limit", "5"}); err != nil {
		t.Fatalf("factory run: %v", err)
	}

	// Spec audit using a file input.
	specPath := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(specPath, []byte(`{"problem":"p","target_user":"u","core_workflow":"w","features":["f"]}`), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	if err := runSpecAuditSubcommand([]string{
		"--config", cfgPath,
		"--store", "postgres",
		"--spec-type", "vertical_spec",
		"--vertical-id", verticalID,
		"--spec-file", specPath,
		"--requested-by", "empire-coordinator",
	}); err != nil {
		t.Fatalf("spec-audit: %v", err)
	}

	// Template publish/plan/apply: publish minimal templates via legacy json flags.
	agentsT2 := filepath.Join(t.TempDir(), "agents_t2.json")
	bootT2 := filepath.Join(t.TempDir(), "boot_t2.json")
	seedT2 := filepath.Join(t.TempDir(), "seed_t2.json")
	_ = os.WriteFile(agentsT2, []byte(`[{"role":"opco-ceo","parent_role":"","type":"sonnet","system_prompt":"CEO {vertical_id}","tools":[],"subscriptions":["board.*"]},{"role":"vp-product","parent_role":"opco-ceo","type":"haiku","system_prompt":"VP","tools":[],"subscriptions":["board.*"]}]`), 0o644)
	_ = os.WriteFile(bootT2, []byte(`[{"event_pattern":"board.*","subscriber_role":"opco-ceo","reason":"tests"}]`), 0o644)
	_ = os.WriteFile(seedT2, []byte(`[]`), 0o644)
	if err := runTemplateSubcommand([]string{
		"publish",
		"--config", cfgPath,
		"--store", "postgres",
		"--version", "t2",
		"--agents-file", agentsT2,
		"--bootstrap-routes-file", bootT2,
		"--seeded-routes-file", seedT2,
		"--created-by", "empire-coordinator",
		"--description", "test",
	}); err != nil {
		t.Fatalf("template publish t2: %v", err)
	}

	// Plan should create a migration because vertical.template_version is NULL/empty.
	if err := runTemplateSubcommand([]string{
		"plan",
		"--config", cfgPath,
		"--store", "postgres",
		"--to-version", "t2",
		"--requested-by", "empire-coordinator",
		"--limit", "10",
	}); err != nil {
		t.Fatalf("template plan: %v", err)
	}
	var migID, mailboxID string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, COALESCE(mailbox_id::text,'')
		FROM template_migrations
		WHERE vertical_id = $1::uuid AND to_version = 't2'
		ORDER BY created_at DESC
		LIMIT 1
	`, verticalID).Scan(&migID, &mailboxID); err != nil {
		t.Fatalf("load migration ids: %v", err)
	}
	if mailboxID == "" {
		t.Fatalf("expected mailbox linked to migration")
	}
	if err := runMailboxSubcommand([]string{"decide", "--config", cfgPath, "--store", "postgres", "--action", "approve", mailboxID}); err != nil {
		t.Fatalf("approve migration mailbox: %v", err)
	}
	if err := runTemplateSubcommand([]string{"apply", "--config", cfgPath, "--store", "postgres", "--executed-by", "empire-coordinator", migID}); err != nil {
		t.Fatalf("template apply: %v", err)
	}

	// Publish a new template (t3) that triggers RECONFIGURE + REMOVE + route changes.
	agentsT3 := filepath.Join(t.TempDir(), "agents_t3.json")
	bootT3 := filepath.Join(t.TempDir(), "boot_t3.json")
	seedT3 := filepath.Join(t.TempDir(), "seed_t3.json")
	_ = os.WriteFile(agentsT3, []byte(`[{"role":"opco-ceo","parent_role":"","type":"sonnet","system_prompt":"CEO changed {vertical_name}","tools":[],"subscriptions":["board.*"]}]`), 0o644)
	_ = os.WriteFile(bootT3, []byte(`[{"event_pattern":"board.chat","subscriber_role":"opco-ceo","reason":"tests"}]`), 0o644)
	_ = os.WriteFile(seedT3, []byte(`[]`), 0o644)
	if err := runTemplateSubcommand([]string{
		"publish",
		"--config", cfgPath,
		"--store", "postgres",
		"--version", "t3",
		"--agents-file", agentsT3,
		"--bootstrap-routes-file", bootT3,
		"--seeded-routes-file", seedT3,
		"--created-by", "empire-coordinator",
	}); err != nil {
		t.Fatalf("template publish t3: %v", err)
	}
	if err := runTemplateSubcommand([]string{
		"plan",
		"--config", cfgPath,
		"--store", "postgres",
		"--to-version", "t3",
		"--requested-by", "empire-coordinator",
		"--limit", "10",
	}); err != nil {
		t.Fatalf("template plan t3: %v", err)
	}
	var migID2, mailboxID2 string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, COALESCE(mailbox_id::text,'')
		FROM template_migrations
		WHERE vertical_id = $1::uuid AND to_version = 't3'
		ORDER BY created_at DESC
		LIMIT 1
	`, verticalID).Scan(&migID2, &mailboxID2); err != nil {
		t.Fatalf("load t3 migration ids: %v", err)
	}
	if err := runMailboxSubcommand([]string{"decide", "--config", cfgPath, "--store", "postgres", "--action", "approve", mailboxID2}); err != nil {
		t.Fatalf("approve t3 migration mailbox: %v", err)
	}
	if err := runTemplateSubcommand([]string{"apply", "--config", cfgPath, "--store", "postgres", "--executed-by", "empire-coordinator", migID2}); err != nil {
		t.Fatalf("template apply t3: %v", err)
	}

	// Ops: record metrics + tick.
	if err := runOpsSubcommand([]string{"record-metrics", "--config", cfgPath, "--store", "postgres", "--vertical-id", verticalID, "--users-total", "12", "--mrr-cents", "2000"}); err != nil {
		t.Fatalf("ops record-metrics: %v", err)
	}
	if err := runOpsSubcommand([]string{"tick", "--config", cfgPath, "--store", "postgres"}); err != nil {
		t.Fatalf("ops tick: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed system.started: %v", err)
	}

	// Directive: default + legacy target syntax.
	if err := runDirectiveSubcommand([]string{"--config", cfgPath, "--store", "postgres", "hello coordinator"}); err != nil {
		t.Fatalf("directive: %v", err)
	}
	if err := runDirectiveSubcommand([]string{"--config", cfgPath, "--store", "postgres", "empire-coordinator", "hello again"}); err != nil {
		t.Fatalf("directive legacy: %v", err)
	}

	// Chat async.
	if err := runChatSubcommand([]string{"--config", cfgPath, "--store", "postgres", "--async=true", verticalID + "/ceo", "ping"}); err != nil {
		t.Fatalf("chat async: %v", err)
	}

	// Telegram command handler: /details + /claim + /reject + /complete_*.
	taskID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'approved', now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed human task: %v", err)
	}
	msg := &telegramMessage{}
	msg.From.Username = "founder"
	msg.Text = "/details " + taskID[:8]
	if out := handleTelegramTaskCommand(ctx, stores, cfg, msg); !strings.Contains(out, "TASK details") {
		t.Fatalf("details output: %q", out)
	}
	msg.Text = "/claim " + taskID[:8]
	if out := handleTelegramTaskCommand(ctx, stores, cfg, msg); !strings.Contains(out, "Claimed") {
		t.Fatalf("claim output: %q", out)
	}
	msg.Text = "/reject " + taskID[:8] + " not now"
	if out := handleTelegramTaskCommand(ctx, stores, cfg, msg); !strings.Contains(out, "Rejected") {
		t.Fatalf("reject output: %q", out)
	}
	msg.Text = "/complete_success " + taskID[:8] + " done"
	if out := handleTelegramTaskCommand(ctx, stores, cfg, msg); !strings.Contains(out, "Completed") {
		t.Fatalf("complete output: %q", out)
	}

	// Ops monitors: steady-state + health evaluation.
	launched := time.Now().Add(-12 * 7 * 24 * time.Hour)
	if _, err := db.ExecContext(ctx, `UPDATE verticals SET launched_at=$2 WHERE id=$1::uuid`, verticalID, launched); err != nil {
		t.Fatalf("set launched_at: %v", err)
	}
	// Force a red warning by combining low users and high spend.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, description, approved_by, created_at)
		VALUES ($1::uuid,$2::uuid,'api',999999,'x','estimated', now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}
	if w, ok, err := evaluateVerticalHealth(ctx, db, verticalID); err != nil || !ok || strings.TrimSpace(w.Severity) == "" {
		t.Fatalf("evaluateVerticalHealth ok=%v err=%v w=%+v", ok, err, w)
	}
	bus := runtime.NewEventBus(stores.EventStore)
	if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
		t.Fatalf("maybeEmitSteadyState: %v", err)
	}

	// Human task expiry loop: processes immediately, then we cancel.
	{
		task2 := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at, requeue_count, review_decision)
			VALUES ($1::uuid,'empire-coordinator',$2::uuid,'verification','x','deferred', now() - interval '10 day', 0,
			        jsonb_build_object('requeue_date', (now() - interval '1 day')::text))
		`, task2, verticalID); err != nil {
			t.Fatalf("seed deferred human task: %v", err)
		}
		task3 := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at, requeue_count)
			VALUES ($1::uuid,'empire-coordinator',$2::uuid,'verification','x','approved', now() - interval '400 hour', 2)
		`, task3, verticalID); err != nil {
			t.Fatalf("seed expirable human task: %v", err)
		}
		loopCtx, cancel := context.WithCancel(ctx)
		go humanTaskExpiryLoop(loopCtx, db, cfg, bus)
		time.Sleep(150 * time.Millisecond)
		cancel()
	}

	// Marginal maintenance loop: processes immediately, then we cancel.
	{
		m1 := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, parked_at, created_at, updated_at)
			VALUES ($1::uuid,'Marginal60','m60','us','marginal_review','factory', now() - interval '61 day', now(), now())
		`, m1); err != nil {
			t.Fatalf("seed marginal kill: %v", err)
		}
		m2 := uuid.NewString()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, parked_at, created_at, updated_at)
			VALUES ($1::uuid,'Marginal14','m14','us','marginal_review','factory', now() - interval '15 day', now(), now())
		`, m2); err != nil {
			t.Fatalf("seed marginal requeue: %v", err)
		}
		loopCtx, cancel := context.WithCancel(ctx)
		go marginalMaintenanceLoop(loopCtx, db, bus)
		time.Sleep(150 * time.Millisecond)
		cancel()
	}

	// Coverage helpers: exercise small pure funcs.
	if normalizeAgentAlias("CTO") != "cto-agent" {
		t.Fatalf("normalizeAgentAlias mismatch")
	}
	if !isUUID(verticalID) {
		t.Fatalf("expected uuid")
	}
	if trunc("hello", 2) != "he" {
		t.Fatalf("trunc mismatch")
	}

	// Ensure bus can dispatch and filterExistingRecipients doesn't explode.
	_ = appendTargetedEvent(ctx, stores, events.Event{Type: "budget.warning", SourceAgent: "test"}, []string{"empire-coordinator", "missing"})
}
