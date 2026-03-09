package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"empireai/internal/config"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

// managerStoreFailLoad forces AgentManager.Recover to fail early so runRuntime
// exercises the recovery error hardening path.
type managerStoreFailLoad struct {
	runtimemanager.ManagerPersistence
}

func (m managerStoreFailLoad) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, context.Canceled
}

func TestRunRuntime_StartupBranches_InboundToolGatewayDashboard_RecoveryHardening(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	cfgPath := writeTempConfig(t, dsn)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	cfg.Runtime.RecoveryOnStartup = true

	t.Setenv("EMPIREAI_ENABLE_DOCKER_WORKSPACES", "false")
	t.Setenv("EMPIREAI_INBOUND_ADDR", "127.0.0.1:0")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_ADDR", "127.0.0.1:0")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_TOKEN", "test-token")
	t.Setenv("EMPIREAI_DASHBOARD_ADDR", "127.0.0.1:0")

	// Notifiers: creation only (no deliveries in DB).
	t.Setenv("EMPIREAI_NOTIFY_WEBHOOK_URL", "http://example.invalid")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_BOT_TOKEN", "t")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_CHAT_ID", "1")

	ctx := context.Background()
	stores := buildStores(ctx, "postgres", cfg, false, filepath.Join(root, "contracts", "ddl-canonical.sql"))
	if stores.SQLDB == nil || stores.ManagerStore == nil || stores.ScheduleStore == nil {
		t.Fatalf("expected postgres stores")
	}
	// Force recover to fail.
	stores.ManagerStore = managerStoreFailLoad{stores.ManagerStore}

	// Seed minimal agent required by schedules FK.
	if _, err := stores.SQLDB.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ('empire-coordinator','stub','empire-coordinator','holding','active','{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	// Seed a schedule so the restore loop executes Register().
	if _, err := stores.SQLDB.ExecContext(ctx, `
		INSERT INTO schedules (id, agent_id, event_type, mode, cron_expr, payload, active, created_at)
		VALUES ($1::uuid, 'empire-coordinator', 'timer.portfolio_digest', 'cron', '*/5 * * * *', '{}'::jsonb, true, now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed schedule: %v", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	if err := runRuntime(runCtx, cfg, stores, false); err != nil {
		t.Fatalf("runRuntime: %v", err)
	}
}

func TestRunRuntime_StartupSchedulesEnsureAfterGlobalAgentSync(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	cfgPath := writeTempConfig(t, dsn)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	cfg.Runtime.RecoveryOnStartup = false

	t.Setenv("EMPIREAI_ENABLE_DOCKER_WORKSPACES", "false")
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", filepath.Join(root, "configs", "agents"))
	t.Setenv("EMPIREAI_INBOUND_ADDR", "")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_ADDR", "")
	t.Setenv("EMPIREAI_DASHBOARD_ADDR", "")

	ctx := context.Background()
	stores := buildStores(ctx, "postgres", cfg, false, filepath.Join(root, "contracts", "ddl-canonical.sql"))
	if stores.SQLDB == nil || stores.ScheduleStore == nil || stores.ManagerStore == nil {
		t.Fatalf("expected postgres stores")
	}

	// Start from empty agents/schedules tables to validate startup ordering.
	if _, err := stores.SQLDB.ExecContext(ctx, `DELETE FROM schedules`); err != nil {
		t.Fatalf("clear schedules: %v", err)
	}
	if _, err := stores.SQLDB.ExecContext(ctx, `DELETE FROM agents`); err != nil {
		t.Fatalf("clear agents: %v", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	if err := runRuntime(runCtx, cfg, stores, false); err != nil {
		t.Fatalf("runRuntime: %v", err)
	}

	var digestCount int
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT count(*)
		FROM schedules
		WHERE agent_id = 'lifecycle-orchestrator'
		  AND event_type = 'timer.portfolio_digest'
		  AND active = true
	`).Scan(&digestCount); err != nil {
		t.Fatalf("query digest schedule: %v", err)
	}
	if digestCount == 0 {
		t.Fatal("expected portfolio digest schedule to be present after startup")
	}

	var infraCount int
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT count(*)
		FROM schedules
		WHERE agent_id = 'holding-devops'
		  AND event_type = 'timer.infra_health_check'
		  AND active = true
	`).Scan(&infraCount); err != nil {
		t.Fatalf("query infra schedule: %v", err)
	}
	if infraCount == 0 {
		t.Fatal("expected infra health schedule to be present after startup")
	}
}
