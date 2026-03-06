package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestShardDispatcher_AssignsPendingShard(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureShardsTable(t, db)

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            "market-research-agent",
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn base agent: %v", err)
	}
	manager.Run(ctx)

	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	scope := mustJSONRaw(map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "Argentina",
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, status, deadline_at, budget_cents, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 4, 'financial_ops+commerce_payments',
			$4::jsonb, 'pending', now() + interval '30 minutes', 50, now())
	`, uuid.NewString(), rootTaskID, scanID, string(scope)); err != nil {
		t.Fatalf("insert shard: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	go dispatcher.Run(ctx)

	var shardStatus, agentID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = db.QueryRowContext(ctx, `
			SELECT status, COALESCE(agent_id, '')
			FROM shards
			WHERE root_task_id = $1::uuid
			LIMIT 1
		`, rootTaskID).Scan(&shardStatus, &agentID)
		if shardStatus == "assigned" && agentID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if shardStatus != "assigned" || agentID == "" {
		t.Fatalf("expected shard assigned with agent_id, got status=%s agent_id=%s", shardStatus, agentID)
	}

	expectedClone := "market-research-agent-shard-0-" + shortID(scanID)
	if agentID != expectedClone {
		t.Fatalf("expected clone id %s, got %s", expectedClone, agentID)
	}

	var persistedStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM agents WHERE id = $1`, expectedClone).Scan(&persistedStatus); err != nil {
		t.Fatalf("lookup clone agent: %v", err)
	}
	if persistedStatus != "ephemeral" {
		t.Fatalf("expected persisted ephemeral status, got %s", persistedStatus)
	}
}

func TestShardDispatcher_RecoversWhenShardsTableAppearsLate(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            "market-research-agent",
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn base agent: %v", err)
	}
	manager.Run(ctx)

	// Simulate startup race: dispatcher starts before schema/table is ready.
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS shards`); err != nil {
		t.Fatalf("drop shards table: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	go dispatcher.Run(ctx)

	// Allow at least one dispatcher tick while the table is missing.
	time.Sleep(60 * time.Millisecond)

	ensureShardsTable(t, db)

	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	scope := mustJSONRaw(map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "Argentina",
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, status, deadline_at, budget_cents, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 4, 'financial_ops+commerce_payments',
			$4::jsonb, 'pending', now() + interval '30 minutes', 50, now())
	`, uuid.NewString(), rootTaskID, scanID, string(scope)); err != nil {
		t.Fatalf("insert shard after delayed table creation: %v", err)
	}

	var shardStatus, agentID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = db.QueryRowContext(ctx, `
			SELECT status, COALESCE(agent_id, '')
			FROM shards
			WHERE root_task_id = $1::uuid
			LIMIT 1
		`, rootTaskID).Scan(&shardStatus, &agentID)
		if shardStatus == "assigned" && agentID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if shardStatus != "assigned" || agentID == "" {
		t.Fatalf("expected shard assignment after delayed table readiness, got status=%s agent_id=%s", shardStatus, agentID)
	}

	expectedPrefix := "market-research-agent-shard-0-"
	if !strings.HasPrefix(agentID, expectedPrefix) {
		t.Fatalf("expected clone id prefix %q, got %q", expectedPrefix, agentID)
	}
}

func TestShardDispatcher_TerminalTimeoutEmitsCompletion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureShardsTable(t, db)

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)
	manager.Run(ctx)

	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	scope := mustJSONRaw(map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "Argentina",
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	})
	agentID := "market-research-agent-shard-0-" + shortID(scanID)
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            agentID,
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn synthetic shard agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, retry_count, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 4, 'financial_ops+commerce_payments',
			$4::jsonb, $5, 'assigned', now() - interval '10 minutes', 50, 2, now())
	`, uuid.NewString(), rootTaskID, scanID, string(scope), agentID); err != nil {
		t.Fatalf("insert assigned shard: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	go dispatcher.Run(ctx)

	var status string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = db.QueryRowContext(ctx, `
			SELECT status
			FROM shards
			WHERE root_task_id = $1::uuid
			LIMIT 1
		`, rootTaskID).Scan(&status)
		if status == "timed_out" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status != "timed_out" {
		t.Fatalf("expected timed_out shard status, got %s", status)
	}

	var evtID string
	var payloadRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, payload::text
		FROM events
		WHERE type = 'market_research.scan_complete'
		  AND source_agent = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, agentID).Scan(&evtID, &payloadRaw); err != nil {
		t.Fatalf("expected synthetic scan_complete event: %v", err)
	}
	if evtID == "" {
		t.Fatal("expected synthetic completion event id")
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		t.Fatalf("decode completion payload: %v", err)
	}
	shard, _ := payload["shard"].(map[string]any)
	if shard == nil || shard["status"] != "timed_out" {
		t.Fatalf("expected shard.status=timed_out, got payload=%v", payload)
	}
}

func TestShardDispatcher_DelaysTeardownUntilAssignmentReceiptSettles(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureShardsTable(t, db)

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            "trend-research-agent-shard-0-testscan",
		Type:          "sonnet",
		Role:          "trend-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"trend_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"trend_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn shard agent: %v", err)
	}
	manager.Run(ctx)

	shardID := uuid.NewString()
	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	agentID := "trend-research-agent-shard-0-testscan"
	scope := mustJSONRaw(map[string]any{"scan_id": scanID, "mode": "saas_trend", "geography": "Argentina"})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, assigned_at, deadline_at, completed_at, budget_cents, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'trend_research', 0, 2, 'technology_enablement',
			$4::jsonb, $5, 'completed', now() - interval '2 minutes', now() + interval '20 minutes', now(), 50, now()
		)
	`, shardID, rootTaskID, scanID, string(scope), agentID); err != nil {
		t.Fatalf("insert terminal shard: %v", err)
	}

	assignEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'trend_research.scan_assigned', 'shard-dispatcher', '{}'::jsonb, now() - interval '3 minutes')
	`, assignEventID); err != nil {
		t.Fatalf("insert assignment event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now() - interval '3 minutes')
	`, assignEventID, agentID); err != nil {
		t.Fatalf("insert assignment delivery: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	dispatcher.SetReceiptGracePeriod(250 * time.Millisecond)
	go dispatcher.Run(ctx)

	time.Sleep(80 * time.Millisecond)

	var pending int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		WHERE d.event_id = $1::uuid
		  AND d.agent_id = $2
		  AND r.event_id IS NULL
	`, assignEventID, agentID).Scan(&pending); err != nil {
		t.Fatalf("count pending receipt: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected assignment delivery to remain pending during grace, got %d", pending)
	}
	if _, ok := manager.GetAgentConfig(agentID); !ok {
		t.Fatal("expected agent to stay alive while receipt is pending within grace period")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := db.QueryRowContext(ctx, `
			SELECT status
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND agent_id = $2
		`, assignEventID, agentID).Scan(&status)
		if err == nil && status == "processed" {
			if _, ok := manager.GetAgentConfig(agentID); ok {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected dispatcher to reconcile pending receipt and teardown terminal agent")
}

func TestShardDispatcher_RequeuesAssignedShardWhenStartupStalled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureShardsTable(t, db)

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)

	cloneID := "market-research-agent-shard-0-stallscan"
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            "market-research-agent",
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn base agent: %v", err)
	}
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            cloneID,
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn synthetic assigned clone: %v", err)
	}
	manager.Run(ctx)

	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	shardID := uuid.NewString()
	scope := mustJSONRaw(map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "Argentina",
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, assigned_at, deadline_at, budget_cents, retry_count, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 4, 'financial_ops+commerce_payments',
			$4::jsonb, $5, 'assigned', now() - interval '5 minutes', now() + interval '20 minutes', 50, 0, now()
		)
	`, shardID, rootTaskID, scanID, string(scope), cloneID); err != nil {
		t.Fatalf("insert assigned shard: %v", err)
	}

	assignEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'market_research.scan_assigned', 'shard-dispatcher', '{}'::jsonb, now() - interval '5 minutes')
	`, assignEventID); err != nil {
		t.Fatalf("insert assignment event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now() - interval '5 minutes')
	`, assignEventID, cloneID); err != nil {
		t.Fatalf("insert assignment delivery: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	dispatcher.SetStartupGracePeriod(80 * time.Millisecond)
	go dispatcher.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	var (
		status     string
		retryCount int
	)
	for time.Now().Before(deadline) {
		if err := db.QueryRowContext(ctx, `
			SELECT status, retry_count
			FROM shards
			WHERE id = $1::uuid
		`, shardID).Scan(&status, &retryCount); err == nil && retryCount >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if retryCount < 1 {
		t.Fatalf("expected startup stall watchdog to increment retry_count, got status=%s retry_count=%d", status, retryCount)
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND agent_id = $2
	`, assignEventID, cloneID).Scan(&receiptStatus); err != nil {
		t.Fatalf("expected reconciled assignment receipt, got err=%v", err)
	}
	if receiptStatus != "processed" {
		t.Fatalf("expected reconciled receipt status=processed, got %q", receiptStatus)
	}
}

func TestShardDispatcher_DoesNotRequeueStartupStallWithActiveLease(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureShardsTable(t, db)

	cfg := testDispatcherConfig(t)
	pg := &store.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, nil, pg)

	cloneID := "market-research-agent-shard-0-livelease"
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            "market-research-agent",
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn base agent: %v", err)
	}
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:            cloneID,
		Type:          "sonnet",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
		Config:        mustJSONRaw(map[string]any{"system_prompt": "x", "subscriptions": []string{"market_research.scan_assigned"}}),
	}); err != nil {
		t.Fatalf("spawn synthetic assigned clone: %v", err)
	}
	manager.Run(ctx)

	rootTaskID := uuid.NewString()
	scanID := uuid.NewString()
	shardID := uuid.NewString()
	scope := mustJSONRaw(map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "Argentina",
		"taxonomy_categories": []string{"financial_ops", "commerce_payments"},
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, assigned_at, deadline_at, budget_cents, retry_count, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 4, 'financial_ops+commerce_payments',
			$4::jsonb, $5, 'assigned', now() - interval '5 minutes', now() + interval '20 minutes', 50, 0, now()
		)
	`, shardID, rootTaskID, scanID, string(scope), cloneID); err != nil {
		t.Fatalf("insert assigned shard: %v", err)
	}

	assignEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'market_research.scan_assigned', 'shard-dispatcher', '{}'::jsonb, now() - interval '5 minutes')
	`, assignEventID); err != nil {
		t.Fatalf("insert assignment event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now() - interval '5 minutes')
	`, assignEventID, cloneID); err != nil {
		t.Fatalf("insert assignment delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			id, agent_id, runtime_mode, provider, session_id, status,
			lock_owner, lock_expires_at, turn_count, created_at, last_used_at
		)
		VALUES (
			$1::uuid, $2, 'cli_test', 'anthropic', 'sess-live', 'active',
			'lease-owner', now() + interval '10 minutes', 0, now() - interval '5 minutes', now()
		)
	`, uuid.NewString(), cloneID); err != nil {
		t.Fatalf("insert active lease session: %v", err)
	}

	dispatcher := rt.NewShardDispatcher(db, bus, manager, cfg.Sharding)
	dispatcher.SetPollInterval(20 * time.Millisecond)
	dispatcher.SetStartupGracePeriod(80 * time.Millisecond)
	go dispatcher.Run(ctx)

	time.Sleep(250 * time.Millisecond)

	var (
		status     string
		retryCount int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT status, retry_count
		FROM shards
		WHERE id = $1::uuid
	`, shardID).Scan(&status, &retryCount); err != nil {
		t.Fatalf("load shard state: %v", err)
	}
	if status != "assigned" || retryCount != 0 {
		t.Fatalf("expected assigned shard with retry_count=0 while lease is active, got status=%s retry_count=%d", status, retryCount)
	}

	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND agent_id = $2
	`, assignEventID, cloneID).Scan(&receiptCount); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if receiptCount != 0 {
		t.Fatalf("expected no reconciled receipt while shard lease is active, got %d", receiptCount)
	}
}

func ensureShardsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS shards (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			root_task_id UUID NOT NULL,
			scan_id UUID,
			stage TEXT NOT NULL,
			shard_index INT NOT NULL,
			shard_count INT NOT NULL,
			shard_key TEXT NOT NULL,
			scope JSONB NOT NULL,
			agent_id TEXT REFERENCES agents(id),
			status TEXT NOT NULL DEFAULT 'pending',
			deadline_at TIMESTAMPTZ NOT NULL,
			budget_cents INT NOT NULL,
			spend_cents INT NOT NULL DEFAULT 0,
			retry_count INT NOT NULL DEFAULT 0,
			error TEXT,
			assigned_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_shards_idempotent ON shards(root_task_id, shard_key);
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}
}

func testDispatcherConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "api"
	cfg.LLM.Session.LockTTL = time.Second
	cfg.LLM.Session.RotateAfterTurns = 1
	cfg.LLM.Session.RotateOnParseFailures = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	return cfg
}

func mustJSONRaw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	if len(b) == 0 {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(b)
}

func shortID(raw string) string {
	raw = strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if len(raw) >= 8 {
		return raw[:8]
	}
	if raw == "" {
		return "unknown"
	}
	return raw
}
