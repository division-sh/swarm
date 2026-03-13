package templateops

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	models "empireai/internal/runtime/actors"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestService_PublishPlanAndHelpers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	svc := NewService(db, pg)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', 'v1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	agentsV1 := []byte(`[{"role":"opco-ceo","parent_role":"","type":"sonnet","system_prompt":"CEO v1","tools":["t"],"subscriptions":["system.started"]},{"role":"vp-product","parent_role":"opco-ceo","type":"haiku","system_prompt":"VP","tools":[],"subscriptions":["board.*"]}]`)
	bootV1 := []byte(`[{"event_pattern":"board.*","subscriber_role":"opco-ceo","reason":"tests"}]`)
	seededV1 := []byte(`[]`)
	if err := svc.PublishTemplate(ctx, "v1", agentsV1, bootV1, seededV1, "tester", "v1"); err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// v2 changes CEO prompt, removes vp-product, and changes routes to create diffs.
	agentsV2 := []byte(`[{"role":"opco-ceo","parent_role":"","type":"sonnet","system_prompt":"CEO v2","tools":["t"],"subscriptions":["system.started"]}]`)
	bootV2 := []byte(`[{"event_pattern":"board.chat","subscriber_role":"opco-ceo","reason":"tests"}]`)
	seededV2 := []byte(`[{"event_pattern":"budget.*","subscriber_id":"empire-coordinator","reason":"broadcast"}]`)
	if err := svc.PublishTemplate(ctx, "v2", agentsV2, bootV2, seededV2, "tester", "v2"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// Planning should create a migration + mailbox item.
	n, err := svc.PlanMigrations(ctx, "v2", "tester", 10)
	if err != nil {
		t.Fatalf("plan migrations: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 migration, got %d", n)
	}

	var migID string
	var planRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, plan
		FROM template_migrations
		WHERE vertical_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, verticalID).Scan(&migID, &planRaw); err != nil {
		t.Fatalf("load migration: %v", err)
	}
	var plan migrationPlan
	if err := json.Unmarshal(planRaw, &plan); err != nil {
		t.Fatalf("unmarshal plan: %v", err)
	}
	if plan.VerticalID == "" || len(plan.Operations) == 0 {
		t.Fatalf("expected plan with operations, got %+v", plan)
	}

	// loadTemplateSnapshot: missing version should error; empty version is allowed.
	if _, err := svc.loadTemplateSnapshot(ctx, "missing"); err == nil {
		t.Fatal("expected missing template error")
	}
	if snap, err := svc.loadTemplateSnapshot(ctx, ""); err != nil || snap.Version != "" {
		t.Fatalf("expected empty snapshot, got %+v err=%v", snap, err)
	}

	// decodeOrBuildPlan should rebuild when invalid.
	if rebuilt, err := svc.decodeOrBuildPlan(ctx, verticalID, "v1", "v2", "tester", []byte("{")); err != nil || len(rebuilt.Operations) == 0 {
		t.Fatalf("expected rebuilt plan, err=%v ops=%d", err, len(rebuilt.Operations))
	}

	// ApplyMigration is intentionally blocked in this service (spec contract).
	if err := svc.ApplyMigration(ctx, migID, "tester"); err == nil {
		t.Fatal("expected ApplyMigration to fail")
	}
}

func TestService_executeOperationTX_CoversAllOps(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	svc := NewService(db, nil)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', 'v1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	agentID := "opco-ceo-" + verticalID
	addCfg := models.AgentConfig{
		ID:         agentID,
		Role:       "opco-ceo",
		Type:       "sonnet",
		Mode:       "operating",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"hi","tools":[]}`),
	}
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:    "ADD_AGENT",
		AgentID: agentID,
		Config:  addCfg,
	}); err != nil {
		t.Fatalf("ADD_AGENT: %v", err)
	}

	// RECONFIGURE_AGENT
	addCfg.Config = json.RawMessage(`{"system_prompt":"changed","tools":["a"]}`)
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:    "RECONFIGURE_AGENT",
		AgentID: agentID,
		Config:  addCfg,
	}); err != nil {
		t.Fatalf("RECONFIGURE_AGENT: %v", err)
	}

	// ADD_ROUTE
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:         "ADD_ROUTE",
		EventPattern: "board.*",
		SubscriberID: agentID,
		Reason:       "tests",
		Source:       "seeded",
	}); err != nil {
		t.Fatalf("ADD_ROUTE: %v", err)
	}

	// REMOVE_ROUTE: allowed for non-bootstrap routes.
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:          "REMOVE_ROUTE",
		EventPattern:  "board.*",
		SubscriberID:  agentID,
		AllowedRemove: true,
	}); err != nil {
		t.Fatalf("REMOVE_ROUTE: %v", err)
	}

	// Bootstrap routes cannot be removed (source filter); ensure the error path is covered.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO routing_rules (vertical_id, event_pattern, subscriber_id, installed_by, reason, status, source, bootstrap_version, created_at)
		VALUES ($1::uuid,'bootstrap.only',$2,$2,'x','active','bootstrap',1, now())
	`, verticalID, agentID); err != nil {
		t.Fatalf("seed bootstrap route: %v", err)
	}
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:          "REMOVE_ROUTE",
		EventPattern:  "bootstrap.only",
		SubscriberID:  agentID,
		AllowedRemove: true,
	}); err == nil {
		t.Fatal("expected REMOVE_ROUTE to be blocked for bootstrap")
	}

	// REMOVE_AGENT
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{
		Type:    "REMOVE_AGENT",
		AgentID: agentID,
	}); err != nil {
		t.Fatalf("REMOVE_AGENT: %v", err)
	}

	// Unsupported op type.
	if err := svc.executeOperationTX(ctx, tx, verticalID, "v2", "tester", migrationOperation{Type: "NOPE"}); err == nil {
		t.Fatal("expected unsupported op error")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestService_failMigration_EmitsAndReturnsError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	svc := NewService(db, pg)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	migID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES ('v2','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'x','x', now())
	`); err != nil {
		t.Fatalf("seed template: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO template_migrations (id, vertical_id, from_version, to_version, plan, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'v1', 'v2', '{}'::jsonb, 'executing', now())
	`, migID, verticalID); err != nil {
		t.Fatalf("seed migration: %v", err)
	}

	if err := svc.failMigration(ctx, migID, verticalID, "tester", errTest("boom")); err == nil {
		t.Fatal("expected error")
	}

	var status string
	_ = db.QueryRowContext(ctx, `SELECT status FROM template_migrations WHERE id=$1::uuid`, migID).Scan(&status)
	if status != "failed" {
		t.Fatalf("expected failed, got %q", status)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }

func TestHelpers_marshalAgentConfig(t *testing.T) {
	cfg := models.AgentConfig{
		Role:          "r",
		Mode:          "m",
		Config:        json.RawMessage(`{"system_prompt":"p","tools":["a"],"constraints":{"k":"v"}}`),
		Subscriptions: []string{"a", "a", "b"},
	}
	out := marshalAgentConfig(cfg)
	if !json.Valid(out) {
		t.Fatalf("expected valid json")
	}
	// normalizeStringList should de-dupe.
	if got := normalizeStringList([]string{"a", " ", "a", "b"}); len(got) != 2 {
		t.Fatalf("expected deduped list, got %v", got)
	}
	// coalesce should pick the first non-empty.
	if coalesce("", "x", "y") != "x" {
		t.Fatal("coalesce mismatch")
	}
	// mustJSON should not crash.
	_ = mustJSON(map[string]any{"t": time.Now().UTC().Format(time.RFC3339)})
}
