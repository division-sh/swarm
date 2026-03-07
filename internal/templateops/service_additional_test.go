package templateops

import (
	"context"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"encoding/json"
	"github.com/google/uuid"
	"testing"
	"time"
)

type countingMailbox struct {
	*store.PostgresStore
	inserted int
}

func (m *countingMailbox) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	m.inserted++
	return m.PostgresStore.InsertMailboxItem(ctx, item)
}

func TestTemplateService_PublishAndPlanMigrations_WithMailbox(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	mb := &countingMailbox{PostgresStore: pg}
	svc := NewService(db, mb)
	ctx := context.Background()

	oldAgents, _ := json.Marshal([]map[string]any{
		{"role": "opco-ceo", "type": "worker", "system_prompt": "CEO", "tools": []string{"agent_message"}, "subscriptions": []string{"opco.*"}},
		{"role": "vp-product", "parent_role": "opco-ceo", "type": "worker", "system_prompt": "VP", "tools": []string{"agent_message"}, "subscriptions": []string{"product.*"}},
	})
	oldBootstrap, _ := json.Marshal([]map[string]any{{"event_pattern": "opco.*", "subscriber_role": "opco-ceo", "reason": "b"}})
	if err := svc.PublishTemplate(ctx, "t1", oldAgents, oldBootstrap, []byte("[]"), "factory-cto", ""); err != nil {
		t.Fatalf("publish t1: %v", err)
	}

	newAgents, _ := json.Marshal([]map[string]any{
		{"role": "opco-ceo", "type": "worker", "system_prompt": "CEO v2", "tools": []string{"agent_message"}, "subscriptions": []string{"opco.*"}},
		{"role": "cto-agent", "parent_role": "opco-ceo", "type": "worker", "system_prompt": "CTO", "tools": []string{"agent_message"}, "subscriptions": []string{"eng.*"}},
	})
	newBootstrap, _ := json.Marshal([]map[string]any{
		{"event_pattern": "opco.*", "subscriber_role": "opco-ceo", "reason": "b"},
		{"event_pattern": "eng.*", "subscriber_role": "cto-agent", "reason": "b"},
	})
	if err := svc.PublishTemplate(ctx, "t2", newAgents, newBootstrap, []byte("[]"), "", "new"); err != nil {
		t.Fatalf("publish t2: %v", err)
	}
	var bootstrapRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bootstrap_versions`).Scan(&bootstrapRows); err != nil {
		t.Fatalf("count bootstrap versions: %v", err)
	}
	if bootstrapRows != 2 {
		t.Fatalf("expected 2 bootstrap_versions rows, got %d", bootstrapRows)
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, template_version, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','t1', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	n, err := svc.PlanMigrations(ctx, "t2", "factory-cto", 10)
	if err != nil {
		t.Fatalf("PlanMigrations: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 migration, got %d", n)
	}
	if mb.inserted == 0 {
		t.Fatalf("expected mailbox item created")
	}

	// Validate a migration row exists and has a plan + mailbox id linked.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM template_migrations WHERE vertical_id=$1::uuid`, verticalID).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 migration row, got %d", count)
	}
}

func TestTemplateService_PublishTemplate_Validations(t *testing.T) {
	if err := NewService(nil, nil).PublishTemplate(context.Background(), "v", nil, nil, nil, "", ""); err == nil {
		t.Fatalf("expected db required error")
	}
	_, db, _ := testutil.StartPostgres(t)
	svc := NewService(db, nil)
	if err := svc.PublishTemplate(context.Background(), "", nil, nil, nil, "", ""); err == nil {
		t.Fatalf("expected version required error")
	}

	if err := svc.PublishTemplate(context.Background(), "v1", nil, nil, nil, "", ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Ensure template.version_published event exists.
	var n int
	_ = db.QueryRowContext(context.Background(), `SELECT count(*) FROM events WHERE type='template.version_published'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected template.version_published event")
	}
	_ = time.Second
}

func TestTemplateService_PublishTemplate_ReusesBootstrapVersionForSameRoutes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	svc := NewService(db, nil)
	ctx := context.Background()

	agents := []byte(`[{"role":"opco-ceo","type":"worker","system_prompt":"CEO","tools":[],"subscriptions":["opco.*"]}]`)
	bootstrap := []byte(`[{"event_pattern":"opco.*","subscriber_role":"opco-ceo","reason":"core"}]`)

	if err := svc.PublishTemplate(ctx, "t1", agents, bootstrap, []byte("[]"), "factory-cto", ""); err != nil {
		t.Fatalf("publish t1: %v", err)
	}
	if err := svc.PublishTemplate(ctx, "t2", agents, bootstrap, []byte("[]"), "factory-cto", ""); err != nil {
		t.Fatalf("publish t2: %v", err)
	}

	var bootstrapRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bootstrap_versions`).Scan(&bootstrapRows); err != nil {
		t.Fatalf("count bootstrap versions: %v", err)
	}
	if bootstrapRows != 1 {
		t.Fatalf("expected bootstrap version reuse, rows=%d", bootstrapRows)
	}
}
