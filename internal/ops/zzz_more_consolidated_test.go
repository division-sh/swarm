package ops

import (
	"context"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestOpsService_RecordMetrics_UpsertAndDefaults(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	svc := NewService(db, nil)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if err := svc.RecordMetrics(ctx, MetricInput{VerticalID: verticalID, UsersTotal: 10, MRRCents: 500}); err != nil {
		t.Fatalf("RecordMetrics: %v", err)
	}

	if err := svc.RecordMetrics(ctx, MetricInput{VerticalID: verticalID, UsersTotal: 11, MRRCents: 600}); err != nil {
		t.Fatalf("RecordMetrics upsert: %v", err)
	}
	var users, mrr int
	if err := db.QueryRowContext(ctx, `
		SELECT users_total, mrr_cents
		FROM vertical_metrics
		WHERE vertical_id=$1::uuid
		ORDER BY period_end DESC
		LIMIT 1
	`, verticalID).Scan(&users, &mrr); err != nil {
		t.Fatalf("load metrics: %v", err)
	}
	if users != 11 || mrr != 600 {
		t.Fatalf("expected updated metrics users=11 mrr=600 got users=%d mrr=%d", users, mrr)
	}
}

func TestOpsService_Tick_EvaluationsCreateMailboxItems(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	svc := NewService(db, pg)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, hired_by, started_at, last_active_at)
		VALUES ('a1', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, 'test', now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	v1 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', now(), now())
	`, v1); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO vertical_metrics (vertical_id, period_start, period_end, users_total, users_new, users_churned, mrr_cents, created_at)
		VALUES ($1::uuid, $2::date, $3::date, 1, 0, 2, 100, now())
	`, v1, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, currency, created_at)
		VALUES ($1::uuid, $2::uuid, 'api', 9999, 'USD', now())
	`, uuid.NewString(), v1); err != nil {
		t.Fatalf("seed spend: %v", err)
	}

	v2 := uuid.NewString()
	v3 := uuid.NewString()
	for _, vid := range []string{v2, v3} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
			VALUES ($1::uuid, 'V', 'v'||substr($1::text,1,6), 'us', 'operating', 'operating', now(), now())
		`, vid); err != nil {
			t.Fatalf("seed vertical %s: %v", vid, err)
		}
	}
	for _, vid := range []string{v1, v2, v3} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO routing_rules (vertical_id, event_pattern, subscriber_id, installed_by, status, source, created_at)
			VALUES ($1::uuid, 'inbound.*', 'a1', 'a1', 'active', 'discovered', now())
			ON CONFLICT DO NOTHING
		`, vid); err != nil {
			t.Fatalf("seed routing_rules: %v", err)
		}
	}

	sum, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if sum.KillCandidates < 1 || sum.BudgetAlerts < 1 || sum.RoutingProposals < 1 {
		t.Fatalf("expected non-zero tick outputs, got %+v", sum)
	}

	// Mailbox should contain at least one of each type we emit.
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE type='vertical_decision' AND vertical_id=$1::uuid`, v1).Scan(&n)
	if n < 1 {
		t.Fatalf("expected vertical_decision mailbox item")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE type='budget_increase' AND vertical_id=$1::uuid`, v1).Scan(&n)
	if n < 1 {
		t.Fatalf("expected budget_increase mailbox item")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE type='escalation'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected escalation mailbox item")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM technical_patterns WHERE pattern_type='routing'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected technical_patterns row")
	}
}
