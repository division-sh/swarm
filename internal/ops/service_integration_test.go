package ops

import (
	"context"
	"testing"
	"time"

	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestOpsService_TickCreatesMailboxSignals(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'OpsCo', 'opsco', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	svc := NewService(db, pg)
	// Low users + low MRR + churn > new => kill criteria candidate.
	if err := svc.RecordMetrics(ctx, MetricInput{
		VerticalID:   verticalID,
		PeriodStart:  time.Now().UTC().Add(-24 * time.Hour),
		PeriodEnd:    time.Now().UTC(),
		UsersTotal:   2,
		UsersNew:     0,
		UsersChurned: 1,
		MRRCents:     0,
	}); err != nil {
		t.Fatalf("RecordMetrics: %v", err)
	}
	// Spend without MRR => budget pressure candidate.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, currency, description, approved_by, created_at)
		VALUES ($1::uuid, $2::uuid, 'api', 5000, 'USD', 'tests', 'empire-coordinator', now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}

	sum, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if sum.KillCandidates == 0 {
		t.Fatalf("expected kill candidates > 0, got %+v", sum)
	}
	if sum.BudgetAlerts == 0 {
		t.Fatalf("expected budget alerts > 0, got %+v", sum)
	}
}

