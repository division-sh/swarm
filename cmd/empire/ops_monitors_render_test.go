package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/digest"
	"empireai/internal/events"
	"empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type scheduleStub struct{ last runtimepipeline.Schedule }

func (s *scheduleStub) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.last = sc
	return nil
}
func (s *scheduleStub) CancelSchedule(context.Context, string, string) error { return nil }
func (s *scheduleStub) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return nil, nil
}
func (s *scheduleStub) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error { return nil }

func TestEnsurePortfolioDigestSchedule_DefaultCron(t *testing.T) {
	if err := ensurePortfolioDigestSchedule(context.Background(), nil); err != nil {
		t.Fatalf("expected nil for nil store")
	}
	st := &scheduleStub{}
	t.Setenv("EMPIREAI_DIGEST_CRON", "")
	if err := ensurePortfolioDigestSchedule(context.Background(), st); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if st.last.EventType != "timer.portfolio_digest" || !strings.Contains(st.last.Cron, "0 9") {
		t.Fatalf("unexpected schedule: %#v", st.last)
	}
}

func TestRenderCompactDigest(t *testing.T) {
	txt := renderCompactDigest("test", digest.Snapshot{
		ActiveVerticals: 2,
		MailboxPending:  1,
		MailboxCritical: 1,
		TopVerticals: []runtime.VerticalDigestRow{
			{Name: "A", Stage: "operating", UsersTotal: 10, MRRCents: 1234},
		},
	})
	if !strings.Contains(txt, "portfolio digest") || !strings.Contains(txt, "trigger=test") || !strings.Contains(txt, "A") {
		t.Fatalf("unexpected digest text: %q", txt)
	}
}

func TestEvaluateVerticalHealth_And_SteadyState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, launched_at, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now() - interval '90 day', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO vertical_metrics (id, vertical_id, period_start, period_end, users_total, users_churned, mrr_cents, csat_avg, created_at)
		VALUES ($1::uuid,$2::uuid, now() - interval '7 day', now(), 2, 1, 100, 2.0, now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, created_at)
		VALUES ($1::uuid,$2::uuid,'api',10000, now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}

	w, ok, err := evaluateVerticalHealth(ctx, db, verticalID)
	if err != nil || !ok {
		t.Fatalf("expected warning ok, ok=%v err=%v", ok, err)
	}
	if strings.TrimSpace(w.Severity) == "" || strings.TrimSpace(w.Recommendation) == "" || len(w.BreachedMetrics) == 0 {
		t.Fatalf("unexpected warning: %#v", w)
	}

	// Steady state: publish event when weeks >=4 and users/mrr positive.
	bus := runtime.NewEventBus(runtimebus.InMemoryEventStore{})
	ch := bus.Subscribe("t", events.EventType("opco.steady_state_reached"))

	// Adjust metrics to positive for steady-state.
	if _, err := db.ExecContext(ctx, `
		UPDATE vertical_metrics SET users_total=10, users_churned=0, mrr_cents=1000, csat_avg=4.5
		WHERE vertical_id=$1::uuid
	`, verticalID); err != nil {
		t.Fatalf("update metrics: %v", err)
	}
	if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
		t.Fatalf("maybeEmitSteadyState: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatalf("expected steady state event")
	}

	// Existing event in DB suppresses re-emit.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid,'opco.steady_state_reached','opco-ceo', $2::uuid, '{}'::jsonb, now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed steady state event: %v", err)
	}
	if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
		t.Fatalf("maybeEmitSteadyState (exists): %v", err)
	}

	// Pre-launch vertical returns ok=false.
	vertical2 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'Pre','pre','us','pre_launch','operating', now(), now())
	`, vertical2); err != nil {
		t.Fatalf("seed vertical2: %v", err)
	}
	if _, ok, err := evaluateVerticalHealth(ctx, db, vertical2); err != nil || ok {
		t.Fatalf("expected ok=false for prelaunch, ok=%v err=%v", ok, err)
	}
}
