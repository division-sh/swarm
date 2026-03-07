package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPortfolioDigestLoop_TelegramPushPath(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	// Seed one active vertical and a critical mailbox item to exercise snapshot counts.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES ($1::uuid, $2::uuid, 'empire-coordinator', 'budget_increase', 'critical', 'pending', '{}'::jsonb, 'x', now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}

	// Fake Telegram API.
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sendMessage") {
			http.NotFound(w, r)
			return
		}
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(ts.Close)

	t.Setenv("EMPIREAI_DIGEST_TOPN", "5")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_BOT_TOKEN", "T")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_CHAT_ID", "123")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_BASE_URL", ts.URL)

	bus := runtime.NewEventBus(pg)
	loopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go portfolioDigestLoop(loopCtx, bus, pg, pg)
	time.Sleep(50 * time.Millisecond)

	_ = bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.portfolio_digest"),
		SourceAgent: "empire-coordinator",
		Payload:     []byte(`{"trigger":"test"}`),
		CreatedAt:   time.Now(),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("expected telegram push call, calls=%d", calls)
}

func TestMaybeEmitSteadyState_OnlyOnce(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	launchedAt := time.Now().Add(-6 * 7 * 24 * time.Hour).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, launched_at, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', $2, now(), now())
	`, verticalID, launchedAt); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO vertical_metrics (vertical_id, period_start, period_end, users_total, mrr_cents, created_at)
		VALUES ($1::uuid, $2::date, $3::date, 5, 500, now())
	`, verticalID, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	bus := runtime.NewEventBus(pg)
	if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
		t.Fatalf("maybeEmitSteadyState: %v", err)
	}
	// Second call should no-op due to existing event.
	if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
		t.Fatalf("maybeEmitSteadyState second: %v", err)
	}
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='opco.steady_state_reached' AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n != 1 {
		t.Fatalf("expected exactly 1 steady_state event, got %d", n)
	}
}
