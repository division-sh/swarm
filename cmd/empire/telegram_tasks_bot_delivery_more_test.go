package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/mailbox"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestTelegramTaskDelivery_DeliverApprovedAndEventDriven(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	// Fake Telegram API server.
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sendMessage") {
			http.NotFound(w, r)
			return
		}
		_ = r.ParseForm()
		if r.Form.Get("chat_id") == "" || strings.TrimSpace(r.Form.Get("text")) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(ts.Close)

	baseURL := ts.URL
	// Our notifier builds endpoint as baseURL + "/botTOKEN/sendMessage"
	parsed, _ := url.Parse(baseURL)
	baseURL = parsed.Scheme + "://" + parsed.Host

	tg := &mailbox.TelegramNotifier{
		BotToken: "T",
		ChatID:   "123",
		BaseURL:  baseURL,
		Client:   &http.Client{Timeout: 2 * time.Second},
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	taskID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, priority, status, reviewed_at, review_decision, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'medium', 'approved', now(), '{}'::jsonb, now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	// Startup sweep.
	deliverApprovedUnnotified(ctx, db, tg)
	if calls < 1 {
		t.Fatalf("expected telegram NotifyText calls, got %d", calls)
	}
	var notified string
	_ = db.QueryRowContext(ctx, `SELECT COALESCE(review_decision->>'telegram_notified_at','') FROM human_tasks WHERE id=$1::uuid`, taskID).Scan(&notified)
	if strings.TrimSpace(notified) == "" {
		t.Fatalf("expected telegram_notified_at set")
	}

	// Event-driven delivery loop.
	bus := runtime.NewEventBus(pg)
	loopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go humanTaskTelegramDeliveryLoop(loopCtx, db, bus, tg)
	time.Sleep(50 * time.Millisecond)

	evtID := uuid.NewString()
	_ = bus.Publish(context.Background(), events.Event{
		ID:          evtID,
		Type:        events.EventType("human_task.approved"),
		SourceAgent: "empire-coordinator",
		Payload:     []byte(`{"task_id":"` + taskID + `"}`),
		CreatedAt:   time.Now(),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected event-driven delivery to call telegram, calls=%d", calls)
}

