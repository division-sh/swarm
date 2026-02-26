package mailbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"empireai/internal/runtime"
)

type notifierStub2 struct {
	err error
}

func (n notifierStub2) NotifyCritical(context.Context, runtime.MailboxItem) error { return n.err }

func TestNotify_Multi_Webhook_Telegram_Email(t *testing.T) {
	item := runtime.MailboxItem{ID: "m1", Type: "spend_request", VerticalID: "v", Summary: "x", TimeoutAt: time.Now()}
	ctx := context.Background()

	// Multi notifier: nils ignored, partial success ok, all fail returns error.
	if NewMultiCriticalNotifier(nil) != nil {
		t.Fatal("expected nil multi notifier when empty")
	}
	m := NewMultiCriticalNotifier(notifierStub2{err: nil}, notifierStub2{err: context.Canceled})
	if err := m.NotifyCritical(ctx, item); err != nil {
		t.Fatalf("expected partial success, got %v", err)
	}
	m2 := NewMultiCriticalNotifier(notifierStub2{err: context.Canceled}, notifierStub2{err: context.DeadlineExceeded})
	if err := m2.NotifyCritical(ctx, item); err == nil {
		t.Fatal("expected all-fail error")
	}

	// Webhook notifier success and non-2xx.
	tsOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.WriteHeader(http.StatusOK)
	}))
	defer tsOK.Close()
	wh := &WebhookNotifier{URL: tsOK.URL, Client: tsOK.Client()}
	if err := wh.NotifyCritical(ctx, item); err != nil {
		t.Fatalf("webhook ok: %v", err)
	}
	tsBad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	defer tsBad.Close()
	wh2 := &WebhookNotifier{URL: tsBad.URL, Client: tsBad.Client()}
	if err := wh2.NotifyCritical(ctx, item); err == nil {
		t.Fatal("expected webhook non-2xx error")
	}

	// Telegram notifier via test server.
	tsTG := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.WriteHeader(http.StatusOK)
	}))
	defer tsTG.Close()
	tg := &TelegramNotifier{BotToken: "tok", ChatID: "1", BaseURL: tsTG.URL, Client: tsTG.Client()}
	if err := tg.NotifyCritical(ctx, item); err != nil {
		t.Fatalf("telegram notify: %v", err)
	}
	if err := tg.NotifyText(ctx, "hello"); err != nil {
		t.Fatalf("telegram notify text: %v", err)
	}

	// Email notifier: missing fields should error (avoid sending mail in tests).
	em := &EmailNotifier{}
	if err := em.NotifyCritical(ctx, item); err == nil {
		t.Fatal("expected email notifier validation error")
	}
}

func TestTelegramNotifier_RetryBeforeSuccess(t *testing.T) {
	var calls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			http.Error(w, "retry", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	tg := &TelegramNotifier{BotToken: "tok", ChatID: "1", BaseURL: ts.URL, Client: ts.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := tg.NotifyText(ctx, "hello"); err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 calls, got %d", got)
	}
}

func TestEmailNotifier_ContextCancellation(t *testing.T) {
	n := &EmailNotifier{
		SMTPAddr: "127.0.0.1:9",
		From:     "ops@example.com",
		To:       []string{"founder@example.com"},
		Timeout:  2 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := n.NotifyCritical(ctx, runtime.MailboxItem{ID: "m1", Type: "critical"}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}
