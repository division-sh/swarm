package mailbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/runtime"
)

func TestWebhookNotifier(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := &WebhookNotifier{URL: srv.URL, Client: srv.Client()}
	if err := n.NotifyCritical(context.Background(), runtime.MailboxItem{ID: "m1", Type: "escalation"}); err != nil {
		t.Fatalf("notify webhook: %v", err)
	}
	if !called {
		t.Fatal("expected webhook call")
	}
}

func TestTelegramNotifier(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := &TelegramNotifier{
		BotToken: "token",
		ChatID:   "chat",
		BaseURL:  srv.URL,
		Client:   srv.Client(),
	}
	if err := n.NotifyCritical(context.Background(), runtime.MailboxItem{ID: "m1", Type: "escalation"}); err != nil {
		t.Fatalf("notify telegram: %v", err)
	}
	if !called {
		t.Fatal("expected telegram call")
	}
}

type notifierStub struct {
	err error
}

func (n notifierStub) NotifyCritical(context.Context, runtime.MailboxItem) error { return n.err }

func TestMultiCriticalNotifier(t *testing.T) {
	m := NewMultiCriticalNotifier(
		notifierStub{err: context.DeadlineExceeded},
		notifierStub{err: nil},
	)
	if m == nil {
		t.Fatal("expected notifier")
	}
	if err := m.NotifyCritical(context.Background(), runtime.MailboxItem{ID: "m1"}); err != nil {
		t.Fatalf("expected at least one notifier success: %v", err)
	}
}
