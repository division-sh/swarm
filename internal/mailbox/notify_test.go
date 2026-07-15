package mailbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestWebhookNotifier(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := &WebhookNotifier{URL: srv.URL, Client: srv.Client()}
	if err := n.NotifyCritical(context.Background(), runtimetools.MailboxItem{ID: "m1", Type: "escalation"}); err != nil {
		t.Fatalf("notify webhook: %v", err)
	}
	if !called {
		t.Fatal("expected webhook call")
	}
}

type notifierStub struct {
	err error
}

func (n notifierStub) NotifyCritical(context.Context, runtimetools.MailboxItem) error { return n.err }

func TestMultiCriticalNotifier(t *testing.T) {
	m := NewMultiCriticalNotifier(
		notifierStub{err: context.DeadlineExceeded},
		notifierStub{err: nil},
	)
	if m == nil {
		t.Fatal("expected notifier")
	}
	if err := m.NotifyCritical(context.Background(), runtimetools.MailboxItem{ID: "m1"}); err != nil {
		t.Fatalf("expected at least one notifier success: %v", err)
	}
}

func TestMailboxNotifierHasNoDirectTelegramDispatchAuthority(t *testing.T) {
	body, err := os.ReadFile("notify.go")
	if err != nil {
		t.Fatalf("read notifier source: %v", err)
	}
	for _, forbidden := range []string{"ChatNotifier", "sendTelegramWithRetry", "api.telegram.org", "BotToken", "ChatID"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("mailbox notifier restored retired direct Telegram authority %q", forbidden)
		}
	}
}
