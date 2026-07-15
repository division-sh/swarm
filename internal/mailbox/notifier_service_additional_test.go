package mailbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type notifierStub2 struct {
	err error
}

func (n notifierStub2) NotifyCritical(context.Context, runtimetools.MailboxItem) error { return n.err }

func TestNotify_Multi_Webhook_Email(t *testing.T) {
	item := runtimetools.MailboxItem{ID: "m1", Type: "spend_request", EntityID: "v", Summary: "x", TimeoutAt: time.Now()}
	ctx := context.Background()

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

	em := &EmailNotifier{}
	if err := em.NotifyCritical(ctx, item); err == nil {
		t.Fatal("expected email notifier validation error")
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
	if err := n.NotifyCritical(ctx, runtimetools.MailboxItem{ID: "m1", Type: "critical"}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

type fakeMailboxStore struct {
	items map[string]runtimetools.MailboxItem
}

func newFakeMailbox(items ...runtimetools.MailboxItem) *fakeMailboxStore {
	m := &fakeMailboxStore{items: map[string]runtimetools.MailboxItem{}}
	for _, it := range items {
		m.items[it.ID] = it
	}
	return m
}

func (m *fakeMailboxStore) InsertMailboxItem(_ context.Context, item runtimetools.MailboxItem) (string, error) {
	if strings.TrimSpace(item.ID) == "" {
		item.ID = "id-" + strings.TrimSpace(item.Type)
	}
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "pending"
	}
	m.items[item.ID] = item
	return item.ID, nil
}

func (m *fakeMailboxStore) ListMailboxItems(_ context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	out := make([]runtimetools.MailboxItem, 0, len(m.items))
	for _, it := range m.items {
		if it.Status == status {
			out = append(out, it)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *fakeMailboxStore) CountMailboxItems(_ context.Context, status string) (int, error) {
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	n := 0
	for _, it := range m.items {
		if it.Status == status {
			n++
		}
	}
	return n, nil
}

func (m *fakeMailboxStore) GetMailboxItem(_ context.Context, id string) (runtimetools.MailboxItem, error) {
	it, ok := m.items[id]
	if !ok {
		return runtimetools.MailboxItem{}, context.Canceled
	}
	return it, nil
}

func (m *fakeMailboxStore) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (m *fakeMailboxStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (m *fakeMailboxStore) MarkMailboxItemNotified(context.Context, string) error { return nil }

func TestMailbox_FilterHelpers(t *testing.T) {
	items := []runtimetools.MailboxItem{
		{ID: "1", Priority: "critical", Type: "escalation"},
		{ID: "2", Priority: "normal", Type: "manual_review"},
		{ID: "3", Priority: "normal", Type: "ops_review"},
	}
	out := filterPending(items, ListOptions{CriticalOnly: true})
	if len(out) != 1 || out[0].ID != "1" {
		t.Fatalf("CriticalOnly filter failed: %#v", out)
	}
	out = filterPending(items, ListOptions{ReviewsOnly: true})
	if len(out) != 3 {
		t.Fatalf("ReviewsOnly should no longer filter mailbox types, got %#v", out)
	}
}
