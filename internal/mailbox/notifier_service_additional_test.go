package mailbox

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	runtimetools "swarm/internal/runtime/tools"
	"sync/atomic"
	"testing"
	"time"
)

type notifierStub2 struct {
	err error
}

func (n notifierStub2) NotifyCritical(context.Context, runtimetools.MailboxItem) error { return n.err }

func TestNotify_Multi_Webhook_Telegram_Email(t *testing.T) {
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

	tsTG := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.WriteHeader(http.StatusOK)
	}))
	defer tsTG.Close()
	tg := &ChatNotifier{BotToken: "tok", ChatID: "1", BaseURL: tsTG.URL, Client: tsTG.Client()}
	if err := tg.NotifyCritical(ctx, item); err != nil {
		t.Fatalf("telegram notify: %v", err)
	}
	if err := tg.NotifyText(ctx, "hello"); err != nil {
		t.Fatalf("telegram notify text: %v", err)
	}

	em := &EmailNotifier{}
	if err := em.NotifyCritical(ctx, item); err == nil {
		t.Fatal("expected email notifier validation error")
	}
}

func TestChatNotifier_RetryBeforeSuccess(t *testing.T) {
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

	tg := &ChatNotifier{BotToken: "tok", ChatID: "1", BaseURL: ts.URL, Client: ts.Client()}
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

func (m *fakeMailboxStore) DecideMailboxItem(_ context.Context, id, status, decision, notes string) error {
	it := m.items[id]
	it.Status = status
	it.Decision = decision
	it.DecisionNotes = notes
	m.items[id] = it
	return nil
}

func (m *fakeMailboxStore) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (m *fakeMailboxStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (m *fakeMailboxStore) MarkMailboxItemNotified(context.Context, string) error { return nil }

func TestMailbox_NormalizeDecisionAction(t *testing.T) {
	for _, tc := range []struct {
		in     string
		status string
		dec    string
	}{
		{"approve", "decided", "approve"},
		{"reject", "decided", "reject"},
		{"more-data", "decided", "more-data"},
		{"deferred", "decided", "deferred"},
		{"timeout", "expired", "timeout"},
	} {
		out, err := NormalizeDecisionAction(tc.in)
		if err != nil {
			t.Fatalf("NormalizeDecisionAction(%q): %v", tc.in, err)
		}
		if out.Status != tc.status || out.Decision != tc.dec {
			t.Fatalf("NormalizeDecisionAction(%q) got=%+v", tc.in, out)
		}
	}
}

func TestMailbox_DecideAndPrints(t *testing.T) {
	ctx := context.Background()
	store := newFakeMailbox(runtimetools.MailboxItem{
		ID:        "m1",
		Type:      "manual_review",
		Priority:  "critical",
		Status:    "pending",
		FromAgent: "a",
		EntityID:  "v",
		Summary:   strings.Repeat("x", 200),
		TimeoutAt: time.Now().Add(10 * time.Minute),
	})

	out, err := Decide(ctx, store, "m1", "approve", "ok")
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if out.Status != "decided" || out.Decision != "approve" {
		t.Fatalf("unexpected outcome: %+v", out)
	}

	var buf bytes.Buffer
	if err := PrintStatus(ctx, store, &buf); err != nil {
		t.Fatalf("PrintStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "mailbox: pending=") {
		t.Fatalf("unexpected PrintStatus output: %q", buf.String())
	}

	buf.Reset()
	if err := PrintPendingWithOptions(ctx, store, &buf, ListOptions{Limit: 10, CriticalOnly: true}); err != nil {
		t.Fatalf("PrintPendingWithOptions: %v", err)
	}
	if !strings.Contains(buf.String(), "mailbox: no pending items") {
		t.Fatalf("expected no pending items after approve, got %q", buf.String())
	}

	_, _ = store.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		ID:        "m2",
		Type:      "ops_review",
		Priority:  "normal",
		Status:    "pending",
		FromAgent: "a2",
		EntityID:  "v2",
		Summary:   "hello",
	})
	buf.Reset()
	if err := PrintPendingWithOptions(ctx, store, &buf, ListOptions{Limit: 10, ReviewsOnly: true}); err != nil {
		t.Fatalf("PrintPending with review filter: %v", err)
	}

	buf.Reset()
	if err := PrintItem(ctx, store, &buf, "m2"); err != nil {
		t.Fatalf("PrintItem: %v", err)
	}
	if !strings.Contains(buf.String(), "mailbox item") || !strings.Contains(buf.String(), "id: m2") {
		t.Fatalf("unexpected PrintItem output: %q", buf.String())
	}
}

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
