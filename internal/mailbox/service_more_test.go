package mailbox

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/runtime"
)

type fakeMailboxStore struct {
	items map[string]runtime.MailboxItem
}

func newFakeMailbox(items ...runtime.MailboxItem) *fakeMailboxStore {
	m := &fakeMailboxStore{items: map[string]runtime.MailboxItem{}}
	for _, it := range items {
		m.items[it.ID] = it
	}
	return m
}

func (m *fakeMailboxStore) InsertMailboxItem(_ context.Context, item runtime.MailboxItem) (string, error) {
	if strings.TrimSpace(item.ID) == "" {
		item.ID = "id-" + strings.TrimSpace(item.Type)
	}
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "pending"
	}
	m.items[item.ID] = item
	return item.ID, nil
}
func (m *fakeMailboxStore) ListMailboxItems(_ context.Context, status string, limit int) ([]runtime.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	out := make([]runtime.MailboxItem, 0, len(m.items))
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
func (m *fakeMailboxStore) GetMailboxItem(_ context.Context, id string) (runtime.MailboxItem, error) {
	it, ok := m.items[id]
	if !ok {
		return runtime.MailboxItem{}, context.Canceled
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
func (m *fakeMailboxStore) ExpireMailboxItems(context.Context, int) ([]runtime.MailboxItem, error) {
	return nil, nil
}
func (m *fakeMailboxStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtime.MailboxItem, error) {
	return nil, nil
}
func (m *fakeMailboxStore) MarkMailboxItemNotified(context.Context, string) error { return nil }

func TestMailbox_NormalizeDecisionAction(t *testing.T) {
	for _, tc := range []struct {
		in     string
		status string
		dec    string
	}{
		{"approve", "approved", "approve"},
		{"APPROVE_SPEND", "approved", "approve"},
		{"reject", "rejected", "reject"},
		{"kill", "rejected", "kill"},
		{"revise", "rejected", "revise"},
		{"more-data", "more_data", "more_data"},
		{"timeout", "timed_out", "timed_out"},
	} {
		out, err := NormalizeDecisionAction(tc.in)
		if err != nil {
			t.Fatalf("NormalizeDecisionAction(%q): %v", tc.in, err)
		}
		if out.Status != tc.status || out.Decision != tc.dec {
			t.Fatalf("NormalizeDecisionAction(%q) got=%+v", tc.in, out)
		}
	}
	if _, err := NormalizeDecisionAction("nope"); err == nil {
		t.Fatalf("expected invalid action error")
	}
}

func TestMailbox_DecideAndPrints(t *testing.T) {
	ctx := context.Background()
	store := newFakeMailbox(runtime.MailboxItem{
		ID:        "m1",
		Type:      "product_spec_review",
		Priority:  "critical",
		Status:    "pending",
		FromAgent: "a",
		VerticalID:"v",
		Summary:   strings.Repeat("x", 200), // exercises truncation
		TimeoutAt: time.Now().Add(10 * time.Minute),
	})

	out, err := Decide(ctx, store, "m1", "approve", "ok")
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if out.Status != "approved" || out.Decision != "approve" {
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

	// Reinsert a pending review item and render it.
	_, _ = store.InsertMailboxItem(ctx, runtime.MailboxItem{
		ID:        "m2",
		Type:      "deploy_review",
		Priority:  "normal",
		Status:    "pending",
		FromAgent: "a2",
		VerticalID:"v2",
		Summary:   "hello",
	})
	buf.Reset()
	if err := PrintPendingWithOptions(ctx, store, &buf, ListOptions{Limit: 10, ReviewsOnly: true}); err != nil {
		t.Fatalf("PrintPending reviews: %v", err)
	}
	if !strings.Contains(buf.String(), "type=deploy_review") {
		t.Fatalf("expected deploy_review in output, got %q", buf.String())
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
	items := []runtime.MailboxItem{
		{ID: "1", Priority: "critical", Type: "escalation"},
		{ID: "2", Priority: "normal", Type: "product_spec_review"},
		{ID: "3", Priority: "normal", Type: "deploy_review"},
	}
	out := filterPending(items, ListOptions{CriticalOnly: true})
	if len(out) != 1 || out[0].ID != "1" {
		t.Fatalf("CriticalOnly filter failed: %#v", out)
	}
	out = filterPending(items, ListOptions{ReviewsOnly: true})
	if len(out) != 2 {
		t.Fatalf("ReviewsOnly filter failed: %#v", out)
	}
	if !isReviewType("product_spec_review") || !isReviewType("deploy_review") || isReviewType("x") {
		t.Fatalf("isReviewType unexpected behavior")
	}
}

