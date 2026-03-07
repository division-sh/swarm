package mailbox

import (
	"bytes"
	"context"
	"testing"

	runtimetools "empireai/internal/runtime/tools"
)

type fakeStore struct{}

func (fakeStore) InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error) {
	return "m1", nil
}
func (fakeStore) DecideMailboxItem(context.Context, string, string, string, string) error { return nil }
func (fakeStore) CountMailboxItems(context.Context, string) (int, error)                  { return 2, nil }
func (fakeStore) GetMailboxItem(_ context.Context, id string) (runtimetools.MailboxItem, error) {
	return runtimetools.MailboxItem{ID: id, Type: "spend_request", Status: "pending"}, nil
}
func (fakeStore) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}
func (fakeStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}
func (fakeStore) MarkMailboxItemNotified(context.Context, string) error { return nil }
func (fakeStore) ListMailboxItems(context.Context, string, int) ([]runtimetools.MailboxItem, error) {
	return []runtimetools.MailboxItem{
		{ID: "m1", Priority: "critical", Type: "product_spec_review"},
		{ID: "m2", Priority: "normal", Type: "spend_request"},
	}, nil
}

func TestGetStatusAndPrint(t *testing.T) {
	st, err := GetStatus(context.Background(), fakeStore{})
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	if st.Pending != 2 || st.Critical != 1 {
		t.Fatalf("unexpected status: %+v", st)
	}
	var b bytes.Buffer
	if err := PrintStatus(context.Background(), fakeStore{}, &b); err != nil {
		t.Fatalf("print status: %v", err)
	}
	if b.Len() == 0 {
		t.Fatal("expected output")
	}
}

func TestPrintPending(t *testing.T) {
	var b bytes.Buffer
	if err := PrintPendingWithOptions(context.Background(), fakeStore{}, &b, ListOptions{Limit: 5, CriticalOnly: true}); err != nil {
		t.Fatalf("print pending: %v", err)
	}
	if b.Len() == 0 {
		t.Fatal("expected output")
	}
}

func TestNormalizeDecisionAction(t *testing.T) {
	out, err := NormalizeDecisionAction("more-data")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if out.Status != "more_data" || out.Decision != "more_data" {
		t.Fatalf("unexpected outcome: %+v", out)
	}
}
