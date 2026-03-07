package digest

import (
	"context"
	"testing"
	"time"

	"empireai/internal/runtime"
	runtimetools "empireai/internal/runtime/tools"
)

type fakePortfolio struct{}

func (fakePortfolio) CountActiveVerticals(context.Context) (int, error) { return 2, nil }
func (fakePortfolio) ListVerticalDigestRows(context.Context, int) ([]runtime.VerticalDigestRow, error) {
	return []runtime.VerticalDigestRow{
		{
			VerticalID:     "v1",
			Name:           "V1",
			Stage:          "operating",
			UsersTotal:     12,
			MRRCents:       5000,
			SpendCents30d:  1000,
			LastMetricDate: time.Now(),
		},
	}, nil
}

type fakeMailbox struct{}

func (fakeMailbox) InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error) {
	return "m1", nil
}
func (fakeMailbox) GetMailboxItem(context.Context, string) (runtimetools.MailboxItem, error) {
	return runtimetools.MailboxItem{}, nil
}
func (fakeMailbox) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}
func (fakeMailbox) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}
func (fakeMailbox) MarkMailboxItemNotified(context.Context, string) error { return nil }
func (fakeMailbox) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}
func (fakeMailbox) CountMailboxItems(context.Context, string) (int, error) { return 3, nil }
func (fakeMailbox) ListMailboxItems(context.Context, string, int) ([]runtimetools.MailboxItem, error) {
	return []runtimetools.MailboxItem{
		{ID: "m1", Priority: "critical"},
		{ID: "m2", Priority: "normal"},
	}, nil
}

func TestBuildSnapshotAndRender(t *testing.T) {
	s, err := BuildSnapshot(context.Background(), fakePortfolio{}, fakeMailbox{}, 5)
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	if s.ActiveVerticals != 2 {
		t.Fatalf("unexpected active count: %d", s.ActiveVerticals)
	}
	if s.MailboxCritical != 1 {
		t.Fatalf("unexpected critical count: %d", s.MailboxCritical)
	}
	txt := RenderText(s)
	if txt == "" {
		t.Fatal("expected non-empty digest text")
	}
}
