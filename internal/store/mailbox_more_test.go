package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Mailbox_CRUD_Expire_Notify(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Insert defaults.
	id, err := s.InsertMailboxItem(ctx, runtime.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "empire-coordinator",
		Type:       "spend_request",
		Summary:    "need approval",
	})
	if err != nil || id == "" {
		t.Fatalf("insert mailbox: id=%q err=%v", id, err)
	}

	// Get should work.
	got, err := s.GetMailboxItem(ctx, id)
	if err != nil {
		t.Fatalf("get mailbox: %v", err)
	}
	if got.Status != "pending" || got.Priority != "normal" {
		t.Fatalf("unexpected defaults: %+v", got)
	}

	// List/Count.
	if n, err := s.CountMailboxItems(ctx, "pending"); err != nil || n < 1 {
		t.Fatalf("count pending n=%d err=%v", n, err)
	}
	items, err := s.ListMailboxItems(ctx, "pending", 10)
	if err != nil || len(items) == 0 {
		t.Fatalf("list pending: n=%d err=%v", len(items), err)
	}

	// Decide transitions pending -> approved.
	if err := s.DecideMailboxItem(ctx, id, "approved", "approve", "ok"); err != nil {
		t.Fatalf("decide: %v", err)
	}
	if err := s.DecideMailboxItem(ctx, id, "approved", "approve", "again"); err == nil {
		t.Fatal("expected decide on non-pending to fail")
	}
	if err := s.DecideMailboxItem(ctx, uuid.NewString(), "nope", "approve", ""); err == nil {
		t.Fatal("expected invalid status error")
	}

	// Expire: insert a pending item with past timeout_at.
	expID, err := s.InsertMailboxItem(ctx, runtime.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "empire-coordinator",
		Type:       "founder_review",
		Priority:   "critical",
		Status:     "pending",
		Context:    []byte(`{"x":1}`),
		TimeoutAt:  time.Now().Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("insert expiring mailbox: %v", err)
	}
	expired, err := s.ExpireMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	found := false
	for _, it := range expired {
		if it.ID == expID {
			found = true
			if it.Status != "timed_out" {
				t.Fatalf("expected timed_out, got %+v", it)
			}
		}
	}
	if !found {
		t.Fatalf("expected expired item in result")
	}

	// Unnotified critical list + mark notified. Use a non-expiring pending critical item.
	critID, err := s.InsertMailboxItem(ctx, runtime.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "empire-coordinator",
		Type:       "spend_request",
		Priority:   "critical",
		Status:     "pending",
		Summary:    "critical",
		TimeoutAt:  time.Now().Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("insert critical mailbox: %v", err)
	}
	crit, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil || len(crit) == 0 {
		t.Fatalf("list unnotified critical: n=%d err=%v", len(crit), err)
	}
	if err := s.MarkMailboxItemNotified(ctx, critID); err != nil {
		t.Fatalf("mark notified: %v", err)
	}
	crit2, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("list unnotified critical 2: %v", err)
	}
	for _, it := range crit2 {
		if it.ID == critID {
			t.Fatalf("expected item to be notified and excluded")
		}
	}
}
