package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Inbound_RecordResolveAndSecrets(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID, `{
		"webhooks": { "whatsapp": { "secret": "s1" } },
		"whatsapp": { "token": "legacy" },
		"whatsapp_webhook_secret": "flat"
	}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Resolve target by slug and pick preferred secret location.
	target, err := s.ResolveInboundTarget(ctx, "testco", "whatsapp")
	if err != nil {
		t.Fatalf("resolve inbound: %v", err)
	}
	if target.VerticalID != verticalID || target.VerticalSlug != "testco" {
		t.Fatalf("unexpected target: %+v", target)
	}
	if target.WebhookSecret != "s1" {
		t.Fatalf("expected preferred webhook secret, got %q", target.WebhookSecret)
	}

	// Record inbound idempotently.
	ok, err := s.RecordInboundEvent(ctx, "evt-1", verticalID, "whatsapp")
	if err != nil || !ok {
		t.Fatalf("record inbound ok=%v err=%v", ok, err)
	}
	ok, err = s.RecordInboundEvent(ctx, "evt-1", verticalID, "whatsapp")
	if err != nil || ok {
		t.Fatalf("expected duplicate record to be no-op ok=%v err=%v", ok, err)
	}

	// Purge should succeed even when nothing is old enough.
	if n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-1*time.Hour), 10); err != nil || n != 0 {
		t.Fatalf("purge n=%d err=%v", n, err)
	}
}

