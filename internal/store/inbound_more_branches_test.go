package store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Inbound_ValidationAndNotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := s.RecordInboundEvent(ctx, "", "v", "p"); err == nil {
		t.Fatal("expected provider_event_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "", "p"); err == nil {
		t.Fatal("expected vertical_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "v", ""); err == nil {
		t.Fatal("expected provider required")
	}

	if _, err := s.ResolveInboundTarget(ctx, "", "p"); err == nil {
		t.Fatal("expected vertical key required")
	}
	if _, err := s.ResolveInboundTarget(ctx, "k", ""); err == nil {
		t.Fatal("expected provider required")
	}
	if _, err := s.ResolveInboundTarget(ctx, "missing", "whatsapp"); err == nil || !strings.Contains(err.Error(), "vertical not found") {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestPostgresStore_Inbound_SecretsLegacyFlatAndDecrypt(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	// Prepare an encrypted secret (pgcrypto is available in the test DB).
	key := "k"
	os.Setenv("EMPIREAI_CREDENTIALS_KEY", key)
	defer os.Unsetenv("EMPIREAI_CREDENTIALS_KEY")

	var enc string
	if err := db.QueryRowContext(ctx, `SELECT encode(pgp_sym_encrypt('senc', $1::text), 'base64')`, key).Scan(&enc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	verticalID1 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','legacy','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID1, `{"whatsapp": {"token":"legacy"}}`); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	target, err := s.ResolveInboundTarget(ctx, "legacy", "whatsapp")
	if err != nil {
		t.Fatalf("resolve legacy: %v", err)
	}
	if target.WebhookSecret != "legacy" {
		t.Fatalf("expected legacy token, got %q", target.WebhookSecret)
	}

	verticalID2 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','flat','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID2, `{"whatsapp_webhook_secret":" flat "}`); err != nil {
		t.Fatalf("seed flat: %v", err)
	}
	target2, err := s.ResolveInboundTarget(ctx, "flat", "whatsapp")
	if err != nil {
		t.Fatalf("resolve flat: %v", err)
	}
	if target2.WebhookSecret != "flat" {
		t.Fatalf("expected flat secret, got %q", target2.WebhookSecret)
	}

	verticalID3 := uuid.NewString()
	b, _ := json.Marshal(map[string]any{
		"webhooks": map[string]any{
			"whatsapp": map[string]any{
				"secret": "enc::" + enc,
			},
		},
	})
	credsEnc := string(b)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','enc','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID3, credsEnc); err != nil {
		t.Fatalf("seed enc: %v", err)
	}
	target3, err := s.ResolveInboundTarget(ctx, "enc", "whatsapp")
	if err != nil {
		t.Fatalf("resolve enc: %v", err)
	}
	if target3.WebhookSecret != "senc" {
		t.Fatalf("expected decrypted secret, got %q", target3.WebhookSecret)
	}

	// Direct helper branch coverage.
	if got := s.extractWebhookSecret(ctx, []byte("{"), "whatsapp"); got != "" {
		t.Fatalf("expected empty secret for invalid json, got %q", got)
	}
	// decryptCredentialValue branches: empty encoded and missing key passthrough.
	os.Unsetenv("EMPIREAI_CREDENTIALS_KEY")
	if got := s.decryptCredentialValue(ctx, "enc::AAAA"); got != "enc::AAAA" {
		t.Fatalf("expected passthrough without key, got %#v", got)
	}
	os.Setenv("EMPIREAI_CREDENTIALS_KEY", key)
	if got := s.decryptCredentialValue(ctx, "enc::"); got != "" {
		t.Fatalf("expected empty encoded to return empty string, got %#v", got)
	}
}

func TestPostgresStore_Inbound_PurgeDeletes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','purge','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if ok, err := s.RecordInboundEvent(ctx, "evt-old", verticalID, "whatsapp"); err != nil || !ok {
		t.Fatalf("record old ok=%v err=%v", ok, err)
	}
	// Force received_at old enough.
	if _, err := db.ExecContext(ctx, `UPDATE inbound_events SET received_at = now() - interval '2 days' WHERE provider_event_id = 'evt-old'`); err != nil {
		t.Fatalf("age event: %v", err)
	}

	n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-24*time.Hour), 0)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 purged row, got %d", n)
	}
}
