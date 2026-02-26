package runtime

import (
	"context"
	"testing"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestAuthorizeRouting_AllBranches(t *testing.T) {
	targetProduct := models.AgentConfig{Role: "backend-agent"}
	targetGrowth := models.AgentConfig{Role: "marketing-agent"}
	targetEng := models.AgentConfig{Role: "qa-agent"}
	targetBad := models.AgentConfig{Role: "random"}

	// Always-allowed roles.
	if err := authorizeRouting(models.AgentConfig{Role: "opco-ceo"}, targetBad, "active"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "empire-coordinator"}, targetBad, "active"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}

	// CoS proposes only.
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "active"); err == nil {
		t.Fatalf("chief-of-staff should reject non-proposed")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "proposed"); err != nil {
		t.Fatalf("chief-of-staff proposed should allow: %v", err)
	}

	// Domain constraints.
	if err := authorizeRouting(models.AgentConfig{Role: "vp-product"}, targetProduct, "active"); err != nil {
		t.Fatalf("vp-product product target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-product"}, targetGrowth, "active"); err == nil {
		t.Fatalf("vp-product should reject growth target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, targetGrowth, "active"); err != nil {
		t.Fatalf("vp-growth growth target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, targetBad, "active"); err == nil {
		t.Fatalf("vp-growth should reject unknown target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, targetEng, "active"); err != nil {
		t.Fatalf("cto-agent eng target should allow: %v", err)
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, targetGrowth, "active"); err == nil {
		t.Fatalf("cto-agent should reject growth target")
	}

	// Unknown role.
	if err := authorizeRouting(models.AgentConfig{Role: "support-agent"}, targetBad, "active"); err == nil {
		t.Fatalf("expected unauthorized routing role error")
	}
}

func TestAuthorizeManage_AllBranches(t *testing.T) {
	// Empire coordinator always allowed.
	if err := authorizeManage(models.AgentConfig{Role: "empire-coordinator"}, "anything", "v"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}
	// Cross-vertical restriction.
	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical restriction")
	}
	// OpCo CEO allowed within vertical.
	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v1"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}
	// Domain constraints.
	if err := authorizeManage(models.AgentConfig{Role: "vp-product"}, "backend-agent", ""); err != nil {
		t.Fatalf("vp-product should manage product agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-product"}, "marketing-agent", ""); err == nil {
		t.Fatalf("vp-product should reject growth agent")
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-growth"}, "marketing-agent", ""); err != nil {
		t.Fatalf("vp-growth should manage growth agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-growth"}, "backend-agent", ""); err == nil {
		t.Fatalf("vp-growth should reject product agent")
	}
	if err := authorizeManage(models.AgentConfig{Role: "cto-agent"}, "qa-agent", ""); err != nil {
		t.Fatalf("cto-agent should manage eng agents: %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "cto-agent"}, "marketing-agent", ""); err == nil {
		t.Fatalf("cto-agent should reject non-eng")
	}
	// Unknown.
	if err := authorizeManage(models.AgentConfig{Role: "support-agent"}, "support-agent", ""); err == nil {
		t.Fatalf("expected unauthorized manage role")
	}
}

func TestDecryptCredentialValue_AndLoadVerticalCredentials_Branches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex.SetSQLDB(db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalID, `{"api_key":"enc::"}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// No key -> leave encrypted string unchanged.
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	if got := ex.decryptCredentialValue(ctx, "enc::AAAA"); got != "enc::AAAA" {
		t.Fatalf("expected passthrough without key, got %#v", got)
	}

	// With key: empty encoded -> "".
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.decryptCredentialValue(ctx, "enc::"); got != "" {
		t.Fatalf("expected empty encoded to return empty string, got %#v", got)
	}

	// With key but invalid base64 -> returns original.
	if got := ex.decryptCredentialValue(ctx, "enc::not-base64"); got != "enc::not-base64" {
		t.Fatalf("expected passthrough on decrypt error, got %#v", got)
	}

	// Valid encrypted value round-trip.
	var enc string
	if err := db.QueryRowContext(ctx, `SELECT encode(pgp_sym_encrypt('secret', 'k'), 'base64')`).Scan(&enc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if got := ex.decryptCredentialValue(ctx, "enc::"+enc); got != "secret" {
		t.Fatalf("expected decrypted secret, got %#v", got)
	}

	// loadVerticalCredentials validations + decode errors (JSONB can be non-object).
	ex2 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex2.loadVerticalCredentials(ctx, verticalID); err == nil {
		t.Fatalf("expected sql db not configured")
	}
	ex2.SetSQLDB(db)
	if _, err := ex2.loadVerticalCredentials(ctx, ""); err == nil {
		t.Fatalf("expected vertical_id required")
	}
	verticalBad := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vb', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalBad, "\"x\""); err != nil {
		t.Fatalf("seed bad creds: %v", err)
	}
	if _, err := ex2.loadVerticalCredentials(ctx, verticalBad); err == nil {
		t.Fatalf("expected decode vertical credentials error")
	}
}
