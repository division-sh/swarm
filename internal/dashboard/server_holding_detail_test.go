package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboard_HoldingVerticalDetail_ReturnsArtifactsAndRelatedRecords(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (
			id, name, slug, geography, stage, mode,
			raw_signals, scores, business_brief, validation_kit,
			created_at, updated_at
		) VALUES (
			$1::uuid, 'Payroll Paraguay', 'payroll-paraguay', 'paraguay', 'ready_for_review', 'factory',
			$2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb,
			now(), now()
		)
	`,
		verticalID,
		`{"opportunity_hypothesis":"Automated payroll compliance for SMBs"}`,
		`{"composite_score":81.25,"viability_score":72}`,
		`{"business_model":"SaaS subscription per employee","pain_points":["manual compliance"]}`,
		`{"summary":"validation complete","gates":{"g1":true,"g2":true,"g3":true,"g4":true}}`,
	); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES
			('opco-ceo-1', 'stub', 'opco-ceo', 'operating', $1::uuid, 'active', '{}'::jsonb, now(), now()),
			('vp-growth-1', 'stub', 'vp-growth', 'operating', $1::uuid, 'idle', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES
			($1::uuid, 'vertical.shortlisted', 'scoring-coordinator', $2::uuid, '{"composite_score":81.25}'::jsonb, now()),
			($3::uuid, 'validation.package_ready', 'pipeline-coordinator', $2::uuid, '{"status":"ready"}'::jsonb, now())
	`, uuid.NewString(), verticalID, uuid.NewString()); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES
			($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'high', 'pending', '{"note":"review"}'::jsonb, 'Ready for founder review', now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, source, metadata, created_at)
		VALUES
			($1::uuid, $2::uuid, 'llm', 3200, 'exact', '{"provider":"anthropic"}'::jsonb, now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed spend_ledger: %v", err)
	}

	pg := &store.PostgresStore{DB: db}
	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/holding/vertical?id="+verticalID, nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	vertical, _ := out["vertical"].(map[string]any)
	if vertical == nil {
		t.Fatalf("missing vertical payload: %s", w.Body.String())
	}
	if got := vertical["id"]; got != verticalID {
		t.Fatalf("expected vertical id %s, got %#v", verticalID, got)
	}
	if got := vertical["composite_score"]; got != "81.25" {
		t.Fatalf("expected composite_score 81.25, got %#v", got)
	}
	brief, _ := vertical["business_brief"].(map[string]any)
	if brief == nil || brief["business_model"] != "SaaS subscription per employee" {
		t.Fatalf("expected business brief in response, got %#v", vertical["business_brief"])
	}

	agents, _ := out["agents"].([]any)
	if len(agents) != 2 {
		t.Fatalf("expected 2 related agents, got %#v", out["agents"])
	}
	events, _ := out["events"].([]any)
	if len(events) < 2 {
		t.Fatalf("expected related events, got %#v", out["events"])
	}
	mailbox, _ := out["mailbox"].([]any)
	if len(mailbox) != 1 {
		t.Fatalf("expected 1 mailbox item, got %#v", out["mailbox"])
	}
	spend, _ := out["spend"].(map[string]any)
	if spend == nil {
		t.Fatalf("missing spend summary: %s", w.Body.String())
	}
	if got, ok := spend["all_time_cents"].(float64); !ok || got < 3200 {
		t.Fatalf("expected all_time_cents >= 3200, got %#v", spend["all_time_cents"])
	}
}
