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
			($1::uuid, 'vertical.shortlisted', 'pipeline-coordinator', $2::uuid, '{"composite_score":81.25}'::jsonb, now()),
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

	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_stage, entered_stage_at,
			transition_history, accumulator_state, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'empire_vertical_pipeline', '2.1.0', 'ready_for_review', now() - interval '15 minutes',
			$2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb, now(), now()
		)
	`,
		verticalID,
		`[{"transition_id":"branding_to_ready","from":"branding","to":"ready_for_review","trigger_event_id":"evt-1","fired_at":"2026-03-07T18:40:00Z","guards_evaluated":["gate_g4_branding"]}]`,
		`{"pipeline-coordinator":{"gates":{"research":true,"spec":true,"cto":true,"brand":true}}}`,
		`[{"timer_id":"portfolio_digest_timer","created_at":"2026-03-07T18:45:00Z","fires_at":"2026-03-08T00:45:00Z","cancelled":false}]`,
		`{"revision_count":2,"source":"test"}`,
	); err != nil {
		t.Fatalf("seed workflow_instances: %v", err)
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
	workflowState, _ := out["workflow_state"].(map[string]any)
	if workflowState == nil {
		t.Fatalf("missing workflow_state payload: %s", w.Body.String())
	}
	if got := workflowState["workflow_version"]; got != "2.1.0" {
		t.Fatalf("expected workflow_version 2.1.0, got %#v", got)
	}
	if got := workflowState["current_state"]; got != "ready_for_review" {
		t.Fatalf("expected current_state ready_for_review, got %#v", got)
	}
	if got, ok := workflowState["transition_count"].(float64); !ok || got != 1 {
		t.Fatalf("expected transition_count 1, got %#v", workflowState["transition_count"])
	}
	if got, ok := workflowState["active_timer_count"].(float64); !ok || got != 1 {
		t.Fatalf("expected active_timer_count 1, got %#v", workflowState["active_timer_count"])
	}
}

func TestDashboard_HoldingVerticalDetail_BackfillsArtifactsFromEvents(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (
			id, name, slug, geography, stage, mode,
			raw_signals, scores, business_brief, mvp_spec, spec_review, cto_feasibility, brand, validation_kit, full_spec,
			created_at, updated_at
		) VALUES (
			$1::uuid, 'EU AI Act Compliance', 'eu-ai-act-compliance', 'eu', 'ready_for_review', 'factory',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
			now(), now()
		)
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at) VALUES
			($1::uuid, 'vertical.scored', 'pipeline-coordinator', $2::uuid, '{"composite_score":84.5,"dimensions":{"pain":88}}'::jsonb, now() - interval '30 minutes'),
			($3::uuid, 'research.completed', 'business-research-agent', $2::uuid, '{"business_brief":{"business_model":"SaaS compliance workflow","opportunity_hypothesis":"AI Act governance cockpit"}}'::jsonb, now() - interval '25 minutes'),
			($4::uuid, 'spec.draft_ready', 'lightweight-spec-agent', $2::uuid, '{"mvp_spec":{"core_workflow":"policy intake -> risk scoring -> evidence export"}}'::jsonb, now() - interval '20 minutes'),
			($5::uuid, 'spec_review.passed', 'spec-reviewer', $2::uuid, '{"checklist":{"scope":"pass","feasibility":"pass"}}'::jsonb, now() - interval '15 minutes'),
			($6::uuid, 'cto.spec_approved', 'factory-cto', $2::uuid, '{"cto_notes":{"decision":"approved","risks":["regulatory drift"]}}'::jsonb, now() - interval '10 minutes'),
			($7::uuid, 'brand.candidates_ready', 'pre-brand-agent', $2::uuid, '{"brand":{"candidates":[{"name":"LexGuard"}]}}'::jsonb, now() - interval '8 minutes'),
			($8::uuid, 'validation.package_ready', 'pipeline-coordinator', $2::uuid, '{"research":{"business_brief":{"target":"SMB legal + compliance"}},"spec":{"features":["risk matrix","audit log"]},"cto_notes":{"capacity":"green"},"brand":{"name":"LexGuard"}}'::jsonb, now() - interval '5 minutes')
	`,
		uuid.NewString(), verticalID,
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
	); err != nil {
		t.Fatalf("seed events: %v", err)
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

	if _, ok := vertical["scores"].(map[string]any); !ok {
		t.Fatalf("expected scores map backfilled from events, got %#v", vertical["scores"])
	}
	if brief, ok := vertical["business_brief"].(map[string]any); !ok || brief["business_model"] == nil {
		t.Fatalf("expected business_brief backfilled, got %#v", vertical["business_brief"])
	}
	if _, ok := vertical["mvp_spec"].(map[string]any); !ok {
		t.Fatalf("expected mvp_spec backfilled, got %#v", vertical["mvp_spec"])
	}
	if _, ok := vertical["spec_review"].(map[string]any); !ok {
		t.Fatalf("expected spec_review backfilled, got %#v", vertical["spec_review"])
	}
	if _, ok := vertical["cto_feasibility"].(map[string]any); !ok {
		t.Fatalf("expected cto_feasibility backfilled, got %#v", vertical["cto_feasibility"])
	}
	if _, ok := vertical["brand"].(map[string]any); !ok {
		t.Fatalf("expected brand backfilled, got %#v", vertical["brand"])
	}
	if _, ok := vertical["validation_kit"].(map[string]any); !ok {
		t.Fatalf("expected validation_kit backfilled, got %#v", vertical["validation_kit"])
	}
}
