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

func TestDashboard_HoldingList_IncludesWorkflowBadges(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (
			id, name, slug, geography, stage, mode, scores, created_at, updated_at
		) VALUES (
			$1::uuid, 'Payroll Peru', 'payroll-peru', 'peru', 'researching', 'factory',
			'{"composite_score":78.5}'::jsonb, now() - interval '2 days', now() - interval '45 minutes'
		)
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state, entered_stage_at,
			transition_history, accumulator_state, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'empire_vertical_pipeline', '2.1.0', 'researching', now() - interval '3 hours',
			$2::jsonb, '{}'::jsonb, $3::jsonb, $4::jsonb, now() - interval '2 days', now()
		)
	`,
		verticalID,
		`[{"transition_id":"shortlisted_to_researching","from":"shortlisted","to":"researching","trigger_event_id":"evt-1","fired_at":"2026-03-07T12:00:00Z","guards_evaluated":["has_vertical_id"]}]`,
		`[{"timer_id":"portfolio_digest_timer","created_at":"2026-03-07T12:05:00Z","fires_at":"2026-03-07T18:05:00Z","cancelled":false}]`,
		`{"revision_count":3}`,
	); err != nil {
		t.Fatalf("seed workflow_instance: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/holding", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("holding status=%d body=%s", w.Code, w.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	verticals, _ := out["verticals"].([]any)
	if len(verticals) == 0 {
		t.Fatalf("expected holding verticals in response")
	}
	var found map[string]any
	for _, raw := range verticals {
		item, _ := raw.(map[string]any)
		if item != nil && item["id"] == verticalID {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatalf("expected seeded vertical in holding response: %s", w.Body.String())
	}
	if got := found["workflow_version"]; got != "2.1.0" {
		t.Fatalf("expected workflow_version 2.1.0, got %#v", got)
	}
	if got := found["workflow_current_state"]; got != "researching" {
		t.Fatalf("expected workflow_current_state researching, got %#v", got)
	}
	if got, ok := found["active_timer_count"].(float64); !ok || got != 1 {
		t.Fatalf("expected active_timer_count 1, got %#v", found["active_timer_count"])
	}
	if got, ok := found["transition_count"].(float64); !ok || got != 1 {
		t.Fatalf("expected transition_count 1, got %#v", found["transition_count"])
	}
	if got, ok := found["revision_count"].(float64); !ok || got != 3 {
		t.Fatalf("expected revision_count 3, got %#v", found["revision_count"])
	}
	if _, ok := found["stage_entered_at"].(string); !ok {
		t.Fatalf("expected stage_entered_at timestamp, got %#v", found["stage_entered_at"])
	}
	workflowSummary, _ := out["workflow_summary"].(map[string]any)
	if workflowSummary == nil {
		t.Fatalf("expected workflow_summary in holding response: %s", w.Body.String())
	}
	if got, ok := workflowSummary["active_timers"].(float64); !ok || got != 1 {
		t.Fatalf("expected workflow_summary active_timers=1, got %#v", workflowSummary["active_timers"])
	}
	if got, ok := workflowSummary["drift"].(float64); !ok || got != 0 {
		t.Fatalf("expected workflow_summary drift=0, got %#v", workflowSummary["drift"])
	}
}
