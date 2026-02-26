package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboard_PipelineShardAction_ErrorBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	// Wrong method branch (call action handler directly).
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/api/pipeline/shards/"+uuid.NewString()+"/retry", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		srv.handlePipelineShardAction(w, req, uuid.NewString(), "retry")
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Invalid shard id.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/not-a-uuid/retry", map[string]any{}))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 invalid id, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// shards table unavailable.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/"+uuid.NewString()+"/retry", map[string]any{}))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 without shards table, got %d body=%s", w.Code, w.Body.String())
		}
	}

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS shards (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			root_task_id UUID NOT NULL,
			scan_id UUID,
			stage TEXT NOT NULL,
			shard_index INT NOT NULL,
			shard_count INT NOT NULL,
			shard_key TEXT NOT NULL,
			scope JSONB NOT NULL,
			agent_id TEXT REFERENCES agents(id),
			status TEXT NOT NULL DEFAULT 'pending',
			deadline_at TIMESTAMPTZ NOT NULL,
			budget_cents INT NOT NULL,
			spend_cents INT NOT NULL DEFAULT 0,
			retry_count INT NOT NULL DEFAULT 0,
			error TEXT,
			assigned_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}

	shardID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (id, root_task_id, stage, shard_index, shard_count, shard_key, scope, status, deadline_at, budget_cents, spend_cents, created_at, completed_at)
		VALUES ($1::uuid, $2::uuid, 'market_research', 0, 1, 'all', '{}'::jsonb, 'completed', now() + interval '1 hour', 100, 50, now(), now())
	`, shardID, uuid.NewString()); err != nil {
		t.Fatalf("seed completed shard: %v", err)
	}

	// Not retryable from completed.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/"+shardID+"/retry", map[string]any{}))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 non-retryable, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Not cancelable from completed.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/"+shardID+"/cancel", map[string]any{}))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 non-cancelable, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Unsupported action branch (call action handler directly).
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/api/pipeline/shards/"+shardID+"/noop", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		srv.handlePipelineShardAction(w, req, shardID, "noop")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 unsupported action, got %d body=%s", w.Code, w.Body.String())
		}
	}
}

func TestDashboard_HelperCoverage_AsFloatAny_ParseStringList_CompactAge(t *testing.T) {
	inputs := []any{
		float64(1.5),
		float32(2.5),
		int(3),
		int64(4),
		int32(5),
		json.Number("6.75"),
		" 7.5 ",
		map[string]any{"unexpected": true},
	}
	for _, in := range inputs {
		_ = asFloatAny(in)
	}

	if got := parseStringList([]string{"a", " a ", "", "b", "a"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("parseStringList []string unexpected: %#v", got)
	}
	if got := parseStringList([]any{"x", " x ", nil, "null", 42}); len(got) != 2 || got[0] != "x" || got[1] != "42" {
		t.Fatalf("parseStringList []any unexpected: %#v", got)
	}
	if got := parseStringList("one, two, one"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("parseStringList string unexpected: %#v", got)
	}
	if got := parseStringList(123); got != nil {
		t.Fatalf("parseStringList default should be nil, got %#v", got)
	}

	if v := compactAge(-5 * time.Second); v != "0s" {
		t.Fatalf("compactAge negative: %q", v)
	}
	if v := compactAge(30 * time.Second); v != "30s" {
		t.Fatalf("compactAge seconds: %q", v)
	}
	if v := compactAge(12 * time.Minute); v != "12m" {
		t.Fatalf("compactAge minutes: %q", v)
	}
	if v := compactAge(5 * time.Hour); v != "5h" {
		t.Fatalf("compactAge hours: %q", v)
	}
	if v := compactAge(49 * time.Hour); v != "2d" {
		t.Fatalf("compactAge days: %q", v)
	}
}
