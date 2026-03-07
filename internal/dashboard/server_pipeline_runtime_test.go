package dashboard

import (
	"bytes"
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

func TestDashboard_PipelineShardsEndpoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
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

	scanID := uuid.NewString()
	scopeA := `{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["financial_ops","commerce_payments"]}`
	scopeB := `{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["customer_ops","marketing_sales"]}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, status, deadline_at, budget_cents, spend_cents, created_at
		)
		VALUES
			($1::uuid, $3::uuid, $2::uuid, 'market_research', 0, 2, 'financial_ops+commerce_payments', $4::jsonb, 'assigned', now() + interval '20 minutes', 50, 31, now() - interval '15 minutes'),
			($5::uuid, $3::uuid, $2::uuid, 'market_research', 1, 2, 'customer_ops+marketing_sales', $6::jsonb, 'completed', now() + interval '20 minutes', 50, 25, now() - interval '8 minutes')
	`, uuid.NewString(), scanID, uuid.NewString(), scopeA, uuid.NewString(), scopeB); err != nil {
		t.Fatalf("seed shards: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/pipeline/shards", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("pipeline shards status=%d body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"scans"`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"scan_id"`)) {
		t.Fatalf("unexpected pipeline shards response: %s", w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"shards_total":2`)) {
		t.Fatalf("expected shards_total=2 response: %s", w.Body.String())
	}
}

func TestDashboard_PipelineShardDetailAndActions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
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

	scanID := uuid.NewString()
	rootTaskID := uuid.NewString()
	failedShardID := uuid.NewString()
	pendingShardID := uuid.NewString()
	failedAgentID := "market-research-agent-shard-0-a1b2c3d4"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now())
	`, failedAgentID); err != nil {
		t.Fatalf("seed shard agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, spend_cents, error, assigned_at, completed_at, created_at
		)
		VALUES
			($1::uuid, $3::uuid, $2::uuid, 'market_research', 0, 2, 'financial_ops+commerce', '{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["financial_ops","commerce_payments"]}'::jsonb, $5, 'timed_out', now() - interval '15 minutes', 50, 41, 'timeout', now() - interval '35 minutes', now() - interval '5 minutes', now() - interval '40 minutes'),
			($4::uuid, $3::uuid, $2::uuid, 'market_research', 1, 2, 'customer_ops+marketing', '{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["customer_ops","marketing_sales"]}'::jsonb, NULL, 'pending', now() + interval '20 minutes', 50, 12, NULL, NULL, NULL, now() - interval '3 minutes')
	`, failedShardID, scanID, rootTaskID, pendingShardID, failedAgentID); err != nil {
		t.Fatalf("seed shards: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'market_research.scan_complete', $4, $2::jsonb, now() - interval '7 minutes'),
			($3::uuid, 'market_research.scan_complete', $4, $5::jsonb, now() - interval '6 minutes'),
			($6::uuid, 'market_research.scan_complete', $4, $7::jsonb, now() - interval '5 minutes'),
			($8::uuid, 'market_research.scan_complete', $4, $9::jsonb, now() - interval '4 minutes')
	`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","signal_strength":82}`,
		uuid.NewString(),
		failedAgentID,
		`{"scan_id":"`+scanID+`","signal_strength":55}`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","high_signal_count":2}`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","shard":{"terminal":true},"signal_strength":99}`,
	); err != nil {
		t.Fatalf("seed shard events: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/pipeline/shards/"+scanID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("detail status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"scan_id":"`+scanID+`"`)) {
			t.Fatalf("expected scan id in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"id":"`+failedShardID+`"`)) {
			t.Fatalf("expected failed shard id in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"reports_count":3`)) {
			t.Fatalf("expected reports_count=3 in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"high_signal_count":3`)) {
			t.Fatalf("expected high_signal_count=3 in detail response: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/api/pipeline/shards/"+failedShardID+"/retry", map[string]any{}))
		if w.Code != http.StatusOK {
			t.Fatalf("retry status=%d body=%s", w.Code, w.Body.String())
		}
		var status, errText string
		if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error, '') FROM shards WHERE id = $1::uuid`, failedShardID).Scan(&status, &errText); err != nil {
			t.Fatalf("query retried shard: %v", err)
		}
		if status != "pending" || errText != "" {
			t.Fatalf("expected retried shard pending with empty error, got status=%s error=%q", status, errText)
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/"+pendingShardID+"/cancel", map[string]any{}))
		if w.Code != http.StatusOK {
			t.Fatalf("cancel status=%d body=%s", w.Code, w.Body.String())
		}
		var status, errText string
		if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error, '') FROM shards WHERE id = $1::uuid`, pendingShardID).Scan(&status, &errText); err != nil {
			t.Fatalf("query canceled shard: %v", err)
		}
		if status != "failed" {
			t.Fatalf("expected canceled shard failed, got status=%s", status)
		}
		if errText == "" {
			t.Fatalf("expected canceled shard error text, got empty")
		}
	}
}

func TestDashboard_PipelineHealthEndpoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'AR', 'latam', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pipeline_transitions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL REFERENCES events(id),
			event_type TEXT NOT NULL,
			handler TEXT NOT NULL,
			pipeline_type TEXT NOT NULL,
			pipeline_id UUID NOT NULL,
			action TEXT NOT NULL,
			state_before JSONB,
			state_after JSONB,
			events_emitted TEXT[],
			drop_reason TEXT,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_transitions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, status, priority, created_at, started_at, completed_at)
		VALUES
			($1::uuid, $4::uuid, 'saas_gap', 'active', 'high', now() - interval '2 hours', now() - interval '2 hours', NULL),
			($2::uuid, $4::uuid, 'saas_trend', 'paused', 'normal', now() - interval '8 hours', now() - interval '8 hours', NULL),
			($3::uuid, $4::uuid, 'local_services', 'completed', 'normal', now() - interval '1 days', now() - interval '1 days', now() - interval '30 minutes')
	`, uuid.NewString(), uuid.NewString(), uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed scan campaigns: %v", err)
	}
	verticalResearch := uuid.NewString()
	verticalReady := uuid.NewString()
	verticalMarginal := uuid.NewString()
	verticalApproved := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, parked_at, created_at, updated_at)
		VALUES
			($1::uuid, 'Research V', 'research-v', 'ar', 'researching', 'factory', NULL, now() - interval '1 days', now() - interval '3 hours'),
			($2::uuid, 'Ready V', 'ready-v', 'ar', 'ready_for_review', 'factory', NULL, now() - interval '1 days', now() - interval '3 hours'),
			($3::uuid, 'Marginal V', 'marginal-v', 'ar', 'marginal_review', 'factory', now() - interval '9 days', now() - interval '10 days', now() - interval '9 days'),
			($4::uuid, 'Approved V', 'approved-v', 'ar', 'approved', 'factory', NULL, now() - interval '2 days', now() - interval '1 hours')
	`, verticalResearch, verticalReady, verticalMarginal, verticalApproved); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'scan.completed', 'scan-runner', '{}'::jsonb, now() - interval '1 hour')
	`, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_transitions (
			event_id, event_type, handler, pipeline_type, pipeline_id, action, error, created_at
		)
		VALUES ($1::uuid, 'scan.completed', 'scanRunner.handleScanRequested', 'scan', $2::uuid, 'error', 'request timeout', now() - interval '20 minutes')
	`, eventID, uuid.NewString()); err != nil {
		t.Fatalf("seed pipeline transition: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/pipeline", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/health/pipeline status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal /health/pipeline: %v", err)
	}
	if _, ok := body["campaigns"]; !ok {
		t.Fatalf("expected campaigns in response: %s", w.Body.String())
	}
	if _, ok := body["alerts"]; !ok {
		t.Fatalf("expected alerts in response: %s", w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/health/pipeline", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/api/health/pipeline status=%d body=%s", w.Code, w.Body.String())
	}
}
