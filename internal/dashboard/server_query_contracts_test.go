package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	empireconfig "empireai/internal/empire/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func decodeJSONMap(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, string(body))
	}
	return out
}

func requireMapKey(t *testing.T, m map[string]any, key string) any {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("expected key %q in %#v", key, m)
	}
	return v
}

func requireMap(t *testing.T, v any, label string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		t.Fatalf("expected %s map, got %#v", label, v)
	}
	return m
}

func requireList(t *testing.T, v any, label string) []any {
	t.Helper()
	items, ok := v.([]any)
	if !ok {
		t.Fatalf("expected %s list, got %#v", label, v)
	}
	return items
}

func TestDashboardQueryContracts_CoreReadEndpoints(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	cfg := &config.Config{
		Extensions: map[string]any{"budget": empireconfig.BudgetConfig{
			HumanTasks: empireconfig.HumanTasksConfig{
				MaxTasksPerWeek: 5,
				BudgetReset:     "monday",
			},
		}},
	}

	verticalID := uuid.NewString()
	eventID := uuid.NewString()
	mailboxID := uuid.NewString()
	openTaskID := uuid.NewString()
	completedTaskID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Payroll Peru', 'payroll-peru', 'peru', 'researching', 'factory', now() - interval '2 days', now() - interval '2 hours')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES
			('empire-coordinator', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now()),
			('terminated-agent', 'stub', 'observer', 'holding', NULL, 'terminated', '{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'vertical.research_started', 'empire-coordinator', $2::uuid, '{}'::jsonb, now() - interval '20 minutes')
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO mailbox (id, event_id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'empire-coordinator', 'vertical_approval', 'critical', 'pending', '{}'::jsonb, 'Need founder review', now() - interval '15 minutes')
	`, mailboxID, eventID, verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (
			id, requesting_agent, vertical_id, category, description, priority, status, created_at
		) VALUES (
			$1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'Call the operator', 'high', 'pending_review', now() - interval '1 hour'
		)
	`, openTaskID, verticalID); err != nil {
		t.Fatalf("seed open task: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (
			id, requesting_agent, vertical_id, category, description, priority, status, reviewed_at, completed_at,
			result, outcome, follow_up_needed, created_at
		) VALUES (
			$1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'Review validation notes', 'medium', 'completed',
			now() - interval '30 minutes', now() - interval '20 minutes',
			'done', 'success', false, now() - interval '2 hours'
		)
	`, completedTaskID, verticalID); err != nil {
		t.Fatalf("seed completed task: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'portfolio.digest_compiled', 'empire-coordinator', '{"summary":"digest ready"}'::jsonb, now() - interval '10 minutes')
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed digest event: %v", err)
	}

	srv := NewServer(db, cfg, pg, pg, nil)
	h := srv.Handler()

	t.Run("overview", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/overview", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("overview status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if _, ok := requireMapKey(t, body, "generated_at").(string); !ok {
			t.Fatalf("expected generated_at string, got %#v", body["generated_at"])
		}
		if got, ok := requireMapKey(t, body, "agents_total").(float64); !ok || got < 2 {
			t.Fatalf("expected agents_total >= 2, got %#v", body["agents_total"])
		}
		if got, ok := requireMapKey(t, body, "mailbox_pending").(float64); !ok || got != 1 {
			t.Fatalf("expected mailbox_pending=1, got %#v", body["mailbox_pending"])
		}
	})

	t.Run("mailbox", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/mailbox?status=pending&limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		summary := requireMap(t, requireMapKey(t, body, "summary"), "mailbox summary")
		if got, ok := summary["critical"].(float64); !ok || got != 1 {
			t.Fatalf("expected critical mailbox count=1, got %#v", summary["critical"])
		}
		items := requireList(t, requireMapKey(t, body, "items"), "mailbox items")
		if len(items) != 1 {
			t.Fatalf("expected 1 mailbox item, got %d", len(items))
		}
		item := requireMap(t, items[0], "mailbox item")
		for _, key := range []string{"id", "event_id", "vertical_id", "vertical_slug", "from_agent", "type", "priority", "status", "summary", "response_minutes"} {
			requireMapKey(t, item, key)
		}
		if item["vertical_slug"] != "payroll-peru" {
			t.Fatalf("expected mailbox vertical_slug payroll-peru, got %#v", item["vertical_slug"])
		}
	})

	t.Run("tasks list", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks?status=open&limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("tasks status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["status"] != "open" {
			t.Fatalf("expected tasks status=open, got %#v", body["status"])
		}
		tasks := requireList(t, requireMapKey(t, body, "tasks"), "tasks")
		if len(tasks) != 1 {
			t.Fatalf("expected 1 open task, got %d", len(tasks))
		}
		task := requireMap(t, tasks[0], "task")
		for _, key := range []string{"id", "requesting_agent", "vertical_id", "vertical_slug", "category", "description", "priority", "status", "assigned_to", "created_at", "follow_up_needed"} {
			requireMapKey(t, task, key)
		}
		weekly := requireMap(t, requireMapKey(t, body, "weekly_budget"), "weekly budget")
		for _, key := range []string{"reset_day", "week_start_utc", "max_tasks_per_week", "approved_this_week"} {
			requireMapKey(t, weekly, key)
		}
	})

	t.Run("task stats", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/stats", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task stats status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["window"] != "30d" {
			t.Fatalf("expected 30d stats window, got %#v", body["window"])
		}
		byCategory := requireMap(t, requireMapKey(t, body, "by_category"), "by_category")
		verification := requireMap(t, requireMapKey(t, byCategory, "verification"), "verification stats")
		if got, ok := verification["success"].(float64); !ok || got != 1 {
			t.Fatalf("expected verification.success=1, got %#v", verification["success"])
		}
	})

	t.Run("digest", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/digest?top=3", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("digest status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		current := requireMap(t, requireMapKey(t, body, "current"), "current digest")
		lastCompiled := requireMap(t, requireMapKey(t, body, "last_compiled"), "last_compiled")
		if got, ok := current["top_n"].(float64); !ok || got != 3 {
			t.Fatalf("expected current.top_n=3, got %#v", current["top_n"])
		}
		requireMapKey(t, current, "text")
		requireMapKey(t, current, "snap")
		requireMapKey(t, lastCompiled, "payload")
	})

	t.Run("control targets", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/control/targets", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("control targets status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		targets := requireList(t, requireMapKey(t, body, "targets"), "targets")
		if len(targets) != 1 {
			t.Fatalf("expected 1 non-terminated target, got %d", len(targets))
		}
		target := requireMap(t, targets[0], "target")
		for _, key := range []string{"agent_id", "role", "vertical_id", "vertical_slug", "status"} {
			requireMapKey(t, target, key)
		}
	})
}

func TestDashboardQueryContracts_RuntimeAndPortfolioReadEndpoints(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	scanID := uuid.NewString()
	rootTaskID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Argentina Payroll', 'argentina-payroll', 'argentina', 'researching', 'factory', now() - interval '3 days', now() - interval '30 hours')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ('market-research-agent', 'stub', 'market-research-agent', 'factory', $1::uuid, 'active', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ('market-research-agent', 'task', '[]'::jsonb, 'investigating operators', 2, 'active', now() - interval '2 hours', now() - interval '10 minutes')
	`); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS runtime_log (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ NOT NULL DEFAULT now(),
			level TEXT NOT NULL DEFAULT 'info',
			component TEXT NOT NULL DEFAULT 'runtime',
			action TEXT NOT NULL DEFAULT 'unknown',
			event_id UUID,
			event_type TEXT,
			agent_id TEXT,
			vertical_id UUID,
			campaign_id UUID,
			scan_id UUID,
			session_id UUID,
			detail JSONB NOT NULL DEFAULT '{}'::jsonb,
			error TEXT,
			duration_us BIGINT
		)
	`); err != nil {
		t.Fatalf("create runtime_log: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runtime_log (ts, level, component, action, agent_id, vertical_id, detail, error, duration_us)
		VALUES (
			now() - interval '5 minutes',
			'warn',
			'mcp-gateway',
			'mcp.tools.call.context_error',
			'market-research-agent',
			$1::uuid,
			'{"attempt":1}'::jsonb,
			'runtime_error code=mcp_context_token_missing component=mcp-gateway operation=mcp.context.resolve retryable=true: missing',
			1234
		)
	`, verticalID); err != nil {
		t.Fatalf("seed runtime log: %v", err)
	}
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
		t.Fatalf("create shards: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, status, deadline_at, budget_cents, spend_cents, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'financial_ops',
			'{"mode":"saas_gap","geography":"Argentina"}'::jsonb, 'assigned', now() + interval '20 minutes', 50, 21, now() - interval '8 minutes'
		)
	`, uuid.NewString(), rootTaskID, scanID); err != nil {
		t.Fatalf("seed shard: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	t.Run("runtime logs", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/runtime/logs?limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("runtime logs status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		logs := requireList(t, requireMapKey(t, body, "runtime_logs"), "runtime_logs")
		if len(logs) != 1 {
			t.Fatalf("expected 1 runtime log, got %d", len(logs))
		}
		logItem := requireMap(t, logs[0], "runtime log")
		for _, key := range []string{"id", "ts", "level", "component", "action", "agent_id", "vertical_id", "detail", "error", "error_code", "error_component", "error_operation", "duration_us"} {
			requireMapKey(t, logItem, key)
		}
		if logItem["error_code"] != "mcp_context_token_missing" {
			t.Fatalf("expected parsed error_code, got %#v", logItem["error_code"])
		}
	})

	t.Run("conversations list", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/conversations?limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("conversations status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		conversations := requireList(t, requireMapKey(t, body, "conversations"), "conversations")
		if len(conversations) != 1 {
			t.Fatalf("expected 1 conversation, got %d", len(conversations))
		}
		item := requireMap(t, conversations[0], "conversation")
		for _, key := range []string{"agent_id", "role", "vertical_id", "vertical_slug", "mode", "turn_count", "summary", "updated_at"} {
			requireMapKey(t, item, key)
		}
	})

	t.Run("funnel", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/funnel", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("funnel status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		stageCounts := requireMap(t, requireMapKey(t, body, "stage_counts"), "stage_counts")
		requireMapKey(t, stageCounts, "researching")
		stuck := requireList(t, requireMapKey(t, body, "stuck"), "stuck")
		if len(stuck) != 1 {
			t.Fatalf("expected 1 stuck vertical, got %d", len(stuck))
		}
		throughput := requireMap(t, requireMapKey(t, body, "throughput"), "throughput")
		for _, key := range []string{"daily", "discoveries_14d", "progressed_14d", "killed_14d", "scoring_completion_rate"} {
			requireMapKey(t, throughput, key)
		}
	})

	t.Run("pipeline shards", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/pipeline/shards?limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("pipeline shards status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		scans := requireList(t, requireMapKey(t, body, "scans"), "scans")
		if len(scans) != 1 {
			t.Fatalf("expected 1 scan rollup, got %d", len(scans))
		}
		scan := requireMap(t, scans[0], "scan")
		for _, key := range []string{"scan_id", "mode", "geography", "shards_total", "shards_pending", "shards_assigned", "shards_completed", "shards_failed", "shards_stuck", "spend_cents", "progress", "updated_at"} {
			requireMapKey(t, scan, key)
		}
		if scan["scan_id"] != scanID {
			t.Fatalf("expected scan_id %s, got %#v", scanID, scan["scan_id"])
		}
	})
}

func TestDashboardQueryContracts_WorkflowReadEndpoints(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	t.Run("graph", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/graph?mode=holding", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("graph status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		requireList(t, requireMapKey(t, body, "nodes"), "graph nodes")
		requireList(t, requireMapKey(t, body, "edges"), "graph edges")
	})

	t.Run("pipeline flow graph", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/pipeline/graph?view=design", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("pipeline graph status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		requireList(t, requireMapKey(t, body, "nodes"), "pipeline graph nodes")
		requireList(t, requireMapKey(t, body, "edges"), "pipeline graph edges")
		meta := requireMap(t, requireMapKey(t, body, "meta"), "pipeline graph meta")
		for _, key := range []string{"workflow_name", "workflow_version", "platform_version", "stages", "rubrics"} {
			requireMapKey(t, meta, key)
		}
	})
}
