package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboardAliasParity_ReadEndpoints(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	mailboxID := uuid.NewString()
	taskID := uuid.NewString()
	scanID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Chile Payroll', 'chile-payroll', 'chile', 'researching', 'factory', now() - interval '2 days', now() - interval '2 hours')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ('empire-coordinator', 'task', '[]'::jsonb, 'alias parity conversation', 1, 'active', now() - interval '1 hour', now() - interval '10 minutes')
	`); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES ($1::uuid, $2::uuid, 'empire-coordinator', 'vertical_approval', 'critical', 'pending', '{}'::jsonb, 'review alias parity', now() - interval '15 minutes')
	`, mailboxID, verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, priority, status, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'check alias parity', 'high', 'pending_review', now() - interval '20 minutes')
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed task: %v", err)
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
		t.Fatalf("create shards table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key, scope, status, deadline_at, budget_cents, spend_cents, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'financial_ops', '{"mode":"saas_gap","geography":"Chile"}'::jsonb, 'assigned', now() + interval '30 minutes', 50, 12, now() - interval '8 minutes'
		)
	`, uuid.NewString(), uuid.NewString(), scanID); err != nil {
		t.Fatalf("seed shards: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	t.Run("tasks", func(t *testing.T) {
		dashboardResp := decodeAliasBody(t, h, "/dashboard/api/tasks?status=open&limit=10")
		apiResp := decodeAliasBody(t, h, "/api/tasks?status=open&limit=10")

		if dashboardResp["status"] != apiResp["status"] {
			t.Fatalf("status mismatch dashboard=%#v api=%#v", dashboardResp["status"], apiResp["status"])
		}
		dashboardTasks := requireList(t, requireMapKey(t, dashboardResp, "tasks"), "dashboard tasks")
		apiTasks := requireList(t, requireMapKey(t, apiResp, "tasks"), "api tasks")
		if len(dashboardTasks) != len(apiTasks) || len(dashboardTasks) != 1 {
			t.Fatalf("task list mismatch dashboard=%d api=%d", len(dashboardTasks), len(apiTasks))
		}
		if requireMap(t, dashboardTasks[0], "dashboard task")["id"] != requireMap(t, apiTasks[0], "api task")["id"] {
			t.Fatalf("expected matching task ids")
		}
	})

	t.Run("mailbox", func(t *testing.T) {
		dashboardResp := decodeAliasBody(t, h, "/dashboard/api/mailbox?status=pending&limit=10")
		apiResp := decodeAliasBody(t, h, "/api/mailbox?status=pending&limit=10")

		dashboardSummary := requireMap(t, requireMapKey(t, dashboardResp, "summary"), "dashboard summary")
		apiSummary := requireMap(t, requireMapKey(t, apiResp, "summary"), "api summary")
		if dashboardSummary["pending"] != apiSummary["pending"] {
			t.Fatalf("pending summary mismatch dashboard=%#v api=%#v", dashboardSummary["pending"], apiSummary["pending"])
		}
		dashboardItems := requireList(t, requireMapKey(t, dashboardResp, "items"), "dashboard mailbox items")
		apiItems := requireList(t, requireMapKey(t, apiResp, "items"), "api mailbox items")
		if len(dashboardItems) != len(apiItems) || len(dashboardItems) != 1 {
			t.Fatalf("mailbox item mismatch dashboard=%d api=%d", len(dashboardItems), len(apiItems))
		}
		if requireMap(t, dashboardItems[0], "dashboard mailbox item")["id"] != requireMap(t, apiItems[0], "api mailbox item")["id"] {
			t.Fatalf("expected matching mailbox ids")
		}
	})

	t.Run("conversations", func(t *testing.T) {
		dashboardResp := decodeAliasBody(t, h, "/dashboard/api/conversations?limit=10")
		apiResp := decodeAliasBody(t, h, "/api/conversations?limit=10")

		dashboardConvos := requireList(t, requireMapKey(t, dashboardResp, "conversations"), "dashboard conversations")
		apiConvos := requireList(t, requireMapKey(t, apiResp, "conversations"), "api conversations")
		if len(dashboardConvos) != len(apiConvos) || len(dashboardConvos) != 1 {
			t.Fatalf("conversation count mismatch dashboard=%d api=%d", len(dashboardConvos), len(apiConvos))
		}
		if requireMap(t, dashboardConvos[0], "dashboard conversation")["agent_id"] != requireMap(t, apiConvos[0], "api conversation")["agent_id"] {
			t.Fatalf("expected matching conversation agent ids")
		}
	})

	t.Run("holding", func(t *testing.T) {
		dashboardResp := decodeAliasBody(t, h, "/dashboard/api/holding")
		apiResp := decodeAliasBody(t, h, "/api/holding")

		dashboardVerticals := requireList(t, requireMapKey(t, dashboardResp, "verticals"), "dashboard verticals")
		apiVerticals := requireList(t, requireMapKey(t, apiResp, "verticals"), "api verticals")
		if len(dashboardVerticals) != len(apiVerticals) || len(dashboardVerticals) != 1 {
			t.Fatalf("holding vertical mismatch dashboard=%d api=%d", len(dashboardVerticals), len(apiVerticals))
		}
		if requireMap(t, dashboardVerticals[0], "dashboard holding vertical")["id"] != requireMap(t, apiVerticals[0], "api holding vertical")["id"] {
			t.Fatalf("expected matching holding vertical ids")
		}
	})

	t.Run("pipeline shards", func(t *testing.T) {
		dashboardResp := decodeAliasBody(t, h, "/dashboard/api/pipeline/shards?limit=10")
		apiResp := decodeAliasBody(t, h, "/api/pipeline/shards?limit=10")

		dashboardScans := requireList(t, requireMapKey(t, dashboardResp, "scans"), "dashboard scans")
		apiScans := requireList(t, requireMapKey(t, apiResp, "scans"), "api scans")
		if len(dashboardScans) != len(apiScans) || len(dashboardScans) != 1 {
			t.Fatalf("scan rollup mismatch dashboard=%d api=%d", len(dashboardScans), len(apiScans))
		}
		if requireMap(t, dashboardScans[0], "dashboard scan")["scan_id"] != requireMap(t, apiScans[0], "api scan")["scan_id"] {
			t.Fatalf("expected matching scan ids")
		}
	})
}

func TestDashboardEventListDetailConsistency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	eventID := uuid.NewString()
	createdAt := time.Now().UTC().Add(-5 * time.Minute)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Argentina Ops', 'argentina-ops', 'argentina', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'board.chat', 'empire-coordinator', $2::uuid, '{"message":"hello"}'::jsonb, $3)
	`, eventID, verticalID, createdAt); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2)
	`, eventID, createdAt.Add(15*time.Second)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, 'empire-coordinator', $2, 'processed', 0, NULL)
	`, eventID, createdAt.Add(20*time.Second)); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	listBody := decodeAliasBody(t, h, "/dashboard/api/events?type=board.chat&limit=10")
	events := requireList(t, requireMapKey(t, listBody, "events"), "events")
	if len(events) != 1 {
		t.Fatalf("expected 1 event in list, got %d", len(events))
	}
	listItem := requireMap(t, events[0], "event list item")
	if listItem["id"] != eventID {
		t.Fatalf("expected listed event id %s, got %#v", eventID, listItem["id"])
	}
	if listItem["type"] != "board.chat" {
		t.Fatalf("expected board.chat type, got %#v", listItem["type"])
	}

	detailBody := decodeAliasBody(t, h, "/dashboard/api/events/"+eventID)
	eventDetail := requireMap(t, requireMapKey(t, detailBody, "event"), "event detail")
	payload := requireMap(t, requireMapKey(t, detailBody, "payload"), "event payload")
	deliveries := requireList(t, requireMapKey(t, detailBody, "deliveries"), "deliveries")
	if eventDetail["id"] != listItem["id"] || eventDetail["type"] != listItem["type"] || eventDetail["source_agent"] != listItem["source_agent"] {
		t.Fatalf("list/detail mismatch list=%#v detail=%#v", listItem, eventDetail)
	}
	if payload["message"] != "hello" {
		t.Fatalf("expected payload.message=hello, got %#v", payload["message"])
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	delivery := requireMap(t, deliveries[0], "delivery")
	if delivery["agent_id"] != "empire-coordinator" || delivery["status"] != "processed" {
		t.Fatalf("unexpected delivery payload: %#v", delivery)
	}

	aliasDetailBody := decodeAliasBody(t, h, "/api/events/"+eventID)
	aliasEventDetail := requireMap(t, requireMapKey(t, aliasDetailBody, "event"), "alias event detail")
	if aliasEventDetail["id"] != eventDetail["id"] || aliasEventDetail["type"] != eventDetail["type"] {
		t.Fatalf("dashboard/api and /api detail mismatch dashboard=%#v api=%#v", eventDetail, aliasEventDetail)
	}
}

func decodeAliasBody(t *testing.T, h http.Handler, path string) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body.String())
	}
	return decodeJSONMap(t, w.Body.Bytes())
}
