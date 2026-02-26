package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboardServer_EventStream_SSE_WithFilters(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
		},
	}

	// Seed minimal rows for queryEvents joins.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ('empire-coordinator','system','empire-coordinator','holding','active','{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	now := time.Date(2026, 2, 16, 6, 0, 0, 0, time.UTC)
	eventID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'opco.ceo_report', 'opco-ceo', $2::uuid, '{}'::jsonb, $3)
	`, eventID, verticalID, now.Add(-1*time.Second)); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'empire-coordinator', now())
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count)
		VALUES ($1::uuid, 'empire-coordinator', now(), 'processed', 0)
	`, eventID); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	srv := NewServer(db, cfg, pg, pg, nil)
	srv.now = func() time.Time { return now }
	h := srv.Handler()

	// Short-lived request context cancels the streaming loop after the first pass.
	streamCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet,
		"/dashboard/api/events/stream?since="+now.Add(-10*time.Second).Format(time.RFC3339)+"&type=opco.*",
		nil,
	).WithContext(streamCtx)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, req)
		close(done)
	}()
	<-done

	body := w.Body.String()
	if !strings.Contains(body, "event: event") || !strings.Contains(body, "opco.ceo_report") {
		t.Fatalf("expected SSE event payload, got: %q", body)
	}
}

func TestDashboardServer_EventStream_IncludesRuntimeLogs(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
		},
	}

	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS runtime_log (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ NOT NULL DEFAULT now(),
			level TEXT NOT NULL,
			component TEXT NOT NULL,
			action TEXT NOT NULL,
			event_id UUID,
			event_type TEXT,
			agent_id TEXT,
			vertical_id UUID,
			campaign_id UUID,
			scan_id UUID,
			session_id UUID,
			detail JSONB,
			error TEXT,
			duration_us INT
		)
	`); err != nil {
		t.Fatalf("create runtime_log: %v", err)
	}
	now := time.Date(2026, 2, 16, 7, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runtime_log (ts, level, component, action, detail)
		VALUES ($1, 'info', 'interceptor', 'consumed', '{"type":"validation.started"}'::jsonb)
	`, now.Add(-1*time.Second)); err != nil {
		t.Fatalf("seed runtime_log: %v", err)
	}

	srv := NewServer(db, cfg, pg, pg, nil)
	srv.now = func() time.Time { return now }
	h := srv.Handler()

	streamCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet,
		"/dashboard/api/events/stream?since="+now.Add(-10*time.Second).Format(time.RFC3339)+"&include_runtime=true&component=interceptor",
		nil,
	).WithContext(streamCtx)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, req)
		close(done)
	}()
	<-done

	body := w.Body.String()
	if !strings.Contains(body, "event: runtime_log") || !strings.Contains(body, "\"component\":\"interceptor\"") {
		t.Fatalf("expected runtime_log SSE payload, got: %q", body)
	}
}
