package dashboard

import (
	"bytes"
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

func TestDashboard_EventDetail_WithDeliveries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v1', 'us', 'operating', 'operating', now(), now())
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

	eventID := uuid.NewString()
	createdAt := time.Now().Add(-10 * time.Second).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'board.chat', 'human', $2::uuid, $3::jsonb, $4)
	`, eventID, verticalID, `{"hello":"world"}`, createdAt); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'empire-coordinator', now())
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	processedAt := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, 'empire-coordinator', $2, 'processed', 0, NULL)
	`, eventID, processedAt); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/events/"+uuid.NewString(), nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/events/"+eventID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("detail status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"deliveries"`)) {
			t.Fatalf("expected deliveries in response: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/events/"+eventID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("alias detail status=%d body=%s", w.Code, w.Body.String())
		}
	}
}

func TestDashboard_Funnel_ThroughputAndStuck(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'A', 'a', 'us', 'discovered', 'factory', now() - interval '2 days', $4),
			($2::uuid, 'B', 'b', 'us', 'operating', 'operating', now() - interval '1 days', now()),
			($3::uuid, 'C', 'c', 'us', 'killed', 'factory', now() - interval '1 days', now())
	`, uuid.NewString(), uuid.NewString(), uuid.NewString(), old); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/funnel", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("funnel status=%d body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"stage_counts"`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"stuck"`)) {
		t.Fatalf("unexpected funnel response: %s", w.Body.String())
	}
}
