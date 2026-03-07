package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboard_TaskView_FoundAndNotFound(t *testing.T) {
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

	taskID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, priority, status, assigned_to, reviewed_at, completed_at, result, outcome, follow_up_needed, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'high', 'completed', 'founder', now(), now(), 'done', 'success', false, now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/"+uuid.NewString(), nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
		}
	}
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/"+taskID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task view status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"task"`)) {
			t.Fatalf("expected task payload, got %s", w.Body.String())
		}
	}
}
