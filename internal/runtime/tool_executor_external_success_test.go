package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeToolExecutor_ExternalProxy_Succeeds_WithVerticalCredentials(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"available":true}`))
	}))
	defer okSrv.Close()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID, `{"registrar":{"endpoint":"`+okSrv.URL+`","api_key":"k1"}}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	scheduler := NewScheduler(func(Schedule) {})
	defer scheduler.Stop()
	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), scheduler, nil)
	exec.SetSQLDB(db)

	actor := models.AgentConfig{
		ID:         "opco-ceo-" + verticalID,
		Role:       "opco-ceo",
		Mode:       "operating",
		Type:       "stub",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["domain_availability_check"]}`),
	}

	out, err := exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{
		"path": "/check",
		"query": map[string]any{
			"domain": "example.com",
		},
		"headers": map[string]any{
			"x-test": "1",
		},
	})
	if err != nil {
		t.Fatalf("external proxy: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["status"] != "ok" {
		t.Fatalf("unexpected output: %v", out)
	}

	// Error status path.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	defer badSrv.Close()
	if _, err := db.ExecContext(ctx, `UPDATE verticals SET credentials=$2::jsonb WHERE id=$1::uuid`, verticalID, `{"registrar":{"endpoint":"`+badSrv.URL+`","api_key":"k1"}}`); err != nil {
		t.Fatalf("update creds: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{"path": "/check"}); err == nil {
		t.Fatal("expected error on 500 response")
	}
}
