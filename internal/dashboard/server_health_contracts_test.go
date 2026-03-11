package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestDashboard_Health_IncludesContractSummary(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/health", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	contracts, _ := body["contracts"].(map[string]any)
	if contracts == nil {
		t.Fatalf("expected contracts summary in health payload: %s", w.Body.String())
	}
	if paths, _ := contracts["paths"].(map[string]any); len(paths) == 0 {
		t.Fatalf("expected contract paths in contracts payload: %#v", contracts["paths"])
	}
	workflow, _ := contracts["workflow"].(map[string]any)
	if workflow == nil {
		t.Fatalf("expected workflow metadata in contracts payload: %#v", contracts)
	}
	if got, _ := workflow["version"].(string); got == "" {
		t.Fatalf("expected non-empty workflow version, got %#v", workflow["version"])
	}
	if got, ok := workflow["validation_stages"].([]any); !ok || len(got) != 4 {
		t.Fatalf("expected 4 validation stages, got %#v", workflow["validation_stages"])
	}
	platform, _ := contracts["platform"].(map[string]any)
	if platform == nil {
		t.Fatalf("expected platform metadata in contracts payload: %#v", contracts)
	}
	if got := platform["version"]; got == nil || got == "" {
		t.Fatalf("expected platform version, got %#v", got)
	}
	verification, _ := contracts["verification_gates"].(map[string]any)
	if verification == nil {
		t.Fatalf("expected verification gate metadata in contracts payload: %#v", contracts)
	}
	if got, ok := verification["count"].(float64); !ok || got <= 0 {
		t.Fatalf("expected verification gate count > 0, got %#v", verification["count"])
	}
	workflowAudit, _ := body["workflow_audit"].(map[string]any)
	if workflowAudit == nil {
		t.Fatalf("expected workflow_audit payload in health response: %s", w.Body.String())
	}
	if _, ok := workflowAudit["warnings"].([]any); !ok {
		t.Fatalf("expected workflow_audit warnings list, got %#v", workflowAudit["warnings"])
	}
	if _, ok := workflowAudit["instances_total"]; !ok {
		t.Fatalf("expected workflow_audit counters, got %#v", workflowAudit)
	}
}
