package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/templateops"
	"empireai/internal/testutil"
)

func TestDashboard_APITemplatesPromptAndPublish(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	agentsDir := filepath.Join("..", "..", "configs", "agents", "templates")
	routesYAML := filepath.Join("..", "..", "configs", "agents", "templates", "routes.yaml")
	agents, bootstrap, seeded, err := templateops.CompileTemplateFromYAML(agentsDir, routesYAML)
	if err != nil {
		t.Fatalf("compile template: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('v1', $1::jsonb, $2::jsonb, $3::jsonb, 'seed', now())
	`, string(agents), string(bootstrap), string(seeded)); err != nil {
		t.Fatalf("seed org_templates: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	// GET template prompt
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/templates/opco-ceo/prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("get prompt status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"template_version":"v1"`)) {
			t.Fatalf("expected v1 template in response: %s", w.Body.String())
		}
	}

	updatedPrompt := "You are an updated OpCo CEO prompt for tests."
	// PUT draft prompt
	{
		w := httptest.NewRecorder()
		body := []byte(`{"prompt":"` + updatedPrompt + `","source":"test","notes":"draft update"}`)
		req := httptest.NewRequest(http.MethodPut, "/api/templates/opco-ceo/prompt", bytes.NewReader(body))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("put draft status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// GET reflects draft/effective prompt.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/templates/opco-ceo/prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("get draft status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"has_draft":true`)) {
			t.Fatalf("expected has_draft=true: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(updatedPrompt)) {
			t.Fatalf("expected updated prompt in effective output: %s", w.Body.String())
		}
	}

	// Publish as v2 and ensure draft is applied + cleared.
	{
		w := httptest.NewRecorder()
		body := []byte(`{"version":"v2","created_by":"factory-cto","description":"prompt update test"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/templates/publish", bytes.NewReader(body))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("publish status=%d body=%s", w.Code, w.Body.String())
		}
	}

	var agentsJSON []byte
	if err := db.QueryRowContext(ctx, `
		SELECT agents
		FROM org_templates
		WHERE version='v2'
	`).Scan(&agentsJSON); err != nil {
		t.Fatalf("load v2 template: %v", err)
	}
	var list []map[string]any
	if err := json.Unmarshal(agentsJSON, &list); err != nil {
		t.Fatalf("parse v2 agents: %v", err)
	}
	found := false
	for _, a := range list {
		if asString(a["role"]) != "opco-ceo" {
			continue
		}
		found = true
		if asString(a["system_prompt"]) != updatedPrompt {
			t.Fatalf("expected updated prompt applied, got: %q", asString(a["system_prompt"]))
		}
	}
	if !found {
		t.Fatal("opco-ceo role not found in v2 template")
	}

	var draftCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM template_prompt_drafts`).Scan(&draftCount); err != nil {
		t.Fatalf("count drafts: %v", err)
	}
	if draftCount != 0 {
		t.Fatalf("expected drafts cleared after publish, got %d", draftCount)
	}
}
