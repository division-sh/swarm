package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboardServer_ConversationDetail_ExtractsArtifactsAndToolResults(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	agentID := "empire-coordinator"
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ($1, 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	sessionRow := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agent_sessions (id, agent_id, runtime_mode, provider, session_id, status, turn_count, created_at)
		VALUES ($1::uuid, $2, 'cli_test', 'anthropic', 's1', 'active', 3, now())
	`, sessionRow, agentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ($1, 'task', '[]'::jsonb, 'sum', 3, 'active', now(), now())
	`, agentID); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}

	now := time.Date(2026, 2, 16, 6, 0, 0, 0, time.UTC)
	turns := []struct {
		idx  int
		req  string
		resp string
	}{
		{1, `{"message":{"role":"tool","content":"tool output 1"}}`, `{"result":"assistant result text"}`},
		{2, `{"message":{"role":"user","content":"x"}}`, `{"content":[{"type":"text","text":"hello"},{"type":"tool_use","name":"sql_execute","input":{"query":"select 1"}}]}`},
		{3, `{}`, `{"tool_calls":[{"name":"agent_message","arguments":{"target":"a"}}],"content":"more text"}`},
	}
	for _, tr := range turns {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, error, created_at)
			VALUES ($1, $2::uuid, $3, $4::jsonb, $5::jsonb, true, 10, 0, '', $6)
		`, agentID, sessionRow, tr.idx, tr.req, tr.resp, now.Add(time.Duration(-tr.idx)*time.Second)); err != nil {
			t.Fatalf("seed turn %d: %v", tr.idx, err)
		}
	}

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/conversations/"+agentID, nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	// Ensure the response includes extracted artifacts; don't overfit exact format.
	b := w.Body.Bytes()
	if !bytes.Contains(b, []byte("assistant result text")) {
		t.Fatalf("expected extracted assistant text in response: %s", string(b))
	}
	if !bytes.Contains(b, []byte("sql_execute")) || !bytes.Contains(b, []byte("agent_message")) {
		t.Fatalf("expected extracted tool calls in response: %s", string(b))
	}
	if !bytes.Contains(b, []byte("tool output 1")) {
		t.Fatalf("expected extracted tool_result in response: %s", string(b))
	}
}

func TestDashboardServer_ConversationDetail_DoesNotTruncateAssistantText(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	agentID := "empire-coordinator"
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ($1, 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	sessionRow := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agent_sessions (id, agent_id, runtime_mode, provider, session_id, status, turn_count, created_at)
		VALUES ($1::uuid, $2, 'cli_test', 'anthropic', 's1', 'active', 1, now())
	`, sessionRow, agentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ($1, 'task', '[]'::jsonb, 'sum', 1, 'active', now(), now())
	`, agentID); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}

	longText := strings.Repeat("x", 900)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, error, created_at)
		VALUES ($1, $2::uuid, 1, '{}'::jsonb, $3::jsonb, true, 10, 0, '', now())
	`, agentID, sessionRow, `{"result":"`+longText+`"}`); err != nil {
		t.Fatalf("seed turn: %v", err)
	}

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/conversations/"+agentID, nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	turns, _ := body["turns"].([]any)
	if len(turns) != 1 {
		t.Fatalf("expected one turn, got %d", len(turns))
	}
	first, _ := turns[0].(map[string]any)
	assistant, _ := first["assistant_text"].(string)
	if assistant != longText {
		t.Fatalf("assistant_text was truncated or altered (len=%d)", len(assistant))
	}
}

func TestDashboardServer_ConversationArtifacts_ReturnsSessionFiles(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	agentID := "market-research-agent-shard-0-abc12345"
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ($1, 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agent_sessions (id, agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1::uuid, $2, 'cli_test', 'anthropic', 'sess-123', 'active', 4, now(), now())
	`, uuid.NewString(), agentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	dockerStub := writeDockerStubScript(t)
	t.Setenv("EMPIREAI_DOCKER_BIN", dockerStub)
	t.Setenv("EMPIREAI_FACTORY_CONTAINER", "empireai-factory")

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/conversations/"+agentID+"/artifacts?lines=20", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, `"workspace_container":"empireai-factory"`) {
		t.Fatalf("expected workspace container in response: %s", body)
	}
	if !strings.Contains(body, `project line`) {
		t.Fatalf("expected project jsonl tail in response: %s", body)
	}
	if !strings.Contains(body, `debug line`) {
		t.Fatalf("expected debug log tail in response: %s", body)
	}
}

func TestDashboardServer_ConversationArtifacts_404WhenNoSession(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	agentID := "market-research-agent-shard-0-nosession"
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ($1, 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/conversations/"+agentID+"/artifacts", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
	}
}

func writeDockerStubScript(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "docker-stub.sh")
	script := `#!/bin/sh
set -eu
if [ "$1" != "exec" ]; then
  echo "unsupported command: $*" >&2
  exit 1
fi
container="$2"
cmd="$3"
if [ "$container" != "empireai-factory" ]; then
  echo "unknown container: $container" >&2
  exit 1
fi
if [ "$cmd" = "find" ]; then
  echo "/home/agent/.claude/projects/-opt-empireai-scaffold/sess-123.jsonl"
  exit 0
fi
if [ "$cmd" = "tail" ]; then
  path="${6:-}"
  case "$path" in
    */sess-123.jsonl)
      echo "project line"
      ;;
    */sess-123.txt)
      echo "debug line"
      ;;
    *)
      echo "missing file" >&2
      exit 1
      ;;
  esac
  exit 0
fi
echo "unsupported exec payload: $*" >&2
exit 1
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write docker stub: %v", err)
	}
	return path
}
