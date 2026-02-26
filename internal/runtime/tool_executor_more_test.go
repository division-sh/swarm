package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeToolExecutor_SetManager_Instagram_Email_SystemTools(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	exec.SetSQLDB(db)
	exec.SetManager(manager)

	// Instagram handle check: invalid handle hits local validation path (no network).
	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "vp-growth",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["instagram_handle_check"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "instagram_handle_check", map[string]any{"handle": "bad!!"})
		if err == nil {
			t.Fatal("expected instagram handle format error")
		}
	}

	// Email API: missing credentials path.
	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "opco-ceo",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["email_api"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "email_api", map[string]any{
			"to":      []string{"a@example.com"},
			"subject": "s",
			"body":    "b",
		})
		if err == nil {
			t.Fatal("expected missing email credentials error")
		}
	}

	// Holding system tools: command paths are exercised, but will fail on machines without systemd/nginx/certbot.
	{
		actor := models.AgentConfig{
			ID:     "holding-devops",
			Role:   "holding-devops",
			Mode:   "holding",
			Config: json.RawMessage(`{"system_prompt":"x","tools":["nginx_reload","systemd_control","certbot_execute"]}`),
		}
		// nginx_reload should attempt nginx -t and fail (or report missing).
		toolCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "nginx_reload", map[string]any{}); err == nil {
			t.Fatal("expected nginx_reload to fail in test environment")
		}
		cancel()
		// systemd_control invalid action.
		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "systemd_control", map[string]any{"action": "nope", "unit": "empireai-x"}); err == nil {
			t.Fatal("expected systemd_control to reject invalid action")
		}
		cancel()
		// certbot_execute missing domain.
		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "certbot_execute", map[string]any{"domain": ""}); err == nil {
			t.Fatal("expected certbot_execute to require domain")
		}
		cancel()
	}
}

func TestToolExecutor_AuthorizationHelpers(t *testing.T) {
	actor := models.AgentConfig{Role: "vp-product", VerticalID: "v1"}
	target := models.AgentConfig{Role: "marketing-agent", VerticalID: "v1"}
	if err := authorizeManage(actor, target.Role, target.VerticalID); err == nil {
		t.Fatal("expected vp-product to be blocked from managing growth agents")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, models.AgentConfig{Role: "vp-growth"}, "active"); err == nil {
		t.Fatal("expected chief-of-staff to only propose routing")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, models.AgentConfig{Role: "backend-agent"}, "active"); err != nil {
		t.Fatalf("expected cto-agent to route eng agents, got %v", err)
	}
}
