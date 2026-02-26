package runtime

import (
	"context"
	"testing"

	"empireai/internal/models"
)

func TestToolExecutor_SystemTools_ValidationAndAuth(t *testing.T) {
	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)

	// nginx_reload only holding-devops
	_, err := exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "opco-ceo"}), "nginx_reload", map[string]any{})
	if err == nil {
		t.Fatal("expected nginx_reload to reject non holding-devops")
	}

	// systemd_control validation
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "systemd_control", map[string]any{
		"action": "nope",
		"unit":   "empireai-x",
	})
	if err == nil {
		t.Fatal("expected invalid action error")
	}
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "systemd_control", map[string]any{
		"action": "restart",
		"unit":   "nginx",
	})
	if err == nil {
		t.Fatal("expected unit prefix error")
	}

	// certbot_execute domain required
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "certbot_execute", map[string]any{
		"domain": "",
	})
	if err == nil {
		t.Fatal("expected domain required error")
	}
}

