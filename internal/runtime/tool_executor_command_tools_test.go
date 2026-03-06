package runtime_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"github.com/google/uuid"
)

type recordingEventStore struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *recordingEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}
func (r *recordingEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func TestRuntimeToolExecutor_CommandTools_SuccessPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-based fake binaries not supported on windows")
	}

	// Put fake binaries in PATH so nginx/systemctl/certbot succeed.
	bin := t.TempDir()
	writeExe := func(name, body string) {
		t.Helper()
		path := filepath.Join(bin, name)
		if err := os.WriteFile(path, []byte(body), 0755); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeExe("nginx", "#!/bin/sh\nexit 0\n")
	writeExe("systemctl", "#!/bin/sh\nexit 0\n")
	writeExe("certbot", "#!/bin/sh\nexit 0\n")

	oldPath := os.Getenv("PATH")
	t.Cleanup(func() { _ = os.Setenv("PATH", oldPath) })
	_ = os.Setenv("PATH", bin+string(os.PathListSeparator)+oldPath)

	rec := &recordingEventStore{}
	bus := rt.NewEventBus(rec)
	ex := rt.NewRuntimeToolExecutor(bus, nil, nil)
	ex.SetConfig(&config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{RotateAfterTurns: 40},
		},
	})

	actor := agentCfg("holding-devops", []string{"nginx_reload", "systemd_control", "certbot_execute"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := ex.Execute(rt.WithActor(ctx, actor), "nginx_reload", map[string]any{}); err != nil {
		t.Fatalf("nginx_reload: %v", err)
	}
	if _, err := ex.Execute(rt.WithActor(ctx, actor), "systemd_control", map[string]any{"action": "restart", "unit": "empireai-orchestrator"}); err != nil {
		t.Fatalf("systemd_control: %v", err)
	}
	if _, err := ex.Execute(rt.WithActor(ctx, actor), "certbot_execute", map[string]any{"domain": "example.com"}); err != nil {
		t.Fatalf("certbot_execute: %v", err)
	}

	// Ensure tool execution events were emitted.
	got := 0
	rec.mu.Lock()
	for _, e := range rec.events {
		if e.Type == "agent.tool_execution" && e.SourceAgent == actor.ID {
			got++
		}
	}
	rec.mu.Unlock()
	if got < 3 {
		t.Fatalf("expected >=3 agent.tool_execution events, got %d", got)
	}
}

func agentCfg(role string, tools []string) models.AgentConfig {
	cfg, _ := json.Marshal(map[string]any{
		"system_prompt": "x",
		"tools":         tools,
	})
	return models.AgentConfig{
		ID:     uuid.NewString(),
		Type:   "stub",
		Role:   role,
		Mode:   "holding",
		Config: cfg,
	}
}
