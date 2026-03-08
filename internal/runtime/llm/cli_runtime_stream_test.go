package llm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	"empireai/internal/runtime/sessions"
)

func TestClaudeCLIRuntime_StreamJSON_WritesMonitorLogAndBuildsResponse(t *testing.T) {
	monitorDir := t.TempDir()
	t.Setenv("EMPIREAI_MONITOR_DIR", monitorDir)

	dir := t.TempDir()
	script := filepath.Join(dir, "claude_stream_stub.sh")
	body := "#!/bin/sh\n" +
		"printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-1\"}'\n" +
		"printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"hello from stream\"}]}}'\n" +
		"printf '%s\\n' '{\"type\":\"result\",\"session_id\":\"sess-1\",\"result\":\"done\"}'\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "stream-json",
				Retries:      1,
			},
		},
	}

	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", &turnCapture{}, nil, nil, nil)
	s, err := rt.StartSession(context.Background(), "agent-stream", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "agent-stream", Role: "analysis-agent", Mode: "factory"})
	resp, err := rt.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if strings.TrimSpace(resp.Message.Content) != "hello from stream" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if strings.TrimSpace(resp.SessionID) != "sess-1" {
		t.Fatalf("unexpected session id: %#v", resp)
	}

	logPath := MonitorLogPath(monitorDir, "agent-stream")
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read monitor log: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "turn.start") || !strings.Contains(text, "assistant: hello from stream") || !strings.Contains(text, "turn.end ok=true") {
		t.Fatalf("unexpected monitor log:\n%s", text)
	}
}
