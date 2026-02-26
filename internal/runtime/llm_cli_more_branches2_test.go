package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

func TestClaudeCLIRuntime_Run_ErrorOutputBranches(t *testing.T) {
	dir := t.TempDir()

	quietFail := filepath.Join(dir, "quiet_fail.sh")
	if err := os.WriteFile(quietFail, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write quiet fail: %v", err)
	}

	noisyFail := filepath.Join(dir, "noisy_fail.sh")
	if err := os.WriteFile(noisyFail, []byte("#!/bin/sh\necho bad 1>&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write noisy fail: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      quietFail,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "claude cli run failed") {
		t.Fatalf("expected run error, got %v", err)
	}
	if strings.Contains(err.Error(), "stderr=") {
		t.Fatalf("expected quiet failure to avoid stderr= wrapper, got %v", err)
	}

	rt.cfg.LLM.ClaudeCLI.Command = noisyFail
	_, err = rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "stderr=bad") {
		t.Fatalf("expected stderr to be included, got %v", err)
	}
}

func TestClaudeCLIRuntime_Run_TimeoutErrorIsExplicit(t *testing.T) {
	dir := t.TempDir()
	sleeper := filepath.Join(dir, "sleepy.sh")
	if err := os.WriteFile(sleeper, []byte("#!/bin/sh\nsleep 2\n"), 0o755); err != nil {
		t.Fatalf("write sleeper: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      sleeper,
				Timeout:      150 * time.Millisecond,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "owner", nil, nil, nil, nil)
	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "timeout after") {
		t.Fatalf("expected explicit timeout error, got: %v", err)
	}
}

func TestSummarizeCLIErrorOutput_Truncates(t *testing.T) {
	raw := strings.Repeat("x", 400)
	out := summarizeCLIErrorOutput(raw)
	if len(out) > 245 {
		t.Fatalf("expected truncation, got len=%d", len(out))
	}
	if !strings.HasSuffix(out, "...") {
		t.Fatalf("expected ..., got %q", out)
	}
}

func TestParseCLIResponse_ToolCallsForms(t *testing.T) {
	resp := parseCLIResponse([]byte(`{
		"result":"ok",
		"session_id":"sess-123",
		"tool_calls":[{"name":"t1","arguments":{"a":1}}, {"name":"","arguments":{"b":2}}],
		"content":[{"type":"tool_use","name":"t2","arguments":{"x":2}}]
	}`))
	if strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("expected ok content, got %q", resp.Message.Content)
	}
	if resp.SessionID != "sess-123" {
		t.Fatalf("expected session id to be parsed, got %q", resp.SessionID)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "t2" && resp.ToolCalls[0].Name != "t1" {
		t.Fatalf("unexpected tool call order: %+v", resp.ToolCalls)
	}

	plain := parseCLIResponse([]byte("hello"))
	if plain.Message.Content != "hello" {
		t.Fatalf("expected plain content passthrough, got %q", plain.Message.Content)
	}
}

func TestClaudeCLIHelpers_ToolNamesAndUnsupportedFlags(t *testing.T) {
	if got := toolNamesCSV([]ToolDefinition{
		{Name: "agent_message"},
		{Name: "sql_execute"},
		{Name: "agent_message"},
		{Name: " "},
	}); got != "agent_message,sql_execute" {
		t.Fatalf("unexpected tool csv: %q", got)
	}
	if !isUnsupportedCLIFlagError(assertErr("unknown option --system-prompt")) {
		t.Fatal("expected unsupported --system-prompt flag detection")
	}
	if !isUnsupportedCLIFlagError(assertErr("unrecognized option '--tools'")) {
		t.Fatal("expected unsupported --tools flag detection")
	}
	if isUnsupportedCLIFlagError(assertErr("unknown option --foo")) {
		t.Fatal("unexpected detection for unrelated flag")
	}
}

func TestClaudeCLIRuntime_EffectiveCLITimeout_FactoryFloorAndEnvOverride(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				Timeout:      30 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	// Factory mode enforces a minimum 300s timeout by default.
	factoryCtx := WithActor(context.Background(), models.AgentConfig{ID: "factory-1", Mode: "factory"})
	if got := rt.effectiveCLITimeout(factoryCtx); got != 300*time.Second {
		t.Fatalf("expected factory timeout floor 300s, got %s", got)
	}

	// Explicit env override should win.
	t.Setenv("EMPIREAI_CLAUDE_TIMEOUT_SECONDS", "45")
	if got := rt.effectiveCLITimeout(factoryCtx); got != 45*time.Second {
		t.Fatalf("expected env timeout override 45s, got %s", got)
	}
}
