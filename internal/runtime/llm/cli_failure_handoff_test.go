package llm

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestClaudeStartedTimeoutIsTerminal(t *testing.T) {
	for _, format := range []string{"json", "stream-json"} {
		t.Run(format, func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), "docker")
			if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{}
			cfg.Workspace.DockerBin = bin
			cfg.LLM.ClaudeCLI.Command = "claude"
			cfg.LLM.ClaudeCLI.OutputFormat = format
			cfg.LLM.ClaudeCLI.Timeout = 100 * time.Millisecond
			runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "worker", nil, nil, nil)
			runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "secret")
			parent := actors.WithActor(unmanagedLLMTestContext(), actors.AgentConfig{ID: "timeout-agent", EntityID: "entity", ExecutionMode: "live"})
			harness, ctx, dispatch := beginClaudeTestCompletion(t, parent, "timeout")
			runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			profile, model := testClaudeProviderSelection(t)
			_, err := runtime.runWithPreparedInput(runCtx, nil, &workspace.Target{Container: "test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "hello", MonitorTurnMeta{}, dispatch, profile, model)
			if dispatch.invocation != completionProviderInvocationStarted || claudeCompletionFailureState(err) != effects.StateOutcomeUncertain || engine.FailureDispositionFor(err) != engine.FailureDispositionTerminal {
				t.Fatalf("started timeout mismatch: invocation=%v err=%v", dispatch.invocation, err)
			}
			failure, ok := failures.As(err)
			if !ok {
				t.Fatal(err)
			}
			cause, ok := failures.As(failure.Unwrap())
			if !ok || cause.Failure.Class != failures.ClassTimeout {
				t.Fatalf("lost timeout cause: %#v", cause)
			}
			settleClaudeTestCompletionFailure(t, harness, ctx, dispatch, err)
		})
	}
}

func TestClaudeCLIUncertainFailureDisposition(t *testing.T) {
	for _, class := range []failures.Class{failures.ClassConnectorFailure, failures.ClassTimeout, failures.ClassDependencyUnavailable, failures.ClassSchemaInvalid} {
		t.Run(string(class), func(t *testing.T) {
			cause := failures.New(class, "original_failure", "claude-cli-adapter", "run", nil)
			failure := &claudeCompletionAttemptFailure{state: effects.StateOutcomeUncertain, err: cause, operation: "wait_streaming", evidence: map[string]any{"stdout": "bounded original reason"}}
			for _, err := range []error{failure, errors.Join(failure, failures.New(failures.ClassDependencyUnavailable, "settlement_failed", "store", "settle", nil))} {
				if got := engine.FailureDispositionFor(err); got != engine.FailureDispositionTerminal {
					t.Fatalf("uncertain disposition = %v", got)
				}
				if claudeCompletionFailureState(err) != effects.StateOutcomeUncertain || !errors.Is(err, cause) {
					t.Fatalf("lost state or original cause: %v", err)
				}
				envelope := failures.FromError(err, "test", "readback").Failure
				if envelope.Class != failures.ClassOutcomeUncertain || envelope.Detail.Attributes["stdout"] != "bounded original reason" {
					t.Fatalf("lost uncertain evidence: %#v", envelope)
				}
				original, ok := envelope.Detail.Attributes["cause_failure"].(failures.Envelope)
				if !ok || original.Class != class || original.Detail.Code != "original_failure" {
					t.Fatalf("lost typed cause: %#v", envelope)
				}
				if _, err := json.Marshal(envelope); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	cause := failures.New(failures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "claude-cli-adapter", "start", nil)
	err := &claudeCompletionAttemptFailure{state: effects.StateTerminalFailure, err: cause, operation: "start"}
	if engine.FailureDispositionFor(err) != engine.FailureDispositionRetry || !errors.Is(err, cause) {
		t.Fatalf("prelaunch policy changed: %v", err)
	}
}

func TestClaudeStructuredErrorSummary(t *testing.T) {
	result := `{"type":"result","is_error":true,"usage":{"noise":"` + strings.Repeat("x", 500) + `"},"errors":["No conversation found with session ID: synthetic-session"],"prompt":"private-prompt"}`
	for _, raw := range []string{result, "{\"type\":\"system\",\"subtype\":\"init\"}\n" + result} {
		if got := summarizeCLIErrorOutput(raw); got != "No conversation found with session ID: synthetic-session" {
			t.Fatalf("lost result explanation: %q", got)
		}
	}
	for _, raw := range []string{
		`{"type":"result","is_error":false,"errors":["private-prompt"],"result":"private-prompt"}`,
		`{"type":"assistant","errors":["private-prompt"]}`,
		`{"type":"result","is_error":true,"errors":[123],"prompt":"private-prompt"}`,
		`{"prompt":"private-prompt"`,
		`["private-prompt"]`,
	} {
		if got := summarizeCLIErrorOutput(raw); strings.Contains(got, "private-prompt") || len(got) > 243 {
			t.Fatalf("unsafe structured output: %q", got)
		}
	}
	secret := strings.Repeat("private-secret", 50)
	for _, raw := range []string{secret + " useful reason", `{"type":"result","is_error":true,"errors":["` + secret + ` useful reason"]}`} {
		if got := summarizeCLIErrorOutput(raw, secret); got != "[REDACTED] useful reason" {
			t.Fatalf("must redact before truncation: %q", got)
		}
	}
	for _, raw := range []string{"", "plain\n error", strings.Repeat("x", 1000), strings.Repeat("\u00e9", 200), "invalid\xff"} {
		if got := summarizeCLIErrorOutput(raw); len(got) > 243 || !utf8.ValidString(got) {
			t.Fatalf("invalid bounded summary: %q", got)
		}
	}
}

func TestClaudeCommandDiagnosticRedaction(t *testing.T) {
	cmd := &exec.Cmd{Args: []string{"docker", "exec", "-e", "CLAUDE_CODE_OAUTH_TOKEN=provider-secret", "container", "claude", "--mcp-config", `{"mcpServers":{"swarm":{"headers":{"Authorization":"Bearer gateway-secret"}}}}`}}
	got := summarizeCLIErrorOutput(`{"type":"result","is_error":true,"errors":["provider-secret gateway-secret"]}`, claudeCommandSecrets(cmd)...)
	if got != "[REDACTED] [REDACTED]" {
		t.Fatalf("credential leak: %s", got)
	}
}
