package llm

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type effectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
	adapter string
}

func (r effectRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Helper()
	if err := r.harness.RequireState(r.adapter, runtimeeffects.StateLaunched); err != nil {
		r.t.Fatal(err)
	}
	return nil, errors.New("injected transport failure")
}

func TestManagedProviderEffectOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		adapter string
		send    func(context.Context, *http.Client) error
	}{
		{
			name: "anthropic_api", adapter: "anthropic_api",
			send: func(ctx context.Context, client *http.Client) error {
				runtime := &AnthropicAPIRuntime{httpClient: client, apiURL: "http://effect.test", apiKey: "test"}
				_, _, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return err
			},
		},
		{
			name: "openai_compatible", adapter: "openai_compatible",
			send: func(ctx context.Context, client *http.Client) error {
				runtime := &OpenAICompatibleRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return err
			},
		},
		{
			name: "openai_responses", adapter: "openai_responses",
			send: func(ctx context.Context, client *http.Client) error {
				runtime := &OpenAIResponsesRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := effecttest.New()
			client := &http.Client{Transport: effectRoundTripper{t: t, harness: harness, adapter: tt.adapter}}
			if err := tt.send(harness.Context("provider-"+tt.name), client); err == nil {
				t.Fatal("provider transport failure returned nil")
			}
			if err := harness.RequireState(tt.adapter, runtimeeffects.StateOutcomeUncertain); err != nil {
				t.Fatal(err)
			}

			stale := effecttest.New()
			stale.AuthorizeErr = errors.New("superseded generation")
			staleClient := &http.Client{Transport: effectRoundTripper{t: t, harness: stale, adapter: tt.adapter}}
			if err := tt.send(stale.Context("provider-stale-"+tt.name), staleClient); err == nil {
				t.Fatal("stale provider effect was admitted")
			}
			if _, launched := stale.StateForAdapter(tt.adapter); launched {
				t.Fatal("stale provider effect reached its primitive")
			}
		})
	}
}

func TestManagedClaudeCLIEffectOutcomes(t *testing.T) {
	harness := effecttest.New()
	runtime := &ClaudeCLIRuntime{}
	cmd := exec.Command("/definitely/missing/swarm-claude-cli")
	if _, err := runtime.runStreaming(harness.Context("claude-cli-start"), cmd, nil, time.Second, "request", MonitorTurnMeta{}); err == nil {
		t.Fatal("missing CLI process returned nil")
	}
	if err := harness.RequireState("claude_cli", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}

	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	marker := t.TempDir() + "/started"
	if _, err := runtime.runStreaming(stale.Context("claude-cli-stale"), exec.Command("sh", "-lc", "touch "+marker), nil, time.Second, "request", MonitorTurnMeta{}); err == nil {
		t.Fatal("stale CLI process was admitted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("stale CLI process reached start: %v", err)
	}
}

func TestManagedRelayEffectOutcomes(t *testing.T) {
	harness := effecttest.New()
	runtime := &ClaudeCLIRuntime{cfg: &config.Config{Workspace: config.WorkspaceConfig{DockerBin: "/definitely/missing/swarm-docker"}}}
	target := &workspace.Target{Backend: workspace.BackendDocker, Container: "effect-container", Workdir: workspace.LogicalWorkspaceMount}
	if _, _, _, err := runtime.runWorkspaceCommand(harness.Context("claude-relay"), target, "payload", "sh", "-lc", "true"); err == nil {
		t.Fatal("missing relay process returned nil")
	}
	if err := harness.RequireState("claude_tool_result_relay", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}

	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	if _, _, _, err := runtime.runWorkspaceCommand(stale.Context("claude-relay-stale"), target, "payload", "sh", "-lc", "true"); err == nil {
		t.Fatal("stale relay process was admitted")
	}
	if _, launched := stale.StateForAdapter("claude_tool_result_relay"); launched {
		t.Fatal("stale relay reached process start")
	}
}
