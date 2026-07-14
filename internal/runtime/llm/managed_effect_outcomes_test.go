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
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type effectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
	adapter string
}

type failingMonitorSink struct{ err error }

func (s failingMonitorSink) OpenTurn(context.Context, MonitorTurnMeta) (MonitorTurnWriter, error) {
	return nil, s.err
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
		send    func(context.Context, *http.Client) (*completionDispatch, error)
	}{
		{
			name: "anthropic_api", adapter: "anthropic_api",
			send: func(ctx context.Context, client *http.Client) (*completionDispatch, error) {
				runtime := &AnthropicAPIRuntime{httpClient: client, apiURL: "http://effect.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return dispatch, err
			},
		},
		{
			name: "openai_compatible", adapter: "openai_compatible",
			send: func(ctx context.Context, client *http.Client) (*completionDispatch, error) {
				runtime := &OpenAICompatibleRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return dispatch, err
			},
		},
		{
			name: "openai_responses", adapter: "openai_responses",
			send: func(ctx context.Context, client *http.Client) (*completionDispatch, error) {
				runtime := &OpenAIResponsesRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`))
				return dispatch, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := effecttest.New()
			client := &http.Client{Transport: effectRoundTripper{t: t, harness: harness, adapter: tt.adapter}}
			ctx := harness.CompletionContext("provider-" + tt.name)
			dispatch, err := tt.send(ctx, client)
			if err == nil {
				t.Fatal("provider transport failure returned nil")
			}
			if got := harness.HeartbeatsForAdapter(tt.adapter); got == 0 {
				t.Fatalf("provider adapter %s did not heartbeat its completion attempt", tt.adapter)
			}
			settleEffectTestCompletionFailure(t, ctx, dispatch, err, runtimeeffects.StateOutcomeUncertain)
			if err := harness.RequireState(tt.adapter, runtimeeffects.StateOutcomeUncertain); err != nil {
				t.Fatal(err)
			}

			stale := effecttest.New()
			stale.AuthorizeErr = errors.New("superseded generation")
			staleClient := &http.Client{Transport: effectRoundTripper{t: t, harness: stale, adapter: tt.adapter}}
			if _, err := tt.send(stale.CompletionContext("provider-stale-"+tt.name), staleClient); err == nil {
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
	ctx := harness.CompletionContext("claude-cli-start")
	attempt, err := runtimeeffects.BeginCompletion(ctx, "claude_cli", []byte("request"), nil)
	if err != nil {
		t.Fatalf("authorize claude attempt: %v", err)
	}
	runtime := &ClaudeCLIRuntime{}
	cmd := exec.Command("/definitely/missing/swarm-claude-cli")
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		t.Fatalf("start claude attempt heartbeat: %v", err)
	}
	_, runErr := runtime.runStreamingPrepared(heartbeatCtx, cmd, nil, time.Second, "request", MonitorTurnMeta{}, attempt)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		t.Fatalf("stop claude attempt heartbeat: %v", heartbeatErr)
	}
	if runErr == nil {
		t.Fatal("missing CLI process returned nil")
	}
	if got := harness.HeartbeatsForAdapter("claude_cli"); got == 0 {
		t.Fatal("claude CLI did not heartbeat its completion attempt")
	}
	if err := harness.RequireState("claude_cli", runtimeeffects.StateLaunched); err != nil {
		t.Fatalf("low-level completion primitive settled independently: %v", err)
	}
	settleEffectTestCompletionFailure(t, ctx, &completionDispatch{handle: attempt}, runErr, claudeCompletionFailureState(runErr))
	if err := harness.RequireState("claude_cli", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}

	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	marker := t.TempDir() + "/started"
	if _, err := runtimeeffects.BeginCompletion(stale.CompletionContext("claude-cli-stale"), "claude_cli", []byte("request"), nil); err == nil {
		t.Fatal("stale CLI process was admitted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("stale CLI process reached start: %v", err)
	}
}

func TestManagedClaudeCLIStreamingSetupFailureSettlesPrelaunch(t *testing.T) {
	harness := effecttest.New()
	ctx := harness.CompletionContext("claude-cli-monitor-prelaunch")
	attempt, err := runtimeeffects.BeginCompletion(ctx, "claude_cli", []byte("request"), nil)
	if err != nil {
		t.Fatalf("authorize claude attempt: %v", err)
	}
	runtime := &ClaudeCLIRuntime{monitor: failingMonitorSink{err: errors.New("injected monitor open failure")}}
	cmd := exec.Command("sh", "-lc", "true")
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		t.Fatalf("start claude attempt heartbeat: %v", err)
	}
	_, runErr := runtime.runStreamingPrepared(heartbeatCtx, cmd, nil, time.Second, "request", MonitorTurnMeta{AgentID: harness.Token.AgentID}, attempt)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		t.Fatalf("stop claude attempt heartbeat: %v", heartbeatErr)
	}
	if runErr == nil {
		t.Fatal("monitor open failure returned nil")
	}
	if err := harness.RequireState("claude_cli", runtimeeffects.StateAuthorized); err != nil {
		t.Fatalf("low-level completion primitive settled independently: %v", err)
	}
	settleEffectTestCompletionFailure(t, ctx, &completionDispatch{handle: attempt}, runErr, runtimeeffects.StateTerminalFailure)
	if err := harness.RequireState("claude_cli", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}
}

func settleEffectTestCompletionFailure(t *testing.T, ctx context.Context, dispatch *completionDispatch, cause error, state runtimeeffects.State) {
	t.Helper()
	if dispatch == nil || dispatch.handle == nil {
		t.Fatal("completion dispatch is missing")
	}
	failure := runtimefailures.FromError(cause, "effect-test", "settle_completion")
	target := dispatch.handle.Attempt().Authority.Target
	err := dispatch.handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: state, Failure: &failure.Failure},
		Usage:      runtimeeffects.CompletionUsage{ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageUnavailable},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Memory: target.Memory, FlowInstance: target.FlowInstance, Failure: &failure.Failure,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: "global", AgentID: target.AgentID, Model: "test-model",
			BackendProfile: "test", Provider: "test", Transport: "test",
			ResolvedModel: "test-model", InvocationType: "test",
		},
		Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("settle outer completion: %v", err)
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
