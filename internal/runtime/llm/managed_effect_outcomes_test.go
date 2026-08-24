package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type effectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
	adapter string
}

type failingMonitorSink struct{ err error }

type noInvocationRoundTripper struct{ calls *atomic.Int32 }

func (s failingMonitorSink) OpenTurn(context.Context, MonitorTurnMeta) (MonitorTurnWriter, error) {
	return nil, s.err
}

func (r noInvocationRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, errors.New("provider primitive must remain unreachable")
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
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
		{
			name: "openai_compatible", adapter: "openai_compatible",
			send: func(ctx context.Context, client *http.Client) (*completionDispatch, error) {
				runtime := &OpenAICompatibleRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
		{
			name: "openai_responses", adapter: "openai_responses",
			send: func(ctx context.Context, client *http.Client) (*completionDispatch, error) {
				runtime := &OpenAIResponsesRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := effecttest.New()
			client := &http.Client{Transport: effectRoundTripper{t: t, harness: harness, adapter: tt.adapter}}
			ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, "provider-"+tt.name))
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
			if _, err := tt.send(managedEffectHarnessContext(t, stale, "provider-stale-"+tt.name), staleClient); err == nil {
				t.Fatal("stale provider effect was admitted")
			}
			if _, launched := stale.StateForAdapter(tt.adapter); launched {
				t.Fatal("stale provider effect reached its primitive")
			}
		})
	}
}

func TestManagedProviderLaunchBoundaryFailureSettlesAttemptOnly(t *testing.T) {
	type transportCase struct {
		name    string
		backend string
		invoke  func(context.Context, llmselection.Profile, llmselection.ResolvedModel, *atomic.Int32) (*completionDispatch, error)
	}
	transports := []transportCase{
		{
			name: "anthropic", backend: llmselection.BackendAnthropic,
			invoke: func(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, calls *atomic.Int32) (*completionDispatch, error) {
				runtime := &AnthropicAPIRuntime{httpClient: &http.Client{Transport: noInvocationRoundTripper{calls: calls}}, apiURL: "http://unreachable.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendAdmittedRequest(ctx, profile, model, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
		{
			name: "openai_compatible", backend: llmselection.BackendOpenAICompatible,
			invoke: func(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, calls *atomic.Int32) (*completionDispatch, error) {
				runtime := &OpenAICompatibleRuntime{httpClient: &http.Client{Transport: noInvocationRoundTripper{calls: calls}}, baseURL: "http://unreachable.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendAdmittedRequest(ctx, profile, model, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
		{
			name: "openai_responses", backend: llmselection.BackendOpenAIResponses,
			invoke: func(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, calls *atomic.Int32) (*completionDispatch, error) {
				runtime := &OpenAIResponsesRuntime{httpClient: &http.Client{Transport: noInvocationRoundTripper{calls: calls}}, baseURL: "http://unreachable.test", apiKey: "test"}
				_, _, dispatch, err := runtime.sendAdmittedRequest(ctx, profile, model, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return dispatch, err
			},
		},
		{
			name: "claude_cli", backend: llmselection.BackendClaudeCLI,
			invoke: func(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, calls *atomic.Int32) (*completionDispatch, error) {
				dir := t.TempDir()
				marker := dir + "/provider-invoked"
				docker := dir + "/docker"
				if err := os.WriteFile(docker, []byte("#!/bin/sh\n: >"+marker+"\nexit 97\n"), 0o755); err != nil {
					t.Fatalf("write Claude invocation marker: %v", err)
				}
				cfg := &config.Config{}
				cfg.Workspace.DockerBin = docker
				cfg.LLM.ClaudeCLI.Command = "claude"
				cfg.LLM.ClaudeCLI.OutputFormat = "json"
				runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "effect-test", nil, nil, nil)
				runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
				attempt, err := beginManagedTestCompletion(t, ctx, claudeCLICompletionAdapter, []byte("request"))
				if err != nil {
					return nil, err
				}
				dispatch := newCompletionDispatch(attempt, "")
				dispatch.providerModel = model
				_, err = runtime.runWithPreparedInput(
					ctx, nil, &workspace.Target{Backend: workspace.BackendDocker, Container: "effect-test", Workdir: "/workspace"},
					"request", MonitorTurnMeta{}, dispatch, profile, model,
				)
				if _, statErr := os.Stat(marker); statErr == nil {
					calls.Add(1)
				} else if !os.IsNotExist(statErr) {
					t.Fatalf("inspect Claude invocation marker: %v", statErr)
				}
				return dispatch, err
			},
		},
		{
			name: "mock", backend: llmselection.BackendMock,
			invoke: func(ctx context.Context, _ llmselection.Profile, model llmselection.ResolvedModel, calls *atomic.Int32) (*completionDispatch, error) {
				actor := runtimeactors.AgentConfig{ID: "effect-test-agent", ExecutionMode: runtimeeffects.ExecutionModeMock}
				_, _, _, dispatch, err := executeMockCompletionWithExecutor(
					ctx, actor, nil, []byte(`{"round":1}`), model, false, managedProviderCallForEffectTest(t, ctx),
					func(context.Context, pythonmodule.Request) (pythonmodule.Result, error) {
						calls.Add(1)
						return pythonmodule.Result{}, errors.New("provider primitive must remain unreachable")
					},
				)
				return dispatch, err
			},
		},
	}
	modes := []struct {
		name      string
		configure func(*effecttest.Harness)
	}{
		{
			name: "initial_heartbeat_rejected",
			configure: func(h *effecttest.Harness) {
				h.HeartbeatErr = errors.New("injected initial heartbeat rejection")
			},
		},
		{
			name: "launch_marker_error_before_commit",
			configure: func(h *effecttest.Harness) {
				h.MarkErr = errors.New("injected launch marker rejection")
			},
		},
		{
			name: "launch_marker_commit_then_error",
			configure: func(h *effecttest.Harness) {
				h.MarkErr = errors.New("injected launch marker acknowledgement loss")
				h.MarkCommitThenErr = true
			},
		},
	}

	for _, transport := range transports {
		for _, mode := range modes {
			t.Run(transport.name+"/"+mode.name, func(t *testing.T) {
				harness := effecttest.New()
				mode.configure(harness)
				ctx := managedEffectHarnessContext(t, harness, "no-invocation-"+transport.name+"-"+mode.name)
				if transport.backend == llmselection.BackendMock {
					ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
				}
				ctx = llmTestWorkContext(t, ctx)
				profile := mustAdmissionProfile(t, transport.backend)
				model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
				var calls atomic.Int32
				dispatch, invokeErr := transport.invoke(ctx, profile, model, &calls)
				if invokeErr == nil {
					t.Fatal("launch-boundary failure returned nil")
				}
				if calls.Load() != 0 {
					t.Fatalf("provider primitive invocations = %d, want 0", calls.Load())
				}
				if dispatch == nil || dispatch.handle == nil || dispatch.invocation != completionProviderInvocationNotStarted {
					t.Fatalf("dispatch = %#v, want exact not-started disposition", dispatch)
				}
				failure := runtimefailures.FromError(invokeErr, "managed-provider-test", "launch_boundary")
				if _, err := settleCompletionTurn(
					ctx, dispatch, dispatch.handle.Attempt().Authority.Target.ID, AgentTurnRecord{}, nil, profile,
					unavailableCompletionUsage(model.ConcreteModel), runtimeeffects.StateTerminalFailure, &failure.Failure,
					map[string]any{"stage": "launch_boundary"},
				); err != nil {
					t.Fatalf("settle no-invocation attempt: %v", err)
				}
				if err := harness.RequireState(dispatch.handle.Attempt().Adapter, runtimeeffects.StateTerminalFailure); err != nil {
					t.Fatal(err)
				}
				if got := harness.CompletionCount(); got != 0 {
					t.Fatalf("turn-bearing settlements = %d, want 0", got)
				}
				if got := harness.ProjectedSpendCount(); got != 0 {
					t.Fatalf("projected spend settlements = %d, want 0", got)
				}
			})
		}
	}
}

func TestManagedClaudeCLIEffectOutcomes(t *testing.T) {
	harness := effecttest.New()
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, "claude-cli-start"))
	attempt, err := beginManagedTestCompletion(t, ctx, "claude_cli", []byte("request"))
	if err != nil {
		t.Fatalf("authorize claude attempt: %v", err)
	}
	runtime := &ClaudeCLIRuntime{}
	cmd := exec.Command("/definitely/missing/swarm-claude-cli")
	dispatch := newCompletionDispatch(attempt, "")
	_, dispatch.providerModel = testClaudeProviderSelection(t)
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		t.Fatalf("start claude attempt heartbeat: %v", err)
	}
	_, runErr := runtime.runStreamingPrepared(heartbeatCtx, cmd, nil, time.Second, "request", MonitorTurnMeta{}, dispatch)
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
	if dispatch.invocation != completionProviderInvocationNotStarted {
		t.Fatalf("Claude start rejection invocation=%d, want not started", dispatch.invocation)
	}
	failure, ok := runtimefailures.As(runErr)
	if !ok || failure.Failure.Detail.Code != "claude_cli_process_start_failed" || !failure.Failure.Retryable {
		t.Fatalf("Claude start rejection failure=%#v, want retryable exact launch rejection", failure)
	}
	settleClaudeTestCompletionFailure(t, harness, ctx, dispatch, runErr)
	if err := harness.RequireState("claude_cli", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}

	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	marker := t.TempDir() + "/started"
	staleCtx := managedEffectHarnessContext(t, stale, "claude-cli-stale")
	if _, err := beginManagedTestCompletion(t, staleCtx, "claude_cli", []byte("request")); err == nil {
		t.Fatal("stale CLI process was admitted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("stale CLI process reached start: %v", err)
	}
}

func TestManagedClaudeCLIStreamingSetupFailureSettlesPrelaunch(t *testing.T) {
	harness := effecttest.New()
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, "claude-cli-monitor-prelaunch"))
	attempt, err := beginManagedTestCompletion(t, ctx, "claude_cli", []byte("request"))
	if err != nil {
		t.Fatalf("authorize claude attempt: %v", err)
	}
	runtime := &ClaudeCLIRuntime{monitor: failingMonitorSink{err: errors.New("injected monitor open failure")}}
	cmd := exec.Command("sh", "-lc", "true")
	dispatch := newCompletionDispatch(attempt, "")
	_, dispatch.providerModel = testClaudeProviderSelection(t)
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		t.Fatalf("start claude attempt heartbeat: %v", err)
	}
	_, runErr := runtime.runStreamingPrepared(heartbeatCtx, cmd, nil, time.Second, "request", MonitorTurnMeta{AgentID: harness.Token.AgentID}, dispatch)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		t.Fatalf("stop claude attempt heartbeat: %v", heartbeatErr)
	}
	if runErr == nil {
		t.Fatal("monitor open failure returned nil")
	}
	if err := harness.RequireState("claude_cli", runtimeeffects.StateAuthorized); err != nil {
		t.Fatalf("low-level completion primitive settled independently: %v", err)
	}
	settleClaudeTestCompletionFailure(t, harness, ctx, dispatch, runErr)
	if err := harness.RequireState("claude_cli", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}
}

func TestManagedClaudeCLIStartedProcessIsTurnEligible(t *testing.T) {
	harness := effecttest.New()
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, "claude-cli-started"))
	attempt, err := beginManagedTestCompletion(t, ctx, "claude_cli", []byte("request"))
	if err != nil {
		t.Fatalf("authorize Claude attempt: %v", err)
	}
	dispatch := newCompletionDispatch(attempt, "")
	_, dispatch.providerModel = testClaudeProviderSelection(t)
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		t.Fatalf("start Claude attempt heartbeat: %v", err)
	}
	_, runErr := (&ClaudeCLIRuntime{}).runStreamingPrepared(
		heartbeatCtx, exec.Command("sh", "-lc", "exit 23"), nil, time.Second, "request", MonitorTurnMeta{}, dispatch,
	)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		t.Fatalf("stop Claude attempt heartbeat: %v", heartbeatErr)
	}
	if runErr == nil {
		t.Fatal("failing started Claude process returned nil")
	}
	if dispatch.invocation != completionProviderInvocationStarted {
		t.Fatalf("Claude process invocation=%d, want started", dispatch.invocation)
	}
	settleClaudeTestCompletionFailure(t, harness, ctx, dispatch, runErr)
	if err := harness.RequireState("claude_cli", runtimeeffects.StateOutcomeUncertain); err != nil {
		t.Fatal(err)
	}
	if got := harness.CompletionCount(); got != 1 {
		t.Fatalf("turn-bearing settlements = %d, want 1", got)
	}
	if got := harness.ProjectedSpendCount(); got != 1 {
		t.Fatalf("projected spend settlements = %d, want 1", got)
	}
}

func settleEffectTestCompletionFailure(t *testing.T, ctx context.Context, dispatch *completionDispatch, cause error, state runtimeeffects.State) {
	t.Helper()
	if dispatch == nil || dispatch.handle == nil {
		t.Fatal("completion dispatch is missing")
	}
	failure := runtimefailures.FromError(cause, "effect-test", "settle_completion")
	target := dispatch.handle.Attempt().Authority.Target
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("managed capability surface is missing from completion context")
	}
	surfaceJSON, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed capability surface: %v", err)
	}
	_, err = dispatch.handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: state, Failure: &failure.Failure},
		Usage:      runtimeeffects.CompletionUsage{ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageUnavailable},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Identity: agentmemory.Identity{RunID: target.RunID, Agent: target.AgentIdentity},
			Memory:   target.Memory, FlowInstance: target.FlowInstance, CapabilitySurfaceID: surface.ID,
			CapabilitySurface: surfaceJSON, Failure: &failure.Failure,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, AgentIdentity: target.AgentIdentity, Model: "test-model",
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
	if _, _, _, err := runtime.runWorkspaceCommand(harness.CompletionContext("claude-relay"), target, "payload", "sh", "-lc", "true"); err == nil {
		t.Fatal("missing relay process returned nil")
	}
	if err := harness.RequireState("claude_tool_result_relay", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}

	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	if _, _, _, err := runtime.runWorkspaceCommand(stale.CompletionContext("claude-relay-stale"), target, "payload", "sh", "-lc", "true"); err == nil {
		t.Fatal("stale relay process was admitted")
	}
	if _, launched := stale.StateForAdapter("claude_tool_result_relay"); launched {
		t.Fatal("stale relay reached process start")
	}
}
