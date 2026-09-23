package llm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

type committedPhaseEffectStore struct {
	*effecttest.Harness
	launchFault      error
	observationFault error
	heartbeatFault   error
	heartbeatMode    string
	heartbeats       atomic.Int32
	afterLaunch      func()
}

func (s *committedPhaseEffectStore) HeartbeatCompletionAttempt(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time, lease time.Duration) error {
	if s.heartbeatMode == "unacknowledged" {
		return s.heartbeatFault
	}
	if err := s.Harness.HeartbeatCompletionAttempt(ctx, attempt, at, lease); err != nil {
		return err
	}
	s.heartbeats.Add(1)
	if s.heartbeatMode == "foreign" {
		attempt.AttemptID = "foreign-attempt"
	}
	err := runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationHeartbeat, attempt, s.heartbeatFault)
	if s.heartbeatMode == "joined" {
		return errors.Join(err, context.Canceled)
	}
	return err
}

func (s *committedPhaseEffectStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) error {
	if err := s.Harness.MarkExternalAttemptLaunched(ctx, attempt, at); err != nil {
		return err
	}
	if s.afterLaunch != nil {
		s.afterLaunch()
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationLaunch, attempt, s.launchFault)
}

func (s *committedPhaseEffectStore) MarkExternalAttemptResponseObserved(ctx context.Context, attempt runtimeeffects.Attempt, evidence map[string]any, at time.Time) error {
	if err := s.Harness.MarkExternalAttemptResponseObserved(ctx, attempt, evidence, at); err != nil {
		return err
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationObservation, attempt, s.observationFault)
}

type committedPhaseResponseTransport struct {
	calls *atomic.Int32
	body  string
}

func (r committedPhaseResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(r.body)), Header: make(http.Header)}, nil
}

func TestHTTPCompletionCommittedPhasesPreserveDispatchAndResponse(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
		send func(context.Context, *http.Client) ([]byte, *completionDispatch, error)
	}{
		{
			name: "anthropic_api",
			body: `{"model":"test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`,
			send: func(ctx context.Context, client *http.Client) ([]byte, *completionDispatch, error) {
				runtime := &AnthropicAPIRuntime{httpClient: client, apiURL: "http://effect.test", apiKey: "test"}
				raw, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return raw, dispatch, err
			},
		},
		{
			name: "openai_compatible",
			body: `{"model":"test","choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`,
			send: func(ctx context.Context, client *http.Client) ([]byte, *completionDispatch, error) {
				runtime := &OpenAICompatibleRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				raw, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return raw, dispatch, err
			},
		},
		{
			name: "openai_responses",
			body: `{"id":"resp_1","model":"test","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`,
			send: func(ctx context.Context, client *http.Client) ([]byte, *completionDispatch, error) {
				runtime := &OpenAIResponsesRuntime{httpClient: client, baseURL: "http://effect.test", apiKey: "test"}
				raw, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
				return raw, dispatch, err
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			base := effecttest.New()
			launchFault := errors.New("launch cleanup failed after commit")
			observationFault := errors.New("observation cleanup failed after commit")
			store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault, observationFault: observationFault}
			ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
			ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
			var calls atomic.Int32
			raw, dispatch, err := testCase.send(ctx, &http.Client{Transport: committedPhaseResponseTransport{calls: &calls, body: testCase.body}})
			if err != nil || calls.Load() != 1 || string(raw) != testCase.body || dispatch == nil || dispatch.state != runtimeeffects.StateSettled || dispatch.invocation != completionProviderInvocationStarted {
				t.Fatalf("provider dispatch = calls:%d raw:%q dispatch:%+v err:%v", calls.Load(), raw, dispatch, err)
			}
			if !errors.Is(dispatch.mutationErr, launchFault) || !errors.Is(dispatch.mutationErr, observationFault) {
				t.Fatalf("committed phase diagnostics = %v", dispatch.mutationErr)
			}
			if err := base.RequireState(testCase.name, runtimeeffects.StateResponseObserved); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPCompletionAcknowledgedHeartbeatPreservesProviderResponseAndSettlement(t *testing.T) {
	base := effecttest.New()
	fault := errors.New("heartbeat cleanup failed after renewal commit")
	store := &committedPhaseEffectStore{Harness: base, heartbeatFault: fault}
	ctx := managedEffectHarnessContext(t, base, t.Name())
	authority, ok := runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("completion authority missing")
	}
	ownedTarget := authority.Target
	ownedTarget.EntityID = "77777777-7777-4777-8777-777777777777"
	ctx = llmTestWorkContext(t, runtimeeffects.WithUsageTarget(ctx, ownedTarget))
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, store, base))
	var calls atomic.Int32
	runtime := &OpenAICompatibleRuntime{
		httpClient: &http.Client{Transport: committedPhaseResponseTransport{calls: &calls, body: `{"model":"test","choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`}},
		baseURL:    "http://effect.test", apiKey: "test",
	}
	raw, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
	if err != nil || calls.Load() != 1 || len(raw) == 0 || dispatch == nil || dispatch.state != runtimeeffects.StateSettled || dispatch.invocation != completionProviderInvocationStarted {
		t.Fatalf("provider dispatch = calls:%d raw:%q dispatch:%+v err:%v", calls.Load(), raw, dispatch, err)
	}
	if store.heartbeats.Load() < 2 || !errors.Is(dispatch.mutationErr, fault) {
		t.Fatalf("heartbeat commits=%d diagnostics=%v", store.heartbeats.Load(), dispatch.mutationErr)
	}
	profile := mustAdmissionProfile(t, llmselection.BackendOpenAICompatible)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	dispatch.providerModel = model
	dispatch.request = []byte(`{"model":"test"}`)
	target := dispatch.handle.Attempt().Authority.Target
	session := &Session{ID: target.SessionID, AgentID: target.AgentID, Memory: target.Memory, MemoryIdentity: target.AgentIdentity.Normalize()}
	if err := bindCompletionProjection(dispatch, session, Message{Role: "user", Content: "hello"}, nil); err != nil {
		t.Fatal(err)
	}
	response := &Response{Message: Message{Role: "assistant", Content: "done"}, Raw: raw}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("managed capability surface missing")
	}
	turn := AgentTurnRecord{
		AgentID: target.AgentID, Identity: target.AgentIdentity, Memory: target.Memory,
		SessionID: target.SessionID, RunID: target.RunID, EntityID: target.EntityID,
		FlowInstance: target.FlowInstance, CapabilitySurface: &surface,
	}
	result, settleErr := settleCompletionTurn(ctx, dispatch, target.ID, turn, response, profile,
		unavailableCompletionUsage(model.ConcreteModel), runtimeeffects.StateSettled, nil, nil)
	if !result.Committed || !errors.Is(settleErr, fault) || calls.Load() != 1 {
		t.Fatalf("completion settlement = result:%+v calls:%d err:%v", result, calls.Load(), settleErr)
	}
	if err := base.RequireState("openai_compatible", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPCompletionUnacknowledgedOrForeignHeartbeatFailsClosed(t *testing.T) {
	for _, mode := range []string{"unacknowledged", "foreign", "joined"} {
		t.Run(mode, func(t *testing.T) {
			base := effecttest.New()
			fault := errors.New("heartbeat authority unavailable")
			store := &committedPhaseEffectStore{Harness: base, heartbeatFault: fault, heartbeatMode: mode}
			ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
			ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, store, base))
			var calls atomic.Int32
			runtime := &OpenAICompatibleRuntime{
				httpClient: &http.Client{Transport: noInvocationRoundTripper{calls: &calls}},
				baseURL:    "http://effect.test", apiKey: "test",
			}
			_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
			if !errors.Is(err, fault) || calls.Load() != 0 || dispatch == nil || dispatch.invocation != completionProviderInvocationNotStarted || dispatch.mutationErr != nil {
				t.Fatalf("heartbeat refusal = calls:%d dispatch:%+v err:%v", calls.Load(), dispatch, err)
			}
			if err := base.RequireState("openai_compatible", runtimeeffects.StateAuthorized); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompletionAcknowledgedPeriodicHeartbeatKeepsExecutionAuthority(t *testing.T) {
	base := effecttest.New()
	fault := errors.New("periodic heartbeat cleanup failed after renewal commit")
	store := &committedPhaseEffectStore{Harness: base, heartbeatFault: fault}
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, store, base))
	handle, err := beginManagedTestCompletion(t, ctx, "openai_compatible", []byte(`{"model":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeatWithTiming(ctx, handle, time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for store.heartbeats.Load() < 3 {
		select {
		case <-heartbeatCtx.Done():
			t.Fatalf("acknowledged renewal canceled execution: %v", context.Cause(heartbeatCtx))
		case <-deadline:
			t.Fatal("periodic heartbeat did not renew")
		case <-time.After(time.Millisecond):
		}
	}
	dispatch := newCompletionDispatch(handle, runtimeeffects.StateSettled)
	dispatch.markProviderInvocationStarted()
	if err := finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil); err != nil || dispatch.state != runtimeeffects.StateSettled || !errors.Is(dispatch.mutationErr, fault) {
		t.Fatalf("periodic heartbeat finish = state:%s diagnostic:%v err:%v", dispatch.state, dispatch.mutationErr, err)
	}
}

func TestHTTPCompletionCommittedLaunchCanceledBeforeInvocationSettlesNoDispatch(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("launch cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault}
	parent := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	store.afterLaunch = cancel
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	var calls atomic.Int32
	runtime := &OpenAICompatibleRuntime{
		httpClient: &http.Client{Transport: committedPhaseResponseTransport{calls: &calls, body: `{"choices":[]}`}},
		baseURL:    "http://effect.test", apiKey: "test",
	}
	_, _, dispatch, err := runtime.sendRequest(ctx, []byte(`{"model":"test"}`), managedProviderCallForEffectTest(t, ctx))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, launchFault) || calls.Load() != 0 || dispatch == nil || dispatch.invocation != completionProviderInvocationNotStarted {
		t.Fatalf("pre-invocation gate = calls:%d dispatch:%+v err:%v", calls.Load(), dispatch, err)
	}
	profile := mustAdmissionProfile(t, llmselection.BackendOpenAICompatible)
	dispatch.providerModel = mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	_, settleErr := settleCompletionTurn(ctx, dispatch, "", AgentTurnRecord{}, nil, profile, unavailableCompletionUsage("test"), runtimeeffects.StateTerminalFailure, nil, nil)
	if !errors.Is(settleErr, launchFault) {
		t.Fatalf("terminal no-dispatch settlement lost committed diagnostic: %v", settleErr)
	}
	if err := base.RequireState("openai_compatible", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}
}

func TestMockCompletionCommittedPhasesPreserveExecutionAndResponse(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("mock launch cleanup failed after commit")
	observationFault := errors.New("mock observation cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault, observationFault: observationFault}
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	actor := runtimeactors.AgentConfig{ID: "mock-phase-test", ExecutionMode: runtimeeffects.ExecutionModeMock}
	var calls atomic.Int32
	response, raw, _, dispatch, err := executeMockCompletionWithExecutor(
		ctx, actor, nil, []byte(`{"round":1}`), llmselection.ResolvedModel{ConcreteModel: "test-model"}, false, managedProviderCallForEffectTest(t, ctx),
		func(context.Context, pythonmodule.Request) (pythonmodule.Result, error) {
			calls.Add(1)
			return pythonmodule.Result{Output: []byte(`{"text":"done"}`), FuelConsumed: 7}, nil
		},
	)
	if err != nil || calls.Load() != 1 || response == nil || response.Message.Content != "done" || string(raw) != `{"text":"done"}` || dispatch == nil || dispatch.state != runtimeeffects.StateSettled {
		t.Fatalf("mock dispatch = calls:%d response:%+v raw:%q dispatch:%+v err:%v", calls.Load(), response, raw, dispatch, err)
	}
	if !errors.Is(dispatch.mutationErr, launchFault) || !errors.Is(dispatch.mutationErr, observationFault) {
		t.Fatalf("committed mock phase diagnostics = %v", dispatch.mutationErr)
	}
	if err := base.RequireState("mock_python", runtimeeffects.StateResponseObserved); err != nil {
		t.Fatal(err)
	}
}

func TestClaudeCLICommittedPhasesPreserveProcessAndResponse(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("cli launch cleanup failed after commit")
	observationFault := errors.New("cli observation cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault, observationFault: observationFault}
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	profile, model := testClaudeProviderSelection(t)
	handle, err := beginManagedTestCompletion(t, ctx, "claude_cli", []byte("request"))
	if err != nil {
		t.Fatal(err)
	}
	dispatch := newCompletionDispatch(handle, "")
	dispatch.providerModel = model

	dir := t.TempDir()
	marker := filepath.Join(dir, "invoked")
	script := filepath.Join(dir, "fake-docker.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nset -eu\ncat >/dev/null\nprintf invoked >'"+marker+"'\nprintf '{\"result\":\"done\"}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Workspace.DockerBin = script
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.OutputFormat = "json"
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "phase-test", nil, nil, nil)
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	response, err := runtime.runWithPreparedInput(ctx, nil, &workspace.Target{Backend: workspace.BackendDocker, Container: "phase-test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "request", MonitorTurnMeta{}, dispatch, profile, model)
	if err != nil || response == nil || response.Message.Content != "done" || string(response.Raw) != `{"result":"done"}` {
		t.Fatalf("cli response = %+v, err=%v", response, err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("committed launch did not execute process: %v", err)
	}
	if !errors.Is(dispatch.mutationErr, launchFault) || !errors.Is(dispatch.mutationErr, observationFault) || dispatch.invocation != completionProviderInvocationStarted {
		t.Fatalf("cli dispatch = %+v", dispatch)
	}
	if err := base.RequireState("claude_cli", runtimeeffects.StateResponseObserved); err != nil {
		t.Fatal(err)
	}
}

func TestToolResultRelayCommittedLaunchExecutesOnce(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("relay launch cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault}
	ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "relay-phase-test", nil, nil, nil)
	var calls atomic.Int32
	runtime.execWorkspaceFn = func(_ context.Context, _ *workspace.Target, stdin string, args ...string) ([]byte, []byte, int, error) {
		calls.Add(1)
		if stdin != "exact relay" || len(args) != 1 || args[0] != "write" {
			t.Fatalf("relay primitive = stdin:%q args:%q", stdin, args)
		}
		return []byte("written"), nil, 0, nil
	}
	stdout, _, code, err := runtime.runWorkspaceCommand(ctx, &workspace.Target{Backend: workspace.BackendDocker, Container: "relay-test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "exact relay", "write")
	if err != nil || calls.Load() != 1 || string(stdout) != "written" || code != 0 {
		t.Fatalf("relay = calls:%d stdout:%q code:%d err:%v", calls.Load(), stdout, code, err)
	}
	if err := base.RequireState("claude_tool_result_relay", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
}

func TestToolResultRelayCommittedLaunchCanceledBeforeDispatch(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("relay launch cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault}
	parent := llmTestWorkContext(t, managedEffectHarnessContext(t, base, t.Name()))
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	store.afterLaunch = cancel
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "relay-phase-test", nil, nil, nil)
	var calls atomic.Int32
	runtime.execWorkspaceFn = func(context.Context, *workspace.Target, string, ...string) ([]byte, []byte, int, error) {
		calls.Add(1)
		return nil, nil, 0, nil
	}
	_, _, _, err := runtime.runWorkspaceCommand(ctx, &workspace.Target{Backend: workspace.BackendDocker, Container: "relay-test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "exact relay", "write")
	if !errors.Is(err, context.Canceled) || !errors.Is(err, launchFault) || calls.Load() != 0 {
		t.Fatalf("relay pre-dispatch gate = calls:%d err:%v", calls.Load(), err)
	}
	if err := base.RequireState("claude_tool_result_relay", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}
}

func TestStartupProbeCommittedLaunchCanceledBeforeProcessDispatch(t *testing.T) {
	base := effecttest.New()
	launchFault := errors.New("startup launch cleanup failed after commit")
	store := &committedPhaseEffectStore{Harness: base, launchFault: launchFault}
	actor := runtimeactors.AgentConfig{ID: "startup-phase-test", ExecutionMode: "live"}
	actor.Identity = testAgentIdentity(actor.ID, "")
	parent, surface := testManagedCLISurfaceContext(t, actor, nil)
	parent = base.StartupProbeContext(parent, surface)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	store.afterLaunch = cancel
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(store, base, base, base))
	handle, err := runtimeeffects.BeginStartupProbe(ctx, "claude_cli_startup_probe", []byte("request"), nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "invoked")
	script := filepath.Join(dir, "fake-docker.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf invoked >'"+marker+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Workspace.DockerBin = script
	cfg.LLM.ClaudeCLI.Command = "claude"
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(0), "startup-phase-test", nil, nil, nil)
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	_, err = runtime.runUntilCLIStartupInit(ctx, nil, &workspace.Target{Backend: workspace.BackendDocker, Container: "startup-test", Workdir: "/workspace", ClaudeState: claudeStateStub{}}, "request", handle)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, launchFault) {
		t.Fatalf("startup pre-dispatch gate = %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("canceled startup probe reached process: %v", err)
	}
	if err := base.RequireState("claude_cli_startup_probe", runtimeeffects.StateTerminalFailure); err != nil {
		t.Fatal(err)
	}
}
