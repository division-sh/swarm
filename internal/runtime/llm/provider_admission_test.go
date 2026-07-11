package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestProviderAdmissionDefaultsToNoLimitWhenUnconfigured(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	registry := NewProviderAdmissionRegistry(&config.Config{})

	for i := 0; i < 5; i++ {
		release, err := registry.Admit(context.Background(), profile, model)
		if err != nil {
			t.Fatalf("Admit attempt %d: %v", i+1, err)
		}
		release()
	}
}

func TestProviderAdmissionEmptyProfilePolicyDoesNotRequireModel(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	cfg := &config.Config{
		LLM: config.LLMConfig{
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendAnthropic: {},
			},
		},
	}
	registry := NewProviderAdmissionRegistry(cfg)

	if _, err := resolveProviderAdmissionModel(context.Background(), cfg, registry, profile); err != nil {
		t.Fatalf("resolveProviderAdmissionModel: %v", err)
	}
	release, err := registry.Admit(context.Background(), profile, llmselection.ResolvedModel{})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	release()
}

func TestProviderAdmissionRateLimitFailsClosedAtMaxWait(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	registry := NewProviderAdmissionRegistry(&config.Config{
		LLM: config.LLMConfig{
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendAnthropic: {
					RateLimit:        "1/s",
					RateLimitMaxWait: "0s",
				},
			},
		},
	})

	release, err := registry.Admit(context.Background(), profile, model)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	release()

	_, err = registry.Admit(context.Background(), profile, model)
	requireProviderAdmissionRateLimited(t, err)
}

func TestProviderAdmissionConcurrencyLimitFailsClosedAtMaxWait(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	registry := NewProviderAdmissionRegistry(&config.Config{
		LLM: config.LLMConfig{
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendAnthropic: {
					MaxConcurrency:        1,
					MaxConcurrencyMaxWait: "0s",
				},
			},
		},
	})

	release, err := registry.Admit(context.Background(), profile, model)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}

	_, err = registry.Admit(context.Background(), profile, model)
	requireProviderAdmissionRateLimited(t, err)

	release()
	release, err = registry.Admit(context.Background(), profile, model)
	if err != nil {
		t.Fatalf("third Admit after release: %v", err)
	}
	release()
}

func TestProviderAdmissionDoesNotHoldConcurrencyWhileWaitingForRate(t *testing.T) {
	controller := newLLMProviderAdmissionController()
	policy := llmProviderAdmissionPolicy{
		Profile:       mustAdmissionProfile(t, llmselection.BackendAnthropic),
		BucketName:    "anthropic/api/regular",
		RateBucketKey: "rate",
		Rate: config.LLMProviderRateLimit{
			Enabled: true,
			Limit:   1,
			Period:  time.Hour,
			MaxWait: time.Hour,
		},
		ConcurrencyKey: "concurrency",
		Concurrency: config.LLMProviderConcurrencyLimit{
			Enabled: true,
			Limit:   1,
			MaxWait: 0,
		},
	}
	controller.rateBucket(policy.RateBucketKey).scheduled = []time.Time{time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		release, err := controller.Admit(ctx, policy)
		if err == nil {
			release()
		}
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)

	bucket := controller.concurrencyBucket(policy.ConcurrencyKey, policy.Concurrency.Limit)
	if got := len(bucket.tokens); got != 1 {
		t.Fatalf("available concurrency tokens = %d, want rate waiter not to hold token", got)
	}

	cancel()
	if err := <-done; err == nil {
		t.Fatal("Admit error = nil, want context cancellation after test cleanup")
	}
}

func TestProviderAdmissionRollsBackRateReservationWhenConcurrencyRejects(t *testing.T) {
	controller := newLLMProviderAdmissionController()
	policy := llmProviderAdmissionPolicy{
		Profile:       mustAdmissionProfile(t, llmselection.BackendAnthropic),
		BucketName:    "anthropic/api/regular",
		RateBucketKey: "rate",
		Rate: config.LLMProviderRateLimit{
			Enabled: true,
			Limit:   2,
			Period:  time.Hour,
			MaxWait: 0,
		},
		ConcurrencyKey: "concurrency",
		Concurrency: config.LLMProviderConcurrencyLimit{
			Enabled: true,
			Limit:   1,
			MaxWait: 0,
		},
	}

	release, err := controller.Admit(context.Background(), policy)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	_, err = controller.Admit(context.Background(), policy)
	requireProviderAdmissionRateLimited(t, err)

	release()
	release, err = controller.Admit(context.Background(), policy)
	if err != nil {
		t.Fatalf("third Admit after concurrency rejection: %v", err)
	}
	release()
}

func TestProviderAdmissionModelPolicyOverridesProfilePolicy(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	regular := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	cheap := mustAdmissionModel(t, profile, llmselection.ModelAliasCheap)
	registry := NewProviderAdmissionRegistry(&config.Config{
		LLM: config.LLMConfig{
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendAnthropic: {
					RateLimit:        "1/s",
					RateLimitMaxWait: "0s",
					Models: map[string]config.LLMProviderLimitPolicy{
						llmselection.ModelAliasRegular: {
							RateLimit:        "2/s",
							RateLimitMaxWait: "0s",
						},
					},
				},
			},
		},
	})

	for i := 0; i < 2; i++ {
		release, err := registry.Admit(context.Background(), profile, regular)
		if err != nil {
			t.Fatalf("regular Admit %d: %v", i+1, err)
		}
		release()
	}
	_, err := registry.Admit(context.Background(), profile, regular)
	requireProviderAdmissionRateLimited(t, err)

	release, err := registry.Admit(context.Background(), profile, cheap)
	if err != nil {
		t.Fatalf("cheap first Admit: %v", err)
	}
	release()
	_, err = registry.Admit(context.Background(), profile, cheap)
	requireProviderAdmissionRateLimited(t, err)
}

func TestAnthropicProviderAdmissionNeverRedispatchesAmbiguousFailure(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := requests.Add(1)
		w.Header().Set("content-type", "application/json")
		switch n {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"provider busy"}}`))
		default:
			t.Fatalf("unexpected request %d", n)
		}
	}))
	defer server.Close()

	cfg := &config.Config{
		LLM: config.LLMConfig{
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendAnthropic: {
					RateLimit:        "1/40ms",
					RateLimitMaxWait: "500ms",
				},
			},
		},
	}
	runtime := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil)
	runtime.apiURL = server.URL
	runtime.apiKey = "test-key"

	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent-1", Model: llmselection.ModelAliasRegular})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"}); err == nil {
		t.Fatal("ContinueSession succeeded after ambiguous provider status")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want exactly one request with no redispatch", got)
	}
}

func TestOpenAICompatibleProviderAdmissionRejectsBeforeHTTPDispatch(t *testing.T) {
	cfg := openAICompatibleTestConfig("")
	cfg.LLM.ProviderLimits = map[string]config.LLMProviderLimitPolicy{
		llmselection.BackendOpenAICompatible: {
			MaxConcurrency:        1,
			MaxConcurrencyMaxWait: "0s",
		},
	}

	entered := make(chan struct{})
	releaseServer := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(entered)
			<-releaseServer
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer server.Close()
	cfg.LLM.OpenAICompatible.BaseURL = server.URL

	runtime := NewOpenAICompatibleRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil)
	runtime.apiKey = "test-key"
	profile := mustAdmissionProfile(t, llmselection.BackendOpenAICompatible)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	firstErr := make(chan error, 1)
	go func() {
		_, _, err := runtime.sendAdmittedRequest(context.Background(), profile, model, []byte(`{"model":"gpt-compatible","messages":[{"role":"user","content":"hello"}]}`))
		firstErr <- err
	}()
	<-entered

	_, _, err := runtime.sendAdmittedRequest(context.Background(), profile, model, []byte(`{"model":"gpt-compatible","messages":[{"role":"user","content":"second"}]}`))
	requireProviderAdmissionRateLimited(t, err)
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want second request rejected before HTTP dispatch", got)
	}

	close(releaseServer)
	if err := <-firstErr; err != nil {
		t.Fatalf("first request: %v", err)
	}
}

func TestOpenAIResponsesProviderAdmissionRejectsBeforeHTTPDispatch(t *testing.T) {
	cfg := openAIResponsesTestConfig("")
	cfg.LLM.ProviderLimits = map[string]config.LLMProviderLimitPolicy{
		llmselection.BackendOpenAIResponses: {
			MaxConcurrency:        1,
			MaxConcurrencyMaxWait: "0s",
		},
	}

	entered := make(chan struct{})
	releaseServer := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(entered)
			<-releaseServer
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer server.Close()
	cfg.LLM.OpenAIResponses.BaseURL = server.URL

	runtime := NewOpenAIResponsesRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil)
	runtime.apiKey = "test-key"
	profile := mustAdmissionProfile(t, llmselection.BackendOpenAIResponses)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	firstErr := make(chan error, 1)
	go func() {
		_, _, err := runtime.sendAdmittedRequest(context.Background(), profile, model, []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"hello"}]}`))
		firstErr <- err
	}()
	<-entered

	_, _, err := runtime.sendAdmittedRequest(context.Background(), profile, model, []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"second"}]}`))
	requireProviderAdmissionRateLimited(t, err)
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want second request rejected before HTTP dispatch", got)
	}

	close(releaseServer)
	if err := <-firstErr; err != nil {
		t.Fatalf("first request: %v", err)
	}
}

func TestClaudeCLIProviderAdmissionRejectsBeforeSubprocessDispatch(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")
	cfg := &config.Config{
		LLM: config.LLMConfig{
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "claude",
				OutputFormat: "json",
			},
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendClaudeCLI: {
					MaxConcurrency:        1,
					MaxConcurrencyMaxWait: "0s",
				},
			},
		},
	}
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil, nil)
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent-1", Model: llmselection.ModelAliasRegular})
	scriptPath, countFile := writeProviderAdmissionFakeDocker(t, `cat >/dev/null
printf '%s\n' '{"result":"done"}'
`)
	cfg.Workspace.DockerBin = scriptPath
	target := &workspace.Target{Container: "swarm-agent", Workdir: "/workspace"}
	profile := mustAdmissionProfile(t, llmselection.BackendClaudeCLI)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)

	release, err := runtime.providerAdmission.Admit(ctx, profile, model)
	if err != nil {
		t.Fatalf("hold admission slot: %v", err)
	}
	defer release()

	_, err = runtime.runWithInput(ctx, nil, target, "hello", MonitorTurnMeta{})
	requireProviderAdmissionRateLimited(t, err)
	if got := readProviderAdmissionFakeDockerInvocations(t, countFile); got != 0 {
		t.Fatalf("fake docker invocations = %d, want admission rejection before subprocess dispatch", got)
	}
}

func TestClaudeCLIUnstructuredPromptTransportFailureDoesNotRetry(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")
	cfg := &config.Config{
		LLM: config.LLMConfig{
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "claude",
				OutputFormat: "json",
			},
			ProviderLimits: map[string]config.LLMProviderLimitPolicy{
				llmselection.BackendClaudeCLI: {
					RateLimit:        "1/h",
					RateLimitMaxWait: "0s",
				},
			},
		},
	}
	runtime := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil, nil)
	runtime.providerCredentials = testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent-1", Model: llmselection.ModelAliasRegular})
	scriptPath, countFile := writeProviderAdmissionFakeDocker(t, `cat >/dev/null
if [ "$count" = "1" ]; then
  printf '%s\n' 'input must be provided either through stdin or as a prompt argument when using --print' >&2
  exit 1
fi
printf '%s\n' '{"result":"done"}'
`)
	cfg.Workspace.DockerBin = scriptPath
	target := &workspace.Target{Container: "swarm-agent", Workdir: "/workspace"}

	_, fallback, err := runtime.runWithPromptTransportFallback(ctx, []string{"--print"}, target, "hello", MonitorTurnMeta{})
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassConnectorFailure || failure.Failure.Detail.Code != "claude_cli_process_failed" {
		t.Fatalf("failure = %#v, want generic connector failure", failure)
	}
	if fallback.Attempted || fallback.Used {
		t.Fatalf("fallback = %#v, want no prose-triggered retry", fallback)
	}
	if got := readProviderAdmissionFakeDockerInvocations(t, countFile); got != 1 {
		t.Fatalf("fake docker invocations = %d, want one subprocess attempt", got)
	}
}

func mustAdmissionProfile(t *testing.T, backend string) llmselection.Profile {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		t.Fatalf("ResolveActiveBackend(%q): %v", backend, err)
	}
	return profile
}

func mustAdmissionModel(t *testing.T, profile llmselection.Profile, alias string) llmselection.ResolvedModel {
	t.Helper()
	model, err := llmselection.ResolveModel(profile, llmselection.ModelResolution{Model: alias})
	if err != nil {
		t.Fatalf("ResolveModel(%q, %q): %v", profile.ID, alias, err)
	}
	return model
}

func requireProviderAdmissionRateLimited(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want provider admission rate_limited runtime error")
	}
	runtimeErr, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("error = %T %v, want runtime error", err, err)
	}
	if runtimeErr.Failure.Class != runtimefailures.ClassConnectorFailure || runtimeErr.Failure.Detail.Code != llmProviderRateLimitedCode || runtimeErr.Failure.Component != llmProviderAdmissionComponent || runtimeErr.Failure.Operation != llmProviderAdmissionOperation || !runtimeErr.Failure.Retryable {
		t.Fatalf("runtime error = %#v, want provider admission rate_limited retryable error", runtimeErr)
	}
}

func writeProviderAdmissionFakeDocker(t *testing.T, body string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	countFile := filepath.Join(dir, "invocations")
	scriptPath := filepath.Join(dir, "fake-docker.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"count_file=" + strconv.Quote(countFile),
		"count=0",
		"if [ -f \"$count_file\" ]; then count=$(cat \"$count_file\"); fi",
		"count=$((count+1))",
		"printf '%s' \"$count\" >\"$count_file\"",
		body,
	}, "\n") + "\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}
	return scriptPath, countFile
}

func readProviderAdmissionFakeDockerInvocations(t *testing.T, countFile string) int {
	t.Helper()
	raw, err := os.ReadFile(countFile)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("read fake docker invocation count: %v", err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse fake docker invocation count %q: %v", raw, err)
	}
	return count
}
