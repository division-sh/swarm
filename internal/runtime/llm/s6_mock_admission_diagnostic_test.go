package llm

import (
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func s6MockPolicyConfig(policy config.LLMProviderLimitPolicy) *config.Config {
	return &config.Config{LLM: config.LLMConfig{
		Backend:        llmselection.BackendAnthropic,
		Session:        config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 1, RotateOnParseFailures: 1},
		ProviderLimits: map[string]config.LLMProviderLimitPolicy{llmselection.BackendMock: policy},
	}}
}

func TestS6MockConfiguredBucketDiagnostic(t *testing.T) {
	profile := mustAdmissionProfile(t, llmselection.BackendMock)
	model := mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)
	limit := config.LLMProviderLimitPolicy{MaxConcurrency: 1, MaxConcurrencyMaxWait: "0s"}
	for _, tc := range []struct {
		name, key string
		policy    config.LLMProviderLimitPolicy
	}{
		{"profile", "llm_provider:mock:mock:cli:*", limit},
		{"alias", "llm_provider:mock:mock:cli:regular", config.LLMProviderLimitPolicy{Models: map[string]config.LLMProviderLimitPolicy{model.ModelAlias: limit}}},
		{"concrete", "llm_provider:mock:mock:cli:" + model.ConcreteModel, config.LLMProviderLimitPolicy{Models: map[string]config.LLMProviderLimitPolicy{model.ConcreteModel: limit}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := s6MockPolicyConfig(tc.policy)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("production mock config refused: %v", err)
			}
			registry := NewProviderAdmissionRegistry(cfg)
			policy, found, err := registry.policy(profile, model)
			if err != nil || !found || policy.ConcurrencyKey != tc.key || policy.Concurrency.Limit != 1 {
				t.Fatalf("mock bucket = %+v found=%t error=%v", policy, found, err)
			}
			release, err := registry.Admit(unmanagedLLMTestContext(), profile, model)
			if err != nil {
				t.Fatal(err)
			}
			_, refused := registry.Admit(unmanagedLLMTestContext(), profile, model)
			release()
			requireProviderAdmissionRateLimited(t, refused)
			release, err = registry.Admit(unmanagedLLMTestContext(), profile, model)
			if err != nil {
				t.Fatalf("release did not restore exact bucket: %v", err)
			}
			release()
		})
	}
	registry := NewProviderAdmissionRegistry(&config.Config{})
	var releases []func()
	for i := 0; i < 3; i++ {
		release, err := registry.Admit(unmanagedLLMTestContext(), profile, model)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
	if len(registry.controller.concurrency) != 0 {
		t.Fatal("absent mock policy created a numeric concurrency bucket")
	}
}

func TestS6MockCapturedLiveModelBucketDiagnostic(t *testing.T) {
	live := mustAdmissionProfile(t, llmselection.BackendClaudeCLI)
	actor := resolvedMockAgent("s6-captured-model")
	actor.LLMBackend, actor.Model = "", llmselection.ModelAliasRegular
	models := llmselection.ModelAliases{llmselection.ModelAliasRegular: {llmselection.BackendClaudeCLI: "s6-configured-live-model"}}
	resolved, err := ResolveAgentExecution(executionposture.MockOnly, live, models, actor)
	if err != nil {
		t.Fatal(err)
	}
	profile := mustAdmissionProfile(t, resolved.Actor.ResolvedLLMBackend)
	model := llmselection.ResolvedModel{ModelAlias: resolved.Actor.Model, ConcreteModel: resolved.Actor.ResolvedModel}
	cfg := s6MockPolicyConfig(config.LLMProviderLimitPolicy{Models: map[string]config.LLMProviderLimitPolicy{
		"s6-configured-live-model": {MaxConcurrency: 1, MaxConcurrencyMaxWait: "0s"},
	}})
	cfg.LLM.Backend, cfg.LLM.Models = live.ID, models
	cfg.LLM.ClaudeCLI.Command, cfg.LLM.ClaudeCLI.OutputFormat = "claude", "json"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	registry := NewProviderAdmissionRegistry(cfg)
	policy, found, err := registry.policy(profile, model)
	if err != nil || !found || policy.ConcurrencyKey != "llm_provider:mock:mock:cli:s6-configured-live-model" {
		t.Fatalf("captured mock/live model bucket: %+v found=%t err=%v", policy, found, err)
	}
	release, err := registry.Admit(unmanagedLLMTestContext(), profile, model)
	if err != nil {
		t.Fatal(err)
	}
	_, refused := registry.Admit(unmanagedLLMTestContext(), profile, model)
	release()
	requireProviderAdmissionRateLimited(t, refused)
	if _, found, err := registry.policy(profile, mustAdmissionModel(t, profile, llmselection.ModelAliasRegular)); err != nil || found {
		t.Fatalf("builtin mock model must not replace captured selection: found=%t err=%v", found, err)
	}
}

// This characterizes the current bypass, not the future DC-8 dispatcher.
func TestS6MockFactoryExplicitCapBypassDiagnostic(t *testing.T) {
	cfg := s6MockPolicyConfig(config.LLMProviderLimitPolicy{MaxConcurrency: 1, MaxConcurrencyMaxWait: "0s", RateLimit: "1/h", RateLimitMaxWait: "0s"})
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	harness := effecttest.New()
	options := mockHostRuntimeOptions(t)
	registry := sessions.NewInMemoryRegistry(time.Minute)
	runtime, err := (RuntimeFactory{
		Cfg: cfg, Sessions: registry, LiveSessions: NewTransientLiveSessionAcquirer(registry), LockOwner: "s6-mock-diagnostic",
		Workspaces: options.Workspaces, CompletionController: liveTestCompletionController(harness, harness, harness, harness),
	}).buildProfile(mustAdmissionProfile(t, llmselection.BackendMock))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("s6-mock-%d", i)
		source := []byte("def handle(input):\n    return {'text': 'done', 'usage': {'input_tokens': 1, 'output_tokens': 1}}\n")
		turnCtx := testManagedConversationContext(t, harness, id, "s6/"+id, "worker")
		actor, _ := runtimeactors.ActorFromContext(turnCtx)
		actor.ExecutionMode = runtimeeffects.ExecutionModeMock
		actor.Mock = mockperformance.Performance{Kind: "python", SourcePath: "mocks/agent.py", Source: source, Digest: pythonSourceDigest(source)}
		turnCtx = runtimeeffects.WithExecutionMode(runtimeactors.WithActor(turnCtx, actor), runtimeeffects.ExecutionModeMock)
		conversation := newTestManagedConversation(t, id, "s6/"+id, "worker", nil, testMemory(), 2, runtime)
		conversation.SetToolExecutor(openAIToolExecutor{})
		response, err := conversation.RunManaged(turnCtx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEventWithMode(id, runtimeeffects.ExecutionModeMock)})
		if err != nil {
			t.Fatalf("actual mock completion %d: %v", i, err)
		}
		if response == nil || response.Message.Content != "done" {
			t.Fatalf("actual mock completion %d: response=%+v", i, response)
		}
	}
	t.Log("validated mock rate_limit=1/h; two real Python completions succeeded; registry saturation covered separately, no dispatcher repair applied")
}
