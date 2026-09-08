package llm

import (
	"reflect"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestCommandSelectionSeparatesSourceDoubleFromLiveDescriptor(t *testing.T) {
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	source := resolvedMockAgent("declared")
	source.Model = llmselection.ModelAliasRegular
	before := source
	for _, purpose := range []executionposture.Posture{executionposture.Live, executionposture.MockOnly} {
		t.Run(string(purpose), func(t *testing.T) {
			got, err := ResolveAgentExecution(purpose, profile, llmselection.EffectiveModelAliases(nil), source)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(source, before) {
				t.Fatal("selection mutated authored source")
			}
			if got.Actor.ExecutionMode != purpose.RootMode() {
				t.Fatalf("mode = %s", got.Actor.ExecutionMode)
			}
			if purpose == executionposture.Live && got.Actor.Mock.Configured() {
				t.Fatal("live descriptor retained executable double")
			}
			if purpose == executionposture.MockOnly && !reflect.DeepEqual(got.Actor.Mock, source.Mock) {
				t.Fatal("mock descriptor lost exact performance")
			}
			if got.Actor.Model != source.Model || got.Actor.ResolvedModel == "" {
				t.Fatal("live model metadata was lost")
			}
			if _, err := ValidateAgentExecutionDescriptor(profile, got.Actor); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDescriptorReadPreservesWriteTimeModel(t *testing.T) {
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	actor := resolvedClaudeAgent("stored")
	actor.Model = "alias-removed-after-write"
	actor.ResolvedModel = "exact-model-at-write"
	runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{}, &observedClaudeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtimes.ResolveAgentRuntime(actor)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Actor, actor) {
		t.Fatalf("read rewrote descriptor: %#v", got.Actor)
	}
}

func TestDescriptorReadRejectsCorruptionWithoutRepair(t *testing.T) {
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupt := range map[string]func(*models.AgentConfig){
		"missing backend":          func(a *models.AgentConfig) { a.ResolvedLLMBackend = "" },
		"missing resolved model":   func(a *models.AgentConfig) { a.Model = "regular"; a.ResolvedModel = "" },
		"wrong provider":           func(a *models.AgentConfig) { a.ResolvedLLMProvider = "anthropic" },
		"wrong transport":          func(a *models.AgentConfig) { a.ResolvedLLMTransport = "api" },
		"wrong causal mode":        func(a *models.AgentConfig) { a.ExecutionMode = executionposture.Live.RootMode() },
		"missing artifact":         func(a *models.AgentConfig) { a.Mock.Source = nil },
		"retired authored pin":     func(a *models.AgentConfig) { a.LLMBackend = llmselection.BackendMock },
		"conflicting authored pin": func(a *models.AgentConfig) { a.LLMBackend = llmselection.BackendOpenAIResponses },
	} {
		t.Run(name, func(t *testing.T) {
			actor := resolvedMockAgent("stored")
			corrupt(&actor)
			before := actor
			if _, err := ValidateAgentExecutionDescriptor(profile, actor); err == nil {
				t.Fatal("corruption accepted")
			}
			if !reflect.DeepEqual(actor, before) {
				t.Fatal("corruption repaired during read")
			}
		})
	}
	live := resolvedClaudeAgent("live-with-artifact")
	live.Mock = resolvedMockAgent("mock").Mock
	if _, err := ValidateAgentExecutionDescriptor(profile, live); err == nil {
		t.Fatal("live executable double accepted")
	}
}
