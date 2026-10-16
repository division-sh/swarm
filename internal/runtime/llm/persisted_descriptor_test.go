package llm

import (
	"reflect"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestPersistedDescriptorSeparatesExecutionFromConfiguredModelProfile(t *testing.T) {
	live, err := selection.ResolveLiveBackend(selection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	for _, posture := range []executionposture.Posture{executionposture.Live, executionposture.MockOnly} {
		t.Run(string(posture), func(t *testing.T) {
			source := resolvedMockAgent("persisted-profile")
			source.LLMBackend, source.Model = live.ID, "alias-retired-after-write"
			selected, err := ResolveAgentExecution(posture, live, selection.ModelAliases{
				source.Model: {live.ID: "exact-write-time-model"},
			}, source)
			if err != nil {
				t.Fatal(err)
			}
			actor, before := selected.Actor, selected.Actor
			profile, err := ValidatePersistedAgentExecutionDescriptor(actor)
			if err != nil || profile != selected.Selection.Profile || !reflect.DeepEqual(actor, before) {
				t.Fatalf("persisted selection changed: profile=%+v error=%v", profile, err)
			}
			configured, err := ValidateAgentExecutionDescriptor(live, actor)
			if err != nil || !reflect.DeepEqual(configured, selected.Selection) {
				t.Fatalf("configured selection changed: %+v error=%v", configured, err)
			}
			other, err := selection.ResolveLiveBackend(selection.BackendOpenAIResponses)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateAgentExecutionDescriptor(other, actor); err == nil {
				t.Fatal("configured/authored mismatch accepted")
			}
			mock, err := selection.ResolveActiveBackend(selection.BackendMock)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateAgentExecutionDescriptor(mock, actor); err == nil {
				t.Fatal("mock execution became a public configured selector")
			}
		})
	}
}

func TestPersistedDescriptorRejectsCorruptionWithoutReselection(t *testing.T) {
	for name, corrupt := range map[string]func(*models.AgentConfig){
		"missing backend": func(a *models.AgentConfig) { a.ResolvedLLMBackend = "" },
		"unknown backend": func(a *models.AgentConfig) { a.ResolvedLLMBackend = "unknown" },
		"mode":            func(a *models.AgentConfig) { a.ExecutionMode = executionposture.Live.RootMode() },
		"provider":        func(a *models.AgentConfig) { a.ResolvedLLMProvider = "anthropic" },
		"transport":       func(a *models.AgentConfig) { a.ResolvedLLMTransport = "api" },
		"model":           func(a *models.AgentConfig) { a.Model = "regular"; a.ResolvedModel = "" },
		"artifact source": func(a *models.AgentConfig) { a.Mock.Source = nil },
		"artifact digest": func(a *models.AgentConfig) { a.Mock.Digest = "" },
		"artifact kind":   func(a *models.AgentConfig) { a.Mock.Kind = "unknown" },
		"retired pin":     func(a *models.AgentConfig) { a.LLMBackend = selection.BackendMock },
		"unknown pin":     func(a *models.AgentConfig) { a.LLMBackend = "unknown" },
	} {
		t.Run(name, func(t *testing.T) {
			actor := resolvedMockAgent("corrupt-persisted")
			corrupt(&actor)
			before := actor
			if _, err := ValidatePersistedAgentExecutionDescriptor(actor); err == nil || !reflect.DeepEqual(actor, before) {
				t.Fatalf("corruption accepted or repaired: %v", err)
			}
		})
	}
	for _, kind := range []string{"artifact", "authored pin"} {
		t.Run("live/"+kind, func(t *testing.T) {
			actor := resolvedClaudeAgent("live-corrupt")
			if kind == "artifact" {
				actor.Mock = resolvedMockAgent("artifact").Mock
			} else {
				actor.LLMBackend = selection.BackendOpenAIResponses
			}
			before := actor
			if _, err := ValidatePersistedAgentExecutionDescriptor(actor); err == nil || !reflect.DeepEqual(actor, before) {
				t.Fatalf("live corruption accepted or repaired: %v", err)
			}
		})
	}
}
