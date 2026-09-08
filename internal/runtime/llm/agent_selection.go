package llm

import (
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type ResolvedAgentExecution struct {
	Actor     models.AgentConfig
	Selection llmselection.AgentExecutionSelection
}

// ResolveAgentExecution projects authored actor intent through the canonical
// selection owner. It mutates only derived fields on the returned copy.
func ResolveAgentExecution(posture executionposture.Posture, configuredDefault llmselection.Profile, aliases llmselection.ModelAliases, actor models.AgentConfig) (ResolvedAgentExecution, error) {
	actor.NormalizeRuntimeDescriptor()
	selection, err := llmselection.ResolveAgentExecutionSelection(llmselection.AgentExecutionSelectionInput{
		Posture:           posture,
		ConfiguredDefault: configuredDefault,
		AuthoredBackend:   actor.LLMBackend,
		MockConfigured:    actor.Mock.Configured(),
	})
	if err != nil {
		return ResolvedAgentExecution{}, err
	}
	if selection.ArtifactRequirement == llmselection.ArtifactRequired &&
		(actor.Mock.Kind != mockperformance.KindPython || len(actor.Mock.Source) == 0 || strings.TrimSpace(actor.Mock.Digest) == "") {
		return ResolvedAgentExecution{}, fmt.Errorf("selected mock execution has no compiled Python performance")
	}
	if selection.ArtifactRequirement == llmselection.ArtifactForbidden {
		actor.Mock = mockperformance.Performance{}
	}

	actor.ResolvedLLMBackend = selection.Profile.ID
	actor.ResolvedLLMProvider = selection.Profile.Provider
	actor.ResolvedLLMTransport = selection.Profile.Transport
	actor.ExecutionMode = selection.Mode
	actor.ResolvedModel = ""
	if strings.TrimSpace(actor.Model) != "" {
		resolved, resolveErr := llmselection.ResolveModel(selection.ModelProfile, llmselection.ModelResolution{
			Model:  actor.Model,
			Models: aliases,
		})
		if resolveErr != nil {
			return ResolvedAgentExecution{}, resolveErr
		}
		actor.Model = resolved.ModelAlias
		actor.ResolvedModel = resolved.ConcreteModel
	}
	return ResolvedAgentExecution{Actor: actor, Selection: selection}, nil
}

// ValidateAgentExecutionDescriptor consumes write-time truth. It never resolves
// models again, substitutes a configured backend, or rewrites causal mode.
func ValidateAgentExecutionDescriptor(configuredDefault llmselection.Profile, actor models.AgentConfig) (llmselection.AgentExecutionSelection, error) {
	if _, err := llmselection.ResolveLiveBackend(configuredDefault.ID); err != nil {
		return llmselection.AgentExecutionSelection{}, err
	}
	if strings.TrimSpace(actor.ResolvedLLMBackend) == "" {
		return llmselection.AgentExecutionSelection{}, fmt.Errorf("resolved llm backend is required before dispatch")
	}
	profile, err := llmselection.ResolveActiveBackend(actor.ResolvedLLMBackend)
	if err != nil {
		return llmselection.AgentExecutionSelection{}, err
	}
	if actor.LLMBackend != "" {
		pin, err := llmselection.ResolveLiveBackend(actor.LLMBackend)
		if err != nil {
			return llmselection.AgentExecutionSelection{}, err
		}
		if pin.ID != configuredDefault.ID {
			return llmselection.AgentExecutionSelection{}, fmt.Errorf("authored backend %q conflicts with configured runtime backend %q", pin.ID, configuredDefault.ID)
		}
	}
	mode, err := llmselection.ExecutionModeForProfile(profile)
	if err != nil {
		return llmselection.AgentExecutionSelection{}, err
	}
	if actor.ExecutionMode != mode || actor.ResolvedLLMProvider != profile.Provider || actor.ResolvedLLMTransport != profile.Transport {
		return llmselection.AgentExecutionSelection{}, fmt.Errorf("agent execution descriptor conflicts with persisted backend %q", profile.ID)
	}
	if strings.TrimSpace(actor.Model) != "" && strings.TrimSpace(actor.ResolvedModel) == "" {
		return llmselection.AgentExecutionSelection{}, fmt.Errorf("agent execution descriptor has no resolved model for authored model %q", actor.Model)
	}
	requirement := llmselection.ArtifactForbidden
	if profile.ID == llmselection.BackendMock {
		requirement = llmselection.ArtifactRequired
		if actor.Mock.Kind != mockperformance.KindPython || len(actor.Mock.Source) == 0 || strings.TrimSpace(actor.Mock.Digest) == "" {
			return llmselection.AgentExecutionSelection{}, fmt.Errorf("selected mock execution has no compiled Python performance")
		}
	} else {
		if profile.ID != configuredDefault.ID {
			return llmselection.AgentExecutionSelection{}, fmt.Errorf("persisted backend %q conflicts with configured runtime backend %q", profile.ID, configuredDefault.ID)
		}
		if actor.Mock.Configured() {
			return llmselection.AgentExecutionSelection{}, fmt.Errorf("live execution descriptor carries a mock performance")
		}
	}
	return llmselection.AgentExecutionSelection{Profile: profile, ModelProfile: configuredDefault, Mode: mode, ArtifactRequirement: requirement}, nil
}
