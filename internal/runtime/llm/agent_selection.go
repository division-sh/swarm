package llm

import (
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type ResolvedAgentExecution struct {
	Actor     models.AgentConfig
	Selection llmselection.AgentExecutionSelection
}

// ResolveAgentExecution projects authored actor intent through the canonical
// selection owner. It mutates only derived fields on the returned copy.
func ResolveAgentExecution(configuredDefault llmselection.Profile, aliases llmselection.ModelAliases, actor models.AgentConfig) (ResolvedAgentExecution, error) {
	actor.NormalizeRuntimeDescriptor()
	selection, err := llmselection.ResolveAgentExecutionSelection(llmselection.AgentExecutionSelectionInput{
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
