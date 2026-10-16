package contracts

import (
	"fmt"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
)

// AssembleAgentPrompt renders only the declaration's immutable resolved intent.
func AssembleAgentPrompt(entry AgentRegistryEntry) (runtimeagentintent.DerivedPrompt, error) {
	if err := entry.ResolvedIntent.Validate(); err != nil {
		return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("resolved agent intent: %w", err)
	}
	return runtimeagentintent.IntentOnlyPrompt(entry.ResolvedIntent)
}
