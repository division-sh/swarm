package runforkreadiness

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

// PreparedActorCensus is the complete fixed readiness projection, not the first
// event's dispatch set. No future template instance is inferred here.
func PreparedActorCensus(blueprints []manager.AgentMaterializationBlueprint) ([]manager.AgentMaterializationBlueprint, error) {
	actors := make([]manager.AgentMaterializationBlueprint, 0, len(blueprints))
	seen := make(map[agentidentity.Plan]manager.AgentMaterializationBlueprint, len(blueprints))
	for _, actor := range blueprints {
		if err := actor.Identity.Validate(); err != nil {
			return nil, fmt.Errorf("selected preparation actor: %w", err)
		}
		if actor.Identity != actor.Identity.Normalize() {
			return nil, fmt.Errorf("selected preparation actor is not canonical")
		}
		if !actor.Config.Identity.IsZero() {
			return nil, fmt.Errorf("selected preparation cannot adopt a live actor configuration")
		}
		if previous, exists := seen[actor.Identity]; exists {
			// A static declaration may also be required by its flow. They describe
			// one actor only when the complete configuration and prompt agree.
			prior, err := manager.AgentConfigPlanRevision(previous.Config, actor.Identity)
			current, currentErr := manager.AgentConfigPlanRevision(actor.Config, actor.Identity)
			priorPrompt, promptErr := previous.Config.DerivedSystemPrompt()
			currentPrompt, currentPromptErr := actor.Config.DerivedSystemPrompt()
			if err != nil || currentErr != nil || promptErr != nil || currentPromptErr != nil || prior != current || priorPrompt != currentPrompt {
				return nil, fmt.Errorf("selected preparation has conflicting declarations for actor %s", actor.Identity.Description())
			}
			continue
		}
		seen[actor.Identity] = actor
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return agentidentity.LessPlan(actors[i].Identity, actors[j].Identity) })
	return actors, nil
}
