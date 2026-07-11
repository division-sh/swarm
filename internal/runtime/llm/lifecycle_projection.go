package llm

import (
	"context"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func requireCurrentProviderProjection(ctx context.Context, agentID string) error {
	current, err := runtimeeffects.ProjectionCurrent(ctx)
	if err != nil {
		return err
	}
	if current {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassSupersededGeneration, "superseded_generation", "llm-runtime", "project_provider_turn", map[string]any{"agent_id": strings.TrimSpace(agentID)})
}
