package currentstate

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

type Identity struct {
	RunID    string
	EntityID string
}

func RequireRunID(ctx context.Context) (string, error) {
	runID, ok, err := RunIDFromContext(ctx)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("entity_state current-state run_id is required")
	}
	return runID, nil
}

func RunIDFromContext(ctx context.Context) (string, bool, error) {
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			runID = strings.TrimSpace(inbound.RunID)
		}
	}
	if runID == "" {
		return "", false, nil
	}
	runID, err := ValidateRunID(runID)
	if err != nil {
		return "", true, err
	}
	return runID, true, nil
}

func ValidateRunID(runID string) (string, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", fmt.Errorf("entity_state current-state run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", fmt.Errorf("entity_state current-state run_id must be uuid: %w", err)
	}
	return runID, nil
}

func RequireIdentity(ctx context.Context, entityID string) (Identity, error) {
	runID, err := RequireRunID(ctx)
	if err != nil {
		return Identity{}, err
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return Identity{}, fmt.Errorf("entity_state current-state entity_id is required")
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return Identity{}, fmt.Errorf("entity_state current-state entity_id must be uuid: %w", err)
	}
	return Identity{RunID: runID, EntityID: entityID}, nil
}
