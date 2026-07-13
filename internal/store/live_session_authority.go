package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func requirePostgresLiveSessionAuthority(ctx context.Context, tx *sql.Tx, agentID, operation string, permitStaleEvidence bool) (bool, error) {
	var epoch, generation int64
	var phase string
	err := tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE agent_id = $1
		FOR UPDATE
	`, strings.TrimSpace(agentID)).Scan(&epoch, &generation, &phase)
	return evaluateLiveSessionAuthority(ctx, agentID, operation, epoch, generation, phase, permitStaleEvidence, err)
}

func requireSQLiteLiveSessionAuthority(ctx context.Context, tx *sql.Tx, agentID, operation string, permitStaleEvidence bool) (bool, error) {
	var epoch, generation int64
	var phase string
	err := tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE agent_id = ?
	`, strings.TrimSpace(agentID)).Scan(&epoch, &generation, &phase)
	return evaluateLiveSessionAuthority(ctx, agentID, operation, epoch, generation, phase, permitStaleEvidence, err)
}

func evaluateLiveSessionAuthority(ctx context.Context, agentID, operation string, epoch, generation int64, phase string, permitStaleEvidence bool, queryErr error) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	operation = strings.TrimSpace(operation)
	if queryErr != nil {
		if errors.Is(queryErr, sql.ErrNoRows) {
			return false, fmt.Errorf("live session lifecycle cell not found for agent=%s", agentID)
		}
		return false, queryErr
	}
	if _, ok := runtimeeffects.DifferentOwnerFromContext(ctx); ok {
		return true, nil
	}
	token, ok := runtimeeffects.LifecycleTokenFromContext(ctx)
	current := ok && token.AgentID == agentID && token.RuntimeEpoch == epoch && int64(token.Generation) == generation && strings.TrimSpace(phase) == "running"
	if current {
		return true, nil
	}
	if permitStaleEvidence && ok && token.AgentID == agentID {
		return false, nil
	}
	return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_current", "selected-live-session-store", operation, map[string]any{
		"agent_id": agentID, "current_epoch": epoch, "current_generation": generation, "current_phase": strings.TrimSpace(phase),
	})
}
