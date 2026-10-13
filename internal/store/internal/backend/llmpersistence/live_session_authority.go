package llmpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

func requirePostgresLiveSessionAuthority(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, operation string, permitStaleEvidence bool) (bool, error) {
	return requireLiveSessionAuthority(ctx, tx, identity, operation, permitStaleEvidence, false)
}

func requireSQLiteLiveSessionAuthority(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, operation string, permitStaleEvidence bool) (bool, error) {
	return requireLiveSessionAuthority(ctx, tx, identity, operation, permitStaleEvidence, true)
}

func requireLiveSessionAuthority(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, operation string, permitStaleEvidence, sqlite bool) (bool, error) {
	identity = identity.Normalize()
	operation = strings.TrimSpace(operation)
	token, hasToken := runtimeeffects.LifecycleTokenFromContext(ctx)
	observation, err := storeagent.LockAgentExecutionTx(ctx, tx, identity, token, sqlite)
	if err != nil {
		return false, err
	}
	if !observation.Found {
		return false, fmt.Errorf("live session lifecycle cell not found for %s", identity.Description())
	}
	if _, ok := runtimeeffects.DifferentOwnerFromContext(ctx); ok {
		return true, nil
	}
	if observation.Current {
		return true, nil
	}
	if permitStaleEvidence && hasToken && token.Identity.Normalize() == identity {
		return false, nil
	}
	return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_current", "selected-live-session-store", operation, map[string]any{
		"agent_identity": identity, "current_epoch": observation.State.RuntimeEpoch,
		"current_generation": observation.State.Generation, "current_phase": observation.State.Phase,
	})
}
