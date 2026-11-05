package llmpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
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
	if err := requireLiveSessionInstanceLife(ctx, tx, identity, sqlite); err != nil {
		return false, err
	}
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

func requireLiveSessionInstanceLife(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, sqlite bool) error {
	life := events.DeliveryContextFromContext(ctx).Life
	if life == nil {
		return nil
	}
	if err := life.Validate(); err != nil {
		return err
	}
	path := identity.FlowInstance()
	if identity.Route.Presence == runtimeagentidentity.RouteRoot {
		path = identity.RunID
	}
	if life.RunID != identity.RunID || life.FlowInstance != path {
		return fmt.Errorf("session captured life differs from its exact actor instance")
	}
	// Take the existing header lock before the actor/session locks, as the reset
	// transaction does. A read followed by a later lease write is not a fence.
	header, found, err := workflowheader.LoadForMutation(ctx, tx, !sqlite, life.RunID, "", life.FlowInstance)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("session captured life has no constructed instance")
	}
	if header.Life != life.Life {
		owner := flowidentity.RunScopedFlowInstance{RunID: life.RunID, Route: flowidentity.RouteForInstancePath(life.FlowInstance)}
		return pipeline.InstanceRestarted(owner, life.Life, header.Life)
	}
	return nil
}
