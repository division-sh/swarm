package llmpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

func requirePostgresLiveSessionAuthority(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, operation string, permitStaleEvidence bool) (bool, error) {
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return false, err
	}
	var epoch, generation int64
	var phase string
	err = tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE run_id = $1::uuid AND agent_id = $2 AND agent_name_owner = $3 AND agent_name_source = $4
		  AND agent_route_presence = $5 AND flow_scope_key = $6
		  AND flow_instance_id = $7 AND flow_instance = $8
		FOR UPDATE
	`, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	return evaluateLiveSessionAuthority(ctx, identity, operation, epoch, generation, phase, permitStaleEvidence, err)
}

func requireSQLiteLiveSessionAuthority(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity, operation string, permitStaleEvidence bool) (bool, error) {
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return false, err
	}
	var epoch, generation int64
	var phase string
	err = tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE run_id = ? AND agent_id = ? AND agent_name_owner = ? AND agent_name_source = ?
		  AND agent_route_presence = ? AND flow_scope_key = ?
		  AND flow_instance_id = ? AND flow_instance = ?
	`, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	return evaluateLiveSessionAuthority(ctx, identity, operation, epoch, generation, phase, permitStaleEvidence, err)
}

func evaluateLiveSessionAuthority(ctx context.Context, identity runtimeagentidentity.Identity, operation string, epoch, generation int64, phase string, permitStaleEvidence bool, queryErr error) (bool, error) {
	identity = identity.Normalize()
	operation = strings.TrimSpace(operation)
	if queryErr != nil {
		if errors.Is(queryErr, sql.ErrNoRows) {
			return false, fmt.Errorf("live session lifecycle cell not found for %s", identity.Description())
		}
		return false, queryErr
	}
	if _, ok := runtimeeffects.DifferentOwnerFromContext(ctx); ok {
		return true, nil
	}
	token, ok := runtimeeffects.LifecycleTokenFromContext(ctx)
	current := ok && token.Identity.Normalize() == identity && token.RuntimeEpoch == epoch && int64(token.Generation) == generation && strings.TrimSpace(phase) == "running"
	if current {
		return true, nil
	}
	if permitStaleEvidence && ok && token.Identity.Normalize() == identity {
		return false, nil
	}
	return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_current", "selected-live-session-store", operation, map[string]any{
		"agent_identity": identity, "current_epoch": epoch, "current_generation": generation, "current_phase": strings.TrimSpace(phase),
	})
}
