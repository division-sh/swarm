package llmpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	token, hasToken := runtimeeffects.LifecycleTokenFromContext(ctx)
	observation, err := storeagent.LockAgentExecutionTx(ctx, tx, identity, token, sqlite)
	if err != nil {
		return false, err
	}
	if err := requireLiveSessionInstanceLife(ctx, tx, identity); err != nil {
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

func requireLiveSessionInstanceLife(ctx context.Context, tx *sql.Tx, identity runtimeagentidentity.Identity) error {
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
	return requireSessionInstanceLife(ctx, tx, *life)
}

func requireSessionInstanceLife(ctx context.Context, tx *sql.Tx, life events.InstanceLife) error {
	// The existing lifecycle lock precedes this observation and all session
	// writes. Do not take a late header lock inside completion settlement.
	header, found, err := workflowheader.LoadForRead(ctx, tx, life.RunID, "", life.FlowInstance)
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

// This census is an observation within the caller's native instance mutation,
// not permission to reset or a substitute for acquiring the actor lifecycle set.
func ReadActiveInstanceLifeSessionLeasesTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife, now time.Time) (int, error) {
	if tx == nil || now.IsZero() {
		return 0, fmt.Errorf("instance session census requires its transaction and observation time")
	}
	if err := life.Validate(); err != nil {
		return 0, err
	}
	if err := requireSessionInstanceLife(ctx, tx, life); err != nil {
		return 0, err
	}
	presence, path := string(runtimeagentidentity.RoutePresent), life.FlowInstance
	if life.FlowInstance == life.RunID {
		presence, path = string(runtimeagentidentity.RouteRoot), ""
	}
	rows, err := tx.QueryContext(ctx, `SELECT lease_holder, lease_grant_id, lease_expires_at
		FROM agent_sessions WHERE run_id=$1 AND agent_route_presence=$2 AND flow_instance=$3
		AND status='active' ORDER BY session_id`, life.RunID, presence, path)
	if err != nil {
		return 0, fmt.Errorf("read exact instance session leases: %w", err)
	}
	defer rows.Close()
	active := 0
	for rows.Next() {
		var holder, grant sql.NullString
		var rawExpiry any
		if err := rows.Scan(&holder, &grant, &rawExpiry); err != nil {
			return 0, err
		}
		expires, present, err := sqliteTimeValue(rawExpiry)
		if err != nil {
			return 0, fmt.Errorf("decode instance session lease expiry: %w", err)
		}
		if !holder.Valid && !grant.Valid && !present {
			continue
		}
		if !holder.Valid || holder.String == "" || !grant.Valid || grant.String == "" || !present {
			return 0, fmt.Errorf("instance session lease requires its exact holder, grant and expiry together")
		}
		if expires.After(now) {
			active++
		}
	}
	return active, rows.Err()
}

func (s *LLMPostgresOwner) ReadActiveInstanceLifeSessionLeasesTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife, now time.Time) (int, error) {
	return ReadActiveInstanceLifeSessionLeasesTx(ctx, tx, life, now)
}

func (s *LLMSQLiteOwner) ReadActiveInstanceLifeSessionLeasesTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife, now time.Time) (int, error) {
	return ReadActiveInstanceLifeSessionLeasesTx(ctx, tx, life, now)
}
