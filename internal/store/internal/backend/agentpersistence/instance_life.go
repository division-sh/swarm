package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

// Acquire the existing lifecycle locks before workflow header locks. Session
// writers use those same actor rows before observing their captured life.
func LockInstanceLifeActorsTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife, binding processbinding.Binding, sqlite bool) ([]manager.AgentLifecycleState, error) {
	if tx == nil {
		return nil, fmt.Errorf("instance actor census requires its native transaction")
	}
	if err := life.Validate(); err != nil {
		return nil, err
	}
	if err := AuthorizeDynamicFlowActivationTx(ctx, tx, binding, life.RunID, sqlite); err != nil {
		return nil, err
	}
	presence, path := string(agentidentity.RoutePresent), life.FlowInstance
	if path == life.RunID {
		presence, path = string(agentidentity.RouteRoot), ""
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_id,agent_name_owner,agent_name_source,
		agent_route_presence,flow_scope_key,flow_instance_id,flow_instance FROM agents
		WHERE run_id=$1 AND agent_route_presence=$2 AND flow_instance=$3 AND lifecycle_phase<>'terminated'`, life.RunID, presence, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var identities []agentidentity.Identity
	for rows.Next() {
		var id, owner, source, route, scope, instance, flow string
		if err := rows.Scan(&id, &owner, &source, &route, &scope, &instance, &flow); err != nil {
			return nil, err
		}
		identity, err := IdentityFromColumns(life.RunID, id, owner, source, route, scope, instance, flow)
		if err != nil {
			return nil, err
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	sort.Slice(identities, func(i, j int) bool { return agentidentity.Less(identities[i], identities[j]) })
	states := make([]manager.AgentLifecycleState, 0, len(identities))
	for _, identity := range identities {
		if err := lockLifecycleIdentityTx(ctx, tx, identity, sqlite); err != nil {
			return nil, err
		}
		state, _, found, err := loadAgentLifecycleState(ctx, tx, identity, sqlite, true)
		if err != nil {
			return nil, err
		}
		if !found || !state.ProcessBinding.Equal(binding) {
			return nil, fmt.Errorf("instance restart actor differs from its exact current process binding")
		}
		states = append(states, state)
	}
	return states, nil
}

func lockLifecycleIdentityTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, sqlite bool) error {
	if sqlite {
		return nil
	}
	fingerprint, err := identity.Fingerprint()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "swarm:agent-lifecycle:"+fingerprint)
	return err
}
