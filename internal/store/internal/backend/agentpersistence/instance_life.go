package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Acquire the existing lifecycle locks before workflow header locks. Session
// writers use those same actor rows before observing their captured life.
func LockInstanceLifeActorsTx(ctx context.Context, tx *sql.Tx, lives []events.InstanceLife, binding processbinding.Binding, sqlite bool) error {
	if tx == nil {
		return fmt.Errorf("instance actor census requires its native transaction")
	}
	identities, err := instanceLifeActorIdentitiesTx(ctx, tx, lives)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		if err := lockLifecycleIdentityTx(ctx, tx, identity, sqlite); err != nil {
			return err
		}
		if _, _, found, err := loadAgentLifecycleState(ctx, tx, identity, sqlite, true); err != nil {
			return err
		} else if !found {
			return instanceLifeActorCensusChanged()
		}
	}
	// The lifecycle owner takes actor locks before the source-set head. Acquire
	// the complete publication's actor set first, not one instance at a time.
	for _, life := range lives {
		if err := AuthorizeDynamicFlowActivationTx(ctx, tx, binding, life.RunID, sqlite); err != nil {
			return err
		}
	}
	current, err := instanceLifeActorIdentitiesTx(ctx, tx, lives)
	if err != nil {
		return err
	}
	if len(current) != len(identities) {
		return instanceLifeActorCensusChanged()
	}
	for index, identity := range identities {
		if identity != current[index] {
			return instanceLifeActorCensusChanged()
		}
	}
	return nil
}

func (s *AgentPostgresOwner) ReadAgentLifecycleOutcomeTx(ctx context.Context, attempt *mutationprotocol.Attempt, request manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error) {
	if attempt == nil {
		return manager.AgentLifecycleTransitionResult{}, false, fmt.Errorf("lifecycle outcome requires its native transaction")
	}
	var result manager.AgentLifecycleTransitionResult
	var found bool
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		result, found, err = loadPostgresLifecycleOperationResult(ctx, tx, request)
		return err
	})
	return result, found, err
}

func (s *AgentSQLiteOwner) ReadAgentLifecycleOutcomeTx(ctx context.Context, attempt *mutationprotocol.Attempt, request manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error) {
	if attempt == nil {
		return manager.AgentLifecycleTransitionResult{}, false, fmt.Errorf("lifecycle outcome requires its native transaction")
	}
	var result manager.AgentLifecycleTransitionResult
	var found bool
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		result, found, err = loadSQLiteLifecycleOperationResult(ctx, tx, request)
		return err
	})
	return result, found, err
}

func instanceLifeActorCensusChanged() error {
	return failures.New(failures.ClassLifecycleConflict, "instance_restart_actor_census_changed", "flow-instance", "restart", nil)
}

func instanceLifeActorIdentitiesTx(ctx context.Context, tx *sql.Tx, lives []events.InstanceLife) ([]agentidentity.Identity, error) {
	unique := map[agentidentity.Identity]bool{}
	for _, life := range lives {
		if err := life.Validate(); err != nil {
			return nil, err
		}
		identities, err := readInstanceLifeActorIdentitiesTx(ctx, tx, life)
		if err != nil {
			return nil, err
		}
		for _, identity := range identities {
			unique[identity] = true
		}
	}
	identities := make([]agentidentity.Identity, 0, len(unique))
	for identity := range unique {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return agentidentity.Less(identities[i], identities[j]) })
	return identities, nil
}

func readInstanceLifeActorIdentitiesTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife) ([]agentidentity.Identity, error) {
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
	return identities, nil
}

// Called after the complete native publication prefix owns these actor rows.
// This read does not acquire a late actor lock under the source-set head.
func ReadInstanceLifeActorsTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife, binding processbinding.Binding, sqlite bool) ([]manager.AgentLifecycleState, error) {
	identities, err := instanceLifeActorIdentitiesTx(ctx, tx, []events.InstanceLife{life})
	if err != nil {
		return nil, err
	}
	states := make([]manager.AgentLifecycleState, 0, len(identities))
	for _, identity := range identities {
		state, _, found, err := loadAgentLifecycleState(ctx, tx, identity, sqlite, false)
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
