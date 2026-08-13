package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func AuthorizePostgresAgentTopologyMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) error {
	return authorizeAgentTopologyMutation(ctx, tx, req, false)
}

func AuthorizeSQLiteAgentTopologyMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) error {
	return authorizeAgentTopologyMutation(ctx, tx, req, true)
}

func authorizeAgentTopologyMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, sqlite bool) error {
	if tx == nil {
		return fmt.Errorf("agent topology admission requires a transaction")
	}
	if err := req.Topology.Validate(); err != nil {
		return fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	if req.Topology.Lifetime != runtimeagenttopology.LifetimeDurableManaged {
		return topologyConflict(req, "ephemeral_execution_is_not_persistable")
	}
	switch req.Topology.Authority.Kind {
	case runtimeagenttopology.AuthorityStaticDeclarationPlan:
		return authorizeStaticDeclarationMutation(ctx, tx, req, sqlite)
	case runtimeagenttopology.AuthorityFlowReadinessPlan:
		return authorizeFlowReadinessMutation(ctx, tx, req, sqlite)
	case runtimeagenttopology.AuthorityEphemeralExecution:
		return topologyConflict(req, "ephemeral_execution_is_not_persistable")
	default:
		return topologyConflict(req, "unknown_topology_authority")
	}
}

func authorizeStaticDeclarationMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, sqlite bool) error {
	authority := req.Topology.Authority.Static
	query := `SELECT revision, plan FROM agent_topology_source_set_head WHERE singleton_id = 1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var revision string
	var raw []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&revision, &raw); err != nil {
		if err == sql.ErrNoRows {
			return topologyConflict(req, "static_source_set_missing")
		}
		return err
	}
	if strings.TrimSpace(revision) != strings.TrimSpace(authority.SourceSetRevision) {
		return topologyConflict(req, "static_source_set_changed")
	}
	var plan runtimeagenttopology.SourceSetPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return fmt.Errorf("decode static source-set plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return fmt.Errorf("validate static source-set plan: %w", err)
	}
	desired, found, err := desiredAgentForIdentity(plan, req.Identity)
	if err != nil {
		return err
	}
	present := req.TargetPhase != runtimemanager.AgentLifecycleTerminated
	if present != found {
		return topologyConflict(req, "static_desired_state_mismatch")
	}
	if !present {
		return nil
	}
	if desired.Source.BundleHash != authority.BundleHash || desired.Source.BundleSource != authority.BundleSource {
		return topologyConflict(req, "static_source_coordinate_mismatch")
	}
	if strings.TrimSpace(desired.ConfigRevision) != strings.TrimSpace(req.ConfigRevision) {
		return topologyConflict(req, "static_config_revision_mismatch")
	}
	return nil
}

func authorizeFlowReadinessMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, sqlite bool) error {
	authority := req.Topology.Authority.Readiness
	query := `
		SELECT readiness.plan, instance.status
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		WHERE readiness.run_id = ? AND readiness.instance_id = ?`
	args := []any{authority.RunID, authority.InstancePath}
	if !sqlite {
		query = `
			SELECT readiness.plan, instance.status
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			WHERE readiness.run_id = $1::uuid AND readiness.instance_id = $2
			FOR UPDATE OF readiness, instance`
	}
	var raw []byte
	var instanceStatus string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw, &instanceStatus); err != nil {
		if err == sql.ErrNoRows {
			return topologyConflict(req, "readiness_owner_missing")
		}
		return err
	}
	fingerprint, err := canonicaljson.HashRaw(raw)
	if err != nil {
		return fmt.Errorf("hash flow readiness plan: %w", err)
	}
	if fingerprint != strings.TrimSpace(authority.PlanFingerprint) {
		return topologyConflict(req, "readiness_plan_changed")
	}
	var plan struct {
		Agents []struct {
			Identity       runtimeagentidentity.Identity `json:"identity"`
			ConfigRevision string                        `json:"config_revision"`
		} `json:"agents"`
	}
	if err := canonicaljson.DecodeInto(raw, &plan); err != nil {
		return fmt.Errorf("decode flow readiness plan: %w", err)
	}
	desiredRevision := ""
	for _, agent := range plan.Agents {
		if agent.Identity.Normalize() != req.Identity.Normalize() {
			continue
		}
		if desiredRevision != "" {
			return topologyConflict(req, "readiness_agent_duplicated")
		}
		desiredRevision = strings.TrimSpace(agent.ConfigRevision)
	}
	present := req.TargetPhase != runtimemanager.AgentLifecycleTerminated
	if present {
		if strings.TrimSpace(instanceStatus) != "active" {
			return topologyConflict(req, "readiness_instance_not_active")
		}
		if desiredRevision != strings.TrimSpace(req.ConfigRevision) {
			return topologyConflict(req, "readiness_agent_revision_mismatch")
		}
		return nil
	}
	if desiredRevision != "" && strings.TrimSpace(instanceStatus) != "terminated" {
		return topologyConflict(req, "readiness_agent_still_declared_on_active_instance")
	}
	return nil
}

func desiredAgentForIdentity(plan runtimeagenttopology.SourceSetPlan, identity runtimeagentidentity.Identity) (runtimeagenttopology.DesiredAgent, bool, error) {
	identity = identity.Normalize()
	var found *runtimeagenttopology.DesiredAgent
	for _, desired := range plan.Agents {
		if desired.Identity.Normalize() != identity {
			continue
		}
		if found != nil {
			return runtimeagenttopology.DesiredAgent{}, false, fmt.Errorf("static source-set duplicates identity %s", identity.Description())
		}
		copy := desired
		found = &copy
	}
	if found == nil {
		return runtimeagenttopology.DesiredAgent{}, false, nil
	}
	return *found, true, nil
}

func topologyConflict(req runtimemanager.AgentLifecycleTransition, reason string) error {
	return runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"agent_topology_authority_conflict",
		"agent-lifecycle-store",
		req.OperationKind,
		map[string]any{"agent_id": req.AgentID, "reason": reason, "authority_kind": req.Topology.Authority.Kind},
	)
}
