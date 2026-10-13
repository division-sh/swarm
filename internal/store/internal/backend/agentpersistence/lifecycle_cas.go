package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

func applyLifecycleCellTx(ctx context.Context, tx *sql.Tx, req manager.AgentLifecycleTransition, previous manager.AgentLifecycleState, exists bool, result manager.AgentLifecycleTransitionResult, sqlite bool) error {
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	topology, err := canonicaljson.Bytes(req.Topology)
	if err != nil {
		return err
	}
	if !exists {
		return insertLifecycleCellTx(ctx, tx, req, result, fields, topology, sqlite)
	}
	predecessorTopology, err := canonicaljson.Bytes(previous.Topology)
	if err != nil {
		return err
	}
	args := []any{
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, fields.RunID,
		lifecycleAgentStatus(req), string(result.Phase), result.Generation, result.RuntimeEpoch,
		result.ConfigRevision, string(result.RunMode), result.TransitionID,
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		req.Now.UTC(), string(req.Topology.Authority.Kind), string(topology), string(req.Topology.Lifetime),
		previous.RuntimeEpoch, previous.Generation, string(previous.Phase), previous.ConfigRevision, string(previous.RunMode),
		nullUUID(previous.LastTransitionID),
		previous.ProcessBinding.ProcessAuthorityID, previous.ProcessBinding.ProcessOwnerID,
		previous.ProcessBinding.ProcessBootID, previous.ProcessBinding.GenerationGrantID,
		previous.ProcessBinding.BundleHash, previous.ProcessBinding.RuntimeInstanceID, previous.ProcessBinding.RuntimeGeneration,
		string(previous.Topology.Authority.Kind), string(predecessorTopology), string(previous.Topology.Lifetime),
	}
	query := `UPDATE agents SET
		status=$9, lifecycle_phase=$10, lifecycle_generation=$11, lifecycle_runtime_epoch=$12,
		lifecycle_config_revision=$13, lifecycle_run_mode=$14, lifecycle_last_transition_id=$15::uuid,
		lifecycle_process_authority_id=$16::uuid, lifecycle_process_owner_id=$17,
		lifecycle_process_boot_id=$18::uuid, lifecycle_generation_grant_id=$19::uuid,
		lifecycle_bundle_hash=$20, lifecycle_runtime_instance_id=$21::uuid, lifecycle_runtime_generation=$22,
		last_active_at=$23, topology_authority_kind=$24, topology_admission=$25::jsonb, execution_lifetime=$26`
	if req.Agent != nil {
		projection, err := ProjectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
		if err != nil {
			return err
		}
		query += `, role=$43, model=$44, llm_backend=$45, memory_enabled=$46,
			parent_agent_id=$47, entity_id=$48::uuid, config=$49::jsonb,
			subscriptions=$50::jsonb, emit_events=$51::jsonb, tools=$52::jsonb,
			permissions=$53::jsonb, runtime_descriptor=$54::jsonb`
		args = append(args, projection.Role, projection.Model, projection.LLMBackend, projection.MemoryEnabled,
			nullString(projection.ParentAgentID), nullUUID(projection.EntityID), string(projection.ConfigJSON),
			string(projection.SubscriptionsJSON), string(projection.EmitEventsJSON), string(projection.ToolsJSON),
			string(projection.PermissionsJSON), string(projection.RuntimeDescriptor))
	}
	query += ` WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
		AND agent_route_presence=$4 AND flow_scope_key=$5 AND flow_instance_id=$6 AND flow_instance=$7 AND run_id=$8::uuid
		AND lifecycle_runtime_epoch=$27 AND lifecycle_generation=$28 AND lifecycle_phase=$29
		AND lifecycle_config_revision=$30 AND lifecycle_run_mode=$31
		AND lifecycle_last_transition_id IS NOT DISTINCT FROM $32::uuid
		AND lifecycle_process_authority_id=$33::uuid AND lifecycle_process_owner_id=$34
		AND lifecycle_process_boot_id=$35::uuid AND lifecycle_generation_grant_id=$36::uuid
		AND lifecycle_bundle_hash=$37 AND lifecycle_runtime_instance_id=$38::uuid AND lifecycle_runtime_generation=$39
		AND topology_authority_kind=$40 AND topology_admission=$41::jsonb AND execution_lifetime=$42`
	return executeLifecycleCAS(ctx, tx, lifecycleCellSQL(query, sqlite), args, req, previous, true)
}

func insertLifecycleCellTx(ctx context.Context, tx *sql.Tx, req manager.AgentLifecycleTransition, result manager.AgentLifecycleTransitionResult, fields agentidentity.StorageFields, topology []byte, sqlite bool) error {
	if req.Agent == nil {
		return lifecycleConflict(req, manager.AgentLifecycleState{}, false)
	}
	projection, err := ProjectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
	if err != nil {
		return err
	}
	startedAt := req.Agent.StartedAt
	if startedAt.IsZero() {
		startedAt = req.Now
	}
	query := `INSERT INTO agents (
		agent_id, agent_name_owner, agent_name_source, agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
		role, model, llm_backend, memory_enabled, parent_agent_id, entity_id,
		config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, turn_count, last_active_at, created_at,
		lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_config_revision, lifecycle_run_mode, lifecycle_last_transition_id,
		lifecycle_process_authority_id, lifecycle_process_owner_id, lifecycle_process_boot_id, lifecycle_generation_grant_id,
		lifecycle_bundle_hash, lifecycle_runtime_instance_id, lifecycle_runtime_generation,
		topology_authority_kind, topology_admission, execution_lifetime, run_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::uuid,
			$14::jsonb, $15::jsonb, $16::jsonb, $17::jsonb, $18::jsonb, $19::jsonb, $20, 0, $21, $22,
			$23, $24, $25, $26, $27, $28::uuid, $29::uuid, $30, $31::uuid, $32::uuid,
			$33, $34::uuid, $35, $36, $37::jsonb, $38, $39::uuid)
		ON CONFLICT (run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance) DO NOTHING`
	args := []any{fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		projection.Role, projection.Model, projection.LLMBackend, projection.MemoryEnabled,
		nullString(projection.ParentAgentID), nullUUID(projection.EntityID), string(projection.ConfigJSON),
		string(projection.SubscriptionsJSON), string(projection.EmitEventsJSON), string(projection.ToolsJSON),
		string(projection.PermissionsJSON), string(projection.RuntimeDescriptor), lifecycleAgentStatus(req), req.Now.UTC(), startedAt.UTC(),
		string(result.Phase), result.Generation, result.RuntimeEpoch, result.ConfigRevision, string(result.RunMode), result.TransitionID,
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID, req.ProcessBinding.BundleHash,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		string(req.Topology.Authority.Kind), string(topology), string(req.Topology.Lifetime), fields.RunID}
	return executeLifecycleCAS(ctx, tx, lifecycleCellSQL(query, sqlite), args, req, manager.AgentLifecycleState{}, false)
}

func lifecycleCellSQL(query string, sqlite bool) string {
	if sqlite {
		return strings.NewReplacer("::uuid", "", "::jsonb", "", "$", "?").Replace(query)
	}
	return query
}

func executeLifecycleCAS(ctx context.Context, tx *sql.Tx, query string, args []any, req manager.AgentLifecycleTransition, previous manager.AgentLifecycleState, exists bool) error {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read agent lifecycle CAS cardinality: %w", err)
	}
	if affected != 1 {
		return lifecycleConflict(req, previous, exists)
	}
	return nil
}
