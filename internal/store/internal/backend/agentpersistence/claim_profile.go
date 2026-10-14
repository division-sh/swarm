package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *AgentPostgresOwner) ReadReceiverClaimProfileTx(ctx context.Context, tx *sql.Tx, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) (deliverylifecycle.AgentClaimProfile, error) {
	return readReceiverClaimProfileTx(ctx, tx, event, route, authority, false)
}

func (s *AgentSQLiteOwner) ReadReceiverClaimProfileTx(ctx context.Context, tx *sql.Tx, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) (deliverylifecycle.AgentClaimProfile, error) {
	return readReceiverClaimProfileTx(ctx, tx, event, route, authority, true)
}

func readReceiverClaimProfileTx(ctx context.Context, tx *sql.Tx, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, sqlite bool) (deliverylifecycle.AgentClaimProfile, error) {
	if tx == nil || !route.Recipient.IsAgent() || route.AgentIdentity.RunID != event.RunID() ||
		authority.Validate() != nil || authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("ordinary agent profile capture requires exact transactional delivery admission")
	}
	ready, err := receiverExecutionReadyTx(ctx, tx, route, authority, true, sqlite)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	if !ready {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("agent profile capture requires the exact admitted lifecycle/grant")
	}
	// The existing admission operation retains the generation and cell locks.
	// Hydrate only this exact locked configuration, never the whole live catalog.
	state, _, found, err := loadAgentLifecycleState(ctx, tx, route.AgentIdentity, sqlite, false)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	if !found {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("admitted profile lifecycle is missing")
	}
	cfg, err := loadClaimAgentConfigTx(ctx, tx, state)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	plan, err := cfg.Identity.Plan()
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	revision, err := manager.AgentConfigPlanRevision(cfg, plan)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	if revision != state.ConfigRevision {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("captured agent descriptor differs from its immutable configuration revision")
	}
	profile, err := selection.ResolveActiveBackend(cfg.ResolvedLLMBackend)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	if _, err := llm.ValidateAgentExecutionDescriptor(profile, cfg); err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		return deliverylifecycle.AgentClaimProfile{}, err
	}
	evidence := deliverylifecycle.AgentClaimProfile{
		DeliveryID: id, EventID: event.ID(), Identity: cfg.Identity,
		ProfileID: profile.ID, ConfigurationRevision: revision, BundleHash: state.ProcessBinding.BundleHash,
		ExecutionID: authority.ExecutionID(), ExecutionGeneration: authority.Generation(),
		AgentRuntimeEpoch: state.RuntimeEpoch, AgentGeneration: state.Generation,
	}
	if !evidence.MatchesFirstAdmission(event, route, authority) {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("captured agent profile differs from exact first-claim admission")
	}
	return evidence, nil
}

func loadClaimAgentConfigTx(ctx context.Context, tx *sql.Tx, state manager.AgentLifecycleState) (actors.AgentConfig, error) {
	fields, err := IdentityFields(state.Identity)
	if err != nil {
		return actors.AgentConfig{}, err
	}
	row := PersistedAgentProjection{Identity: fields, AgentID: fields.AgentID, FlowInstance: fields.FlowInstancePath}
	err = tx.QueryRowContext(ctx, `SELECT role,model,llm_backend,memory_enabled,
		COALESCE(parent_agent_id,''),COALESCE(CAST(entity_id AS TEXT),''),config,runtime_descriptor,
		subscriptions,emit_events,tools,permissions FROM agents
		WHERE run_id=$1 AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4
		AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7 AND flow_instance=$8
		AND lifecycle_config_revision=$9 AND lifecycle_runtime_epoch=$10 AND lifecycle_generation=$11
		AND lifecycle_phase='running'`, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		state.ConfigRevision, state.RuntimeEpoch, state.Generation).Scan(&row.Role, &row.Model, &row.LLMBackend,
		&row.MemoryEnabled, &row.ParentAgentID, &row.EntityID, &row.ConfigJSON, &row.RuntimeDescriptor,
		&row.SubscriptionsJSON, &row.EmitEventsJSON, &row.ToolsJSON, &row.PermissionsJSON)
	if err != nil {
		return actors.AgentConfig{}, fmt.Errorf("read exact locked agent configuration: %w", err)
	}
	return HydratePersistedAgentConfig(row)
}
