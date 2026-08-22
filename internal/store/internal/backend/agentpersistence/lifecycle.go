package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

var _ runtimemanager.AgentLifecycleStateReader = (*AgentPostgresOwner)(nil)
var _ runtimemanager.AgentLifecycleStateReader = (*AgentSQLiteOwner)(nil)
var _ runtimemanager.AgentLifecycleCellCensus = (*AgentPostgresOwner)(nil)
var _ runtimemanager.AgentLifecycleCellCensus = (*AgentSQLiteOwner)(nil)
var _ runtimemanager.AgentLifecycleDiagnosticPersistence = (*AgentPostgresOwner)(nil)
var _ runtimemanager.AgentLifecycleDiagnosticPersistence = (*AgentSQLiteOwner)(nil)

func (s *AgentPostgresOwner) LoadAgentLifecycleState(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
) (runtimemanager.AgentLifecycleState, bool, error) {
	fields, err := IdentityFields(identity)
	if err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	var state runtimemanager.AgentLifecycleState
	var generation int64
	var topologyRaw []byte
	err = s.backend.QueryRowContext(ctx, `
			SELECT agent_id, lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
			       lifecycle_config_revision, lifecycle_run_mode,
			       lifecycle_process_authority_id::text, lifecycle_process_owner_id,
			       lifecycle_process_boot_id::text, lifecycle_generation_grant_id::text,
			       lifecycle_bundle_hash, lifecycle_bundle_source,
			       lifecycle_runtime_instance_id::text, lifecycle_runtime_generation,
			       topology_admission
		FROM agents
		WHERE agent_id = $1 AND agent_name_owner = $2 AND agent_name_source = $3
		  AND agent_route_presence = $4 AND flow_scope_key = $5
		  AND flow_instance_id = $6 AND flow_instance = $7
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(
		&state.AgentID,
		&state.RuntimeEpoch,
		&generation,
		&state.Phase,
		&state.ConfigRevision,
		&state.RunMode,
		&state.ProcessBinding.ProcessAuthorityID,
		&state.ProcessBinding.ProcessOwnerID,
		&state.ProcessBinding.ProcessBootID,
		&state.ProcessBinding.GenerationGrantID,
		&state.ProcessBinding.BundleHash,
		&state.ProcessBinding.BundleSource,
		&state.ProcessBinding.RuntimeInstanceID,
		&state.ProcessBinding.RuntimeGeneration,
		&topologyRaw,
	)
	if err == sql.ErrNoRows {
		return runtimemanager.AgentLifecycleState{}, false, nil
	}
	if err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	state.Identity = identity.Normalize()
	state.Generation = uint64(generation)
	if err := json.Unmarshal(topologyRaw, &state.Topology); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, fmt.Errorf("decode agent topology admission: %w", err)
	}
	if err := state.Topology.Validate(); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	if err := state.ProcessBinding.Validate(); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	return state, true, nil
}

func (s *AgentSQLiteOwner) LoadAgentLifecycleState(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
) (runtimemanager.AgentLifecycleState, bool, error) {
	fields, err := IdentityFields(identity)
	if err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	var state runtimemanager.AgentLifecycleState
	var generation int64
	var topologyRaw []byte
	err = s.backend.QueryRowContext(ctx, `
			SELECT agent_id, lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
			       lifecycle_config_revision, lifecycle_run_mode,
			       lifecycle_process_authority_id, lifecycle_process_owner_id,
			       lifecycle_process_boot_id, lifecycle_generation_grant_id,
			       lifecycle_bundle_hash, lifecycle_bundle_source,
			       lifecycle_runtime_instance_id, lifecycle_runtime_generation,
			       topology_admission
		FROM agents
		WHERE agent_id = ? AND agent_name_owner = ? AND agent_name_source = ?
		  AND agent_route_presence = ? AND flow_scope_key = ?
		  AND flow_instance_id = ? AND flow_instance = ?
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(
		&state.AgentID,
		&state.RuntimeEpoch,
		&generation,
		&state.Phase,
		&state.ConfigRevision,
		&state.RunMode,
		&state.ProcessBinding.ProcessAuthorityID,
		&state.ProcessBinding.ProcessOwnerID,
		&state.ProcessBinding.ProcessBootID,
		&state.ProcessBinding.GenerationGrantID,
		&state.ProcessBinding.BundleHash,
		&state.ProcessBinding.BundleSource,
		&state.ProcessBinding.RuntimeInstanceID,
		&state.ProcessBinding.RuntimeGeneration,
		&topologyRaw,
	)
	if err == sql.ErrNoRows {
		return runtimemanager.AgentLifecycleState{}, false, nil
	}
	if err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	state.Identity = identity.Normalize()
	state.Generation = uint64(generation)
	if err := json.Unmarshal(topologyRaw, &state.Topology); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, fmt.Errorf("decode agent topology admission: %w", err)
	}
	if err := state.Topology.Validate(); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	if err := state.ProcessBinding.Validate(); err != nil {
		return runtimemanager.AgentLifecycleState{}, false, err
	}
	return state, true, nil
}

func (s *AgentPostgresOwner) ListDurableAgentLifecycleStates(ctx context.Context) ([]runtimemanager.AgentLifecycleState, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
		       lifecycle_config_revision, lifecycle_run_mode,
		       lifecycle_process_authority_id::text, lifecycle_process_owner_id,
		       lifecycle_process_boot_id::text, lifecycle_generation_grant_id::text,
		       lifecycle_bundle_hash, lifecycle_bundle_source,
		       lifecycle_runtime_instance_id::text, lifecycle_runtime_generation,
		       topology_authority_kind, topology_admission, execution_lifetime
		FROM agents
		ORDER BY agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		         flow_scope_key, flow_instance_id, flow_instance
	`)
	if err != nil {
		return nil, fmt.Errorf("query durable PostgreSQL lifecycle cell census: %w", err)
	}
	defer rows.Close()
	return scanDurableAgentLifecycleStates(rows)
}

func (s *AgentSQLiteOwner) ListDurableAgentLifecycleStates(ctx context.Context) ([]runtimemanager.AgentLifecycleState, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
		       lifecycle_config_revision, lifecycle_run_mode,
		       lifecycle_process_authority_id, lifecycle_process_owner_id,
		       lifecycle_process_boot_id, lifecycle_generation_grant_id,
		       lifecycle_bundle_hash, lifecycle_bundle_source,
		       lifecycle_runtime_instance_id, lifecycle_runtime_generation,
		       topology_authority_kind, topology_admission, execution_lifetime
		FROM agents
		ORDER BY agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		         flow_scope_key, flow_instance_id, flow_instance
	`)
	if err != nil {
		return nil, fmt.Errorf("query durable SQLite lifecycle cell census: %w", err)
	}
	defer rows.Close()
	return scanDurableAgentLifecycleStates(rows)
}

func scanDurableAgentLifecycleStates(rows *sql.Rows) ([]runtimemanager.AgentLifecycleState, error) {
	states := make([]runtimemanager.AgentLifecycleState, 0)
	for rows.Next() {
		var state runtimemanager.AgentLifecycleState
		var nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string
		var topologyAuthorityKind, executionLifetime string
		var generation int64
		var topologyRaw []byte
		if err := rows.Scan(
			&state.AgentID, &nameOwner, &nameSource, &routePresence,
			&flowScopeKey, &flowInstanceID, &flowInstance,
			&state.RuntimeEpoch, &generation, &state.Phase,
			&state.ConfigRevision, &state.RunMode,
			&state.ProcessBinding.ProcessAuthorityID,
			&state.ProcessBinding.ProcessOwnerID,
			&state.ProcessBinding.ProcessBootID,
			&state.ProcessBinding.GenerationGrantID,
			&state.ProcessBinding.BundleHash,
			&state.ProcessBinding.BundleSource,
			&state.ProcessBinding.RuntimeInstanceID,
			&state.ProcessBinding.RuntimeGeneration,
			&topologyAuthorityKind, &topologyRaw, &executionLifetime,
		); err != nil {
			return nil, fmt.Errorf("scan durable lifecycle cell census: %w", err)
		}
		if generation < 0 {
			return nil, errors.New("durable lifecycle cell generation is negative")
		}
		identity, err := IdentityFromColumns(
			state.AgentID, nameOwner, nameSource, routePresence,
			flowScopeKey, flowInstanceID, flowInstance,
		)
		if err != nil {
			return nil, err
		}
		state.Identity = identity
		state.Generation = uint64(generation)
		if err := json.Unmarshal(topologyRaw, &state.Topology); err != nil {
			return nil, fmt.Errorf("decode durable lifecycle topology admission: %w", err)
		}
		if err := state.Topology.Validate(); err != nil {
			return nil, err
		}
		persistedLifetime := runtimeagenttopology.ExecutionLifetime(strings.TrimSpace(executionLifetime))
		if persistedLifetime != state.Topology.Lifetime {
			return nil, errors.New("durable lifecycle execution lifetime differs from topology admission")
		}
		persistedAuthorityKind := runtimeagenttopology.AuthorityKind(strings.TrimSpace(topologyAuthorityKind))
		if persistedAuthorityKind != state.Topology.Authority.Kind {
			return nil, errors.New("durable lifecycle authority kind differs from topology admission")
		}
		if persistedLifetime != runtimeagenttopology.LifetimeDurableManaged {
			return nil, errors.New("durable lifecycle census returned a non-durable cell")
		}
		switch state.Topology.Authority.Kind {
		case runtimeagenttopology.AuthorityStaticDeclarationPlan, runtimeagenttopology.AuthorityFlowReadinessPlan:
		default:
			return nil, fmt.Errorf("durable lifecycle census returned unsupported authority %q", state.Topology.Authority.Kind)
		}
		if err := state.ProcessBinding.Validate(); err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read durable lifecycle cell census: %w", err)
	}
	return states, nil
}

func (s *AgentPostgresOwner) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT outbox_id::text, operation_id::text,
		       agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       event_name, payload, created_at
		FROM agent_lifecycle_diagnostic_outbox
		WHERE projected_at IS NULL
		ORDER BY created_at, outbox_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgentLifecycleDiagnostics(rows)
}

func (s *AgentSQLiteOwner) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT outbox_id, operation_id,
		       agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       event_name, payload, created_at
		FROM agent_lifecycle_diagnostic_outbox
		WHERE projected_at IS NULL
		ORDER BY created_at, outbox_id
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgentLifecycleDiagnostics(rows)
}

func scanAgentLifecycleDiagnostics(rows *sql.Rows) ([]runtimemanager.AgentLifecycleDiagnostic, error) {
	out := make([]runtimemanager.AgentLifecycleDiagnostic, 0)
	for rows.Next() {
		var item runtimemanager.AgentLifecycleDiagnostic
		var raw []byte
		var rawCreatedAt any
		var nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string
		if err := rows.Scan(
			&item.OutboxID,
			&item.OperationID,
			&item.AgentID,
			&nameOwner,
			&nameSource,
			&routePresence,
			&flowScopeKey,
			&flowInstanceID,
			&flowInstance,
			&item.EventName,
			&raw,
			&rawCreatedAt,
		); err != nil {
			return nil, err
		}
		identity, err := IdentityFromColumns(
			item.AgentID,
			nameOwner,
			nameSource,
			routePresence,
			flowScopeKey,
			flowInstanceID,
			flowInstance,
		)
		if err != nil {
			return nil, err
		}
		item.Identity = identity
		createdAt, _, err := sqliteTime(rawCreatedAt)
		if err != nil {
			return nil, fmt.Errorf("decode lifecycle diagnostic created_at: %w", err)
		}
		item.CreatedAt = createdAt
		if err := json.Unmarshal(raw, &item.Payload); err != nil {
			return nil, fmt.Errorf("decode lifecycle diagnostic payload: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *AgentPostgresOwner) MarkAgentLifecycleDiagnosticProjected(ctx context.Context, outboxID string, at time.Time) error {
	res, err := s.backend.ExecContext(ctx, `UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = $2 WHERE outbox_id = $1::uuid AND projected_at IS NULL`, outboxID, at.UTC())
	return requireSingleLifecycleDiagnosticProjection(res, err)
}

func (s *AgentSQLiteOwner) MarkAgentLifecycleDiagnosticProjected(ctx context.Context, outboxID string, at time.Time) error {
	return s.backend.RunTransaction(ctx, "sqlite mark lifecycle diagnostic projected", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = ? WHERE outbox_id = ? AND projected_at IS NULL`, at.UTC(), outboxID)
		return requireSingleLifecycleDiagnosticProjection(res, err)
	})
}

func requireSingleLifecycleDiagnosticProjection(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("lifecycle diagnostic projection conflict")
	}
	return nil
}

func (s *AgentPostgresOwner) CommitAgentLifecycleTransitionTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if tx == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("PostgreSQL agent lifecycle transaction is required")
	}
	req, err := normalizeLifecycleTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result, err := commitPostgresAgentLifecycleTransitionTx(ctx, tx, story, req, s.providerDrains)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if _, err := privaterunforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := story.Finalize(ctx); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func (s *AgentSQLiteOwner) CommitAgentLifecycleTransitionTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if tx == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("SQLite agent lifecycle transaction is required")
	}
	req, err := normalizeLifecycleTransition(req)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectSQLite)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result, err := commitSQLiteAgentLifecycleTransitionTx(ctx, tx, story, req, s.providerDrains)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := story.Finalize(ctx); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func commitPostgresAgentLifecycleTransitionTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, req runtimemanager.AgentLifecycleTransition, drains ProviderAttemptDrainPostgresCapturer) (runtimemanager.AgentLifecycleTransitionResult, error) {
	fingerprint, err := req.Identity.Fingerprint()
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "swarm:agent-lifecycle:"+fingerprint); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	previous, exists, err := loadPostgresLifecycleCell(ctx, tx, req.Identity)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := AuthorizePostgresAgentTopologyMutation(ctx, tx, req); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if result, ok, err := loadPostgresLifecycleOperationResult(ctx, tx, req); err != nil || ok {
		return result, err
	}
	if err := rejectPostgresPendingDrainTransition(ctx, tx, req, previous, exists); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := validateLifecycleExpectation(req, previous, exists); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result := lifecycleResult(req, previous, exists)
	if err := captureLifecycleProviderDrainsPostgres(ctx, tx, story, drains, req, previous, exists, &result); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result.Subordinate, err = applyPostgresLifecycleSubordinate(ctx, tx, req)
	if err == nil {
		err = applyPostgresLifecycleCell(ctx, tx, req, result)
	}
	if err == nil {
		err = insertPostgresLifecycleEvidence(ctx, tx, req, result)
	}
	if err == nil {
		err = story.Record(ctx, agentLifecycleAuthorActivityDraft(req, result))
	}
	return result, err
}

func commitSQLiteAgentLifecycleTransitionTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, req runtimemanager.AgentLifecycleTransition, drains ProviderAttemptDrainSQLiteCapturer) (runtimemanager.AgentLifecycleTransitionResult, error) {
	previous, exists, err := loadSQLiteLifecycleCell(ctx, tx, req.Identity)
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := AuthorizeSQLiteAgentTopologyMutation(ctx, tx, req); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if result, ok, err := loadSQLiteLifecycleOperationResult(ctx, tx, req); err != nil || ok {
		return result, err
	}
	if err := rejectSQLitePendingDrainTransition(ctx, tx, req, previous, exists); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if err := validateLifecycleExpectation(req, previous, exists); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result := lifecycleResult(req, previous, exists)
	if err := captureLifecycleProviderDrainsSQLite(ctx, tx, story, drains, req, previous, exists, &result); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	result.Subordinate, err = applySQLiteLifecycleSubordinate(ctx, tx, req)
	if err == nil {
		err = applySQLiteLifecycleCellTx(ctx, tx, req, result)
	}
	if err == nil {
		err = insertSQLiteLifecycleEvidenceTx(ctx, tx, req, result)
	}
	if err == nil {
		err = story.Record(ctx, agentLifecycleAuthorActivityDraft(req, result))
	}
	return result, err
}

func rejectPostgresPendingDrainTransition(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) error {
	if !exists || previous.Phase != runtimemanager.AgentLifecycleDraining || req.TargetPhase == runtimemanager.AgentLifecycleDraining {
		return nil
	}
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	var pending bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runtime_provider_attempt_drains WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3 AND agent_route_presence=$4 AND flow_scope_key=$5 AND flow_instance_id=$6 AND flow_instance=$7 AND successor_runtime_epoch=$8 AND successor_generation=$9 AND state='pending')`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, previous.Epoch, previous.Generation).Scan(&pending)
	if err != nil || !pending {
		return err
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_transition_blocked", "agent-lifecycle-store", req.OperationKind, map[string]any{"agent_id": req.AgentID, "runtime_epoch": previous.Epoch, "generation": previous.Generation, "target_phase": req.TargetPhase})
}

func rejectSQLitePendingDrainTransition(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) error {
	if !exists || previous.Phase != runtimemanager.AgentLifecycleDraining || req.TargetPhase == runtimemanager.AgentLifecycleDraining {
		return nil
	}
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	var pending int
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runtime_provider_attempt_drains WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=? AND successor_runtime_epoch=? AND successor_generation=? AND state='pending')`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, previous.Epoch, previous.Generation).Scan(&pending)
	if err != nil || pending == 0 {
		return err
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_transition_blocked", "agent-lifecycle-store", req.OperationKind, map[string]any{"agent_id": req.AgentID, "runtime_epoch": previous.Epoch, "generation": previous.Generation, "target_phase": req.TargetPhase})
}

func (s *AgentPostgresOwner) runPostgresLifecycleMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.CaptureCurrentTransaction(txctx, tx); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *AgentSQLiteOwner) runSQLiteLifecycleMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, "sqlite commit agent lifecycle transition", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func agentLifecycleAuthorActivityDraft(req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) runtimeauthoractivity.Draft {
	previousGeneration := result.PreviousGeneration
	nextGeneration := result.Generation
	return runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindAgentLifecycle, Transition: string(result.Phase),
		SourceOwner: "agent_lifecycle_transition_facts", SourceIdentity: result.TransitionID,
		DedupKey: "agent-transition:" + result.TransitionID, OccurredAt: req.Now.UTC(), AgentID: result.AgentID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "agent", SubjectID: result.AgentID, PreviousPhase: string(result.PreviousPhase),
			NextPhase: string(result.Phase), PreviousGeneration: &previousGeneration, NextGeneration: &nextGeneration,
			RunMode: string(result.RunMode),
		},
	}
}

type lifecycleCell struct {
	Epoch      int64
	Generation uint64
	Phase      runtimemanager.AgentLifecyclePhase
	Binding    runtimemanager.ProcessExecutionBinding
}

func normalizeLifecycleTransition(req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransition, error) {
	req.Identity = req.Identity.Normalize()
	if err := req.Identity.Validate(); err != nil {
		return runtimemanager.AgentLifecycleTransition{}, err
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID != req.Identity.AgentID() {
		return runtimemanager.AgentLifecycleTransition{}, fmt.Errorf("lifecycle transition agent_id disagrees with identity")
	}
	if req.Agent != nil {
		agentIdentity, err := req.Agent.Config.ConcreteIdentity()
		if err != nil {
			return runtimemanager.AgentLifecycleTransition{}, err
		}
		if agentIdentity != req.Identity {
			return runtimemanager.AgentLifecycleTransition{}, fmt.Errorf("lifecycle transition agent config identity changed")
		}
		if !req.Agent.Topology.Equal(req.Topology) {
			return runtimemanager.AgentLifecycleTransition{}, fmt.Errorf("lifecycle transition agent topology changed")
		}
	}
	if err := req.Topology.Validate(); err != nil {
		return runtimemanager.AgentLifecycleTransition{}, fmt.Errorf("lifecycle topology admission: %w", err)
	}
	plan, err := req.Subordinate.Normalize()
	if err != nil {
		return runtimemanager.AgentLifecycleTransition{}, err
	}
	req.Subordinate = plan
	if err := validateLifecycleTransition(req); err != nil {
		return runtimemanager.AgentLifecycleTransition{}, err
	}
	return req, nil
}

func validateLifecycleTransition(req runtimemanager.AgentLifecycleTransition) error {
	for name, value := range map[string]string{
		"operation_id": req.OperationID, "operation_kind": req.OperationKind, "request_hash": req.RequestHash,
		"agent_id": req.AgentID, "trigger": req.Trigger, "config_revision": req.ConfigRevision,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if _, err := uuid.Parse(req.OperationID); err != nil {
		return fmt.Errorf("operation_id must be a UUID: %w", err)
	}
	if req.TargetEpoch <= 0 || req.TargetGeneration == 0 || req.TargetPhase == "" || req.RunMode == "" {
		return fmt.Errorf("complete target lifecycle state is required")
	}
	if err := req.ProcessBinding.Validate(); err != nil {
		return fmt.Errorf("lifecycle process binding: %w", err)
	}
	if req.Now.IsZero() {
		return fmt.Errorf("lifecycle transition time is required")
	}
	return nil
}

func validateLifecycleExpectation(req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) error {
	if !exists {
		if req.OperationKind == "spawn" && req.ExpectedGeneration == 0 && req.ExpectedPhase == "" {
			return nil
		}
		return lifecycleConflict(req, previous, false)
	}
	if previous.Epoch != req.ExpectedEpoch || previous.Generation != req.ExpectedGeneration || previous.Phase != req.ExpectedPhase {
		return lifecycleConflict(req, previous, true)
	}
	switch req.OperationKind {
	case "process_takeover":
		if previous.Binding.Equal(req.ProcessBinding) {
			return lifecycleConflict(req, previous, true)
		}
	case "source_set_rebind", "source_set_retire":
		if previous.Binding.ProcessAuthorityID != req.ProcessBinding.ProcessAuthorityID ||
			previous.Binding.ProcessOwnerID != req.ProcessBinding.ProcessOwnerID ||
			previous.Binding.ProcessBootID != req.ProcessBinding.ProcessBootID {
			return lifecycleConflict(req, previous, true)
		}
	default:
		if !previous.Binding.Equal(req.ProcessBinding) {
			return lifecycleConflict(req, previous, true)
		}
	}
	return nil
}

func lifecycleConflict(req runtimemanager.AgentLifecycleTransition, current lifecycleCell, exists bool) error {
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle-store", req.OperationKind, map[string]any{
		"agent_id": req.AgentID, "expected_epoch": req.ExpectedEpoch, "expected_generation": req.ExpectedGeneration,
		"expected_phase": req.ExpectedPhase, "current_exists": exists, "current_epoch": current.Epoch,
		"current_generation": current.Generation, "current_phase": current.Phase,
	})
}

func lifecycleResult(req runtimemanager.AgentLifecycleTransition, previous lifecycleCell, exists bool) runtimemanager.AgentLifecycleTransitionResult {
	previousPhase := runtimemanager.AgentLifecyclePhase("absent")
	if exists {
		previousPhase = previous.Phase
	}
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: previous.Epoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: previous.Generation, Generation: req.TargetGeneration,
		PreviousPhase: previousPhase, Phase: req.TargetPhase, ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
		Topology: req.Topology, ProcessBinding: req.ProcessBinding,
		Subordinate: runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action},
	}
}

const providerAttemptDrainLifetime = 5 * time.Minute

func captureLifecycleProviderDrainsPostgres(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	drains ProviderAttemptDrainPostgresCapturer,
	req runtimemanager.AgentLifecycleTransition,
	previous lifecycleCell,
	exists bool,
	result *runtimemanager.AgentLifecycleTransitionResult,
) error {
	capture, ok, err := lifecycleProviderDrainCapture(req, previous, exists, result)
	if err != nil || !ok {
		return err
	}
	if drains == nil {
		return fmt.Errorf("agent lifecycle PostgreSQL provider-drain owner is not bound")
	}
	captured, err := drains.CaptureProviderAttemptDrainsPostgresTx(ctx, tx, story, capture)
	if err != nil {
		return err
	}
	applyLifecycleProviderDrainResult(req, captured, result)
	return nil
}

func captureLifecycleProviderDrainsSQLite(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	drains ProviderAttemptDrainSQLiteCapturer,
	req runtimemanager.AgentLifecycleTransition,
	previous lifecycleCell,
	exists bool,
	result *runtimemanager.AgentLifecycleTransitionResult,
) error {
	capture, ok, err := lifecycleProviderDrainCapture(req, previous, exists, result)
	if err != nil || !ok {
		return err
	}
	if drains == nil {
		return fmt.Errorf("agent lifecycle SQLite provider-drain owner is not bound")
	}
	captured, err := drains.CaptureProviderAttemptDrainsSQLiteTx(ctx, tx, story, capture)
	if err != nil {
		return err
	}
	applyLifecycleProviderDrainResult(req, captured, result)
	return nil
}

func lifecycleProviderDrainCapture(
	req runtimemanager.AgentLifecycleTransition,
	previous lifecycleCell,
	exists bool,
	result *runtimemanager.AgentLifecycleTransitionResult,
) (runtimeeffects.ProviderAttemptDrainCapture, bool, error) {
	if !exists || previous.Phase != runtimemanager.AgentLifecycleRunning || req.TargetGeneration <= previous.Generation {
		return runtimeeffects.ProviderAttemptDrainCapture{}, false, nil
	}
	var target runtimeeffects.ProviderDrainTarget
	switch req.TargetPhase {
	case runtimemanager.AgentLifecycleRunning:
		target = runtimeeffects.ProviderDrainTargetRunning
	case runtimemanager.AgentLifecycleTerminated:
		target = runtimeeffects.ProviderDrainTargetTerminated
	case runtimemanager.AgentLifecycleFailed:
		target = runtimeeffects.ProviderDrainTargetFailed
	default:
		return runtimeeffects.ProviderAttemptDrainCapture{}, false, fmt.Errorf("lifecycle supersession target %q cannot own provider drains", req.TargetPhase)
	}
	capture := runtimeeffects.ProviderAttemptDrainCapture{
		Predecessor: runtimeeffects.LifecycleToken{
			RuntimeEpoch: previous.Epoch,
			Identity:     req.Identity,
			AgentID:      req.AgentID,
			Generation:   previous.Generation,
		},
		SuccessorRuntimeEpoch: req.TargetEpoch,
		SuccessorGeneration:   req.TargetGeneration,
		Target:                target,
		LifecycleOperationID:  req.OperationID,
		LifecycleTransitionID: result.TransitionID,
		CapturedAt:            req.Now.UTC(),
		ExpiresAt:             req.Now.UTC().Add(providerAttemptDrainLifetime),
	}
	return capture, true, capture.Validate()
}

func applyLifecycleProviderDrainResult(
	req runtimemanager.AgentLifecycleTransition,
	captured runtimeeffects.ProviderAttemptDrainCaptureResult,
	result *runtimemanager.AgentLifecycleTransitionResult,
) {
	result.ProviderDrainCount = captured.Captured
	if captured.Captured == 0 {
		return
	}
	result.ProviderDrainTarget = req.TargetPhase
	if req.TargetPhase == runtimemanager.AgentLifecycleTerminated || req.TargetPhase == runtimemanager.AgentLifecycleFailed {
		result.Phase = runtimemanager.AgentLifecycleDraining
	}
}

type lifecycleSessionRow struct {
	SessionID    string
	RunID        string
	FlowInstance string
	Status       string
}

func applyPostgresLifecycleSubordinate(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimesessions.LifecycleMutationOutcome, error) {
	outcome := runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action}
	if req.Subordinate.Action == runtimesessions.LifecycleMutationNone {
		return outcome, nil
	}
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return outcome, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id::text, run_id::text, flow_instance, status
		FROM agent_sessions
		WHERE agent_id = $1 AND agent_name_owner = $2 AND agent_name_source = $3
		  AND agent_route_presence = $4 AND flow_scope_key = $5
		  AND flow_instance_id = $6 AND flow_instance = $7
		  AND status IN ('active', 'suspended')
		ORDER BY session_id
		FOR UPDATE
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return outcome, fmt.Errorf("lock lifecycle subordinate session set: %w", err)
	}
	defer rows.Close()
	var selected []lifecycleSessionRow
	for rows.Next() {
		var row lifecycleSessionRow
		if err := rows.Scan(&row.SessionID, &row.RunID, &row.FlowInstance, &row.Status); err != nil {
			return outcome, err
		}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		return outcome, err
	}
	if err := rows.Close(); err != nil {
		return outcome, err
	}
	for _, row := range selected {
		mutation, err := applyPostgresLifecycleSessionMutation(ctx, tx, req, row)
		if err != nil {
			return outcome, err
		}
		outcome.Sessions = append(outcome.Sessions, mutation)
	}
	return outcome, nil
}

func applyPostgresLifecycleSessionMutation(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, row lifecycleSessionRow) (runtimesessions.LifecycleSessionMutation, error) {
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return runtimesessions.LifecycleSessionMutation{}, err
	}
	mutation := runtimesessions.LifecycleSessionMutation{
		PreviousSessionID: row.SessionID, RunID: row.RunID, FlowInstance: row.FlowInstance, PreviousStatus: row.Status,
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated', termination_reason = $2, termination_detail = NULLIF($3, ''),
		    successor_session_id = NULL, terminated_at = $4, lease_holder = NULL, lease_expires_at = NULL, updated_at = $4
		WHERE session_id = $1::uuid AND status IN ('active', 'suspended')
	`, row.SessionID, req.Subordinate.TerminationReason.String(), req.Subordinate.TerminationDetail, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("terminate lifecycle subordinate session %s: %w", row.SessionID, err)
	}
	if req.Subordinate.Action != runtimesessions.LifecycleMutationRotateCurrentSet {
		return mutation, nil
	}
	mutation.SuccessorSessionID = runtimesessions.LifecycleSuccessorSessionID(req.OperationID, row.SessionID)
	mutation.SuccessorStatus = row.Status
	runtimeState, err := json.Marshal(map[string]any{
		"summary": req.Subordinate.CheckpointSummary, "retries_from_session_id": row.SessionID,
		"rotation_operation_id": req.OperationID,
	})
	if err != nil {
		return mutation, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			conversation, turn_count, runtime_state, lease_holder, lease_expires_at,
			status, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'[]'::jsonb, 0, $10::jsonb, NULL, NULL, $11, $12, $12
		)
	`, mutation.SuccessorSessionID, row.RunID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, string(runtimeState),
		row.Status, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("insert lifecycle subordinate successor for %s: %w", row.SessionID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id = $2::uuid, updated_at = $3 WHERE session_id = $1::uuid AND status = 'terminated'`, row.SessionID, mutation.SuccessorSessionID, req.Now.UTC()); err != nil {
		return mutation, fmt.Errorf("link lifecycle subordinate successor for %s: %w", row.SessionID, err)
	}
	return mutation, nil
}

func applySQLiteLifecycleSubordinate(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimesessions.LifecycleMutationOutcome, error) {
	outcome := runtimesessions.LifecycleMutationOutcome{Action: req.Subordinate.Action}
	if req.Subordinate.Action == runtimesessions.LifecycleMutationNone {
		return outcome, nil
	}
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return outcome, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id, run_id, flow_instance, status
		FROM agent_sessions
		WHERE agent_id = ? AND agent_name_owner = ? AND agent_name_source = ?
		  AND agent_route_presence = ? AND flow_scope_key = ?
		  AND flow_instance_id = ? AND flow_instance = ?
		  AND status IN ('active', 'suspended')
		ORDER BY session_id
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return outcome, fmt.Errorf("lock sqlite lifecycle subordinate session set: %w", err)
	}
	var selected []lifecycleSessionRow
	for rows.Next() {
		var row lifecycleSessionRow
		if err := rows.Scan(&row.SessionID, &row.RunID, &row.FlowInstance, &row.Status); err != nil {
			_ = rows.Close()
			return outcome, err
		}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return outcome, err
	}
	if err := rows.Close(); err != nil {
		return outcome, err
	}
	for _, row := range selected {
		mutation := runtimesessions.LifecycleSessionMutation{
			PreviousSessionID: row.SessionID, RunID: row.RunID, FlowInstance: row.FlowInstance, PreviousStatus: row.Status,
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE agent_sessions
			SET status = 'terminated', termination_reason = ?, termination_detail = ?, successor_session_id = NULL,
			    terminated_at = ?, lease_holder = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE session_id = ? AND status IN ('active', 'suspended')
		`, req.Subordinate.TerminationReason.String(), nullString(req.Subordinate.TerminationDetail), req.Now.UTC(), req.Now.UTC(), row.SessionID); err != nil {
			return outcome, fmt.Errorf("terminate sqlite lifecycle subordinate session %s: %w", row.SessionID, err)
		}
		if req.Subordinate.Action == runtimesessions.LifecycleMutationRotateCurrentSet {
			mutation.SuccessorSessionID = runtimesessions.LifecycleSuccessorSessionID(req.OperationID, row.SessionID)
			mutation.SuccessorStatus = row.Status
			runtimeState, err := json.Marshal(map[string]any{
				"summary": req.Subordinate.CheckpointSummary, "retries_from_session_id": row.SessionID,
				"rotation_operation_id": req.OperationID,
			})
			if err != nil {
				return outcome, err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO agent_sessions (
					session_id, run_id, agent_id, agent_name_owner, agent_name_source,
					agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
					memory_enabled, memory_source,
					conversation, turn_count, runtime_state, lease_holder, lease_expires_at,
					status, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', 0, ?, NULL, NULL, ?, ?, ?)
			`, mutation.SuccessorSessionID, row.RunID, fields.AgentID, fields.NameOwner,
				fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
				fields.FlowInstanceID, fields.FlowInstancePath,
				string(runtimeState), row.Status, req.Now.UTC(), req.Now.UTC()); err != nil {
				return outcome, fmt.Errorf("insert sqlite lifecycle subordinate successor for %s: %w", row.SessionID, err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id = ?, updated_at = ? WHERE session_id = ? AND status = 'terminated'`, mutation.SuccessorSessionID, req.Now.UTC(), row.SessionID); err != nil {
				return outcome, fmt.Errorf("link sqlite lifecycle subordinate successor for %s: %w", row.SessionID, err)
			}
		}
		outcome.Sessions = append(outcome.Sessions, mutation)
	}
	return outcome, nil
}

func loadPostgresLifecycleCell(
	ctx context.Context,
	tx *sql.Tx,
	identity runtimeagentidentity.Identity,
) (lifecycleCell, bool, error) {
	fields, err := IdentityFields(identity)
	if err != nil {
		return lifecycleCell{}, false, err
	}
	var cell lifecycleCell
	var generation int64
	err = tx.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
			       lifecycle_process_authority_id::text, lifecycle_process_owner_id,
			       lifecycle_process_boot_id::text, lifecycle_generation_grant_id::text
			       , lifecycle_bundle_hash, lifecycle_bundle_source,
			       lifecycle_runtime_instance_id::text, lifecycle_runtime_generation
		FROM agents
		WHERE agent_id = $1 AND agent_name_owner = $2 AND agent_name_source = $3
		  AND agent_route_presence = $4 AND flow_scope_key = $5
		  AND flow_instance_id = $6 AND flow_instance = $7
		FOR UPDATE
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(
		&cell.Epoch,
		&generation,
		&cell.Phase,
		&cell.Binding.ProcessAuthorityID,
		&cell.Binding.ProcessOwnerID,
		&cell.Binding.ProcessBootID,
		&cell.Binding.GenerationGrantID,
		&cell.Binding.BundleHash,
		&cell.Binding.BundleSource,
		&cell.Binding.RuntimeInstanceID,
		&cell.Binding.RuntimeGeneration,
	)
	if err == sql.ErrNoRows {
		return lifecycleCell{}, false, nil
	}
	if err != nil {
		return lifecycleCell{}, false, err
	}
	cell.Generation = uint64(generation)
	return cell, true, nil
}

func loadSQLiteLifecycleCell(
	ctx context.Context,
	tx *sql.Tx,
	identity runtimeagentidentity.Identity,
) (lifecycleCell, bool, error) {
	fields, err := IdentityFields(identity)
	if err != nil {
		return lifecycleCell{}, false, err
	}
	var cell lifecycleCell
	var generation int64
	err = tx.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase,
			       lifecycle_process_authority_id, lifecycle_process_owner_id,
			       lifecycle_process_boot_id, lifecycle_generation_grant_id
			       , lifecycle_bundle_hash, lifecycle_bundle_source,
			       lifecycle_runtime_instance_id, lifecycle_runtime_generation
		FROM agents
		WHERE agent_id = ? AND agent_name_owner = ? AND agent_name_source = ?
		  AND agent_route_presence = ? AND flow_scope_key = ?
		  AND flow_instance_id = ? AND flow_instance = ?
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(
		&cell.Epoch,
		&generation,
		&cell.Phase,
		&cell.Binding.ProcessAuthorityID,
		&cell.Binding.ProcessOwnerID,
		&cell.Binding.ProcessBootID,
		&cell.Binding.GenerationGrantID,
		&cell.Binding.BundleHash,
		&cell.Binding.BundleSource,
		&cell.Binding.RuntimeInstanceID,
		&cell.Binding.RuntimeGeneration,
	)
	if err == sql.ErrNoRows {
		return lifecycleCell{}, false, nil
	}
	if err != nil {
		return lifecycleCell{}, false, err
	}
	cell.Generation = uint64(generation)
	return cell, true, nil
}

func loadPostgresLifecycleOperationResult(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	var requestHash string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_hash, result FROM agent_lifecycle_operations WHERE operation_id = $1::uuid`, req.OperationID).Scan(&requestHash, &raw)
	return decodeLifecycleOperationResult(req, requestHash, raw, err)
}

func loadSQLiteLifecycleOperationResult(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	var requestHash string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_hash, result FROM agent_lifecycle_operations WHERE operation_id = ?`, req.OperationID).Scan(&requestHash, &raw)
	return decodeLifecycleOperationResult(req, requestHash, raw, err)
}

func decodeLifecycleOperationResult(req runtimemanager.AgentLifecycleTransition, requestHash string, raw []byte, err error) (runtimemanager.AgentLifecycleTransitionResult, bool, error) {
	if err == sql.ErrNoRows {
		return runtimemanager.AgentLifecycleTransitionResult{}, false, nil
	}
	if err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, false, err
	}
	if requestHash != req.RequestHash {
		return runtimemanager.AgentLifecycleTransitionResult{}, true, runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "lifecycle_operation_request_conflict", "agent-lifecycle-store", req.OperationKind, map[string]any{"operation_id": req.OperationID})
	}
	var result runtimemanager.AgentLifecycleTransitionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, true, fmt.Errorf("decode lifecycle operation result: %w", err)
	}
	if result.Identity.Normalize() != req.Identity {
		return runtimemanager.AgentLifecycleTransitionResult{}, true, runtimefailures.New(
			runtimefailures.ClassConflictingDuplicate,
			"lifecycle_operation_identity_conflict",
			"agent-lifecycle-store",
			req.OperationKind,
			map[string]any{"operation_id": req.OperationID},
		)
	}
	result.Replayed = true
	return result, true, nil
}

func applyPostgresLifecycleCell(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	if req.Agent != nil {
		topologyRaw, err := canonicaljson.Bytes(req.Topology)
		if err != nil {
			return err
		}
		projection, err := ProjectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
		if err != nil {
			return err
		}
		startedAt := req.Agent.StartedAt
		if startedAt.IsZero() {
			startedAt = req.Now
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO agents (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				role, model, llm_backend, memory_enabled, memory_source, parent_agent_id, entity_id,
				config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, turn_count, last_active_at, created_at,
					lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_config_revision, lifecycle_run_mode, lifecycle_last_transition_id,
					lifecycle_process_authority_id, lifecycle_process_owner_id, lifecycle_process_boot_id, lifecycle_generation_grant_id,
					lifecycle_bundle_hash, lifecycle_bundle_source, lifecycle_runtime_instance_id, lifecycle_runtime_generation,
					topology_authority_kind, topology_admission, execution_lifetime)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13,''), NULLIF($14,'')::uuid,
				$15::jsonb, $16::jsonb, $17::jsonb, $18::jsonb, $19::jsonb, $20::jsonb, $21, 0, $22, $23,
					$24, $25, $26, $27, $28, $29::uuid, $30::uuid, $31, $32::uuid, $33::uuid,
					$34, $35, $36::uuid, $37, $38, $39::jsonb, $40)
			ON CONFLICT (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance
			) DO UPDATE SET role=EXCLUDED.role, model=EXCLUDED.model,
				llm_backend=EXCLUDED.llm_backend, memory_enabled=EXCLUDED.memory_enabled, memory_source=EXCLUDED.memory_source, parent_agent_id=EXCLUDED.parent_agent_id,
				entity_id=EXCLUDED.entity_id, config=EXCLUDED.config, subscriptions=EXCLUDED.subscriptions, emit_events=EXCLUDED.emit_events,
				tools=EXCLUDED.tools, permissions=EXCLUDED.permissions, runtime_descriptor=EXCLUDED.runtime_descriptor, status=EXCLUDED.status,
				last_active_at=EXCLUDED.last_active_at, lifecycle_phase=EXCLUDED.lifecycle_phase,
				lifecycle_generation=EXCLUDED.lifecycle_generation, lifecycle_runtime_epoch=EXCLUDED.lifecycle_runtime_epoch,
					lifecycle_config_revision=EXCLUDED.lifecycle_config_revision, lifecycle_run_mode=EXCLUDED.lifecycle_run_mode,
					lifecycle_last_transition_id=EXCLUDED.lifecycle_last_transition_id,
					lifecycle_process_authority_id=EXCLUDED.lifecycle_process_authority_id,
					lifecycle_process_owner_id=EXCLUDED.lifecycle_process_owner_id,
					lifecycle_process_boot_id=EXCLUDED.lifecycle_process_boot_id,
					lifecycle_generation_grant_id=EXCLUDED.lifecycle_generation_grant_id,
					lifecycle_bundle_hash=EXCLUDED.lifecycle_bundle_hash,
					lifecycle_bundle_source=EXCLUDED.lifecycle_bundle_source,
					lifecycle_runtime_instance_id=EXCLUDED.lifecycle_runtime_instance_id,
					lifecycle_runtime_generation=EXCLUDED.lifecycle_runtime_generation,
				topology_authority_kind=EXCLUDED.topology_authority_kind, topology_admission=EXCLUDED.topology_admission,
				execution_lifetime=EXCLUDED.execution_lifetime
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			projection.Role, projection.Model, projection.LLMBackend, projection.MemoryEnabled, projection.MemorySource,
			projection.ParentAgentID, projection.EntityID, string(projection.ConfigJSON), string(projection.SubscriptionsJSON), string(projection.EmitEventsJSON),
			string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor), lifecycleAgentStatus(req), req.Now.UTC(), startedAt.UTC(),
			string(result.Phase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID,
			req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
			req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
			req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
			req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
			string(req.Topology.Authority.Kind), string(topologyRaw), string(req.Topology.Lifetime))
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE agents
		SET status=$8, lifecycle_phase=$9, lifecycle_generation=$10,
			    lifecycle_runtime_epoch=$11, lifecycle_config_revision=$12,
			    lifecycle_run_mode=$13, lifecycle_last_transition_id=$14::uuid,
			    lifecycle_process_authority_id=$15::uuid, lifecycle_process_owner_id=$16,
			    lifecycle_process_boot_id=$17::uuid, lifecycle_generation_grant_id=$18::uuid,
			    lifecycle_bundle_hash=$19, lifecycle_bundle_source=$20,
			    lifecycle_runtime_instance_id=$21::uuid, lifecycle_runtime_generation=$22,
			    last_active_at=$23, topology_authority_kind=$24, topology_admission=$25::jsonb, execution_lifetime=$26
		WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
		  AND agent_route_presence=$4 AND flow_scope_key=$5
		  AND flow_instance_id=$6 AND flow_instance=$7
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		lifecycleAgentStatus(req), string(result.Phase), req.TargetGeneration,
		req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID,
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		req.Now.UTC(), string(req.Topology.Authority.Kind), mustTopologyJSON(req.Topology), string(req.Topology.Lifetime))
	return err
}

func applySQLiteLifecycleCellTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	if req.Agent != nil {
		topologyRaw, err := canonicaljson.Bytes(req.Topology)
		if err != nil {
			return err
		}
		projection, err := ProjectPersistedAgentConfig(req.Agent.Config, req.Agent.ParentAgentID)
		if err != nil {
			return err
		}
		startedAt := req.Agent.StartedAt
		if startedAt.IsZero() {
			startedAt = req.Now
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO agents (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				role, model, llm_backend, memory_enabled, memory_source, parent_agent_id, entity_id,
				config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, turn_count, last_active_at, created_at,
					lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_config_revision, lifecycle_run_mode, lifecycle_last_transition_id,
					lifecycle_process_authority_id, lifecycle_process_owner_id, lifecycle_process_boot_id, lifecycle_generation_grant_id,
					lifecycle_bundle_hash, lifecycle_bundle_source, lifecycle_runtime_instance_id, lifecycle_runtime_generation,
					topology_authority_kind, topology_admission, execution_lifetime)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance
			) DO UPDATE SET role=excluded.role, model=excluded.model,
				llm_backend=excluded.llm_backend, memory_enabled=excluded.memory_enabled, memory_source=excluded.memory_source, parent_agent_id=excluded.parent_agent_id,
				entity_id=excluded.entity_id, config=excluded.config, subscriptions=excluded.subscriptions, emit_events=excluded.emit_events,
				tools=excluded.tools, permissions=excluded.permissions, runtime_descriptor=excluded.runtime_descriptor, status=excluded.status,
				last_active_at=excluded.last_active_at, lifecycle_phase=excluded.lifecycle_phase,
				lifecycle_generation=excluded.lifecycle_generation, lifecycle_runtime_epoch=excluded.lifecycle_runtime_epoch,
					lifecycle_config_revision=excluded.lifecycle_config_revision, lifecycle_run_mode=excluded.lifecycle_run_mode,
					lifecycle_last_transition_id=excluded.lifecycle_last_transition_id,
					lifecycle_process_authority_id=excluded.lifecycle_process_authority_id,
					lifecycle_process_owner_id=excluded.lifecycle_process_owner_id,
					lifecycle_process_boot_id=excluded.lifecycle_process_boot_id,
					lifecycle_generation_grant_id=excluded.lifecycle_generation_grant_id,
					lifecycle_bundle_hash=excluded.lifecycle_bundle_hash,
					lifecycle_bundle_source=excluded.lifecycle_bundle_source,
					lifecycle_runtime_instance_id=excluded.lifecycle_runtime_instance_id,
					lifecycle_runtime_generation=excluded.lifecycle_runtime_generation,
				topology_authority_kind=excluded.topology_authority_kind, topology_admission=excluded.topology_admission,
				execution_lifetime=excluded.execution_lifetime
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			projection.Role, projection.Model, projection.LLMBackend, projection.MemoryEnabled, projection.MemorySource,
			nullString(projection.ParentAgentID), nullUUID(projection.EntityID), string(projection.ConfigJSON), string(projection.SubscriptionsJSON),
			string(projection.EmitEventsJSON), string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor), lifecycleAgentStatus(req),
			req.Now.UTC(), startedAt.UTC(), string(result.Phase), req.TargetGeneration, req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID,
			req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
			req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
			req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
			req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
			string(req.Topology.Authority.Kind), string(topologyRaw), string(req.Topology.Lifetime))
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE agents
		SET status=?, lifecycle_phase=?, lifecycle_generation=?,
			    lifecycle_runtime_epoch=?, lifecycle_config_revision=?,
			    lifecycle_run_mode=?, lifecycle_last_transition_id=?, last_active_at=?,
			    lifecycle_process_authority_id=?, lifecycle_process_owner_id=?,
			    lifecycle_process_boot_id=?, lifecycle_generation_grant_id=?,
			    lifecycle_bundle_hash=?, lifecycle_bundle_source=?,
			    lifecycle_runtime_instance_id=?, lifecycle_runtime_generation=?,
			    topology_authority_kind=?, topology_admission=?, execution_lifetime=?
		WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=?
		  AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=?
	`, lifecycleAgentStatus(req), string(result.Phase), req.TargetGeneration,
		req.TargetEpoch, req.ConfigRevision, string(req.RunMode), result.TransitionID,
		req.Now.UTC(), req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		string(req.Topology.Authority.Kind), mustTopologyJSON(req.Topology), string(req.Topology.Lifetime),
		fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID,
		fields.FlowInstancePath)
	return err
}

func mustTopologyJSON(topology runtimeagenttopology.Admission) string {
	raw, err := canonicaljson.Bytes(topology)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func lifecycleAgentStatus(req runtimemanager.AgentLifecycleTransition) string {
	switch req.TargetPhase {
	case runtimemanager.AgentLifecycleTerminated:
		return "terminated"
	case runtimemanager.AgentLifecycleFailed:
		return "failed"
	default:
		if req.Agent != nil && strings.TrimSpace(req.Agent.Status) != "" {
			return agentPersistedStatus(req.Agent.Status)
		}
		return "active"
	}
}

func insertPostgresLifecycleEvidence(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	raw, _ := json.Marshal(result)
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_operations (
			operation_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			operation_kind, request_hash, expected_epoch, expected_generation,
			target_generation, target_phase, config_revision, run_mode,
			process_authority_id, process_owner_id, process_boot_id, generation_grant_id,
			bundle_hash, bundle_source, runtime_instance_id, runtime_generation,
			state, result, created_at, updated_at, completed_at
		) VALUES (
			$1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17::uuid,$18,$19::uuid,$20::uuid,$21,$22,$23::uuid,$24,'succeeded',$25::jsonb,$26,$26,$26
		)
	`, req.OperationID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID,
		fields.FlowInstancePath, req.OperationKind, req.RequestHash,
		req.ExpectedEpoch, req.ExpectedGeneration, req.TargetGeneration,
		string(req.TargetPhase), req.ConfigRevision, string(req.RunMode),
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		string(raw), req.Now.UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_transition_facts (
			transition_id, operation_id, agent_id, agent_name_owner,
			agent_name_source, agent_route_presence, flow_scope_key,
			flow_instance_id, flow_instance, trigger, previous_phase, next_phase,
				previous_generation, next_generation, runtime_epoch, config_revision,
				run_mode, process_authority_id, process_owner_id, process_boot_id,
				generation_grant_id, bundle_hash, bundle_source, runtime_instance_id,
				runtime_generation, created_at
			) VALUES (
				$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::uuid,$19,$20::uuid,$21::uuid,$22,$23,$24::uuid,$25,$26
		)
		`, result.TransitionID, req.OperationID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, req.Trigger,
		string(result.PreviousPhase), string(result.Phase),
		result.PreviousGeneration, result.Generation, result.RuntimeEpoch,
		result.ConfigRevision, string(result.RunMode),
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		req.Now.UTC()); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_diagnostic_outbox (
			outbox_id, operation_id, agent_id, agent_name_owner,
			agent_name_source, agent_route_presence, flow_scope_key,
			flow_instance_id, flow_instance, event_name, payload, created_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,
			'platform.agent_lifecycle_transition',$10::jsonb,$11
		)
	`, uuid.NewString(), req.OperationID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, string(raw), req.Now.UTC())
	return err
}

func insertSQLiteLifecycleEvidenceTx(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	raw, _ := json.Marshal(result)
	fields, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_operations (
			operation_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			operation_kind, request_hash, expected_epoch, expected_generation,
			target_generation, target_phase, config_revision, run_mode,
			process_authority_id, process_owner_id, process_boot_id, generation_grant_id,
			bundle_hash, bundle_source, runtime_instance_id, runtime_generation,
			state, result, created_at, updated_at, completed_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'succeeded',?,?,?,?)
	`, req.OperationID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID,
		fields.FlowInstancePath, req.OperationKind, req.RequestHash,
		req.ExpectedEpoch, req.ExpectedGeneration, req.TargetGeneration,
		string(req.TargetPhase), req.ConfigRevision, string(req.RunMode),
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		string(raw), req.Now.UTC(), req.Now.UTC(), req.Now.UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_transition_facts (
			transition_id, operation_id, agent_id, agent_name_owner,
			agent_name_source, agent_route_presence, flow_scope_key,
			flow_instance_id, flow_instance, trigger, previous_phase, next_phase,
			previous_generation, next_generation, runtime_epoch, config_revision,
			run_mode, process_authority_id, process_owner_id, process_boot_id,
			generation_grant_id, bundle_hash, bundle_source, runtime_instance_id,
			runtime_generation, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, result.TransitionID, req.OperationID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, req.Trigger,
		string(result.PreviousPhase), string(result.Phase),
		result.PreviousGeneration, result.Generation, result.RuntimeEpoch,
		result.ConfigRevision, string(result.RunMode),
		req.ProcessBinding.ProcessAuthorityID, req.ProcessBinding.ProcessOwnerID,
		req.ProcessBinding.ProcessBootID, req.ProcessBinding.GenerationGrantID,
		req.ProcessBinding.BundleHash, req.ProcessBinding.BundleSource,
		req.ProcessBinding.RuntimeInstanceID, req.ProcessBinding.RuntimeGeneration,
		req.Now.UTC()); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_lifecycle_diagnostic_outbox (
			outbox_id, operation_id, agent_id, agent_name_owner,
			agent_name_source, agent_route_presence, flow_scope_key,
			flow_instance_id, flow_instance, event_name, payload, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,'platform.agent_lifecycle_transition',?,?)
	`, uuid.NewString(), req.OperationID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, string(raw), req.Now.UTC())
	return err
}
