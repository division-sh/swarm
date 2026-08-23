package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/google/uuid"
)

type completionRecoveryAttempt struct {
	OperationID          string
	AttemptID            string
	AuthorityKind        string
	AuthorityID          string
	AuthorityEvidence    string
	AgentFrame           []byte
	OperationMode        string
	AttemptMode          string
	Adapter              string
	Transport            string
	State                string
	TargetKind           string
	TargetID             string
	TargetOrdinal        int
	CapabilitySurfaceID  string
	CapabilitySurface    string
	ExecutionOwner       string
	FenceGeneration      int64
	LeaseExpiresAt       time.Time
	AgentID              string
	AgentNameOwner       string
	AgentNameSource      string
	AgentRoutePresence   string
	FlowScopeKey         string
	FlowInstanceID       string
	FlowInstance         string
	RuntimeEpoch         int64
	Generation           int64
	OriginDeliveryID     string
	OriginRunID          string
	OriginRouteIdentity  string
	OriginClaimToken     string
	OriginClaimVersion   int64
	OriginSubscriber     string
	OriginKind           string
	OriginDirectiveID    string
	OriginDirectiveOwner string
}

type CompletionRecoveryAttempt = completionRecoveryAttempt

type completionRecoveryAuthorityEvidence struct {
	ActorTokenID  string `json:"actor_token_id"`
	ExecutionMode string `json:"execution_mode"`
	UsageTarget   struct {
		Kind          string                 `json:"kind"`
		ID            string                 `json:"id"`
		Ordinal       int                    `json:"ordinal"`
		RunID         string                 `json:"run_id"`
		AgentID       string                 `json:"agent_id"`
		AgentIdentity agentidentity.Identity `json:"agent_identity"`
		SessionID     string                 `json:"session_id"`
		MemoryEnabled bool                   `json:"memory_enabled"`
		MemorySource  string                 `json:"memory_source"`
		FlowInstance  string                 `json:"flow_instance"`
		EntityID      string                 `json:"entity_id"`
	} `json:"usage_target"`
}

type CompletionRecoveryAuthorityEvidence = completionRecoveryAuthorityEvidence

func reconcileCompletionAttemptsPostgres(ctx context.Context, tx *sql.Tx, llm *storellm.LLMPostgresOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, story *privateauthoractivity.Mutation, now time.Time) (runtimeeffects.RecoverySummary, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.operation_id::text,a.attempt_id::text,o.authority_kind,o.authority_id,o.authority_evidence::text,o.agent_frame_bytes,
		       o.execution_mode,a.execution_mode,
		       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id::text,COALESCE(a.target_ordinal,0),
		       COALESCE(a.capability_surface_id::text,''),COALESCE(s.surface::text,''),
		       a.execution_owner,a.fence_generation,a.lease_expires_at,
		       COALESCE(o.agent_id,''),COALESCE(o.agent_name_owner,''),COALESCE(o.agent_name_source,''),
		       COALESCE(o.agent_route_presence,''),COALESCE(o.flow_scope_key,''),COALESCE(o.flow_instance_id,''),COALESCE(o.flow_instance,''),
		       COALESCE(o.runtime_epoch,0),COALESCE(o.generation,0),
		       COALESCE(a.origin_kind,''),COALESCE(a.origin_delivery_id::text,''),COALESCE(a.origin_run_id::text,''),COALESCE(a.origin_route_identity,''),
		       COALESCE(a.origin_claim_token::text,''),COALESCE(a.origin_claim_version,0),COALESCE(a.origin_subscriber_id,''),
		       COALESCE(a.origin_directive_operation_id::text,''),COALESCE(a.origin_directive_owner_id,'')
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=a.capability_surface_id
		LEFT JOIN agents g ON o.authority_kind='normal_agent'
		  AND g.agent_id=o.agent_id
		  AND g.agent_name_owner=o.agent_name_owner
		  AND g.agent_name_source=o.agent_name_source
		  AND g.agent_route_presence=o.agent_route_presence
		  AND g.flow_scope_key=o.flow_scope_key
		  AND g.flow_instance_id=o.flow_instance_id
		  AND g.flow_instance=o.flow_instance
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND `+postgresProviderCompletionRecoveryOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= $1 OR
		    (o.authority_kind='normal_agent' AND (
		      g.agent_id IS NULL OR g.lifecycle_runtime_epoch IS DISTINCT FROM o.runtime_epoch OR
		      g.lifecycle_generation IS DISTINCT FROM o.generation OR g.lifecycle_phase <> 'running'
		    )) OR
		    (o.authority_kind='normal_agent' AND NOT EXISTS (
		      SELECT 1 FROM runs run
		      WHERE run.run_id=COALESCE(NULLIF(o.lineage->>'run_id',''),NULLIF(o.authority_evidence #>> '{usage_target,run_id}',''))::uuid
		        AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		    ))
		  )
		ORDER BY a.authorized_at,a.attempt_id
		FOR UPDATE OF o,a
	`, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, fmt.Errorf("list completion attempts for recovery: %w", err)
	}
	attempts, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return reconcileCompletionAttempts(ctx, tx, llm, nil, delivery, directives, story, true, attempts, now)
}

func reconcileCompletionAttemptsSQLite(ctx context.Context, tx *sql.Tx, llm *storellm.LLMSQLiteOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, story *privateauthoractivity.Mutation, now time.Time) (runtimeeffects.RecoverySummary, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.operation_id,a.attempt_id,o.authority_kind,o.authority_id,o.authority_evidence,o.agent_frame_bytes,
		       o.execution_mode,a.execution_mode,
		       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id,COALESCE(a.target_ordinal,0),
		       COALESCE(a.capability_surface_id,''),COALESCE(s.surface,''),
		       a.execution_owner,a.fence_generation,a.lease_expires_at,
		       COALESCE(o.agent_id,''),COALESCE(o.agent_name_owner,''),COALESCE(o.agent_name_source,''),
		       COALESCE(o.agent_route_presence,''),COALESCE(o.flow_scope_key,''),COALESCE(o.flow_instance_id,''),COALESCE(o.flow_instance,''),
		       COALESCE(o.runtime_epoch,0),COALESCE(o.generation,0),
		       COALESCE(a.origin_kind,''),COALESCE(a.origin_delivery_id,''),COALESCE(a.origin_run_id,''),COALESCE(a.origin_route_identity,''),
		       COALESCE(a.origin_claim_token,''),COALESCE(a.origin_claim_version,0),COALESCE(a.origin_subscriber_id,''),
		       COALESCE(a.origin_directive_operation_id,''),COALESCE(a.origin_directive_owner_id,'')
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=a.capability_surface_id
		LEFT JOIN agents g ON o.authority_kind='normal_agent'
		  AND g.agent_id=o.agent_id
		  AND g.agent_name_owner=o.agent_name_owner
		  AND g.agent_name_source=o.agent_name_source
		  AND g.agent_route_presence=o.agent_route_presence
		  AND g.flow_scope_key=o.flow_scope_key
		  AND g.flow_instance_id=o.flow_instance_id
		  AND g.flow_instance=o.flow_instance
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND `+sqliteProviderCompletionRecoveryOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= ? OR
		    (o.authority_kind='normal_agent' AND (
		      g.agent_id IS NULL OR g.lifecycle_runtime_epoch IS NOT o.runtime_epoch OR
		      g.lifecycle_generation IS NOT o.generation OR g.lifecycle_phase <> 'running'
		    )) OR
		    (o.authority_kind='normal_agent' AND NOT EXISTS (
		      SELECT 1 FROM runs run
		      WHERE run.run_id=COALESCE(NULLIF(json_extract(o.lineage,'$.run_id'),''),NULLIF(json_extract(o.authority_evidence,'$.usage_target.run_id'),''))
		        AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		    ))
		  )
		ORDER BY a.authorized_at,a.attempt_id
	`, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, fmt.Errorf("list sqlite completion attempts for recovery: %w", err)
	}
	attempts, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return reconcileCompletionAttempts(ctx, tx, nil, llm, delivery, directives, story, false, attempts, now)
}

func scanCompletionRecoveryAttempts(rows *sql.Rows) ([]completionRecoveryAttempt, error) {
	defer rows.Close()
	var attempts []completionRecoveryAttempt
	for rows.Next() {
		var attempt completionRecoveryAttempt
		var lease conversationForkTimeValue
		if err := rows.Scan(&attempt.OperationID, &attempt.AttemptID, &attempt.AuthorityKind, &attempt.AuthorityID,
			&attempt.AuthorityEvidence, &attempt.AgentFrame, &attempt.OperationMode, &attempt.AttemptMode, &attempt.Adapter, &attempt.Transport, &attempt.State,
			&attempt.TargetKind, &attempt.TargetID, &attempt.TargetOrdinal,
			&attempt.CapabilitySurfaceID, &attempt.CapabilitySurface,
			&attempt.ExecutionOwner, &attempt.FenceGeneration, &lease,
			&attempt.AgentID, &attempt.AgentNameOwner, &attempt.AgentNameSource, &attempt.AgentRoutePresence,
			&attempt.FlowScopeKey, &attempt.FlowInstanceID, &attempt.FlowInstance,
			&attempt.RuntimeEpoch, &attempt.Generation,
			&attempt.OriginKind, &attempt.OriginDeliveryID, &attempt.OriginRunID, &attempt.OriginRouteIdentity,
			&attempt.OriginClaimToken, &attempt.OriginClaimVersion, &attempt.OriginSubscriber,
			&attempt.OriginDirectiveID, &attempt.OriginDirectiveOwner); err != nil {
			return nil, fmt.Errorf("scan completion attempt for recovery: %w", err)
		}
		if !lease.Valid {
			return nil, fmt.Errorf("completion attempt %s has no lease expiry", attempt.AttemptID)
		}
		attempt.LeaseExpiresAt = lease.Time
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read completion attempts for recovery: %w", err)
	}
	return attempts, nil
}

func reconcileCompletionAttempts(ctx context.Context, tx *sql.Tx, postgresLLM *storellm.LLMPostgresOwner, sqliteLLM *storellm.LLMSQLiteOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, story *privateauthoractivity.Mutation, postgres bool, attempts []completionRecoveryAttempt, now time.Time) (runtimeeffects.RecoverySummary, error) {
	var summary runtimeeffects.RecoverySummary
	for _, recovered := range attempts {
		state := runtimeeffects.StateTerminalFailure
		failureClass := runtimefailures.ClassLifecycleConflict
		failureCode := "effect_recovery_prelaunch_abandoned"
		if recovered.State == string(runtimeeffects.StateLaunched) || recovered.State == string(runtimeeffects.StateResponseObserved) {
			state = runtimeeffects.StateOutcomeUncertain
			failureClass = runtimefailures.ClassOutcomeUncertain
			failureCode = "effect_recovery_outcome_unconfirmed"
		}
		failureErr := runtimefailures.New(failureClass, failureCode, "external-effects", "startup_reconcile", map[string]any{
			"operation_id": recovered.OperationID, "attempt_id": recovered.AttemptID,
			"recovered_at": now.UTC().Format(time.RFC3339Nano),
		})
		failure := failureErr.(*runtimefailures.Error).Failure
		attempt, settlement, err := completionRecoverySettlement(recovered, state, &failure, now)
		if err != nil {
			return runtimeeffects.RecoverySummary{}, err
		}
		resolution := providerDrainRecoveryResolution{Kind: completionSettlementCurrent}
		if postgres {
			resolution, err = resolveProviderDrainRecoveryPostgres(ctx, tx, attempt, now)
		} else {
			resolution, err = resolveProviderDrainRecoverySQLite(ctx, tx, attempt, now)
		}
		if err != nil {
			return runtimeeffects.RecoverySummary{}, err
		}
		if resolution.Kind == completionSettlementDrained && state == runtimeeffects.StateTerminalFailure {
			return runtimeeffects.RecoverySummary{}, fmt.Errorf("authorized provider attempt %s cannot own a predecessor drain", attempt.AttemptID)
		}
		if resolution.Expired {
			failureErr = runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "provider_attempt_drain_expired", "external-effects", "startup_reconcile", map[string]any{
				"operation_id": recovered.OperationID, "attempt_id": recovered.AttemptID,
				"recovered_at": now.UTC().Format(time.RFC3339Nano),
			})
			failure = failureErr.(*runtimefailures.Error).Failure
			attempt, settlement, err = completionRecoverySettlement(recovered, runtimeeffects.StateOutcomeUncertain, &failure, now)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		projectCurrent := resolution.Kind == completionSettlementCurrent
		if postgres {
			if err := insertCompletionTargetPostgres(ctx, tx, postgresLLM, attempt, settlement, projectCurrent); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if err := postgresLLM.RecordCompletionTurnAuthorActivityTx(ctx, story, attempt, settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if state != runtimeeffects.StateTerminalFailure {
				if _, err := insertCompletionSpendPostgres(ctx, tx, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, attempt.AttemptID); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			_, err := settleExternalAttemptPostgres(ctx, tx, settlement.Settlement)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		} else {
			if err := insertCompletionTargetSQLite(ctx, tx, sqliteLLM, attempt, settlement, projectCurrent); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if err := sqliteLLM.RecordCompletionTurnAuthorActivityTx(ctx, story, attempt, settlement); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if state != runtimeeffects.StateTerminalFailure {
				if _, err := insertCompletionSpendSQLite(ctx, tx, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=?`, attempt.AttemptID); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			_, err := settleExternalAttemptSQLiteTx(ctx, tx, settlement.Settlement)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		if attempt.Authority.Kind == runtimeeffects.AuthorityNormalAgent {
			if resolution.Kind == completionSettlementDrained {
				if _, err := settleProviderDrainRecovery(ctx, tx, story, attempt, settlement, resolution.Drain, resolution.Expired, postgres, delivery, directives); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			} else if err := settleProviderDrainOriginRecovery(ctx, tx, story, settlement, attempt.Origin, delivery, directives); err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		if state == runtimeeffects.StateTerminalFailure {
			summary.PrelaunchTerminal++
		} else {
			summary.OutcomeUncertain++
		}
	}
	return summary, nil
}

func completionRecoverySettlement(recovered completionRecoveryAttempt, state runtimeeffects.State, failure *runtimefailures.Envelope, now time.Time) (runtimeeffects.Attempt, runtimeeffects.CompletionSettlement, error) {
	var evidence completionRecoveryAuthorityEvidence
	if err := json.Unmarshal([]byte(recovered.AuthorityEvidence), &evidence); err != nil {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("decode completion recovery authority evidence for %s: %w", recovered.AttemptID, err)
	}
	mode, ok := executionmode.Parse(recovered.OperationMode)
	if !ok || recovered.AttemptMode != recovered.OperationMode || evidence.ExecutionMode != recovered.OperationMode {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery execution mode conflicts for attempt %s", recovered.AttemptID)
	}
	if evidence.UsageTarget.Kind != recovered.TargetKind || evidence.UsageTarget.ID != recovered.TargetID || evidence.UsageTarget.Ordinal != recovered.TargetOrdinal {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target evidence conflicts with attempt %s", recovered.AttemptID)
	}
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetKind(recovered.TargetKind), ID: recovered.TargetID, Ordinal: recovered.TargetOrdinal,
		RunID: evidence.UsageTarget.RunID, AgentID: evidence.UsageTarget.AgentID,
		AgentIdentity: evidence.UsageTarget.AgentIdentity, SessionID: evidence.UsageTarget.SessionID,
		Memory:       agentmemory.Plan{Enabled: evidence.UsageTarget.MemoryEnabled, Source: agentmemory.Source(evidence.UsageTarget.MemorySource)},
		FlowInstance: evidence.UsageTarget.FlowInstance, EntityID: evidence.UsageTarget.EntityID,
	}
	if !target.Valid() {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target for attempt %s is invalid", recovered.AttemptID)
	}
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityKind(recovered.AuthorityKind), ID: recovered.AuthorityID, Target: target, ExecutionMode: mode}
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		identity, err := agentidentity.FromStorageFields(agentidentity.StorageFields{
			AgentID: recovered.AgentID, NameOwner: recovered.AgentNameOwner, NameSource: recovered.AgentNameSource,
			RoutePresence: recovered.AgentRoutePresence, FlowScopeKey: recovered.FlowScopeKey,
			FlowInstanceID: recovered.FlowInstanceID, FlowInstancePath: recovered.FlowInstance,
		})
		if err != nil {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery normal-agent identity for attempt %s: %w", recovered.AttemptID, err)
		}
		if same, err := agentidentity.Equal(identity, target.AgentIdentity); err != nil || !same {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery normal-agent identity conflicts for attempt %s", recovered.AttemptID)
		}
		authority.Normal = runtimeeffects.LifecycleToken{
			RuntimeEpoch: recovered.RuntimeEpoch, Identity: identity, AgentID: recovered.AgentID, Generation: uint64(recovered.Generation),
		}
		authority.ExecutionOwner = recovered.ExecutionOwner
		authority.LeaseExpiresAt = recovered.LeaseExpiresAt.UTC()
		authority.FenceGeneration = uint64(recovered.FenceGeneration)
	}
	if target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		authority.ForkChat.ForkTurnID = target.ID
	}
	attempt := runtimeeffects.Attempt{
		OperationID: recovered.OperationID, AttemptID: recovered.AttemptID, Authority: authority,
		Kind: runtimeeffects.KindProviderTurn, Adapter: recovered.Adapter, Transport: recovered.Transport,
	}
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		origin, err := decodeCompletionOrigin(recovered.OriginKind, recovered.OriginDeliveryID, recovered.OriginRunID, recovered.OriginRouteIdentity, recovered.OriginClaimToken, recovered.OriginClaimVersion, recovered.OriginSubscriber, recovered.OriginDirectiveID, recovered.OriginDirectiveOwner)
		if err != nil {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery origin claim for attempt %s: %w", recovered.AttemptID, err)
		}
		if origin.Kind == runtimeeffects.CompletionOriginDelivery && (origin.Delivery.RunID() != target.RunID || origin.Delivery.SubscriberID() != authority.Normal.AgentID) {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery origin claim conflicts for attempt %s", recovered.AttemptID)
		}
		attempt.Origin = origin
	}
	agentID := strings.TrimSpace(target.AgentID)
	if agentID == "" {
		agentID = strings.TrimSpace(evidence.ActorTokenID)
	}
	if agentID == "" {
		agentID = strings.TrimSpace(recovered.AuthorityID)
	}
	flowInstance := strings.TrimSpace(target.FlowInstance)
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(target.RunID)
	}
	if flowInstance == "" {
		flowInstance = "global"
	}
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{
			OperationID: recovered.OperationID, AttemptID: recovered.AttemptID, Authority: authority,
			State: state, Failure: failure, Evidence: map[string]any{"recovered": true}, Now: now,
		},
		Usage: runtimeeffects.CompletionUsage{ResolvedModel: "unknown", Exactness: runtimeeffects.CompletionUsageUnavailable},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: target.EntityID, FlowInstance: flowInstance, AgentID: agentID, Model: "unknown", ModelAlias: "unknown",
			BackendProfile: "unknown", Provider: recovered.Adapter, Transport: recovered.Transport,
			ResolvedModel: "unknown", InvocationType: coalesce(recovered.Adapter, "completion_recovery"),
		},
		Now: now,
	}
	if target.Kind == runtimeeffects.UsageTargetAgentTurn {
		if _, err := uuid.Parse(strings.TrimSpace(target.SessionID)); err != nil || agentID == "" {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery agent-turn identity for attempt %s is incomplete", recovered.AttemptID)
		}
		surface, err := storemanagedcapability.Decode([]byte(recovered.CapabilitySurface))
		if err != nil || surface.ID != recovered.CapabilitySurfaceID ||
			!runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(target, surface) {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery capability surface for attempt %s is invalid or mismatched: %w", recovered.AttemptID, err)
		}
		frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
		if err != nil {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery frame for attempt %s: %w", recovered.AttemptID, err)
		}
		if err := runtimeeffects.ValidateManagedAgentFrame(frame, authority, surface); err != nil {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery frame for attempt %s is mismatched: %w", recovered.AttemptID, err)
		}
		settlement.AgentTurn = &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: agentID, SessionID: target.SessionID,
			Identity: agentmemory.Identity{RunID: target.RunID, Agent: target.AgentIdentity},
			Memory:   target.Memory, FlowInstance: target.FlowInstance, EntityID: target.EntityID,
			TriggerEventID: frame.Turn.Event.ID, TriggerEventType: frame.Turn.Event.Type,
			CapabilitySurfaceID: surface.ID, CapabilitySurface: json.RawMessage(recovered.CapabilitySurface), Failure: failure,
		}
		settlement.Spend.AgentIdentity = target.AgentIdentity
	}
	return attempt, settlement, nil
}

func CompletionRecoverySettlement(recovered CompletionRecoveryAttempt, state runtimeeffects.State, failure *runtimefailures.Envelope, now time.Time) (runtimeeffects.Attempt, runtimeeffects.CompletionSettlement, error) {
	return completionRecoverySettlement(recovered, state, failure, now)
}

func reconcileCompletionParentAuthoritiesPostgres(ctx context.Context, tx *sql.Tx, now time.Time) error {
	return reconcileCompletionParentAuthorities(ctx, tx, true, now)
}

func reconcileCompletionParentAuthoritiesSQLite(ctx context.Context, tx *sql.Tx, now time.Time) error {
	return reconcileCompletionParentAuthorities(ctx, tx, false, now)
}

func reconcileCompletionParentAuthorities(ctx context.Context, tx *sql.Tx, postgres bool, now time.Time) error {
	preparedFailure, err := recoveryFailureJSON(runtimefailures.ClassLifecycleConflict, "completion_parent_recovery_prelaunch_abandoned", now)
	if err != nil {
		return err
	}
	uncertainFailure, err := recoveryFailureJSON(runtimefailures.ClassOutcomeUncertain, "completion_parent_recovery_outcome_unconfirmed", now)
	if err != nil {
		return err
	}
	if postgres {
		if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns SET state='abandoned',lease_expires_at=NULL,failure=$1::jsonb,terminal_at=$2,updated_at=$2 WHERE state='prepared' AND lease_expires_at<=$2`, string(preparedFailure), now); err != nil {
			return fmt.Errorf("recover prepared forkchat groups: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns f SET state='outcome_uncertain',lease_expires_at=NULL,failure=$1::jsonb,terminal_at=$2,updated_at=$2 WHERE f.state='executing' AND f.lease_expires_at<=$2 AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=f.fork_turn_id AND a.state IN ('authorized','launched','response_observed'))`, string(uncertainFailure), now); err != nil {
			return fmt.Errorf("recover executing forkchat groups: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions e SET state='failed',lease_expires_at=NULL,failure=CASE WHEN e.state='prepared' THEN $1::jsonb ELSE $2::jsonb END,terminal_at=$3,updated_at=$3 WHERE e.state IN ('prepared','running') AND e.lease_expires_at<=$3 AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=e.execution_id AND a.state IN ('authorized','launched','response_observed'))`, string(preparedFailure), string(uncertainFailure), now); err != nil {
			return fmt.Errorf("fail expired selected-contract runtime executions: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',updated_at=$1 WHERE state='failed' AND terminal_at=$1`, now); err != nil {
			return fmt.Errorf("close recovered selected-contract runtime executions: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns SET state='abandoned',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE state='prepared' AND lease_expires_at<=?`, string(preparedFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite prepared forkchat groups: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns AS f SET state='outcome_uncertain',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE f.state='executing' AND f.lease_expires_at<=? AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=f.fork_turn_id AND a.state IN ('authorized','launched','response_observed'))`, string(uncertainFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite executing forkchat groups: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions AS e SET state='failed',lease_expires_at=NULL,failure=CASE WHEN e.state='prepared' THEN ? ELSE ? END,terminal_at=?,updated_at=? WHERE e.state IN ('prepared','running') AND e.lease_expires_at<=? AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=e.execution_id AND a.state IN ('authorized','launched','response_observed'))`, string(preparedFailure), string(uncertainFailure), now, now, now); err != nil {
		return fmt.Errorf("fail expired sqlite selected-contract runtime executions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',updated_at=? WHERE state='failed' AND terminal_at=?`, now, now); err != nil {
		return fmt.Errorf("close recovered sqlite selected-contract runtime executions: %w", err)
	}
	return nil
}

func recoveryFailureJSON(class runtimefailures.Class, code string, now time.Time) ([]byte, error) {
	err := runtimefailures.New(class, code, "completion-authority", "startup_reconcile", map[string]any{"recovered_at": now.UTC().Format(time.RFC3339Nano)})
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if !ok {
		return nil, fmt.Errorf("construct completion parent recovery failure")
	}
	return json.Marshal(failure)
}
