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
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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
	CapabilityProjection storemanagedcapability.StoredProjection
	ExecutionOwner       string
	FenceGeneration      int64
	LeaseExpiresAt       time.Time
	SessionGrantID       string
	SessionLockOwner     string
	AgentRunID           string
	LineageRunID         string
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
	AuthorityKind              string                           `json:"authority_kind,omitempty"`
	AuthorityID                string                           `json:"authority_id,omitempty"`
	ExecutionOwner             string                           `json:"execution_owner,omitempty"`
	FenceGeneration            uint64                           `json:"fence_generation,omitempty"`
	ProviderRequest            runtimeeffects.CompletionRequest `json:"provider_request"`
	ExecutionID                string                           `json:"execution_id"`
	ForkRunID                  string                           `json:"fork_run_id"`
	Generation                 uint64                           `json:"generation"`
	AdmissionFingerprint       string                           `json:"admission_fingerprint"`
	ContainerPlanFingerprint   string                           `json:"container_plan_fingerprint"`
	ActorCensusFingerprint     string                           `json:"actor_census_fingerprint"`
	EffectiveConfigFingerprint string                           `json:"effective_config_fingerprint"`
	ActorTokenID               string                           `json:"actor_token_id"`
	ExecutionMode              string                           `json:"execution_mode"`
	UsageTarget                struct {
		Kind          string                 `json:"kind"`
		ID            string                 `json:"id"`
		Ordinal       int                    `json:"ordinal"`
		RunID         string                 `json:"run_id"`
		AgentID       string                 `json:"agent_id"`
		AgentIdentity agentidentity.Identity `json:"agent_identity"`
		SessionID     string                 `json:"session_id"`
		MemoryEnabled bool                   `json:"memory_enabled"`
		FlowInstance  string                 `json:"flow_instance"`
		EntityID      string                 `json:"entity_id"`
	} `json:"usage_target"`
}

type CompletionRecoveryAuthorityEvidence = completionRecoveryAuthorityEvidence

func reconcileCompletionAttemptsPostgres(ctx context.Context, tx *sql.Tx, llm *storellm.LLMPostgresOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, mutation *mutationprotocol.Attempt, allowed map[string]struct{}, now time.Time, selectedExecutionID string) (runtimeeffects.RecoverySummary, error) {
	attempts, err := loadCompletionRecoveryAttemptsPostgres(ctx, tx, now, selectedExecutionID, nil)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	attempts = filterCompletionRecoveryAttempts(attempts, allowed)
	return reconcileCompletionAttempts(ctx, tx, llm, nil, delivery, directives, mutation, true, attempts, now)
}

func loadCompletionRecoveryAttemptsPostgres(ctx context.Context, tx *sql.Tx, now time.Time, selectedExecutionID string, identity *agentidentity.Identity) ([]completionRecoveryAttempt, error) {
	predicate, args, err := startupCompletionRecoveryFilter(identity, true)
	if err != nil {
		return nil, err
	}
	args = append([]any{now, selectedExecutionID}, args...)
	rows, err := tx.QueryContext(ctx, postgresCompletionRecoverySelect+storeagent.CompletionRecoveryAgentJoinSQL()+`
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND ((o.authority_kind='selected_contract_fork' AND o.selected_execution_id::text=$2) OR
		  ($2='' AND o.authority_kind<>'selected_contract_fork' AND `+postgresProviderCompletionRecoveryOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= $1 OR
		    (o.authority_kind='normal_agent' AND (
		      `+storeagent.CompletionRecoveryAgentStaleSQL(false)+`
		    )) OR
		    (o.authority_kind='normal_agent' AND NOT EXISTS (
		      SELECT 1 FROM runs run
		      WHERE run.run_id=COALESCE(NULLIF(o.lineage->>'run_id',''),NULLIF(o.authority_evidence #>> '{usage_target,run_id}',''))::uuid
		        AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		    ))
		  )
		  ))
		`+predicate+` ORDER BY a.authorized_at,a.attempt_id
		FOR UPDATE OF o,a
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list completion attempts for recovery: %w", err)
	}
	return scanCompletionRecoveryAttempts(rows)
}

func reconcileCompletionAttemptsSQLite(ctx context.Context, tx *sql.Tx, llm *storellm.LLMSQLiteOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, mutation *mutationprotocol.Attempt, allowed map[string]struct{}, now time.Time, selectedExecutionID string) (runtimeeffects.RecoverySummary, error) {
	attempts, err := loadCompletionRecoveryAttemptsSQLite(ctx, tx, now, selectedExecutionID, nil)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	attempts = filterCompletionRecoveryAttempts(attempts, allowed)
	return reconcileCompletionAttempts(ctx, tx, nil, llm, delivery, directives, mutation, false, attempts, now)
}

func loadCompletionRecoveryAttemptsSQLite(ctx context.Context, tx *sql.Tx, now time.Time, selectedExecutionID string, identity *agentidentity.Identity) ([]completionRecoveryAttempt, error) {
	predicate, args, err := startupCompletionRecoveryFilter(identity, false)
	if err != nil {
		return nil, err
	}
	args = append([]any{selectedExecutionID, selectedExecutionID, now}, args...)
	rows, err := tx.QueryContext(ctx, sqliteCompletionRecoverySelect+storeagent.CompletionRecoveryAgentJoinSQL()+`
		WHERE o.effect_kind='provider_turn' AND a.usage_target_kind IS NOT NULL
		  AND a.state IN ('authorized','launched','response_observed')
		  AND ((o.authority_kind='selected_contract_fork' AND o.selected_execution_id=?) OR
		  (?='' AND o.authority_kind<>'selected_contract_fork' AND `+sqliteProviderCompletionRecoveryOwnerPredicate+`
		  AND (
		    a.lease_expires_at <= ? OR
		    (o.authority_kind='normal_agent' AND (
		      `+storeagent.CompletionRecoveryAgentStaleSQL(true)+`
		    )) OR
		    (o.authority_kind='normal_agent' AND NOT EXISTS (
		      SELECT 1 FROM runs run
		      WHERE run.run_id=COALESCE(NULLIF(json_extract(o.lineage,'$.run_id'),''),NULLIF(json_extract(o.authority_evidence,'$.usage_target.run_id'),''))
		        AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		    ))
		  )
		  ))
		`+predicate+` ORDER BY a.authorized_at,a.attempt_id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite completion attempts for recovery: %w", err)
	}
	return scanCompletionRecoveryAttempts(rows)
}

func startupCompletionRecoveryFilter(identity *agentidentity.Identity, postgres bool) (string, []any, error) {
	if identity == nil {
		return "", nil, nil
	}
	fields, err := agentIdentityFields(*identity)
	if err != nil {
		return "", nil, err
	}
	predicate := ` AND o.authority_kind='normal_agent' AND o.agent_run_id=? AND o.agent_id=?
		AND o.agent_name_owner=? AND o.agent_name_source=? AND o.agent_route_presence=?
		AND o.flow_scope_key=? AND o.flow_instance_id=? AND o.flow_instance=?`
	if postgres {
		predicate = ` AND o.authority_kind='normal_agent' AND o.agent_run_id=$3::uuid AND o.agent_id=$4
			AND o.agent_name_owner=$5 AND o.agent_name_source=$6 AND o.agent_route_presence=$7
			AND o.flow_scope_key=$8 AND o.flow_instance_id=$9 AND o.flow_instance=$10`
	}
	return predicate, []any{fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath}, nil
}

// Startup takeover and predecessor settlement share the original mutation. A
// refused recovery must not leave a successor that invalidates a pending drain.
func (s *EffectPostgresOwner) RecoverStartupProviderAttemptsPostgresTx(ctx context.Context, mutation *mutationprotocol.Attempt, request runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := validateStartupProviderRecovery(ctx, tx, request, result, false); err != nil {
			return err
		}
		attempts, err := loadCompletionRecoveryAttemptsPostgres(ctx, tx, request.Now, "", &request.Identity)
		if err != nil {
			return err
		}
		if err := admitStartupProviderRecovery(request, attempts); err != nil {
			return err
		}
		if _, err := reconcileCompletionAttempts(ctx, tx, s.llm, nil, s.delivery, s.directives, mutation, true, attempts, request.Now); err != nil {
			return err
		}
		return finishStartupProviderRecovery(ctx, tx, mutation, s.lifecycle, request, attempts, true)
	})
}

func (s *EffectSQLiteOwner) RecoverStartupProviderAttemptsSQLiteTx(ctx context.Context, mutation *mutationprotocol.Attempt, request runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := validateStartupProviderRecovery(ctx, tx, request, result, true); err != nil {
			return err
		}
		attempts, err := loadCompletionRecoveryAttemptsSQLite(ctx, tx, request.Now, "", &request.Identity)
		if err != nil {
			return err
		}
		if err := admitStartupProviderRecovery(request, attempts); err != nil {
			return err
		}
		if _, err := reconcileCompletionAttempts(ctx, tx, nil, s.llm, s.delivery, s.directives, mutation, false, attempts, request.Now); err != nil {
			return err
		}
		return finishStartupProviderRecovery(ctx, tx, mutation, s.lifecycle, request, attempts, false)
	})
}

func validateStartupProviderRecovery(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult, sqlite bool) error {
	if req.OperationKind != "process_takeover" ||
		result.OperationID != req.OperationID || result.Identity != req.Identity ||
		result.PreviousEpoch != req.ExpectedEpoch || result.PreviousGeneration != req.ExpectedGeneration || result.PreviousPhase != req.ExpectedPhase ||
		result.RuntimeEpoch != req.TargetEpoch || result.Generation != req.TargetGeneration || result.Phase != req.TargetPhase ||
		result.ConfigRevision != req.ConfigRevision || result.RunMode != req.RunMode || !result.Topology.Equal(req.Topology) ||
		!result.ProcessBinding.Equal(req.ProcessBinding) || !req.StartupRecovery.Now().Equal(req.Now) {
		return fmt.Errorf("startup provider recovery requires the exact native takeover result and recovery request")
	}
	if err := req.StartupRecovery.Validate(); err != nil {
		return err
	}
	if err := storeagent.AuthorizeGenerationMutationTx(ctx, tx, req, sqlite); err != nil {
		return err
	}
	observation, err := storeagent.LockAgentExecutionTx(ctx, tx, req.Identity, runtimeeffects.LifecycleToken{
		Identity: req.Identity, AgentID: req.AgentID, RuntimeEpoch: result.RuntimeEpoch, Generation: result.Generation,
	}, sqlite)
	if err != nil {
		return err
	}
	state := observation.State
	if !observation.Found || state.RuntimeEpoch != result.RuntimeEpoch || state.Generation != result.Generation ||
		state.Phase != result.Phase || state.LastTransitionID != result.TransitionID ||
		state.ConfigRevision != result.ConfigRevision || state.RunMode != result.RunMode ||
		!state.Topology.Equal(result.Topology) || !state.ProcessBinding.Equal(req.ProcessBinding) {
		return fmt.Errorf("startup provider recovery differs from the transaction's exact lifecycle cell")
	}
	return nil
}

func admitStartupProviderRecovery(request runtimemanager.AgentLifecycleTransition, attempts []completionRecoveryAttempt) error {
	for _, attempt := range attempts {
		candidate := externalEffectRecoveryCandidate{
			AttemptID: attempt.AttemptID, OperationMode: attempt.OperationMode,
			AttemptMode: attempt.AttemptMode, AuthorityEvidence: attempt.AuthorityEvidence,
		}
		if err := candidate.admit(request.StartupRecovery); err != nil {
			return err
		}
	}
	return nil
}

func finishStartupProviderRecovery(ctx context.Context, tx *sql.Tx, mutation *mutationprotocol.Attempt, lifecycle mutationprotocol.CandidateWriter, request runtimemanager.AgentLifecycleTransition, attempts []completionRecoveryAttempt, postgres bool) error {
	for _, attempt := range attempts {
		if err := recordSettledExternalEffectStory(ctx, mutation, tx, runtimeeffects.Settlement{AttemptID: attempt.AttemptID, OperationID: attempt.OperationID, State: runtimeeffects.StateOutcomeUncertain}, postgres); err != nil {
			return err
		}
	}
	if len(attempts) == 0 {
		return nil
	}
	terminal, err := externalEffectRunTerminal(ctx, tx, postgres, request.Identity.RunID)
	if err != nil || terminal {
		return err
	}
	_, err = mutation.RequestCompletion(ctx, lifecycle, request.Identity.RunID, nil)
	return err
}

func filterCompletionRecoveryAttempts(attempts []completionRecoveryAttempt, allowed map[string]struct{}) []completionRecoveryAttempt {
	filtered := make([]completionRecoveryAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		if _, ok := allowed[strings.TrimSpace(attempt.AttemptID)]; ok {
			filtered = append(filtered, attempt)
		}
	}
	return filtered
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
			&attempt.ExecutionOwner, &attempt.FenceGeneration, &lease, &attempt.SessionGrantID, &attempt.SessionLockOwner,
			&attempt.AgentRunID, &attempt.LineageRunID,
			&attempt.AgentID, &attempt.AgentNameOwner, &attempt.AgentNameSource, &attempt.AgentRoutePresence,
			&attempt.FlowScopeKey, &attempt.FlowInstanceID, &attempt.FlowInstance,
			&attempt.RuntimeEpoch, &attempt.Generation,
			&attempt.OriginKind, &attempt.OriginDeliveryID, &attempt.OriginRunID, &attempt.OriginRouteIdentity,
			&attempt.OriginClaimToken, &attempt.OriginClaimVersion, &attempt.OriginSubscriber,
			&attempt.OriginDirectiveID, &attempt.OriginDirectiveOwner,
			&attempt.CapabilityProjection.ExecutionKind, &attempt.CapabilityProjection.ExecutionAuthorityID,
			&attempt.CapabilityProjection.RunID, &attempt.CapabilityProjection.SelectedExecutionID, &attempt.CapabilityProjection.SelectedForkRunID); err != nil {
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

func reconcileCompletionAttempts(ctx context.Context, tx *sql.Tx, postgresLLM *storellm.LLMPostgresOwner, sqliteLLM *storellm.LLMSQLiteOwner, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, mutation *mutationprotocol.Attempt, postgres bool, attempts []completionRecoveryAttempt, now time.Time) (runtimeeffects.RecoverySummary, error) {
	var summary runtimeeffects.RecoverySummary
	for _, recovered := range attempts {
		prelaunch := recovered.State == string(runtimeeffects.StateAuthorized)
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
		disposition, err := providerRecoveryDisposition(ctx, tx, postgres, recovered, attempt, resolution.Expired, now)
		if err != nil {
			return runtimeeffects.RecoverySummary{}, err
		}
		var retryBase time.Duration
		if disposition == runtimedelivery.FailureRetry {
			retryBase, err = providerResumeRetryBaseTx(ctx, tx, attempt, now)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		projectCurrent := resolution.Kind == completionSettlementCurrent
		if projectCurrent && !prelaunch {
			projectCurrent, err = completionSessionGrantCurrent(ctx, tx, postgres, attempt, now)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
			if !projectCurrent {
				settlement = completionProviderHeadUncertainty(settlement, fmt.Errorf("recovered completion session grant is no longer current"))
			}
		}
		turnCtx := ctx
		if !prelaunch && settlement.AgentTurn != nil {
			turnCtx, err = completionRecoveryTurnContext(ctx, tx, recovered, attempt, settlement, postgres)
			if err != nil {
				return runtimeeffects.RecoverySummary{}, err
			}
		}
		if postgres {
			if !prelaunch {
				if err := insertCompletionTargetPostgres(ctx, tx, postgresLLM, mutation, attempt, settlement, projectCurrent); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
				if err := postgresLLM.RecordCompletionTurnAuthorActivityTx(turnCtx, mutation, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
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
			if !prelaunch {
				if err := insertCompletionTargetSQLite(ctx, tx, sqliteLLM, mutation, attempt, settlement, projectCurrent); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
				if err := sqliteLLM.RecordCompletionTurnAuthorActivityTx(turnCtx, mutation, attempt, settlement); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
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
		// Selected retries use the same boot-owned recovery settlement after its
		// predecessor fence. Nonresumable selected work keeps whole-run termination.
		if attempt.Authority.Kind == runtimeeffects.AuthorityNormalAgent || disposition == runtimedelivery.FailureRetry {
			if resolution.Kind == completionSettlementDrained {
				if _, err := settleProviderDrainRecovery(ctx, tx, mutation, attempt, settlement, resolution.Drain, resolution.Expired, postgres, delivery, directives, disposition, retryBase); err != nil {
					return runtimeeffects.RecoverySummary{}, err
				}
			} else if err := settleProviderDrainOriginRecovery(ctx, mutation, settlement, attempt.Origin, delivery, directives, disposition, retryBase); err != nil {
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

func completionRecoveryTurnContext(ctx context.Context, tx *sql.Tx, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, postgres bool) (context.Context, error) {
	if settlement.AgentTurn == nil {
		return ctx, nil
	}
	source, err := loadExternalEffectStorySource(ctx, tx, recovered.AttemptID, postgres)
	if err != nil {
		return nil, err
	}
	frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
	if err != nil {
		return nil, fmt.Errorf("completion recovery scope frame: %w", err)
	}
	turn := settlement.AgentTurn
	if source.AttemptID != attempt.AttemptID || source.Kind != string(runtimeeffects.KindProviderTurn) ||
		source.AuthorityKind != string(attempt.Authority.Kind) || source.AuthorityID != attempt.Authority.ID ||
		source.AgentID != attempt.Authority.Normal.AgentID || source.ExecutionMode != string(attempt.Authority.ExecutionMode) ||
		source.BundleHash != frame.Session.SourceArtifact.Hash ||
		turn.TurnID != attempt.Authority.Target.ID || turn.RunID != frame.Turn.Event.RunID ||
		turn.TriggerEventID != frame.Turn.Event.ID || turn.Identity != frame.Session.AgentIdentity {
		return nil, fmt.Errorf("completion recovery source scope conflicts for attempt %s", recovered.AttemptID)
	}
	// The recovery scan may contain several bundles. Its current runtime is the
	// occurrence owner; each immutable operation remains the bundle owner.
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, source.BundleHash)
	if err != nil {
		return nil, err
	}
	return runtimeauthoractivity.WithScope(ctx, scope), nil
}

func completionRecoverySettlement(recovered completionRecoveryAttempt, state runtimeeffects.State, failure *runtimefailures.Envelope, now time.Time) (runtimeeffects.Attempt, runtimeeffects.CompletionSettlement, error) {
	var evidence completionRecoveryAuthorityEvidence
	if err := json.Unmarshal([]byte(recovered.AuthorityEvidence), &evidence); err != nil {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("decode completion recovery authority evidence for %s: %w", recovered.AttemptID, err)
	}
	if err := evidence.ProviderRequest.Validate(); err != nil {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, err
	}
	mode, ok := executionmode.Parse(recovered.OperationMode)
	if !ok || recovered.AttemptMode != recovered.OperationMode || evidence.ExecutionMode != recovered.OperationMode {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery execution mode conflicts for attempt %s", recovered.AttemptID)
	}
	evidenceTargetID, evidenceTargetErr := uuid.Parse(strings.TrimSpace(evidence.UsageTarget.ID))
	storedTargetID, storedTargetErr := uuid.Parse(strings.TrimSpace(recovered.TargetID))
	if evidenceTargetErr != nil || storedTargetErr != nil || evidence.UsageTarget.Kind != recovered.TargetKind || evidenceTargetID != storedTargetID || evidence.UsageTarget.Ordinal != recovered.TargetOrdinal {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target evidence conflicts with attempt %s", recovered.AttemptID)
	}
	// UUID storage may normalize text; immutable authority retains its admitted spelling.
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetKind(recovered.TargetKind), ID: evidence.UsageTarget.ID, Ordinal: recovered.TargetOrdinal,
		RunID: evidence.UsageTarget.RunID, AgentID: evidence.UsageTarget.AgentID,
		AgentIdentity: evidence.UsageTarget.AgentIdentity, SessionID: evidence.UsageTarget.SessionID,
		Memory:       agentmemory.Plan{Enabled: evidence.UsageTarget.MemoryEnabled},
		FlowInstance: evidence.UsageTarget.FlowInstance, EntityID: evidence.UsageTarget.EntityID,
	}
	if !target.Valid() {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery target for attempt %s is invalid", recovered.AttemptID)
	}
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityKind(recovered.AuthorityKind), ID: recovered.AuthorityID, Target: target, ExecutionMode: mode, CompletionRequest: evidence.ProviderRequest}
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		runID := strings.TrimSpace(target.RunID)
		for _, axis := range []struct {
			name  string
			value string
		}{
			{name: "persisted agent", value: recovered.AgentRunID},
			{name: "lineage", value: recovered.LineageRunID},
			{name: "concrete agent", value: target.AgentIdentity.Normalize().RunID},
		} {
			if strings.TrimSpace(axis.value) == "" || strings.TrimSpace(axis.value) != runID {
				return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery %s run identity conflicts for attempt %s", axis.name, recovered.AttemptID)
			}
		}
		identity, err := agentidentity.FromStorageFields(agentidentity.StorageFields{
			RunID:   target.RunID,
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
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		authority.SelectedFork = runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: evidence.ExecutionID, ForkRunID: evidence.ForkRunID, Generation: evidence.Generation,
			AdmissionFingerprint: evidence.AdmissionFingerprint, ContainerPlanFingerprint: evidence.ContainerPlanFingerprint,
			ActorCensusFingerprint: evidence.ActorCensusFingerprint, EffectiveConfigFingerprint: evidence.EffectiveConfigFingerprint,
		}
		authority.ExecutionOwner, authority.FenceGeneration, authority.LeaseExpiresAt = recovered.ExecutionOwner, uint64(recovered.FenceGeneration), recovered.LeaseExpiresAt.UTC()
		if !authority.Valid() || authority.ID != evidence.ExecutionID || evidence.ForkRunID != target.RunID || recovered.LineageRunID != target.RunID || recovered.AgentRunID != "" {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery selected authority contradicts its immutable execution for attempt %s", recovered.AttemptID)
		}
	}
	if target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		authority.ForkChat.ForkTurnID = target.ID
	}
	attempt := runtimeeffects.Attempt{
		OperationID: recovered.OperationID, AttemptID: recovered.AttemptID, Authority: authority,
		Kind: runtimeeffects.KindProviderTurn, Adapter: recovered.Adapter, Transport: recovered.Transport,
		SessionGrantID: recovered.SessionGrantID, SessionLockOwner: recovered.SessionLockOwner,
	}
	if authority.HasBusinessTurnOrigin() {
		origin, err := decodeCompletionOrigin(recovered.OriginKind, recovered.OriginDeliveryID, recovered.OriginRunID, recovered.OriginRouteIdentity, recovered.OriginClaimToken, recovered.OriginClaimVersion, recovered.OriginSubscriber, recovered.OriginDirectiveID, recovered.OriginDirectiveOwner)
		if err != nil {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery origin claim for attempt %s: %w", recovered.AttemptID, err)
		}
		if origin.Kind == runtimeeffects.CompletionOriginDelivery && (origin.Delivery.RunID() != target.RunID || origin.Delivery.SubscriberID() != target.AgentID) {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery origin claim conflicts for attempt %s", recovered.AttemptID)
		}
		attempt.Origin = origin
	} else if strings.TrimSpace(recovered.AgentRunID) != "" {
		return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery non-agent authority carries agent run identity for attempt %s", recovered.AttemptID)
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
		Usage: runtimeeffects.CompletionUsage{Exactness: runtimeeffects.CompletionUsageUnavailable,
			Evidence: runtimeeffects.CompletionEvidence{Request: evidence.ProviderRequest, Models: []runtimeeffects.CompletionModelObservation{}, CostProvenance: runtimeeffects.CompletionCostUnavailable}},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: target.EntityID, FlowInstance: flowInstance, AgentID: agentID, Model: evidence.ProviderRequest.RequestedModel, ModelAlias: evidence.ProviderRequest.ModelAlias,
			BackendProfile: evidence.ProviderRequest.BackendProfile, Provider: evidence.ProviderRequest.Provider, Transport: evidence.ProviderRequest.Transport,
			InvocationType: coalesce(recovered.Adapter, "completion_recovery"),
		},
		Now: now,
	}
	if target.Kind == runtimeeffects.UsageTargetAgentTurn {
		if _, err := uuid.Parse(strings.TrimSpace(target.SessionID)); err != nil || agentID == "" {
			return runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{}, fmt.Errorf("completion recovery agent-turn identity for attempt %s is incomplete", recovered.AttemptID)
		}
		surface, err := storemanagedcapability.DecodeStored([]byte(recovered.CapabilitySurface), recovered.CapabilityProjection)
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
		if recovered.State == string(runtimeeffects.StateAuthorized) {
			return attempt, settlement, nil
		}
		settlement.AgentTurn = &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: agentID, SessionID: target.SessionID,
			Identity: target.AgentIdentity,
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
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns SET state='abandoned',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE state='prepared' AND lease_expires_at<=?`, string(preparedFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite prepared forkchat groups: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversation_fork_turns AS f SET state='outcome_uncertain',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE f.state='executing' AND f.lease_expires_at<=? AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=f.fork_turn_id AND a.state IN ('authorized','launched','response_observed'))`, string(uncertainFailure), now, now, now); err != nil {
		return fmt.Errorf("recover sqlite executing forkchat groups: %w", err)
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
