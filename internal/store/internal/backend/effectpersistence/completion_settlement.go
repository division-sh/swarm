package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/google/uuid"
)

var _ runtimeeffects.CompletionStore = (*EffectPostgresOwner)(nil)
var _ runtimeeffects.CompletionStore = (*EffectSQLiteOwner)(nil)

func (s *EffectPostgresOwner) SettleCompletion(ctx context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	var providerHeadErr error
	var spendRecorded bool
	var originSettled bool
	var finalization *runtimeeffects.ProviderDrainFinalization
	var disposition runtimeeffects.CompletionSettlementDisposition
	var continuation *runtimeeffects.Attempt
	err := withRunLifecycleCandidateHandoff(ctx, func(handoff *runLifecycleCandidateHandoffReservation) error {
		return s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
			providerHeadErr = nil
			spendRecorded = false
			originSettled = false
			finalization = nil
			disposition = ""
			continuation = nil
			attemptSettlement := settlement
			permit, err := resolveCompletionSettlementPermitPostgres(txctx, tx, attempt)
			if err != nil {
				return err
			}
			if permit.Kind == completionSettlementDrained {
				drained, err := runtimeeffects.ProjectDrainedCompletionSettlement(attempt, attemptSettlement)
				if err != nil {
					return err
				}
				attemptSettlement = drained.CompletionSettlement()
				disposition = runtimeeffects.CompletionSettlementDrained
			} else {
				disposition = runtimeeffects.CompletionSettlementCurrent
			}
			if err := requireCompletionAttemptPostgres(txctx, tx, attempt, attemptSettlement); err != nil {
				return err
			}
			if permit.Kind == completionSettlementCurrent && attemptSettlement.ProviderHead != nil {
				req := completionProviderHeadSettlement(attempt, attemptSettlement)
				if providerHeadErr = requireProviderHeadLifecyclePostgres(txctx, tx, req); providerHeadErr == nil {
					providerHeadErr = promoteProviderHeadPostgres(txctx, tx, req)
				}
				if providerHeadErr != nil {
					attemptSettlement = completionProviderHeadUncertainty(attemptSettlement, providerHeadErr)
				}
			}
			if err := insertCompletionTargetPostgres(txctx, tx, s.llm, attempt, attemptSettlement, permit.Kind == completionSettlementCurrent); err != nil {
				return err
			}
			if err := declareCompletionTargetEffects(effects, attemptSettlement); err != nil {
				return err
			}
			if err := s.llm.RecordCompletionTurnAuthorActivityTx(txctx, story, attempt, attemptSettlement); err != nil {
				return err
			}
			spendRecorded, err = insertCompletionSpendPostgres(txctx, tx, attempt, attemptSettlement)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, attempt.AttemptID); err != nil {
				return fmt.Errorf("release completion budget reservations: %w", err)
			}
			changed, err := settleExternalAttemptPostgres(txctx, tx, attemptSettlement.Settlement)
			if err != nil {
				return err
			}
			if attemptSettlement.Settlement.CompletionProjectionPhase == runtimeeffects.CompletionProjectionResponseSettled {
				admitted, err := loadSettledCompletionContinuationPostgres(txctx, tx, attempt)
				if err != nil {
					return err
				}
				continuation = &admitted
			}
			if permit.Kind == completionSettlementDrained {
				finalization, err = s.settleProviderDrainTx(txctx, tx, story, attempt, attemptSettlement, permit.Drain)
				if err != nil {
					return err
				}
				if err := declareProviderOriginEffects(effects, permit.Drain.Origin, attempt.Authority.Target.RunID); err != nil {
					return err
				}
				originSettled = true
			}
			if changed && strings.TrimSpace(attempt.Authority.Target.RunID) != "" {
				if _, err := s.requestCompletionCandidate(txctx, tx, attempt.Authority.Target.RunID, nil, handoff); err != nil {
					return err
				}
			}
			if err := recordExternalEffectStory(txctx, story, externalEffectStorySourceFromAttempt(attempt), attemptSettlement.Settlement.State, attemptSettlement.Settlement.Failure, attemptSettlement.Now.UTC()); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	result := runtimeeffects.CompletionSettlementResult{
		Committed: true, Disposition: disposition, SpendRecorded: spendRecorded, AttemptID: attempt.AttemptID, EntityID: settlement.Spend.EntityID,
		Origin: attempt.Origin, OriginSettled: originSettled, Finalization: finalization,
	}
	if continuation != nil {
		var err error
		result, err = runtimeeffects.AdmitCommittedCompletionContinuation(result, *continuation)
		if err != nil {
			return result, err
		}
	}
	return result, providerHeadErr
}

func (s *EffectSQLiteOwner) SettleCompletion(ctx context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	var providerHeadErr error
	var spendRecorded bool
	var originSettled bool
	var finalization *runtimeeffects.ProviderDrainFinalization
	var disposition runtimeeffects.CompletionSettlementDisposition
	var continuation *runtimeeffects.Attempt
	err := withRunLifecycleCandidateHandoff(ctx, func(handoff *runLifecycleCandidateHandoffReservation) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite settle completion", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
			providerHeadErr = nil
			spendRecorded = false
			originSettled = false
			finalization = nil
			disposition = ""
			continuation = nil
			attemptSettlement := settlement
			permit, err := resolveCompletionSettlementPermitSQLite(txctx, tx, attempt)
			if err != nil {
				return err
			}
			if permit.Kind == completionSettlementDrained {
				drained, err := runtimeeffects.ProjectDrainedCompletionSettlement(attempt, attemptSettlement)
				if err != nil {
					return err
				}
				attemptSettlement = drained.CompletionSettlement()
				disposition = runtimeeffects.CompletionSettlementDrained
			} else {
				disposition = runtimeeffects.CompletionSettlementCurrent
			}
			if err := requireCompletionAttemptSQLite(txctx, tx, attempt, attemptSettlement); err != nil {
				return err
			}
			if permit.Kind == completionSettlementCurrent && attemptSettlement.ProviderHead != nil {
				req := completionProviderHeadSettlement(attempt, attemptSettlement)
				if providerHeadErr = requireProviderHeadLifecycleSQLiteTx(txctx, tx, req); providerHeadErr == nil {
					providerHeadErr = promoteProviderHeadSQLiteTx(txctx, tx, req)
				}
				if providerHeadErr != nil {
					attemptSettlement = completionProviderHeadUncertainty(attemptSettlement, providerHeadErr)
				}
			}
			if err := insertCompletionTargetSQLite(txctx, tx, s.llm, attempt, attemptSettlement, permit.Kind == completionSettlementCurrent); err != nil {
				return err
			}
			if err := declareCompletionTargetEffects(effects, attemptSettlement); err != nil {
				return err
			}
			if err := s.llm.RecordCompletionTurnAuthorActivityTx(txctx, story, attempt, attemptSettlement); err != nil {
				return err
			}
			spendRecorded, err = insertCompletionSpendSQLite(txctx, tx, attempt, attemptSettlement)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=?`, attempt.AttemptID); err != nil {
				return fmt.Errorf("release sqlite completion budget reservations: %w", err)
			}
			changed, err := settleExternalAttemptSQLiteTx(txctx, tx, attemptSettlement.Settlement)
			if err != nil {
				return err
			}
			if attemptSettlement.Settlement.CompletionProjectionPhase == runtimeeffects.CompletionProjectionResponseSettled {
				admitted, err := loadSettledCompletionContinuationSQLite(txctx, tx, attempt)
				if err != nil {
					return err
				}
				continuation = &admitted
			}
			if permit.Kind == completionSettlementDrained {
				finalization, err = s.settleProviderDrainTx(txctx, tx, story, attempt, attemptSettlement, permit.Drain)
				if err != nil {
					return err
				}
				if err := declareProviderOriginEffects(effects, permit.Drain.Origin, attempt.Authority.Target.RunID); err != nil {
					return err
				}
				originSettled = true
			}
			if changed && strings.TrimSpace(attempt.Authority.Target.RunID) != "" {
				if _, err := s.requestCompletionCandidate(txctx, tx, attempt.Authority.Target.RunID, nil, handoff); err != nil {
					return err
				}
			}
			if err := recordExternalEffectStory(txctx, story, externalEffectStorySourceFromAttempt(attempt), attemptSettlement.Settlement.State, attemptSettlement.Settlement.Failure, attemptSettlement.Now.UTC()); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	result := runtimeeffects.CompletionSettlementResult{
		Committed: true, Disposition: disposition, SpendRecorded: spendRecorded, AttemptID: attempt.AttemptID, EntityID: settlement.Spend.EntityID,
		Origin: attempt.Origin, OriginSettled: originSettled, Finalization: finalization,
	}
	if continuation != nil {
		var err error
		result, err = runtimeeffects.AdmitCommittedCompletionContinuation(result, *continuation)
		if err != nil {
			return result, err
		}
	}
	return result, providerHeadErr
}

func completionProviderHeadUncertainty(settlement runtimeeffects.CompletionSettlement, cause error) runtimeeffects.CompletionSettlement {
	failure := runtimefailures.FromError(cause, "llm-completion-authority", "settle_provider_head")
	settlement.ProviderHead = nil
	settlement.Settlement.State = runtimeeffects.StateOutcomeUncertain
	settlement.Settlement.CompletionProjectionPhase = ""
	settlement.Settlement.Failure = &failure.Failure
	if settlement.Settlement.Evidence == nil {
		settlement.Settlement.Evidence = map[string]any{}
	}
	settlement.Settlement.Evidence["provider_head_precondition_conflict"] = true
	settlement.Settlement.Evidence["provider_head_failure_code"] = failure.Failure.Detail.Code
	if settlement.AgentTurn != nil {
		settlement.AgentTurn.Failure = &failure.Failure
	}
	return settlement
}

type completionProviderHeadRequest struct {
	runtimeeffects.Settlement
	Token                runtimeeffects.LifecycleToken
	Identity             agentmemory.Identity
	SessionID            string
	LockOwner            string
	ExpectedProviderHead string
	NewProviderHead      string
}

func completionProviderHeadSettlement(attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) completionProviderHeadRequest {
	head := settlement.ProviderHead
	return completionProviderHeadRequest{
		Settlement:           settlement.Settlement,
		Token:                attempt.Authority.Normal,
		Identity:             head.Identity,
		SessionID:            head.SessionID,
		LockOwner:            head.LockOwner,
		ExpectedProviderHead: head.ExpectedProviderHead,
		NewProviderHead:      head.NewProviderHead,
	}
}

func requireCompletionAttemptPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	var kind, authorityID, operationMode, attemptMode, owner, state, capabilitySurfaceID string
	var fence int64
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, o.execution_mode, a.execution_mode,
		       a.execution_owner, a.fence_generation, a.state,
		       COALESCE(a.capability_surface_id::text,'')
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid
		FOR UPDATE
	`, attempt.AttemptID, attempt.OperationID).Scan(&kind, &authorityID, &operationMode, &attemptMode, &owner, &fence, &state, &capabilitySurfaceID)
	return validateCompletionAttemptRow(attempt, settlement, kind, authorityID, operationMode, attemptMode, owner, uint64(fence), state, capabilitySurfaceID, err)
}

func requireCompletionAttemptSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	var kind, authorityID, operationMode, attemptMode, owner, state, capabilitySurfaceID string
	var fence int64
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, o.execution_mode, a.execution_mode,
		       a.execution_owner, a.fence_generation, a.state,
		       COALESCE(a.capability_surface_id,'')
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=? AND a.operation_id=?
	`, attempt.AttemptID, attempt.OperationID).Scan(&kind, &authorityID, &operationMode, &attemptMode, &owner, &fence, &state, &capabilitySurfaceID)
	return validateCompletionAttemptRow(attempt, settlement, kind, authorityID, operationMode, attemptMode, owner, uint64(fence), state, capabilitySurfaceID, err)
}

func validateCompletionAttemptRow(attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, kind, authorityID, operationMode, attemptMode, owner string, fence uint64, state, capabilitySurfaceID string, err error) error {
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("completion attempt %s does not exist", attempt.AttemptID)
		}
		return fmt.Errorf("lock completion attempt: %w", err)
	}
	mode := string(attempt.Authority.ExecutionMode)
	if kind != string(attempt.Authority.Kind) || authorityID != attempt.Authority.ID || operationMode != mode || attemptMode != mode || owner != attempt.Authority.ExecutionOwner || fence != attempt.Authority.FenceGeneration {
		return fmt.Errorf("completion attempt authority or fence is stale")
	}
	expectedSurfaceID := ""
	if settlement.AgentTurn != nil {
		expectedSurfaceID = strings.TrimSpace(settlement.AgentTurn.CapabilitySurfaceID)
	}
	if strings.TrimSpace(capabilitySurfaceID) != expectedSurfaceID {
		return fmt.Errorf("completion attempt capability surface does not match settlement")
	}
	switch runtimeeffects.State(state) {
	case runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved:
		return nil
	case runtimeeffects.StateAuthorized:
		if settlement.AgentTurn == nil {
			return nil
		}
		return fmt.Errorf("completion attempt cannot materialize an agent turn from state %s", state)
	default:
		return fmt.Errorf("completion attempt is already terminal in state %s", state)
	}
}

func completionAgentFramePostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, turn *runtimeeffects.CompletionAgentTurn, surface managedcapabilities.Surface) ([]byte, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT o.agent_frame_bytes
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid
	`, attempt.AttemptID, attempt.OperationID).Scan(&raw)
	return validateCompletionAgentFrame(raw, attempt, turn, surface, err)
}

func completionAgentFrameSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, turn *runtimeeffects.CompletionAgentTurn, surface managedcapabilities.Surface) ([]byte, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT o.agent_frame_bytes
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=? AND a.operation_id=?
	`, attempt.AttemptID, attempt.OperationID).Scan(&raw)
	return validateCompletionAgentFrame(raw, attempt, turn, surface, err)
}

func validateCompletionAgentFrame(raw []byte, attempt runtimeeffects.Attempt, turn *runtimeeffects.CompletionAgentTurn, surface managedcapabilities.Surface, loadErr error) ([]byte, error) {
	if loadErr != nil {
		return nil, fmt.Errorf("load completion operation frame: %w", loadErr)
	}
	if turn == nil || attempt.Authority.Target.Kind != runtimeeffects.UsageTargetAgentTurn {
		return nil, fmt.Errorf("completion operation frame requires agent-turn settlement")
	}
	frame, err := agentframe.DecodeDurable(raw)
	if err != nil {
		return nil, fmt.Errorf("hydrate completion operation frame: %w", err)
	}
	if err := runtimeeffects.ValidateManagedAgentFrame(frame, attempt.Authority, surface); err != nil {
		return nil, fmt.Errorf("validate completion operation frame authority: %w", err)
	}
	sameActor, err := runtimeagentidentity.Equal(frame.Session.AgentIdentity, turn.Identity.Agent)
	if err != nil || !sameActor || frame.Turn.Event.RunID != strings.TrimSpace(turn.RunID) ||
		frame.Turn.Event.ID != strings.TrimSpace(turn.TriggerEventID) || frame.Turn.Event.Type != strings.TrimSpace(turn.TriggerEventType) ||
		frame.Turn.Capability.SurfaceID != strings.TrimSpace(turn.CapabilitySurfaceID) || frame.FrameID != "agent-frame:v1:"+strings.TrimSpace(turn.TurnID) {
		return nil, fmt.Errorf("completion operation frame does not match immutable turn evidence")
	}
	return append([]byte(nil), raw...), nil
}

func insertCompletionTargetPostgres(ctx context.Context, tx *sql.Tx, llm *storellm.LLMPostgresOwner, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, projectCurrentMemory ...bool) error {
	if attempt.Authority.Target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		return insertForkCompletionPostgres(ctx, tx, attempt, settlement)
	}
	t := settlement.AgentTurn
	if len(projectCurrentMemory) == 0 || projectCurrentMemory[0] {
		if err := llm.EnsureCompletionTurnMemoryTx(ctx, tx, completionTurnRecord(t)); err != nil {
			return err
		}
	}
	surface, err := storemanagedcapability.InsertPostgres(ctx, tx, t.CapabilitySurface)
	if err != nil {
		return err
	}
	if surface.ID != strings.TrimSpace(t.CapabilitySurfaceID) || surface.Authority.ID != strings.TrimSpace(t.TurnID) {
		return fmt.Errorf("completion turn capability surface identity mismatch")
	}
	if err := storemanagedcapability.ValidateAgentTurn(surface, t.Identity.Agent, t.SessionID, t.RunID); err != nil {
		return err
	}
	agentFrame, err := completionAgentFramePostgres(ctx, tx, attempt, t, surface)
	if err != nil {
		return err
	}
	fields, err := agentIdentityFields(t.Identity.Agent)
	if err != nil {
		return err
	}
	failure, err := completionFailureJSON(t.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count,
			agent_frame_bytes, completion_attempt_id, execution_mode, resolved_model, usage_exactness, input_tokens, output_tokens,
			cache_read_input_tokens, cache_creation_input_tokens, cache_creation_5m_input_tokens,
			cache_creation_1h_input_tokens, provider_reported_cost_usd, failure, created_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9::uuid,$10,$11,$12,NULLIF($13,'')::uuid,
			NULLIF($14,'')::uuid,NULLIF($15,''),NULLIF($16,''),$17::uuid,$18::jsonb,$19::jsonb,
			$20::jsonb,$21::jsonb,$22::jsonb,$23,$24,$25,$26,$27::uuid,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38::jsonb,$39
		)
	`, t.TurnID, t.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, t.SessionID, fields.FlowInstancePath, t.Memory.Enabled, string(t.Memory.Source), t.EntityID,
		t.TriggerEventID, t.TriggerEventType, t.TaskID, t.CapabilitySurfaceID, completionJSON(t.ToolCalls, `[]`),
		completionJSON(t.EmittedEvents, `[]`),
		completionNullableJSON(t.RequestPayload), completionNullableJSON(t.ResponsePayload), completionJSON(t.TurnBlocks, `[]`), t.ParseOK, t.LatencyMS, t.RetryCount,
		agentFrame, attempt.AttemptID, attempt.Authority.ExecutionMode, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, nullableJSON(failure), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert completion agent turn: %w", err)
	}
	return nil
}

func insertCompletionTargetSQLite(ctx context.Context, tx *sql.Tx, llm *storellm.LLMSQLiteOwner, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, projectCurrentMemory ...bool) error {
	if attempt.Authority.Target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		return insertForkCompletionSQLite(ctx, tx, attempt, settlement)
	}
	t := settlement.AgentTurn
	if len(projectCurrentMemory) == 0 || projectCurrentMemory[0] {
		if err := llm.EnsureCompletionTurnMemoryTx(ctx, tx, completionTurnRecord(t), settlement.Now.UTC()); err != nil {
			return err
		}
	}
	surface, err := storemanagedcapability.InsertSQLite(ctx, tx, t.CapabilitySurface)
	if err != nil {
		return err
	}
	if surface.ID != strings.TrimSpace(t.CapabilitySurfaceID) || surface.Authority.ID != strings.TrimSpace(t.TurnID) {
		return fmt.Errorf("sqlite completion turn capability surface identity mismatch")
	}
	if err := storemanagedcapability.ValidateAgentTurn(surface, t.Identity.Agent, t.SessionID, t.RunID); err != nil {
		return err
	}
	agentFrame, err := completionAgentFrameSQLite(ctx, tx, attempt, t, surface)
	if err != nil {
		return err
	}
	fields, err := agentIdentityFields(t.Identity.Agent)
	if err != nil {
		return err
	}
	failure, err := completionFailureJSON(t.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls,
			emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count,
			agent_frame_bytes, completion_attempt_id, execution_mode, resolved_model, usage_exactness, input_tokens, output_tokens,
			cache_read_input_tokens, cache_creation_input_tokens, cache_creation_5m_input_tokens,
			cache_creation_1h_input_tokens, provider_reported_cost_usd, failure, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, t.TurnID, sqliteNullString(t.RunID), fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, t.SessionID, fields.FlowInstancePath, t.Memory.Enabled, string(t.Memory.Source), sqliteNullString(t.EntityID),
		sqliteNullString(t.TriggerEventID), sqliteNullString(t.TriggerEventType), sqliteNullString(t.TaskID), t.CapabilitySurfaceID, completionJSON(t.ToolCalls, `[]`),
		completionJSON(t.EmittedEvents, `[]`),
		completionNullableJSON(t.RequestPayload), completionNullableJSON(t.ResponsePayload), completionJSON(t.TurnBlocks, `[]`), t.ParseOK, t.LatencyMS, t.RetryCount,
		agentFrame, attempt.AttemptID, attempt.Authority.ExecutionMode, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, sqliteNullableJSON(failure), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite completion agent turn: %w", err)
	}
	return nil
}

func completionTurnRecord(t *runtimeeffects.CompletionAgentTurn) runtimellm.AgentTurnRecord {
	return runtimellm.AgentTurnRecord{
		RunID: t.RunID, AgentID: t.AgentID, Identity: t.Identity, SessionID: t.SessionID, Memory: t.Memory,
		FlowInstance: t.FlowInstance, EntityID: t.EntityID,
	}
}

func insertForkCompletionPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	failure, err := completionFailureJSON(settlement.Settlement.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO conversation_fork_turn_completions (
			completion_id,fork_turn_id,ordinal,attempt_id,execution_mode,resolved_model,usage_exactness,input_tokens,output_tokens,
			cache_read_input_tokens,cache_creation_input_tokens,cache_creation_5m_input_tokens,cache_creation_1h_input_tokens,
			provider_reported_cost_usd,state,failure,evidence,created_at
		) VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17::jsonb,$18)
	`, completionChildID(attempt.AttemptID), attempt.Authority.ForkChat.ForkTurnID, attempt.Authority.Target.Ordinal, attempt.AttemptID,
		attempt.Authority.ExecutionMode, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, completionChildState(settlement.Settlement.State),
		nullableJSON(failure), completionJSON(runtimeeffects.EvidenceJSON(settlement.Settlement.Evidence), `{}`), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert forkchat completion child: %w", err)
	}
	return nil
}

func insertForkCompletionSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	failure, err := completionFailureJSON(settlement.Settlement.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO conversation_fork_turn_completions (
			completion_id,fork_turn_id,ordinal,attempt_id,execution_mode,resolved_model,usage_exactness,input_tokens,output_tokens,
			cache_read_input_tokens,cache_creation_input_tokens,cache_creation_5m_input_tokens,cache_creation_1h_input_tokens,
			provider_reported_cost_usd,state,failure,evidence,created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, completionChildID(attempt.AttemptID), attempt.Authority.ForkChat.ForkTurnID, attempt.Authority.Target.Ordinal, attempt.AttemptID,
		attempt.Authority.ExecutionMode, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, completionChildState(settlement.Settlement.State),
		sqliteNullableJSON(failure), completionJSON(runtimeeffects.EvidenceJSON(settlement.Settlement.Evidence), `{}`), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite forkchat completion child: %w", err)
	}
	return nil
}

func insertCompletionSpendPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (bool, error) {
	cost, accounting, basis, insert, err := completionSpendValuesPostgres(ctx, tx, attempt, settlement)
	if err != nil || !insert {
		return false, err
	}
	return true, insertCompletionSpendRow(ctx, tx, true, attempt, settlement, cost, accounting, basis)
}

func insertCompletionSpendSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (bool, error) {
	cost, accounting, basis, insert, err := completionSpendValuesSQLite(ctx, tx, attempt, settlement)
	if err != nil || !insert {
		return false, err
	}
	return true, insertCompletionSpendRow(ctx, tx, false, attempt, settlement, cost, accounting, basis)
}

func completionSpendValuesPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (float64, string, string, bool, error) {
	if settlement.Settlement.State == runtimeeffects.StateTerminalFailure && settlement.Usage.Exactness == runtimeeffects.CompletionUsageUnavailable {
		return 0, "", "", false, nil
	}
	if settlement.Usage.Exactness != runtimeeffects.CompletionUsageUnavailable {
		return settlement.Spend.CostUSD, string(settlement.Usage.Exactness), "", true, nil
	}
	var amount float64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(amount_usd),0) FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, attempt.AttemptID).Scan(&amount); err != nil {
		return 0, "", "", false, fmt.Errorf("read pessimistic completion reservation: %w", err)
	}
	return amount, "estimated", "accounting_unavailable_exhaustion", amount > 0, nil
}

func completionSpendValuesSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (float64, string, string, bool, error) {
	if settlement.Settlement.State == runtimeeffects.StateTerminalFailure && settlement.Usage.Exactness == runtimeeffects.CompletionUsageUnavailable {
		return 0, "", "", false, nil
	}
	if settlement.Usage.Exactness != runtimeeffects.CompletionUsageUnavailable {
		return settlement.Spend.CostUSD, string(settlement.Usage.Exactness), "", true, nil
	}
	var amount float64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(amount_usd),0) FROM runtime_effect_budget_reservations WHERE attempt_id=?`, attempt.AttemptID).Scan(&amount); err != nil {
		return 0, "", "", false, fmt.Errorf("read sqlite pessimistic completion reservation: %w", err)
	}
	return amount, "estimated", "accounting_unavailable_exhaustion", amount > 0, nil
}

func insertCompletionSpendRow(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, cost float64, accounting, basis string) error {
	s := settlement.Spend
	var identityFields runtimeagentidentity.StorageFields
	if !s.AgentIdentity.IsZero() {
		var err error
		identityFields, err = s.AgentIdentity.Normalize().StorageFields()
		if err != nil {
			return fmt.Errorf("completion spend agent identity: %w", err)
		}
	}
	var input, output int64
	if settlement.Usage.InputTokens != nil {
		input = *settlement.Usage.InputTokens
	}
	if settlement.Usage.OutputTokens != nil {
		output = *settlement.Usage.OutputTokens
	}
	query := `INSERT INTO spend_ledger (ledger_id,external_effect_attempt_id,execution_mode,entity_id,flow_instance,agent_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,accounting_basis,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	args := []any{uuid.NewString(), attempt.AttemptID, attempt.Authority.ExecutionMode, sqliteNullString(s.EntityID), s.FlowInstance, s.AgentID, sqliteNullString(identityFields.NameOwner), sqliteNullString(identityFields.NameSource), sqliteNullString(identityFields.RoutePresence), sqliteNullString(identityFields.FlowScopeKey), sqliteNullString(identityFields.FlowInstanceID), s.Model, defaultCompletionIdentity(s.ModelAlias), s.BackendProfile, s.Provider, s.Transport, s.ResolvedModel, input, output, cost, s.InvocationType, accounting, sqliteNullString(basis), settlement.Now.UTC()}
	if postgres {
		query = `INSERT INTO spend_ledger (ledger_id,external_effect_attempt_id,execution_mode,entity_id,flow_instance,agent_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,accounting_basis,created_at) VALUES ($1::uuid,$2::uuid,$3,NULLIF($4,'')::uuid,$5,$6,NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NULLIF($23,''),$24)`
		args[3] = strings.TrimSpace(s.EntityID)
		args[22] = basis
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert completion spend: %w", err)
	}
	return nil
}

func completionFailureJSON(failure *runtimefailures.Envelope) ([]byte, error) {
	if failure == nil {
		return nil, nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return nil, fmt.Errorf("marshal completion failure: %w", err)
	}
	return raw, nil
}

func completionJSON(raw json.RawMessage, fallback string) string {
	if len(raw) == 0 || !json.Valid(raw) {
		return fallback
	}
	return string(raw)
}

func completionNullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	return string(raw)
}

func completionChildID(attemptID string) string {
	id, err := uuid.Parse(strings.TrimSpace(attemptID))
	if err != nil {
		return ""
	}
	return uuid.NewSHA1(id, []byte("forkchat-completion")).String()
}

func completionChildState(state runtimeeffects.State) string {
	switch state {
	case runtimeeffects.StateSettled:
		return "succeeded"
	case runtimeeffects.StateTerminalFailure:
		return "failed"
	default:
		return "outcome_uncertain"
	}
}

func defaultCompletionIdentity(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "unknown"
}
