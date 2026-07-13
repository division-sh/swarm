package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var _ runtimeeffects.CompletionStore = (*PostgresStore)(nil)
var _ runtimeeffects.CompletionStore = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) SettleCompletion(ctx context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("settle completion begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireExternalEffectAuthorityPostgres(ctx, tx, attempt.Authority, false); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	if err := requireCompletionAttemptPostgres(ctx, tx, attempt); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	var providerHeadErr error
	if settlement.ProviderHead != nil {
		req := completionProviderHeadSettlement(attempt, settlement)
		if providerHeadErr = requireProviderHeadLifecyclePostgres(ctx, tx, req); providerHeadErr == nil {
			providerHeadErr = promoteProviderHeadPostgres(ctx, tx, req)
		}
		if providerHeadErr != nil {
			settlement = completionProviderHeadUncertainty(settlement, providerHeadErr)
		}
	}
	if err := insertCompletionTargetPostgres(ctx, tx, attempt, settlement); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	spendRecorded, err := insertCompletionSpendPostgres(ctx, tx, attempt, settlement)
	if err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, attempt.AttemptID); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("release completion budget reservations: %w", err)
	}
	if err := settleExternalAttemptPostgres(ctx, tx, settlement.Settlement); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("settle completion commit: %w", err)
	}
	return runtimeeffects.CompletionSettlementResult{
		Committed: true, SpendRecorded: spendRecorded, AttemptID: attempt.AttemptID, EntityID: settlement.Spend.EntityID,
	}, providerHeadErr
}

func (s *SQLiteRuntimeStore) SettleCompletion(ctx context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	var providerHeadErr error
	var spendRecorded bool
	err := s.runRuntimeMutation(ctx, "sqlite settle completion", func(txctx context.Context, tx *sql.Tx) error {
		providerHeadErr = nil
		spendRecorded = false
		attemptSettlement := settlement
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, attempt.Authority, false); err != nil {
			return err
		}
		if err := requireCompletionAttemptSQLite(txctx, tx, attempt); err != nil {
			return err
		}
		if attemptSettlement.ProviderHead != nil {
			req := completionProviderHeadSettlement(attempt, attemptSettlement)
			if providerHeadErr = requireProviderHeadLifecycleSQLiteTx(txctx, tx, req); providerHeadErr == nil {
				providerHeadErr = promoteProviderHeadSQLiteTx(txctx, tx, req)
			}
			if providerHeadErr != nil {
				attemptSettlement = completionProviderHeadUncertainty(attemptSettlement, providerHeadErr)
			}
		}
		if err := insertCompletionTargetSQLite(txctx, tx, attempt, attemptSettlement); err != nil {
			return err
		}
		var err error
		spendRecorded, err = insertCompletionSpendSQLite(txctx, tx, attempt, attemptSettlement)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=?`, attempt.AttemptID); err != nil {
			return fmt.Errorf("release sqlite completion budget reservations: %w", err)
		}
		return settleExternalAttemptSQLiteTx(txctx, tx, attemptSettlement.Settlement)
	})
	if err != nil {
		return runtimeeffects.CompletionSettlementResult{}, err
	}
	return runtimeeffects.CompletionSettlementResult{
		Committed: true, SpendRecorded: spendRecorded, AttemptID: attempt.AttemptID, EntityID: settlement.Spend.EntityID,
	}, providerHeadErr
}

func completionProviderHeadUncertainty(settlement runtimeeffects.CompletionSettlement, cause error) runtimeeffects.CompletionSettlement {
	failure := runtimefailures.FromError(cause, "llm-completion-authority", "settle_provider_head")
	settlement.ProviderHead = nil
	settlement.Settlement.State = runtimeeffects.StateOutcomeUncertain
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
	AgentID              string
	RuntimeMode          string
	SessionID            string
	ScopeKey             string
	LockOwner            string
	ExpectedProviderHead string
	NewProviderHead      string
}

func completionProviderHeadSettlement(attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) completionProviderHeadRequest {
	head := settlement.ProviderHead
	return completionProviderHeadRequest{
		Settlement:           settlement.Settlement,
		Token:                attempt.Authority.Normal,
		AgentID:              head.AgentID,
		RuntimeMode:          head.RuntimeMode,
		SessionID:            head.SessionID,
		ScopeKey:             head.ScopeKey,
		LockOwner:            head.LockOwner,
		ExpectedProviderHead: head.ExpectedProviderHead,
		NewProviderHead:      head.NewProviderHead,
	}
}

func requireCompletionAttemptPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) error {
	var kind, authorityID, owner, state string
	var fence int64
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, a.execution_owner, a.fence_generation, a.state
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid
		FOR UPDATE
	`, attempt.AttemptID, attempt.OperationID).Scan(&kind, &authorityID, &owner, &fence, &state)
	return validateCompletionAttemptRow(attempt, kind, authorityID, owner, uint64(fence), state, err)
}

func requireCompletionAttemptSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) error {
	var kind, authorityID, owner, state string
	var fence int64
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, a.execution_owner, a.fence_generation, a.state
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=? AND a.operation_id=?
	`, attempt.AttemptID, attempt.OperationID).Scan(&kind, &authorityID, &owner, &fence, &state)
	return validateCompletionAttemptRow(attempt, kind, authorityID, owner, uint64(fence), state, err)
}

func validateCompletionAttemptRow(attempt runtimeeffects.Attempt, kind, authorityID, owner string, fence uint64, state string, err error) error {
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("completion attempt %s does not exist", attempt.AttemptID)
		}
		return fmt.Errorf("lock completion attempt: %w", err)
	}
	if kind != string(attempt.Authority.Kind) || authorityID != attempt.Authority.ID || owner != attempt.Authority.ExecutionOwner || fence != attempt.Authority.FenceGeneration {
		return fmt.Errorf("completion attempt authority or fence is stale")
	}
	switch runtimeeffects.State(state) {
	case runtimeeffects.StateAuthorized, runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved:
		return nil
	default:
		return fmt.Errorf("completion attempt is already terminal in state %s", state)
	}
}

func insertCompletionTargetPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	if attempt.Authority.Target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		return insertForkCompletionPostgres(ctx, tx, attempt, settlement)
	}
	t := settlement.AgentTurn
	failure, err := completionFailureJSON(t.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count,
			completion_attempt_id, resolved_model, usage_exactness, input_tokens, output_tokens,
			cache_read_input_tokens, cache_creation_input_tokens, cache_creation_5m_input_tokens,
			cache_creation_1h_input_tokens, provider_reported_cost_usd, failure, created_at
		) VALUES (
			$1::uuid,NULLIF($2,'')::uuid,$3,$4::uuid,$5,NULLIF($6,''),NULLIF($7,'')::uuid,
			NULLIF($8,'')::uuid,NULLIF($9,''),NULLIF($10,''),$11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,
			$17::jsonb,$18::jsonb,$19::jsonb,$20,$21,$22,$23::uuid,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33::jsonb,$34
		)
	`, t.TurnID, t.RunID, t.AgentID, t.SessionID, t.RuntimeMode, t.ScopeKey, t.EntityID,
		t.TriggerEventID, t.TriggerEventType, t.TaskID, completionJSON(t.AvailableTools, `[]`), completionJSON(t.ToolCalls, `[]`),
		completionJSON(t.EmittedEvents, `[]`), completionJSON(t.MCPServers, `{}`), completionJSON(t.MCPToolsListed, `[]`), completionJSON(t.MCPToolsVisible, `[]`),
		completionNullableJSON(t.RequestPayload), completionNullableJSON(t.ResponsePayload), completionJSON(t.TurnBlocks, `[]`), t.ParseOK, t.LatencyMS, t.RetryCount,
		attempt.AttemptID, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, nullableJSON(failure), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert completion agent turn: %w", err)
	}
	return nil
}

func insertCompletionTargetSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	if attempt.Authority.Target.Kind == runtimeeffects.UsageTargetConversationForkCompletion {
		return insertForkCompletionSQLite(ctx, tx, attempt, settlement)
	}
	t := settlement.AgentTurn
	failure, err := completionFailureJSON(t.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count,
			completion_attempt_id, resolved_model, usage_exactness, input_tokens, output_tokens,
			cache_read_input_tokens, cache_creation_input_tokens, cache_creation_5m_input_tokens,
			cache_creation_1h_input_tokens, provider_reported_cost_usd, failure, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, t.TurnID, sqliteNullString(t.RunID), t.AgentID, t.SessionID, t.RuntimeMode, sqliteNullString(t.ScopeKey), sqliteNullString(t.EntityID),
		sqliteNullString(t.TriggerEventID), sqliteNullString(t.TriggerEventType), sqliteNullString(t.TaskID), completionJSON(t.AvailableTools, `[]`), completionJSON(t.ToolCalls, `[]`),
		completionJSON(t.EmittedEvents, `[]`), completionJSON(t.MCPServers, `{}`), completionJSON(t.MCPToolsListed, `[]`), completionJSON(t.MCPToolsVisible, `[]`),
		completionNullableJSON(t.RequestPayload), completionNullableJSON(t.ResponsePayload), completionJSON(t.TurnBlocks, `[]`), t.ParseOK, t.LatencyMS, t.RetryCount,
		attempt.AttemptID, u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
		u.CacheCreation5mInputTokens, u.CacheCreation1hInputTokens, u.ProviderReportedCostUSD, sqliteNullableJSON(failure), settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite completion agent turn: %w", err)
	}
	return nil
}

func insertForkCompletionPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	failure, err := completionFailureJSON(settlement.Settlement.Failure)
	if err != nil {
		return err
	}
	u := settlement.Usage
	_, err = tx.ExecContext(ctx, `
		INSERT INTO conversation_fork_turn_completions (
			completion_id,fork_turn_id,ordinal,attempt_id,resolved_model,usage_exactness,input_tokens,output_tokens,
			cache_read_input_tokens,cache_creation_input_tokens,cache_creation_5m_input_tokens,cache_creation_1h_input_tokens,
			provider_reported_cost_usd,state,failure,evidence,created_at
		) VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17)
	`, completionChildID(attempt.AttemptID), attempt.Authority.ForkChat.ForkTurnID, attempt.Authority.Target.Ordinal, attempt.AttemptID,
		u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
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
			completion_id,fork_turn_id,ordinal,attempt_id,resolved_model,usage_exactness,input_tokens,output_tokens,
			cache_read_input_tokens,cache_creation_input_tokens,cache_creation_5m_input_tokens,cache_creation_1h_input_tokens,
			provider_reported_cost_usd,state,failure,evidence,created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, completionChildID(attempt.AttemptID), attempt.Authority.ForkChat.ForkTurnID, attempt.Authority.Target.Ordinal, attempt.AttemptID,
		u.ResolvedModel, string(u.Exactness), u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens,
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
	var input, output int64
	if settlement.Usage.InputTokens != nil {
		input = *settlement.Usage.InputTokens
	}
	if settlement.Usage.OutputTokens != nil {
		output = *settlement.Usage.OutputTokens
	}
	query := `INSERT INTO spend_ledger (ledger_id,external_effect_attempt_id,entity_id,flow_instance,agent_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,accounting_basis,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	args := []any{uuid.NewString(), attempt.AttemptID, sqliteNullString(s.EntityID), s.FlowInstance, s.AgentID, s.Model, defaultCompletionIdentity(s.ModelAlias), s.BackendProfile, s.Provider, s.Transport, s.ResolvedModel, input, output, cost, s.InvocationType, accounting, sqliteNullString(basis), settlement.Now.UTC()}
	if postgres {
		query = `INSERT INTO spend_ledger (ledger_id,external_effect_attempt_id,entity_id,flow_instance,agent_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,accounting_basis,created_at) VALUES ($1::uuid,$2::uuid,NULLIF($3,'')::uuid,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NULLIF($17,''),$18)`
		args[2] = strings.TrimSpace(s.EntityID)
		args[16] = basis
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
