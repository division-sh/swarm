package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/google/uuid"
)

const conversationForkChatExecutionLease = 2 * time.Minute

func (s *RunForkPostgresOwner) AdmitOperatorConversationForkChat(ctx context.Context, forkID string, posture executionposture.Posture) error {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.admitOperatorConversationForkChat(ctx, forkID, posture)
}

func (s *RunForkSQLiteOwner) AdmitOperatorConversationForkChat(ctx context.Context, forkID string, posture executionposture.Posture) error {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.admitOperatorConversationForkChat(ctx, forkID, posture)
}

func (s conversationForkStore) admitOperatorConversationForkChat(ctx context.Context, forkID string, posture executionposture.Posture) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if !posture.Valid() {
		return fmt.Errorf("conversation fork chat execution posture is invalid")
	}
	fork, err := s.loadOperatorConversationFork(ctx, forkID)
	if err != nil {
		return err
	}
	if fork.State != "active" {
		return &operatorread.EntityReadParamError{Field: "fork_id", Reason: "must reference an active fork"}
	}
	return admitConversationForkSourceAgent(ctx, s, s.db, fork, posture)
}

func (s *RunForkPostgresOwner) PrepareOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatPrepareRequest) (runtimerunfork.ConversationForkChatPrepared, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkChatPrepared{}, err
	}
	return owner.prepareOperatorConversationForkChat(ctx, req)
}

func (s *RunForkSQLiteOwner) PrepareOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatPrepareRequest) (runtimerunfork.ConversationForkChatPrepared, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkChatPrepared{}, err
	}
	return owner.prepareOperatorConversationForkChat(ctx, req)
}

func (s conversationForkStore) prepareOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatPrepareRequest) (runtimerunfork.ConversationForkChatPrepared, error) {
	forkID, err := normalizeUUIDParam(req.ForkID, "fork_id")
	if err != nil {
		return runtimerunfork.ConversationForkChatPrepared{}, err
	}
	message := strings.TrimSpace(req.Message)
	actorTokenID := strings.TrimSpace(req.ActorTokenID)
	requestHash := strings.TrimSpace(req.RequestHash)
	method := strings.TrimSpace(req.Method)
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if message == "" || actorTokenID == "" || requestHash == "" || method == "" {
		return runtimerunfork.ConversationForkChatPrepared{}, fmt.Errorf("conversation fork chat preparation requires message, method, actor token, and request hash")
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.ConversationForkChatPrepared{}, err
	}

	var prepared runtimerunfork.ConversationForkChatPrepared
	err = s.runForkMutation(ctx, forkID, true, func(txctx context.Context, tx *sql.Tx) error {
		if idempotencyKey != "" {
			if err := rejectConversationForkChatReplay(txctx, s, tx, forkID, method, actorTokenID, idempotencyKey, requestHash); err != nil {
				return err
			}
		}
		fork, err := loadActiveConversationForkForChat(txctx, s, tx, forkID, now)
		if err != nil {
			return err
		}
		if err := admitConversationForkSourceAgent(txctx, s, tx, fork, req.ExecutionPosture); err != nil {
			return err
		}
		sourceBundleHash, err := loadConversationForkChatBundleHash(txctx, s, tx, fork.SourceRunID)
		if err != nil {
			return err
		}
		snapshot, err := ensureConversationForkSnapshot(txctx, s, tx, fork, now)
		if err != nil {
			return err
		}
		policy := runtimerunfork.CanonicalConversationForkSandboxPolicy()
		forkTurnID, turnIndex, occurrenceID, executionOwner, leaseExpiresAt, err := preallocateConversationForkTurn(txctx, s, tx, forkID, sourceBundleHash, method, actorTokenID, idempotencyKey, requestHash, message, now)
		if err != nil {
			return err
		}
		prepared = runtimerunfork.ConversationForkChatPrepared{
			Fork:           fork,
			Snapshot:       snapshot,
			SandboxPolicy:  policy,
			AvailableTools: policy.AvailableToolNames(),
			ForkTurnID:     forkTurnID, SourceBundleHash: sourceBundleHash, TurnIndex: turnIndex, RequestOccurrenceID: occurrenceID,
			RequestHash: requestHash, IdempotencyKey: idempotencyKey, ActorTokenID: actorTokenID,
			ExecutionOwner: executionOwner, LeaseExpiresAt: leaseExpiresAt, FenceGeneration: 1,
		}
		return nil
	})
	if err != nil {
		return runtimerunfork.ConversationForkChatPrepared{}, fmt.Errorf("prepare conversation fork chat: %w", err)
	}
	return prepared, nil
}

func admitConversationForkSourceAgent(
	ctx context.Context,
	owner conversationForkStore,
	q conversationForkQueryer,
	fork runtimerunfork.OperatorConversationForkSession,
	posture executionposture.Posture,
) error {
	if !posture.Valid() {
		return fmt.Errorf("conversation fork chat execution posture is invalid")
	}
	snapshot, err := loadConversationForkSnapshot(ctx, owner, q, fork.ForkID)
	if err == nil {
		return posture.Admit(snapshot.SourceAgent.ExecutionMode, "conversation fork chat source actor admission")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	sourceAgent, err := loadConversationForkSourceAgent(ctx, owner, q, fork.SourceIdentity)
	if err != nil {
		return err
	}
	return posture.Admit(sourceAgent.ExecutionMode, "conversation fork chat source actor admission")
}

func rejectConversationForkChatReplay(
	ctx context.Context,
	owner conversationForkStore,
	tx *sql.Tx,
	forkID, method, actorTokenID, idempotencyKey, requestHash string,
) error {
	var existingID, existingForkID, existingHash, state string
	err := owner.queryRow(ctx, tx, `
		SELECT CAST(fork_turn_id AS TEXT), CAST(fork_id AS TEXT), request_hash, state
		FROM conversation_fork_turns
		WHERE actor_token_id=? AND idempotency_key=?
	`, actorTokenID, idempotencyKey).Scan(&existingID, &existingForkID, &existingHash, &state)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load keyed conversation fork turn: %w", err)
	}
	if existingHash != requestHash || existingForkID != forkID {
		return &apiidempotency.ConflictError{
			OriginalRequestHash: existingHash, ConflictingRequestHash: requestHash,
			Method: method, ResourceID: existingForkID,
		}
	}
	return &runtimerunfork.ConversationForkChatReplayStateError{ForkTurnID: existingID, State: state}
}

func (s *RunForkPostgresOwner) RecordOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatRecordRequest) (runtimerunfork.ConversationForkChatResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkChatResult{}, err
	}
	return owner.recordOperatorConversationForkChat(ctx, req)
}

func (s *RunForkSQLiteOwner) RecordOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatRecordRequest) (runtimerunfork.ConversationForkChatResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkChatResult{}, err
	}
	return owner.recordOperatorConversationForkChat(ctx, req)
}

func (s *RunForkPostgresOwner) FailOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatFailureRequest) error {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.failOperatorConversationForkChat(ctx, req)
}

func (s *RunForkSQLiteOwner) FailOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatFailureRequest) error {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.failOperatorConversationForkChat(ctx, req)
}

func (s *RunForkPostgresOwner) HeartbeatOperatorConversationForkChat(ctx context.Context, prepared runtimerunfork.ConversationForkChatPrepared, now time.Time) error {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.heartbeatOperatorConversationForkChat(ctx, prepared, now)
}

func (s *RunForkSQLiteOwner) HeartbeatOperatorConversationForkChat(ctx context.Context, prepared runtimerunfork.ConversationForkChatPrepared, now time.Time) error {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return err
	}
	return owner.heartbeatOperatorConversationForkChat(ctx, prepared, now)
}

func (s conversationForkStore) heartbeatOperatorConversationForkChat(ctx context.Context, prepared runtimerunfork.ConversationForkChatPrepared, now time.Time) error {
	if prepared.ForkTurnID == "" || prepared.Fork.ForkID == "" || prepared.ActorTokenID == "" || prepared.RequestOccurrenceID == "" ||
		prepared.RequestHash == "" || prepared.SourceBundleHash == "" || prepared.ExecutionOwner == "" || prepared.FenceGeneration == 0 {
		return fmt.Errorf("conversation fork chat heartbeat requires exact prepared authority")
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expires := now.Add(conversationForkChatExecutionLease)
	return s.runForkMutation(ctx, prepared.Fork.ForkID, true, func(txctx context.Context, tx *sql.Tx) error {
		res, err := s.exec(txctx, tx, `
			UPDATE conversation_fork_turns
			SET lease_expires_at=CASE WHEN lease_expires_at>? THEN lease_expires_at ELSE ? END,updated_at=?
			WHERE fork_turn_id=? AND fork_id=? AND actor_token_id=? AND request_occurrence_id=? AND request_hash=?
			  AND bundle_hash=? AND state IN ('prepared','executing') AND execution_owner=? AND fence_generation=? AND `+s.currentLeaseSQL()+`
		`, expires, expires, now, prepared.ForkTurnID, prepared.Fork.ForkID, prepared.ActorTokenID, prepared.RequestOccurrenceID,
			prepared.RequestHash, prepared.SourceBundleHash, prepared.ExecutionOwner, prepared.FenceGeneration)
		if err := requireExactlyOneMutation(res, err, "heartbeat conversation fork chat"); err != nil {
			return err
		}
		return nil
	})
}

func (s conversationForkStore) failOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatFailureRequest) error {
	prepared := req.Prepared
	if prepared.ForkTurnID == "" || prepared.Fork.ForkID == "" || prepared.SourceBundleHash == "" || prepared.RequestOccurrenceID == "" || prepared.RequestHash == "" || prepared.FenceGeneration == 0 || req.Cause == nil {
		return fmt.Errorf("conversation fork chat failure requires exact prepared authority and cause")
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	failure := runtimefailures.FromError(req.Cause, "conversation-fork-chat", "execute")
	failureJSON, err := json.Marshal(failure.Failure)
	if err != nil {
		return err
	}
	state := "failed"
	if req.OutcomeUncertain {
		state = "outcome_uncertain"
	}
	return s.runForkMutation(ctx, prepared.Fork.ForkID, true, func(txctx context.Context, tx *sql.Tx) error {
		res, err := s.exec(txctx, tx, `
			UPDATE conversation_fork_turns
			SET state=?,lease_expires_at=NULL,failure=?,updated_at=?,terminal_at=?
			WHERE fork_turn_id=? AND fork_id=? AND actor_token_id=? AND request_occurrence_id=? AND request_hash=?
			  AND bundle_hash=? AND fence_generation=? AND (state='prepared' OR (state='executing' AND execution_owner=?))
		`, state, string(failureJSON), now, now, prepared.ForkTurnID, prepared.Fork.ForkID, prepared.ActorTokenID,
			prepared.RequestOccurrenceID, prepared.RequestHash, prepared.SourceBundleHash, prepared.FenceGeneration, prepared.ExecutionOwner)
		if err != nil {
			return fmt.Errorf("terminalize failed conversation fork turn: %w", err)
		}
		rows, err := res.RowsAffected()
		if err != nil || rows != 1 {
			return fmt.Errorf("terminalize failed conversation fork turn rejected stale or terminal authority")
		}
		return nil
	})
}

func (s conversationForkStore) recordOperatorConversationForkChat(ctx context.Context, req runtimerunfork.ConversationForkChatRecordRequest) (runtimerunfork.ConversationForkChatResult, error) {
	forkID, err := normalizeUUIDParam(req.ForkID, "fork_id")
	if err != nil {
		return runtimerunfork.ConversationForkChatResult{}, err
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		return runtimerunfork.ConversationForkChatResult{}, &operatorread.EntityReadParamError{Field: "message", Reason: "is required"}
	}
	actorTokenID := strings.TrimSpace(req.ActorTokenID)
	if actorTokenID == "" {
		return runtimerunfork.ConversationForkChatResult{}, &operatorread.EntityReadParamError{Field: "actor_token_id", Reason: "is required"}
	}
	execution := req.Execution
	prepared := req.Prepared
	if prepared.ForkTurnID == "" || prepared.Fork.ForkID != forkID || prepared.RequestHash == "" || prepared.RequestOccurrenceID == "" ||
		prepared.ActorTokenID != actorTokenID || prepared.SourceBundleHash == "" || prepared.ExecutionOwner == "" || prepared.FenceGeneration == 0 ||
		execution.ExecutionOwner != prepared.ExecutionOwner || execution.FenceGeneration != prepared.FenceGeneration {
		return runtimerunfork.ConversationForkChatResult{}, fmt.Errorf("conversation fork chat terminalization requires exact prepared authority")
	}
	execution.AssistantMessage = strings.TrimSpace(execution.AssistantMessage)
	if execution.AssistantMessage == "" {
		return runtimerunfork.ConversationForkChatResult{}, &operatorread.EntityReadParamError{Field: "execution.assistant_message", Reason: "is required"}
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.ConversationForkChatResult{}, err
	}

	var result runtimerunfork.ConversationForkChatResult
	err = s.runForkMutation(ctx, forkID, true, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := loadActiveConversationForkForChat(txctx, s, tx, forkID, now); err != nil {
			return err
		}
		snapshot, err := loadConversationForkSnapshot(txctx, s, tx, forkID)
		if errors.Is(err, sql.ErrNoRows) {
			return &operatorread.EntityReadParamError{Field: "fork_id", Reason: "forkchat snapshot is unavailable"}
		}
		if err != nil {
			return err
		}
		policy := runtimerunfork.CanonicalConversationForkSandboxPolicy()
		if len(execution.AvailableTools) == 0 {
			execution.AvailableTools = policy.AvailableToolNames()
		}
		requestPayload, err := conversationForkChatRequestPayload(message, snapshot, execution.AvailableTools)
		if err != nil {
			return err
		}
		responsePayload, err := conversationForkChatResponsePayload(execution, policy)
		if err != nil {
			return err
		}
		turn, err := completeConversationForkTurn(txctx, s, tx, prepared, actorTokenID, message, execution, requestPayload, responsePayload, policy, now)
		if err != nil {
			return err
		}
		result = runtimerunfork.ConversationForkChatResult{ForkID: forkID, Turn: turn, Snapshot: snapshot, SandboxPolicy: policy}
		return nil
	})
	if err != nil {
		return runtimerunfork.ConversationForkChatResult{}, fmt.Errorf("record conversation fork chat: %w", err)
	}
	return result, nil
}

func completeConversationForkTurn(
	ctx context.Context,
	owner conversationForkStore,
	tx *sql.Tx,
	prepared runtimerunfork.ConversationForkChatPrepared,
	actorTokenID, message string,
	execution runtimerunfork.ConversationForkChatExecution,
	requestPayload, responsePayload json.RawMessage,
	policy runtimerunfork.ConversationForkSandboxPolicy,
	now time.Time,
) (operatorread.OperatorConversationTurn, error) {
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: prepared.ForkTurnID,
		ExecutionOwner: prepared.ExecutionOwner, LeaseExpiresAt: prepared.LeaseExpiresAt, FenceGeneration: prepared.FenceGeneration,
		ExecutionMode: prepared.Snapshot.SourceAgent.ExecutionMode,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID: prepared.ForkTurnID, ForkID: prepared.Fork.ForkID, BundleHash: prepared.SourceBundleHash, ActorTokenID: prepared.ActorTokenID,
			RequestOccurrenceID: prepared.RequestOccurrenceID, RequestHash: prepared.RequestHash,
		},
	}
	if owner.effects == nil {
		return operatorread.OperatorConversationTurn{}, fmt.Errorf("conversation fork external-effect owner is required")
	}
	if err := owner.effects.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
		return operatorread.OperatorConversationTurn{}, err
	}
	if err := owner.effects.RequireCompletionAuthorityNoLiveAttemptsTx(ctx, tx, authority); err != nil {
		return operatorread.OperatorConversationTurn{}, err
	}
	var childCount int
	if err := owner.queryRow(ctx, tx, `SELECT COUNT(*) FROM conversation_fork_turn_completions WHERE fork_turn_id=?`, prepared.ForkTurnID).Scan(&childCount); err != nil {
		return operatorread.OperatorConversationTurn{}, fmt.Errorf("count conversation fork completion children: %w", err)
	}
	if childCount == 0 {
		return operatorread.OperatorConversationTurn{}, fmt.Errorf("conversation fork chat cannot succeed without a settled completion child")
	}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return operatorread.OperatorConversationTurn{}, err
	}
	toolCallsJSON, err := json.Marshal(execution.ToolCalls)
	if err != nil {
		return operatorread.OperatorConversationTurn{}, err
	}
	var createdAt conversationForkTimeValue
	if err := owner.queryRow(ctx, tx, `
		UPDATE conversation_fork_turns
		SET state='succeeded',assistant_message=?,request_payload=?,response_payload=?,tool_calls=?,
		    sandbox_policy=?,snapshot_owner=?,lease_expires_at=NULL,failure=NULL,updated_at=?,terminal_at=?
		WHERE fork_turn_id=? AND state='executing'
		RETURNING created_at
	`, execution.AssistantMessage, string(requestPayload), string(responsePayload), string(toolCallsJSON), string(policyJSON),
		runtimerunfork.ConversationForkChatSnapshotOwner, now, now, prepared.ForkTurnID).Scan(&createdAt); err != nil {
		return operatorread.OperatorConversationTurn{}, fmt.Errorf("terminalize conversation fork turn: %w", err)
	}
	return operatorread.OperatorConversationTurn{
		TurnID: prepared.ForkTurnID, TurnIndex: prepared.TurnIndex,
		ExecutionMode:  string(prepared.Snapshot.SourceAgent.ExecutionMode),
		RequestPayload: cloneRawMessage(requestPayload), ResponsePayload: cloneRawMessage(responsePayload),
		ToolCalls: cloneConversationToolCalls(execution.ToolCalls), ToolResults: cloneConversationToolResults(execution.ToolResults),
		TurnBlocks: conversationForkSandboxTurnBlocks(execution), ParseOK: true, CreatedAt: createdAt.Time,
		AssistantVisibleOutput: execution.AssistantMessage,
	}, nil
}

func loadActiveConversationForkForChat(ctx context.Context, owner conversationForkStore, tx *sql.Tx, forkID string, now time.Time) (runtimerunfork.OperatorConversationForkSession, error) {
	row := owner.queryRow(ctx, tx, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE fork_id = ?`+owner.forUpdate()+`
	`, forkID)
	item, err := scanConversationForkSession(row, now)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunfork.OperatorConversationForkSession{}, runtimerunfork.ErrConversationForkNotFound
	}
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	if item.State != "active" {
		return runtimerunfork.OperatorConversationForkSession{}, &operatorread.EntityReadParamError{Field: "fork_id", Reason: "must reference an active fork"}
	}
	return item, nil
}

func loadConversationForkChatBundleHash(ctx context.Context, owner conversationForkStore, tx *sql.Tx, sourceRunID string) (string, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return "", fmt.Errorf("conversation fork chat requires source-owned run bundle identity")
	}
	var bundleHash string
	if err := owner.queryRow(ctx, tx, `SELECT COALESCE(bundle_hash, '') FROM runs WHERE run_id=?`, sourceRunID).Scan(&bundleHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("conversation fork chat source run %q is unavailable", sourceRunID)
		}
		return "", fmt.Errorf("load conversation fork chat source bundle: %w", err)
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if err := runtimecontracts.ValidateBundleHash(bundleHash); err != nil {
		return "", fmt.Errorf("conversation fork chat source run has invalid bundle identity: %w", err)
	}
	return bundleHash, nil
}

func ensureConversationForkSnapshot(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork runtimerunfork.OperatorConversationForkSession, now time.Time) (runtimerunfork.ConversationForkSnapshot, error) {
	snapshot, err := loadConversationForkSnapshot(ctx, owner, tx, fork.ForkID)
	if err == nil {
		return snapshot, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	sourceTurn, err := loadConversationForkSourceTurn(ctx, owner, tx, fork)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	entities, err := loadConversationForkEntitySnapshot(ctx, owner, tx, fork)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	sourceAgent, err := loadConversationForkSourceAgent(ctx, owner, tx, fork.SourceIdentity)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	snapshot = runtimerunfork.ConversationForkSnapshot{
		ForkID:          fork.ForkID,
		SourceSessionID: fork.SourceSessionID,
		SourceRunID:     fork.SourceRunID,
		SourceAgentID:   fork.SourceAgentID,
		SourceIdentity:  fork.SourceIdentity,
		SourceTurn:      sourceTurn,
		EntitySnapshot:  entities,
		SnapshotOwner:   runtimerunfork.ConversationForkChatSnapshotOwner,
		CreatedAt:       now,
		SourceAgent:     sourceAgent,
	}
	sourceTurnJSON, err := json.Marshal(sourceTurn)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	entitySnapshotJSON, err := json.Marshal(entities)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	sourceAgentJSON, err := json.Marshal(sourceAgent)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	sourceAgentIdentity, err := agentIdentityFields(snapshot.SourceIdentity)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	if _, err := owner.exec(ctx, tx, `
		INSERT INTO conversation_fork_snapshots (
			fork_id, source_session_id, source_run_id, source_agent_id,
			source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_turn_id, fork_point_turn_index, fork_point_selected_at,
			source_turn, entity_snapshot, source_agent_config, snapshot_owner, created_at
		)
		VALUES (
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?, ?
		)
	`, snapshot.ForkID, snapshot.SourceSessionID, nullableConversationForkID(snapshot.SourceRunID),
		sourceAgentIdentity.AgentID, sourceAgentIdentity.NameOwner, sourceAgentIdentity.NameSource,
		sourceAgentIdentity.RoutePresence, sourceAgentIdentity.FlowScopeKey, sourceAgentIdentity.FlowInstanceID,
		sourceAgentIdentity.FlowInstancePath,
		sourceTurn.TurnID, sourceTurn.TurnIndex, sourceTurn.SelectedAt,
		string(sourceTurnJSON), string(entitySnapshotJSON), string(sourceAgentJSON), snapshot.SnapshotOwner, snapshot.CreatedAt); err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("insert conversation fork snapshot: %w", err)
	}
	return snapshot, nil
}

func loadConversationForkSnapshot(ctx context.Context, owner conversationForkStore, q conversationForkQueryer, forkID string) (runtimerunfork.ConversationForkSnapshot, error) {
	row := owner.queryRow(ctx, q, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			source_turn, entity_snapshot, source_agent_config, snapshot_owner, created_at
		FROM conversation_fork_snapshots
		WHERE fork_id = ?
	`, forkID)
	var out runtimerunfork.ConversationForkSnapshot
	var sourceTurnRaw []byte
	var entitiesRaw []byte
	var sourceAgentRaw []byte
	var createdAt conversationForkTimeValue
	var identityFields runtimeagentidentity.StorageFields
	if err := row.Scan(
		&out.ForkID,
		&out.SourceSessionID,
		&out.SourceRunID,
		&identityFields.AgentID,
		&identityFields.NameOwner,
		&identityFields.NameSource,
		&identityFields.RoutePresence,
		&identityFields.FlowScopeKey,
		&identityFields.FlowInstanceID,
		&identityFields.FlowInstancePath,
		&sourceTurnRaw,
		&entitiesRaw,
		&sourceAgentRaw,
		&out.SnapshotOwner,
		&createdAt,
	); err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, err
	}
	var err error
	out.SourceIdentity, err = runtimeagentidentity.FromStorageFields(identityFields)
	if err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork snapshot source identity: %w", err)
	}
	out.SourceAgentID = out.SourceIdentity.AgentID()
	if err := json.Unmarshal(sourceTurnRaw, &out.SourceTurn); err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork source turn snapshot: %w", err)
	}
	if err := json.Unmarshal(entitiesRaw, &out.EntitySnapshot); err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork entity snapshot: %w", err)
	}
	if err := json.Unmarshal(sourceAgentRaw, &out.SourceAgent); err != nil {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("decode conversation fork source agent config: %w", err)
	}
	out.SourceAgent.NormalizeRuntimeDescriptor()
	sourceAgentIdentity, err := out.SourceAgent.ConcreteIdentity()
	if err != nil || sourceAgentIdentity != out.SourceIdentity || !out.SourceAgent.ExecutionMode.Valid() {
		return runtimerunfork.ConversationForkSnapshot{}, fmt.Errorf("conversation fork source agent config conflicts with snapshot identity")
	}
	if out.EntitySnapshot == nil {
		out.EntitySnapshot = []runtimerunfork.ConversationForkEntitySnapshot{}
	}
	out.CreatedAt = createdAt.Time
	return out, nil
}

func loadConversationForkSourceAgent(ctx context.Context, owner conversationForkStore, q conversationForkQueryer, identity runtimeagentidentity.Identity) (runtimeactors.AgentConfig, error) {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return runtimeactors.AgentConfig{}, err
	}
	row := owner.queryRow(ctx, q, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       role, model, llm_backend, memory_enabled, memory_source,
		       COALESCE(parent_agent_id,''), COALESCE(CAST(entity_id AS TEXT),''), config,
		       runtime_descriptor, subscriptions, emit_events, tools, permissions
		FROM agents
		WHERE agent_id = ? AND agent_name_owner = ? AND agent_name_source = ?
		  AND agent_route_presence = ? AND flow_scope_key = ?
		  AND flow_instance_id = ? AND flow_instance = ?
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	var persisted persistedAgentProjection
	if err := row.Scan(
		&persisted.AgentID, &persisted.Identity.NameOwner, &persisted.Identity.NameSource,
		&persisted.Identity.RoutePresence, &persisted.Identity.FlowScopeKey,
		&persisted.Identity.FlowInstanceID, &persisted.Identity.FlowInstancePath,
		&persisted.Role, &persisted.Model, &persisted.LLMBackend,
		&persisted.MemoryEnabled, &persisted.MemorySource, &persisted.ParentAgentID, &persisted.EntityID,
		&persisted.ConfigJSON, &persisted.RuntimeDescriptor, &persisted.SubscriptionsJSON,
		&persisted.EmitEventsJSON, &persisted.ToolsJSON, &persisted.PermissionsJSON,
	); err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("load conversation fork source agent %s: %w", identity.Description(), err)
	}
	persisted.Identity.AgentID = persisted.AgentID
	persisted.FlowInstance = persisted.Identity.FlowInstancePath
	cfg, err := hydratePersistedAgentConfig(persisted)
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("hydrate conversation fork source agent %s: %w", identity.Description(), err)
	}
	return cfg, nil
}

func loadConversationForkSourceTurn(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork runtimerunfork.OperatorConversationForkSession) (runtimerunfork.ConversationForkSourceTurn, error) {
	fields, err := agentIdentityFields(fork.SourceIdentity)
	if err != nil {
		return runtimerunfork.ConversationForkSourceTurn{}, err
	}
	row := owner.queryRow(ctx, tx, `
		SELECT
			CAST(t.turn_id AS TEXT),
			COALESCE(t.request_payload, '{}'),
			COALESCE(t.response_payload, '{}'),
			COALESCE(t.tool_calls, '[]'),
			c.surface,
			t.created_at
		FROM agent_turns t
		JOIN managed_agent_capability_surfaces c ON c.surface_id = t.capability_surface_id
		WHERE t.session_id = ?
		  AND t.turn_id = ?
		  AND t.agent_id = ?
		  AND t.agent_name_owner = ?
		  AND t.agent_name_source = ?
		  AND t.agent_route_presence = ?
		  AND t.flow_scope_key = ?
		  AND t.flow_instance_id = ?
		  AND t.flow_instance = ?
	`, fork.SourceSessionID, fork.ForkPoint.TurnID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	var out runtimerunfork.ConversationForkSourceTurn
	var requestRaw, responseRaw, toolCallsRaw, capabilitySurfaceRaw []byte
	var createdAt conversationForkTimeValue
	if err := row.Scan(&out.TurnID, &requestRaw, &responseRaw, &toolCallsRaw, &capabilitySurfaceRaw, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimerunfork.ConversationForkSourceTurn{}, &operatorread.EntityReadParamError{Field: "fork_id", Reason: "source turn is unavailable"}
		}
		return runtimerunfork.ConversationForkSourceTurn{}, fmt.Errorf("load conversation fork source turn: %w", err)
	}
	out.TurnIndex = fork.ForkPoint.TurnIndex
	out.SelectedAt = fork.ForkPoint.SelectedAt
	out.CreatedAt = createdAt.Time
	out.RequestPayload = cloneRawMessage(requestRaw)
	out.ResponsePayload = cloneRawMessage(responseRaw)
	out.ToolCalls = cloneRawMessage(toolCallsRaw)
	surface, err := storemanagedcapability.Decode(capabilitySurfaceRaw)
	if err != nil {
		return runtimerunfork.ConversationForkSourceTurn{}, fmt.Errorf("decode conversation fork source capability surface: %w", err)
	}
	availableToolsRaw, err := json.Marshal(surface.EffectiveNames())
	if err != nil {
		return runtimerunfork.ConversationForkSourceTurn{}, fmt.Errorf("encode conversation fork source effective tools: %w", err)
	}
	out.AvailableTools = availableToolsRaw
	return out, nil
}

func loadConversationForkEntitySnapshot(ctx context.Context, owner conversationForkStore, tx *sql.Tx, fork runtimerunfork.OperatorConversationForkSession) ([]runtimerunfork.ConversationForkEntitySnapshot, error) {
	if strings.TrimSpace(fork.SourceRunID) == "" {
		return []runtimerunfork.ConversationForkEntitySnapshot{}, nil
	}
	rows, err := owner.query(ctx, tx, `
		SELECT CAST(entity_id AS TEXT), field, new_value, created_at
		FROM entity_mutations
		WHERE run_id = ?
		  AND created_at <= ?
		ORDER BY entity_id ASC, created_at ASC, mutation_id ASC
	`, fork.SourceRunID, fork.ForkPoint.SelectedAt)
	if err != nil {
		return nil, fmt.Errorf("load conversation fork entity mutations: %w", err)
	}
	defer rows.Close()

	type timedProjectionMutation struct {
		mutationlog.ProjectionMutation
		CreatedAt time.Time
	}
	grouped := map[string][]timedProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var entityID, field string
		var raw []byte
		var createdAt conversationForkTimeValue
		if err := rows.Scan(&entityID, &field, &raw, &createdAt); err != nil {
			return nil, fmt.Errorf("scan conversation fork entity mutation: %w", err)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode conversation fork entity mutation %s/%s: %w", entityID, field, err)
		}
		entityID = strings.TrimSpace(entityID)
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = struct{}{}
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], timedProjectionMutation{
			ProjectionMutation: mutationlog.ProjectionMutation{Field: field, NewValue: value},
			CreatedAt:          createdAt.Time,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversation fork entity mutations: %w", err)
	}

	out := make([]runtimerunfork.ConversationForkEntitySnapshot, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		mutations := grouped[entityID]
		projectionMutations := make([]mutationlog.ProjectionMutation, 0, len(mutations))
		var enteredStateAt *time.Time
		for _, mutation := range mutations {
			projectionMutations = append(projectionMutations, mutation.ProjectionMutation)
			if strings.TrimSpace(mutation.Field) == "current_state" {
				tm := mutation.CreatedAt
				enteredStateAt = &tm
			}
		}
		projection, err := mutationlog.ReconstructEntityStateProjection(projectionMutations)
		if err != nil {
			return nil, fmt.Errorf("reconstruct conversation fork entity %s at fork point: %w", entityID, err)
		}
		out = append(out, runtimerunfork.ConversationForkEntitySnapshot{
			EntityID:       entityID,
			CurrentState:   projection.CurrentState,
			EnteredStateAt: enteredStateAt,
			Fields:         projection.Fields,
			Gates:          projection.Gates,
			Accumulator:    projection.Accumulator,
		})
	}
	if out == nil {
		out = []runtimerunfork.ConversationForkEntitySnapshot{}
	}
	return out, nil
}

func preallocateConversationForkTurn(
	ctx context.Context,
	owner conversationForkStore,
	tx *sql.Tx,
	forkID, bundleHash, method, actorTokenID, idempotencyKey, requestHash, message string,
	now time.Time,
) (string, int, string, string, time.Time, error) {
	if idempotencyKey != "" {
		if err := rejectConversationForkChatReplay(ctx, owner, tx, forkID, method, actorTokenID, idempotencyKey, requestHash); err != nil {
			return "", 0, "", "", time.Time{}, err
		}
	}
	var nextIndex int
	if err := owner.queryRow(ctx, tx, `SELECT COALESCE(MAX(turn_index),0)+1 FROM conversation_fork_turns WHERE fork_id=?`, forkID).Scan(&nextIndex); err != nil {
		return "", 0, "", "", time.Time{}, fmt.Errorf("allocate conversation fork turn index: %w", err)
	}
	occurrenceID := uuid.NewString()
	if idempotencyKey != "" {
		occurrenceID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{"conversation.fork_chat", method, actorTokenID, idempotencyKey}, "\x00"))).String()
	}
	forkTurnID := uuid.NewString()
	executionOwner := "forkchat:" + forkTurnID + ":" + uuid.NewString()
	leaseExpiresAt := now.Add(conversationForkChatExecutionLease).UTC()
	if _, err := owner.exec(ctx, tx, `
		INSERT INTO conversation_fork_turns (
			fork_turn_id,fork_id,bundle_hash,turn_index,actor_token_id,request_occurrence_id,request_hash,idempotency_key,
			message,state,execution_owner,lease_expires_at,fence_generation,tool_calls,evidence,created_at,updated_at
		) VALUES (?,?,?,?,?,?,?,NULLIF(?,''),?,'prepared',?,?,1,'[]','{}',?,?)
	`, forkTurnID, forkID, bundleHash, nextIndex, actorTokenID, occurrenceID, requestHash, idempotencyKey, message, executionOwner, leaseExpiresAt, now, now); err != nil {
		return "", 0, "", "", time.Time{}, fmt.Errorf("preallocate conversation fork turn: %w", err)
	}
	return forkTurnID, nextIndex, occurrenceID, executionOwner, leaseExpiresAt, nil
}

func loadConversationForkTurns(ctx context.Context, owner conversationForkStore, db conversationForkQueryer, forkID string) ([]operatorread.OperatorConversationTurn, error) {
	rows, err := owner.query(ctx, db, `
		SELECT
			CAST(fork_turn_id AS TEXT), turn_index,
			(SELECT MIN(c.execution_mode) FROM conversation_fork_turn_completions c WHERE c.fork_turn_id=t.fork_turn_id),
			(SELECT MAX(c.execution_mode) FROM conversation_fork_turn_completions c WHERE c.fork_turn_id=t.fork_turn_id),
			request_payload, response_payload, tool_calls,
			assistant_message, created_at
		FROM conversation_fork_turns t
		WHERE fork_id = ? AND state = 'succeeded'
		ORDER BY turn_index ASC, created_at ASC, fork_turn_id ASC
	`, forkID)
	if err != nil {
		return nil, fmt.Errorf("load conversation fork turns: %w", err)
	}
	defer rows.Close()
	turns := []operatorread.OperatorConversationTurn{}
	for rows.Next() {
		var turn operatorread.OperatorConversationTurn
		var requestRaw, responseRaw, toolCallsRaw []byte
		var minMode, maxMode string
		var assistant string
		var createdAt conversationForkTimeValue
		if err := rows.Scan(&turn.TurnID, &turn.TurnIndex, &minMode, &maxMode, &requestRaw, &responseRaw, &toolCallsRaw, &assistant, &createdAt); err != nil {
			return nil, fmt.Errorf("scan conversation fork turn: %w", err)
		}
		if minMode != maxMode || (minMode != "live" && minMode != "mock") {
			return nil, fmt.Errorf("conversation fork turn %s has invalid completion execution mode", turn.TurnID)
		}
		turn.ExecutionMode = minMode
		if len(toolCallsRaw) > 0 {
			if err := json.Unmarshal(toolCallsRaw, &turn.ToolCalls); err != nil {
				return nil, fmt.Errorf("decode conversation fork turn tool calls: %w", err)
			}
		}
		if turn.ToolCalls == nil {
			turn.ToolCalls = []operatorread.OperatorConversationToolCall{}
		}
		turn.RequestPayload = cloneRawMessage(requestRaw)
		turn.ResponsePayload = cloneRawMessage(responseRaw)
		turn.ToolResults = conversationForkToolResultsFromCalls(turn.ToolCalls)
		turn.TurnBlocks = conversationForkSandboxTurnBlocks(runtimerunfork.ConversationForkChatExecution{
			AssistantMessage: assistant,
			ToolCalls:        turn.ToolCalls,
			ToolResults:      turn.ToolResults,
		})
		turn.ParseOK = true
		turn.CreatedAt = createdAt.Time
		turn.AssistantVisibleOutput = assistant
		turns = append(turns, turn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conversation fork turns: %w", err)
	}
	return turns, nil
}

func conversationForkToolResultsFromCalls(calls []operatorread.OperatorConversationToolCall) []operatorread.OperatorConversationToolResult {
	if len(calls) == 0 {
		return []operatorread.OperatorConversationToolResult{}
	}
	out := make([]operatorread.OperatorConversationToolResult, 0, len(calls))
	for _, call := range calls {
		if len(call.Result) == 0 {
			continue
		}
		out = append(out, operatorread.OperatorConversationToolResult{
			ToolName:  call.Name,
			ToolUseID: call.ToolUseID,
			Output:    cloneRawMessage(call.Result),
		})
	}
	return out
}

func conversationForkSandboxTurnBlocks(execution runtimerunfork.ConversationForkChatExecution) []operatorread.OperatorConversationTurnBlock {
	blocks := []operatorread.OperatorConversationTurnBlock{{
		Kind:  "turn_summary",
		Title: "Forkchat sandbox response",
		Text:  execution.AssistantMessage,
	}}
	for _, call := range execution.ToolCalls {
		blocks = append(blocks, operatorread.OperatorConversationTurnBlock{
			Kind:     "tool_result",
			Title:    call.Name,
			ToolName: call.Name,
			Input:    cloneRawMessage(call.Arguments),
			Output:   cloneRawMessage(call.Result),
		})
	}
	return blocks
}

func conversationForkChatRequestPayload(message string, snapshot runtimerunfork.ConversationForkSnapshot, availableTools []string) (json.RawMessage, error) {
	raw, err := json.Marshal(map[string]any{
		"message":         message,
		"snapshot_owner":  snapshot.SnapshotOwner,
		"snapshot":        snapshot,
		"available_tools": availableTools,
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func conversationForkChatResponsePayload(execution runtimerunfork.ConversationForkChatExecution, policy runtimerunfork.ConversationForkSandboxPolicy) (json.RawMessage, error) {
	raw, err := json.Marshal(map[string]any{
		"message":        execution.AssistantMessage,
		"sandbox_policy": policy,
		"tool_calls":     execution.ToolCalls,
		"tool_results":   execution.ToolResults,
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}
