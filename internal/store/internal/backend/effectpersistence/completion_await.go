package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *EffectPostgresOwner) BindCompletionAwaitTx(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command runtimeeffects.CompletionAwaitPublication) (replycontext.AwaitBinding, error) {
	if err := s.requireCurrent(); err != nil {
		return replycontext.AwaitBinding{}, err
	}
	var binding replycontext.AwaitBinding
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationPostgres(ctx, tx, command.Attempt)
		if err != nil {
			return err
		}
		binding, err = completionAwaitBinding(ctx, record, command, locked)
		if err != nil {
			return err
		}
		return retainCompletionAwait(ctx, tx, command.Attempt, binding, true)
	})
	return binding, err
}

func (s *EffectSQLiteOwner) BindCompletionAwaitTx(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command runtimeeffects.CompletionAwaitPublication) (replycontext.AwaitBinding, error) {
	if err := s.requireCurrent(); err != nil {
		return replycontext.AwaitBinding{}, err
	}
	var binding replycontext.AwaitBinding
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationSQLite(ctx, tx, command.Attempt)
		if err != nil {
			return err
		}
		binding, err = completionAwaitBinding(ctx, record, command, locked)
		if err != nil {
			return err
		}
		return retainCompletionAwait(ctx, tx, command.Attempt, binding, false)
	})
	return binding, err
}

func completionAwaitBinding(ctx context.Context, record replycontext.Record, command runtimeeffects.CompletionAwaitPublication, locked lockedCompletionContinuation) (replycontext.AwaitBinding, error) {
	attempt := command.Attempt
	if record.Await != nil || record.Validate() != nil || record.State != replycontext.StateOpen ||
		attempt.Origin.Validate() != nil || attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery ||
		attempt.InstanceOwnership.Life == nil || attempt.InstanceOwnership.Validate() != nil ||
		attempt.InstanceOwnership.Life.RunID != record.RunID || attempt.InstanceOwnership.Life.FlowInstance != record.Origin.FlowInstance || attempt.InstanceOwnership.Life.Life != record.OriginLife ||
		record.RunID != attempt.Origin.Delivery.RunID() || record.RunID != record.SourceRunID || record.RequestEventID != record.SourceRequestEventID ||
		record.Origin.FlowInstance != attempt.Authority.Target.FlowInstance || record.Origin.EntityID != attempt.Authority.Target.EntityID ||
		locked.phase == runtimeeffects.CompletionProjectionResponseConsumed {
		return replycontext.AwaitBinding{}, fmt.Errorf("await requires its current original delivery, source, actor, and unconsumed response")
	}
	payload, err := runtimeeffects.ReadCompletionContinuationPayload([]byte(locked.evidence), locked.requestFingerprint)
	if err != nil {
		return replycontext.AwaitBinding{}, err
	}
	frameID, callID, err := runtimellm.CompletionAwaitCall(payload, attempt.Adapter, command.CallIndex, command.CompletedResults)
	if err != nil {
		return replycontext.AwaitBinding{}, err
	}
	prefix, err := canonicaljson.Bytes(command.CompletedResults)
	if err != nil {
		return replycontext.AwaitBinding{}, err
	}
	binding := replycontext.AwaitBinding{Version: "reply-await.v1", DeliveryID: attempt.Origin.Delivery.DeliveryID(), ClaimVersion: attempt.Origin.Delivery.Version(),
		ReplyContextID: record.ID, RequestEventID: record.RequestEventID, RunID: record.RunID, OriginLife: record.OriginLife,
		ExecutionOwner: attempt.Authority.ExecutionOwner, OperationID: attempt.OperationID, AttemptID: attempt.AttemptID,
		BundleHash: locked.bundleHash, PlanFingerprint: locked.planFingerprint, FrameID: frameID, ResponseFingerprint: runtimeeffects.Fingerprint(payload),
		CallID: callID, CallIndex: command.CallIndex, CompletedResults: string(prefix)}
	return binding, binding.Validate()
}

func retainCompletionAwait(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, binding replycontext.AwaitBinding, postgres bool) error {
	raw, err := canonicaljson.Bytes(binding)
	if err != nil {
		return err
	}
	query := `UPDATE runtime_external_effect_attempts SET completion_tool_progress=$1 WHERE attempt_id=$2 AND operation_id=$3 AND (completion_tool_progress IS NULL OR completion_tool_progress=$1)`
	if postgres {
		query = `UPDATE runtime_external_effect_attempts SET completion_tool_progress=$1::jsonb WHERE attempt_id=$2::uuid AND operation_id=$3::uuid AND (completion_tool_progress IS NULL OR completion_tool_progress=$1::jsonb)`
	}
	result, err := tx.ExecContext(ctx, query, string(raw), attempt.AttemptID, attempt.OperationID)
	return requireExternalAttemptTransition(result, err)
}
