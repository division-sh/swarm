package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func loadContextCompactionTx(ctx context.Context, tx *sql.Tx, postgres bool, identity agentmemory.Identity, prepared runtimellm.ContextCompactionPreparation) (runtimellm.ContextCompactionResult, bool, error) {
	if err := identity.Validate(); err != nil {
		return runtimellm.ContextCompactionResult{}, false, err
	}
	query := `SELECT a.attempt_id,a.state,o.request_fingerprint,a.authority_evidence,a.evidence FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.operation_id=? ORDER BY a.attempt_ordinal DESC LIMIT 1`
	if postgres {
		query = `SELECT a.attempt_id::text,a.state,o.request_fingerprint,a.authority_evidence,a.evidence FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.operation_id=$1::uuid ORDER BY a.attempt_ordinal DESC LIMIT 1`
	}
	var attemptID, state, fingerprint string
	var authority, evidence []byte
	err := tx.QueryRowContext(ctx, query, prepared.Checkpoint.OperationID).Scan(&attemptID, &state, &fingerprint, &authority, &evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ContextCompactionResult{}, false, nil
	}
	if err != nil {
		return runtimellm.ContextCompactionResult{}, false, err
	}
	var admitted struct {
		Request runtimeeffects.CompletionRequest `json:"provider_request"`
		Target  struct {
			SessionID string               `json:"session_id"`
			Identity  agentmemory.Identity `json:"agent_identity"`
		} `json:"usage_target"`
	}
	if json.Unmarshal(authority, &admitted) != nil || admitted.Request.Purpose != runtimeeffects.ContextCompactionPurpose || admitted.Request.RequestedModel != prepared.Checkpoint.Model || admitted.Target.Identity.Normalize() != identity.Normalize() || admitted.Target.SessionID != prepared.Checkpoint.SessionID || fingerprint != prepared.RequestFingerprint {
		return runtimellm.ContextCompactionResult{}, false, fmt.Errorf("context compaction immutable request or actor mismatch")
	}
	if state != string(runtimeeffects.StateSettled) {
		return runtimellm.ContextCompactionResult{}, false, fmt.Errorf("context compaction attempt is %s; no redispatch", state)
	}
	result, err := runtimellm.ReadContextCompactionResult(evidence, prepared)
	if err != nil || result.Checkpoint.AttemptID != attemptID {
		return runtimellm.ContextCompactionResult{}, false, errors.Join(err, fmt.Errorf("context compaction result does not match its acknowledged attempt"))
	}
	return result, true, nil
}

func (s *EffectPostgresOwner) LoadContextCompaction(ctx context.Context, identity agentmemory.Identity, prepared runtimellm.ContextCompactionPreparation) (runtimellm.ContextCompactionResult, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimellm.ContextCompactionResult{}, false, err
	}
	var result runtimellm.ContextCompactionResult
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, found, err = loadContextCompactionTx(ctx, tx, true, identity, prepared)
		return err
	})
	return result, found, err
}

func (s *EffectSQLiteOwner) LoadContextCompaction(ctx context.Context, identity agentmemory.Identity, prepared runtimellm.ContextCompactionPreparation) (runtimellm.ContextCompactionResult, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimellm.ContextCompactionResult{}, false, err
	}
	var result runtimellm.ContextCompactionResult
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, found, err = loadContextCompactionTx(ctx, tx, false, identity, prepared)
		return err
	})
	return result, found, err
}

func (s *EffectPostgresOwner) InstallContextCompaction(ctx context.Context, lease *sessions.Lease, result runtimellm.ContextCompactionResult) error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("context compaction requires its exact session grant")
	}
	commit := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		var acknowledged runtimellm.ContextCompactionResult
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			acknowledged, err = requireContextCompactionResultTx(ctx, tx, true, lease, result)
			return err
		})
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, s.llm.InstallContextCompactionTx(ctx, attempt, lease, acknowledged)
	})
	return commit.Err()
}

func (s *EffectSQLiteOwner) InstallContextCompaction(ctx context.Context, lease *sessions.Lease, result runtimellm.ContextCompactionResult) error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("context compaction requires its exact session grant")
	}
	commit := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite install context compaction", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		var acknowledged runtimellm.ContextCompactionResult
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			acknowledged, err = requireContextCompactionResultTx(ctx, tx, false, lease, result)
			return err
		})
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, s.llm.InstallContextCompactionTx(ctx, attempt, lease, acknowledged)
	})
	return commit.Err()
}

func requireContextCompactionResultTx(ctx context.Context, tx *sql.Tx, postgres bool, lease *sessions.Lease, proposed runtimellm.ContextCompactionResult) (runtimellm.ContextCompactionResult, error) {
	acknowledged, found, err := loadContextCompactionTx(ctx, tx, postgres, lease.Identity, proposed.Preparation)
	if err != nil {
		return runtimellm.ContextCompactionResult{}, err
	}
	if !found || acknowledged.Checkpoint.ID != proposed.Checkpoint.ID {
		return runtimellm.ContextCompactionResult{}, fmt.Errorf("context compaction is not acknowledged by the selected store")
	}
	return acknowledged, nil
}
