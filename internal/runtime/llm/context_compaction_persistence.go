package llm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type ContextCompactionPreparation struct {
	Checkpoint              ContextCompactionCheckpoint `json:"checkpoint"`
	ConversationFingerprint string                      `json:"conversation_fingerprint"`
	RequestFingerprint      string                      `json:"request_fingerprint"`
}

type ContextCompactionResult struct {
	Preparation ContextCompactionPreparation `json:"preparation"`
	Checkpoint  ContextCompactionCheckpoint  `json:"checkpoint"`
}

type ContextCompactionPersistence interface {
	LoadContextCompaction(context.Context, agentmemory.Identity, ContextCompactionPreparation) (ContextCompactionResult, bool, error)
	InstallContextCompaction(context.Context, *sessions.Lease, ContextCompactionResult) error
}

func ReadContextCompactionResult(evidence []byte, prepared ContextCompactionPreparation) (ContextCompactionResult, error) {
	var envelope struct {
		Result *ContextCompactionResult `json:"context_compaction"`
	}
	if err := json.Unmarshal(evidence, &envelope); err != nil || envelope.Result == nil {
		return ContextCompactionResult{}, contextCompactionLimitation("maintenance_result_missing")
	}
	result := *envelope.Result
	want, err := canonicaljson.Hash(prepared)
	if err != nil {
		return ContextCompactionResult{}, err
	}
	got, err := canonicaljson.Hash(result.Preparation)
	if err != nil || got != want {
		return ContextCompactionResult{}, contextCompactionLimitation("maintenance_request_mismatch")
	}
	checkpoint := result.Checkpoint
	checkpoint.ID, checkpoint.AttemptID, checkpoint.ResultFingerprint = "", "", ""
	checkpoint.Summary, checkpoint.Native = "", nil
	shape, err := canonicaljson.Hash(checkpoint)
	expected, expectedErr := canonicaljson.Hash(prepared.Checkpoint)
	if err != nil || expectedErr != nil || shape != expected {
		return ContextCompactionResult{}, contextCompactionLimitation("maintenance_checkpoint_mismatch")
	}
	id, err := result.Checkpoint.Fingerprint()
	if err != nil || id != result.Checkpoint.ID || !contextCompactionEvidenceComplete(result.Checkpoint) {
		return ContextCompactionResult{}, contextCompactionLimitation("maintenance_evidence_invalid")
	}
	return result, nil
}

func ValidateContextCompactionSettlement(attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	if settlement.Settlement.State != runtimeeffects.StateSettled {
		return nil
	}
	raw, err := json.Marshal(settlement.Settlement.Evidence)
	if err != nil {
		return err
	}
	var envelope struct {
		Result ContextCompactionResult `json:"context_compaction"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	result, err := ReadContextCompactionResult(raw, envelope.Result.Preparation)
	if err != nil {
		return err
	}
	checkpoint := result.Checkpoint
	if checkpoint.OperationID != attempt.OperationID || checkpoint.AttemptID != attempt.AttemptID || checkpoint.SessionID != attempt.Authority.Target.SessionID {
		return contextCompactionLimitation("maintenance_attempt_mismatch")
	}
	return nil
}

func ContextCompactionInstallation(record ConversationRecord, result ContextCompactionResult) ([]ContextCompactionCheckpoint, bool, error) {
	if err := validateContextCompactionCheckpoints(record.SessionID, record.Messages, record.ContextCheckpoints); err != nil {
		return nil, false, err
	}
	checkpoints := cloneContextCompactionCheckpoints(record.ContextCheckpoints)
	parent := ""
	if len(checkpoints) != 0 {
		parent = checkpoints[len(checkpoints)-1].ID
		if parent == result.Checkpoint.ID {
			return checkpoints, false, nil
		}
	}
	fingerprint, err := canonicaljson.Hash(record.Messages)
	if err != nil || fingerprint != result.Preparation.ConversationFingerprint || parent != result.Checkpoint.ParentID {
		return nil, false, contextCompactionLimitation("installation_prefix_or_parent_changed")
	}
	checkpoints = append(checkpoints, result.Checkpoint)
	if err := validateContextCompactionCheckpoints(record.SessionID, record.Messages, checkpoints); err != nil {
		return nil, false, fmt.Errorf("install context compaction: %w", err)
	}
	return checkpoints, true, nil
}
