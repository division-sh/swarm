package sessions

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
)

type RotationRequest struct {
	Identity  agentmemory.Identity
	LockOwner string
	Metadata  RotationMetadata
	Digest    string
}

type RotationReceipt struct {
	OperationID   string `json:"operation_id"`
	RequestDigest string `json:"request_digest"`
	Result        Lease  `json:"result"`
}

type RotationRefusalReason string

const (
	RotationRequestConflict     RotationRefusalReason = "request_conflict"
	RotationSuccessorNotCurrent RotationRefusalReason = "successor_not_current"
)

type RotationRefusal struct {
	Reason RotationRefusalReason
}

func (e *RotationRefusal) Error() string {
	return fmt.Sprintf("session rotation refused: %s", e.Reason)
}

// NormalizeRotationRequest is the single semantic boundary for both keyed and
// unkeyed rotations. The digest excludes the operation ID, which is the lookup key.
func NormalizeRotationRequest(identity agentmemory.Identity, lockOwner string, metadata RotationMetadata) (RotationRequest, error) {
	identity = identity.Normalize()
	if err := agentmemory.ValidateIdentity(identity, false); err != nil {
		return RotationRequest{}, err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	if lockOwner == "" {
		return RotationRequest{}, fmt.Errorf("lockOwner is required")
	}
	metadata.OperationID = strings.TrimSpace(metadata.OperationID)
	metadata.CheckpointSummary = strings.TrimSpace(metadata.CheckpointSummary)
	metadata.RetryReason = strings.TrimSpace(metadata.RetryReason)
	reason := strings.TrimSpace(metadata.TerminationReason.String())
	if reason == "" {
		metadata.TerminationReason = TerminationReasonNormal
	} else {
		metadata.TerminationReason = normalizeTerminationReason(reason)
	}
	if err := validateRuntimeTerminationReason(metadata.TerminationReason); err != nil {
		return RotationRequest{}, err
	}
	canonical := struct {
		Identity          agentmemory.Identity `json:"identity"`
		LockOwner         string               `json:"lock_owner"`
		CheckpointSummary string               `json:"checkpoint_summary"`
		RetryReason       string               `json:"retry_reason"`
		TerminationReason TerminationReason    `json:"termination_reason"`
	}{identity, lockOwner, metadata.CheckpointSummary, metadata.RetryReason, metadata.TerminationReason}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return RotationRequest{}, fmt.Errorf("encode rotation request: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return RotationRequest{Identity: identity, LockOwner: lockOwner, Metadata: metadata, Digest: hex.EncodeToString(sum[:])}, nil
}

// ReplayRotation returns nil only when no keyed receipt exists. A historical
// result is never itself live lease authority.
func ReplayRotation(request RotationRequest, receipt *RotationReceipt, current *Lease, now time.Time) (*Lease, error) {
	if receipt == nil {
		return nil, nil
	}
	if receipt.OperationID != request.Metadata.OperationID || receipt.RequestDigest != request.Digest {
		return nil, &RotationRefusal{Reason: RotationRequestConflict}
	}
	result := receipt.Result
	if current == nil || result.SessionID != current.SessionID || result.Identity.Normalize() != current.Identity.Normalize() ||
		result.LockOwner != current.LockOwner || result.ProviderSessionID != current.ProviderSessionID ||
		result.RetryReason != current.RetryReason || result.RetriesFromSessionID != current.RetriesFromSessionID ||
		!result.ExpiresAt.Equal(current.ExpiresAt) || !result.ExpiresAt.After(now) {
		return nil, &RotationRefusal{Reason: RotationSuccessorNotCurrent}
	}
	return &result, nil
}
