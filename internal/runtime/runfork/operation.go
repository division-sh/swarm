package runfork

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

// ForkOperationRequest is the durable request-to-child identity. The transport
// hash permits lookup before mutable source aliases are resolved. The request
// hash binds caller-selected inputs; ResolvedPoint binds the selected revision
// separately so retry never resolves an alias again.
type ForkOperationRequest struct {
	OperationID       string                    `json:"operation_id"`
	Actor             string                    `json:"actor"`
	IdempotencyKey    string                    `json:"idempotency_key,omitempty"`
	TransportHash     string                    `json:"transport_hash"`
	SourceRunID       string                    `json:"source_run_id"`
	ForkEventID       string                    `json:"fork_event_id"`
	ResolvedPoint     *RunForkPoint             `json:"resolved_point,omitempty"`
	TargetBundleHash  string                    `json:"target_bundle_hash"`
	AllowSourceFreeze bool                      `json:"allow_source_freeze"`
	ContractSelection RunForkContractSelection  `json:"contract_selection"`
	DataPinOverrides  []durabledata.ExplicitPin `json:"data_pin_overrides"`
}

func (r ForkOperationRequest) Canonical() (ForkOperationRequest, string, error) {
	for _, id := range []string{r.OperationID, r.SourceRunID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return ForkOperationRequest{}, "", fmt.Errorf("fork operation requires canonical operation and source-run UUIDs")
		}
	}
	if r.ForkEventID != "" {
		parsed, err := uuid.Parse(r.ForkEventID)
		if err != nil || parsed == uuid.Nil || parsed.String() != r.ForkEventID {
			return ForkOperationRequest{}, "", fmt.Errorf("fork operation event selector must be a canonical UUID")
		}
	}
	if r.ResolvedPoint != nil {
		if err := r.ResolvedPoint.Validate(); err != nil {
			return ForkOperationRequest{}, "", fmt.Errorf("fork operation resolved point: %w", err)
		}
		if r.ResolvedPoint.EventID != r.ForkEventID && r.ForkEventID != "" {
			return ForkOperationRequest{}, "", fmt.Errorf("fork operation resolved point disagrees with event selector")
		}
	}
	if strings.TrimSpace(r.Actor) == "" || strings.TrimSpace(r.TransportHash) == "" || strings.TrimSpace(r.TargetBundleHash) == "" {
		return ForkOperationRequest{}, "", fmt.Errorf("fork operation requires actor, transport hash and selected bundle")
	}
	switch r.ContractSelection.Mode {
	case RunForkContractSelectionModeSelectedContracts:
		if r.ContractSelection.BundleHash != "" {
			return ForkOperationRequest{}, "", fmt.Errorf("selected-contract fork operation cannot carry a bundle-hash selector")
		}
	case RunForkContractSelectionModeBundleHash:
		if r.ContractSelection.BundleHash != r.TargetBundleHash {
			return ForkOperationRequest{}, "", fmt.Errorf("bundle-selected fork operation must bind its exact target bundle")
		}
	default:
		return ForkOperationRequest{}, "", fmt.Errorf("fork operation has unsupported contract selection")
	}
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	pins, err := durabledata.CanonicalExplicitPins(r.DataPinOverrides)
	if err != nil {
		return ForkOperationRequest{}, "", err
	}
	r.DataPinOverrides = pins
	semantic := struct {
		SourceRunID       string                    `json:"source_run_id"`
		ForkEventID       string                    `json:"fork_event_id"`
		TargetBundleHash  string                    `json:"target_bundle_hash"`
		AllowSourceFreeze bool                      `json:"allow_source_freeze"`
		ContractSelection RunForkContractSelection  `json:"contract_selection"`
		DataPinOverrides  []durabledata.ExplicitPin `json:"data_pin_overrides"`
	}{r.SourceRunID, r.ForkEventID, r.TargetBundleHash, r.AllowSourceFreeze, r.ContractSelection, r.DataPinOverrides}
	hash, err := canonicaljson.Hash(semantic)
	if err != nil {
		return ForkOperationRequest{}, "", err
	}
	return r, hash, nil
}

type ForkOperationStatus string

type ForkOperationKeyConflictError struct {
	OriginalRequestHash    string
	ConflictingRequestHash string
}

func (e *ForkOperationKeyConflictError) Error() string {
	return "run.fork idempotency key belongs to a different request"
}

const (
	ForkOperationMaterialized ForkOperationStatus = "materialized"
	ForkOperationActivated    ForkOperationStatus = "activated"
	ForkOperationFailed       ForkOperationStatus = "failed"
	ForkOperationUncertain    ForkOperationStatus = "uncertain"
)

// ForkOperationResult is the immutable activation-time public projection.
// It must not be reconstructed from a later source or child run status.
type ForkOperationResult struct {
	SourceRunID        string            `json:"source_run_id"`
	SourceRunStatus    string            `json:"source_run_status"`
	SourceFrozen       bool              `json:"source_frozen"`
	ForkRunID          string            `json:"fork_run_id"`
	ForkEventID        string            `json:"fork_event_id"`
	ForkPoint          RunForkPoint      `json:"fork_point"`
	ForkRunStatus      string            `json:"fork_run_status"`
	BundleHash         string            `json:"bundle_hash"`
	ExecutedEventCount int               `json:"executed_event_count"`
	DataPins           []durabledata.Pin `json:"data_pins"`
}

func (r ForkOperationResult) Validate(request ForkOperationRequest, forkRunID string) error {
	if request.ResolvedPoint == nil || r.ForkPoint != *request.ResolvedPoint || r.ForkEventID != r.ForkPoint.EventID {
		return fmt.Errorf("fork operation result lost its exact resolved point")
	}
	if err := r.ForkPoint.Validate(); err != nil {
		return fmt.Errorf("fork operation result point: %w", err)
	}
	if r.SourceRunID != request.SourceRunID || (request.ForkEventID != "" && r.ForkEventID != request.ForkEventID) ||
		r.ForkRunID != forkRunID || r.BundleHash != request.TargetBundleHash ||
		r.ForkRunStatus != RunForkActivatedStatus || r.ExecutedEventCount < 0 ||
		strings.TrimSpace(r.SourceRunStatus) == "" || r.SourceFrozen != (r.SourceRunStatus == RunForkSourceFrozenStatus) {
		return fmt.Errorf("fork operation result contradicts its exact activation request")
	}
	seen := make(map[durabledata.DeclarationRef]struct{}, len(r.DataPins))
	for _, pin := range r.DataPins {
		if err := pin.Validate(); err != nil {
			return fmt.Errorf("fork operation result pin: %w", err)
		}
		if pin.RunID != forkRunID || pin.RunState != RunForkActivatedStatus {
			return fmt.Errorf("fork operation result pin has foreign run or lifecycle state")
		}
		if _, duplicate := seen[pin.Declaration]; duplicate {
			return fmt.Errorf("fork operation result repeats declaration %s", pin.Declaration.Key())
		}
		seen[pin.Declaration] = struct{}{}
	}
	return nil
}

type ForkOperationRecord struct {
	Request      ForkOperationRequest  `json:"request"`
	SemanticHash string                `json:"semantic_hash"`
	ForkRunID    string                `json:"fork_run_id"`
	BindingID    string                `json:"binding_id"`
	Status       ForkOperationStatus   `json:"status"`
	Result       *ForkOperationResult  `json:"result,omitempty"`
	Failure      *ForkOperationFailure `json:"failure,omitempty"`
}

type ForkOperationFailure struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

func (f ForkOperationFailure) Validate() error {
	if strings.TrimSpace(f.Code) == "" || strings.TrimSpace(f.Reason) == "" {
		return fmt.Errorf("fork operation failure requires typed code and reason")
	}
	return nil
}

func (r ForkOperationRecord) Validate() error {
	_, hash, err := r.Request.Canonical()
	if err != nil {
		return fmt.Errorf("fork operation request: %w", err)
	}
	if hash != r.SemanticHash {
		return fmt.Errorf("fork operation request and semantic hash disagree")
	}
	if r.Request.ResolvedPoint == nil {
		return fmt.Errorf("materialized fork operation requires its exact resolved point")
	}
	for _, id := range []string{r.ForkRunID, r.BindingID} {
		parsed, parseErr := uuid.Parse(id)
		if parseErr != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("fork operation has invalid child or binding identity")
		}
	}
	switch r.Status {
	case ForkOperationMaterialized:
		if r.Result != nil || r.Failure != nil {
			return fmt.Errorf("materialized fork operation cannot claim a result or failure")
		}
	case ForkOperationActivated:
		if r.Result == nil || r.Failure != nil {
			return fmt.Errorf("activated fork operation requires only exact result")
		}
		return r.Result.Validate(r.Request, r.ForkRunID)
	case ForkOperationFailed, ForkOperationUncertain:
		if r.Result != nil || r.Failure == nil {
			return fmt.Errorf("failed or uncertain fork operation requires only typed failure")
		}
		return r.Failure.Validate()
	default:
		return fmt.Errorf("fork operation status %q is invalid", r.Status)
	}
	return nil
}
