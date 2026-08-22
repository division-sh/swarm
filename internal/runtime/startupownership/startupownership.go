package startupownership

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type State string

const (
	StateActive     State = "active"
	StateReleased   State = "released"
	StateSuperseded State = "superseded"
)

type AcquisitionKind string

const (
	AcquisitionCold          AcquisitionKind = "cold"
	AcquisitionCleanHandoff  AcquisitionKind = "clean_handoff"
	AcquisitionCrashTakeover AcquisitionKind = "crash_takeover"
	AcquisitionRepair        AcquisitionKind = "authority_repair"
)

type AcquisitionFailure string

const (
	AcquisitionTakeoverRequired    AcquisitionFailure = "takeover_required"
	AcquisitionPriorOwnerAmbiguous AcquisitionFailure = "prior_owner_ambiguous"
)

type AcquisitionError struct {
	Failure    AcquisitionFailure
	Detail     string
	RecordedAt time.Time
}

type TerminalCause string

const (
	TerminalReleased            TerminalCause = "released"
	TerminalOwnershipUnprovable TerminalCause = "ownership_unprovable"
	TerminalOwnershipSuperseded TerminalCause = "ownership_superseded"
)

type TerminalResult struct {
	Cause                TerminalCause
	SuccessorAuthorityID string
}

type PossessionError struct {
	Cause                TerminalCause
	SuccessorAuthorityID string
	Err                  error
}

func (e *PossessionError) Error() string {
	if e == nil {
		return "selected-store possession failed"
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return string(e.Cause)
}

func (e *PossessionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *AcquisitionError) Error() string {
	if e == nil {
		return "process startup/topology capability acquisition failed"
	}
	if strings.TrimSpace(e.Detail) == "" {
		return string(e.Failure)
	}
	return string(e.Failure) + ": " + strings.TrimSpace(e.Detail)
}

type AcquireRequest struct {
	OwnerID           string
	BootID            string
	RuntimeInstanceID string
}

func (r AcquireRequest) Validate() error {
	if strings.TrimSpace(r.OwnerID) == "" {
		return errors.New("process capability owner_id is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(r.BootID)); err != nil {
		return fmt.Errorf("process capability boot_id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(r.RuntimeInstanceID)); err != nil {
		return fmt.Errorf("process capability runtime_instance_id is invalid: %w", err)
	}
	return nil
}

type Authority struct {
	AuthorityID            string          `json:"authority_id"`
	AuthorityGeneration    uint64          `json:"authority_generation"`
	TransitionOrdinal      uint64          `json:"transition_ordinal"`
	StateVersion           uint64          `json:"state_version"`
	State                  State           `json:"state"`
	OwnerID                string          `json:"owner_id"`
	BootID                 string          `json:"boot_id"`
	RuntimeInstanceID      string          `json:"runtime_instance_id"`
	Backend                string          `json:"backend"`
	AcquisitionID          string          `json:"acquisition_id"`
	AcquisitionRequestHash string          `json:"acquisition_request_hash"`
	AcquisitionKind        AcquisitionKind `json:"acquisition_kind"`
	PredecessorAuthorityID string          `json:"predecessor_authority_id,omitempty"`
	SuccessorAuthorityID   string          `json:"successor_authority_id,omitempty"`
	RecordedAt             time.Time       `json:"recorded_at"`
}

func (a Authority) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(a.AuthorityID)); err != nil {
		return fmt.Errorf("process capability authority_id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(a.BootID)); err != nil {
		return fmt.Errorf("process capability boot_id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(a.RuntimeInstanceID)); err != nil {
		return fmt.Errorf("process capability runtime_instance_id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(a.AcquisitionID)); err != nil {
		return fmt.Errorf("process capability acquisition_id is invalid: %w", err)
	}
	if a.AuthorityGeneration == 0 || a.TransitionOrdinal == 0 || a.StateVersion == 0 || strings.TrimSpace(a.OwnerID) == "" || strings.TrimSpace(a.Backend) == "" || len(strings.TrimSpace(a.AcquisitionRequestHash)) != sha256.Size*2 || a.RecordedAt.IsZero() {
		return errors.New("process capability authority identity is incomplete")
	}
	req := AcquireRequest{
		OwnerID:           a.OwnerID,
		BootID:            a.BootID,
		RuntimeInstanceID: a.RuntimeInstanceID,
	}
	expectedRequestHash := AcquireRequestHash(req, a.Backend)
	if a.AcquisitionID != a.BootID || a.AcquisitionRequestHash != expectedRequestHash || a.AuthorityID != authorityIDForAcquisition(a.BootID, expectedRequestHash) {
		return errors.New("process capability acquisition binding is invalid")
	}
	if a.AuthorityGeneration == 1 {
		if strings.TrimSpace(a.PredecessorAuthorityID) != "" || a.AcquisitionKind != AcquisitionCold {
			return errors.New("initial process capability authority has invalid lineage")
		}
	} else {
		if _, err := uuid.Parse(strings.TrimSpace(a.PredecessorAuthorityID)); err != nil {
			return fmt.Errorf("process capability predecessor authority is invalid: %w", err)
		}
		if a.AcquisitionKind != AcquisitionCleanHandoff && a.AcquisitionKind != AcquisitionCrashTakeover && a.AcquisitionKind != AcquisitionRepair {
			return errors.New("successor process capability authority has invalid acquisition kind")
		}
	}
	switch a.State {
	case StateActive, StateReleased:
		if strings.TrimSpace(a.SuccessorAuthorityID) != "" {
			return errors.New("non-superseded process capability cannot name a successor")
		}
		return nil
	case StateSuperseded:
		if _, err := uuid.Parse(strings.TrimSpace(a.SuccessorAuthorityID)); err != nil {
			return fmt.Errorf("superseded process capability successor is invalid: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("process capability state %q is invalid", a.State)
	}
}

type AuthorityInspectionStatus string

const (
	AuthorityInspectionEmpty   AuthorityInspectionStatus = "empty"
	AuthorityInspectionValid   AuthorityInspectionStatus = "valid"
	AuthorityInspectionCorrupt AuthorityInspectionStatus = "corrupt"
)

type AuthorityInspection struct {
	Status         AuthorityInspectionStatus `json:"status"`
	Backend        string                    `json:"backend"`
	State          State                     `json:"state,omitempty"`
	OwnerID        string                    `json:"owner_id,omitempty"`
	AuthorityID    string                    `json:"authority_id,omitempty"`
	Generation     uint64                    `json:"generation,omitempty"`
	RecordedAt     time.Time                 `json:"recorded_at,omitempty"`
	FindingsDigest string                    `json:"findings_digest"`
	Detail         string                    `json:"detail"`
}

func (i AuthorityInspection) Validate() error {
	if i.Status != AuthorityInspectionEmpty && i.Status != AuthorityInspectionValid && i.Status != AuthorityInspectionCorrupt {
		return errors.New("authority inspection status is invalid")
	}
	if strings.TrimSpace(i.Backend) == "" || !isCanonicalDigest(i.FindingsDigest) || strings.TrimSpace(i.Detail) == "" {
		return errors.New("authority inspection evidence is incomplete")
	}
	if i.Status == AuthorityInspectionValid {
		if _, err := uuid.Parse(strings.TrimSpace(i.AuthorityID)); err != nil || i.Generation == 0 || i.RecordedAt.IsZero() {
			return errors.New("valid authority inspection identity is incomplete")
		}
	}
	return nil
}

type AuthorityRepairRequest struct {
	OperationID    string
	FindingsDigest string
	Confirmed      bool
}

func (r AuthorityRepairRequest) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(r.OperationID)); err != nil {
		return fmt.Errorf("authority repair operation_id is invalid: %w", err)
	}
	if !isCanonicalDigest(r.FindingsDigest) {
		return errors.New("authority repair findings digest is invalid")
	}
	if !r.Confirmed {
		return errors.New("authority repair requires explicit confirmation")
	}
	return nil
}

type AuthorityRepairResult struct {
	OperationID         string    `json:"operation_id"`
	FindingsDigest      string    `json:"findings_digest"`
	RepairedAuthorityID string    `json:"repaired_authority_id"`
	AuthorityGeneration uint64    `json:"authority_generation"`
	CompletedAt         time.Time `json:"completed_at"`
	UserDataUntouched   bool      `json:"user_data_untouched"`
}

func (r AuthorityRepairResult) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(r.OperationID)); err != nil {
		return fmt.Errorf("authority repair result operation_id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(r.RepairedAuthorityID)); err != nil {
		return fmt.Errorf("authority repair result authority_id is invalid: %w", err)
	}
	if !isCanonicalDigest(r.FindingsDigest) || r.AuthorityGeneration == 0 || r.CompletedAt.IsZero() || !r.UserDataUntouched {
		return errors.New("authority repair result is incomplete")
	}
	return nil
}

func isCanonicalDigest(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "sha256:") && len(value) == len("sha256:")+sha256.Size*2
}

type AuthorityMaintenanceStore interface {
	InspectAuthority(context.Context) (AuthorityInspection, error)
	RepairAuthority(context.Context, AuthorityRepairRequest) (AuthorityRepairResult, error)
}

func NewColdAuthority(req AcquireRequest, backend string) (Authority, error) {
	return NewAuthority(req, backend, 1, "", AcquisitionCold)
}

func NewAuthority(req AcquireRequest, backend string, generation uint64, predecessorID string, kind AcquisitionKind) (Authority, error) {
	if err := req.Validate(); err != nil {
		return Authority{}, err
	}
	backend = strings.TrimSpace(backend)
	requestHash := AcquireRequestHash(req, backend)
	authorityID := authorityIDForAcquisition(req.BootID, requestHash)
	value := Authority{
		AuthorityID: authorityID, AuthorityGeneration: generation, TransitionOrdinal: 1, StateVersion: 1, State: StateActive,
		OwnerID: strings.TrimSpace(req.OwnerID), BootID: strings.TrimSpace(req.BootID),
		RuntimeInstanceID: strings.TrimSpace(req.RuntimeInstanceID), Backend: backend,
		AcquisitionID: strings.TrimSpace(req.BootID), AcquisitionRequestHash: requestHash,
		AcquisitionKind: kind, PredecessorAuthorityID: strings.TrimSpace(predecessorID),
		RecordedAt: time.Now().UTC(),
	}
	return value, value.Validate()
}

func AcquireRequestHash(req AcquireRequest, backend string) string {
	payload := strings.Join([]string{
		strings.TrimSpace(req.OwnerID), strings.TrimSpace(req.BootID),
		strings.TrimSpace(req.RuntimeInstanceID), strings.TrimSpace(backend),
	}, "\x00")
	return fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
}

func authorityIDForAcquisition(bootID, requestHash string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm-process-authority-v1\x00"+strings.TrimSpace(bootID)+"\x00"+requestHash)).String()
}

func ReleasedAuthority(previous Authority) (Authority, error) {
	if err := previous.Validate(); err != nil {
		return Authority{}, err
	}
	if previous.State != StateActive {
		return Authority{}, fmt.Errorf("process capability cannot release from %q", previous.State)
	}
	next := previous
	next.State = StateReleased
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = time.Now().UTC()
	if !next.RecordedAt.After(previous.RecordedAt) {
		next.RecordedAt = previous.RecordedAt.Add(time.Nanosecond)
	}
	return next, next.Validate()
}

func SupersededAuthority(previous Authority, successorID string) (Authority, error) {
	if err := previous.Validate(); err != nil {
		return Authority{}, err
	}
	if previous.State != StateActive {
		return Authority{}, fmt.Errorf("process capability cannot supersede from %q", previous.State)
	}
	if _, err := uuid.Parse(strings.TrimSpace(successorID)); err != nil {
		return Authority{}, fmt.Errorf("process capability successor authority is invalid: %w", err)
	}
	next := previous
	next.State = StateSuperseded
	next.SuccessorAuthorityID = strings.TrimSpace(successorID)
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = time.Now().UTC()
	if !next.RecordedAt.After(previous.RecordedAt) {
		next.RecordedAt = previous.RecordedAt.Add(time.Nanosecond)
	}
	return next, next.Validate()
}

func ValidateTransition(previous *Authority, next Authority) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if previous == nil {
		if next.State != StateActive || next.TransitionOrdinal != 1 || next.StateVersion != 1 {
			return errors.New("initial process capability authority is invalid")
		}
		return nil
	}
	if err := previous.Validate(); err != nil {
		return err
	}
	if previous.State != StateActive || (next.State != StateReleased && next.State != StateSuperseded) ||
		next.AuthorityID != previous.AuthorityID || next.OwnerID != previous.OwnerID ||
		next.BootID != previous.BootID || next.RuntimeInstanceID != previous.RuntimeInstanceID ||
		next.Backend != previous.Backend || next.AuthorityGeneration != previous.AuthorityGeneration ||
		next.AcquisitionID != previous.AcquisitionID || next.AcquisitionRequestHash != previous.AcquisitionRequestHash ||
		next.AcquisitionKind != previous.AcquisitionKind || next.PredecessorAuthorityID != previous.PredecessorAuthorityID ||
		next.TransitionOrdinal != previous.TransitionOrdinal+1 ||
		next.StateVersion != previous.StateVersion+1 || !next.RecordedAt.After(previous.RecordedAt) {
		return errors.New("process capability terminal transition is invalid")
	}
	if next.State == StateReleased && next.SuccessorAuthorityID != "" {
		return errors.New("released process capability cannot name a successor")
	}
	if next.State == StateSuperseded && next.SuccessorAuthorityID == "" {
		return errors.New("superseded process capability must name a successor")
	}
	return nil
}

type GrantTransitionRecorder interface {
	RecordGenerationGrantTransition(context.Context, *GrantEvidence, GrantEvidence) error
}

type SessionTerminalOwner interface {
	SelectedStoreSessionTerminal(TerminalResult)
}

// RetainedSession is implemented only by private selected-store adapters. It
// never crosses process composition; callers receive ProcessCapability.
type RetainedSession interface {
	Authority() (Authority, error)
	ProveCurrent(context.Context) error
	MonitorProveCurrent(context.Context, time.Duration) error
	InstallTerminalOwner(SessionTerminalOwner) error
	GrantTransitionRecorder
	LoadSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error)
	CommitSourceSet(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	ApplyBundleDeleteFinalMutation(context.Context, runtimebundledelete.FinalMutationRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error)
	ReplayBundleDeleteResult(context.Context, runtimebundledelete.FinalMutationRequest) (runtimebundledelete.Result, error)
	ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error)
	CommitAgentLifecycleTransition(context.Context, runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error)
	Release(context.Context) error
}

type Store interface {
	AcquireProcessCapability(context.Context, AcquireRequest) (ProcessCapability, error)
}
