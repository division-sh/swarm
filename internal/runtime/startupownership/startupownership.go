package startupownership

import (
	"context"
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
	StateActive   State = "active"
	StateReleased State = "released"
)

type AcquisitionFailure string

const (
	AcquisitionTakeoverRequired    AcquisitionFailure = "takeover_required"
	AcquisitionPriorOwnerAmbiguous AcquisitionFailure = "prior_owner_ambiguous"
)

type AcquisitionError struct {
	Failure AcquisitionFailure
	Detail  string
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
	AuthorityID       string    `json:"authority_id"`
	TransitionOrdinal uint64    `json:"transition_ordinal"`
	StateVersion      uint64    `json:"state_version"`
	State             State     `json:"state"`
	OwnerID           string    `json:"owner_id"`
	BootID            string    `json:"boot_id"`
	RuntimeInstanceID string    `json:"runtime_instance_id"`
	Backend           string    `json:"backend"`
	RecordedAt        time.Time `json:"recorded_at"`
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
	if a.TransitionOrdinal == 0 || a.StateVersion == 0 || strings.TrimSpace(a.OwnerID) == "" || strings.TrimSpace(a.Backend) == "" || a.RecordedAt.IsZero() {
		return errors.New("process capability authority identity is incomplete")
	}
	switch a.State {
	case StateActive, StateReleased:
		return nil
	default:
		return fmt.Errorf("process capability state %q is invalid", a.State)
	}
}

func NewColdAuthority(req AcquireRequest, backend string) (Authority, error) {
	if err := req.Validate(); err != nil {
		return Authority{}, err
	}
	value := Authority{
		AuthorityID: uuid.NewString(), TransitionOrdinal: 1, StateVersion: 1, State: StateActive,
		OwnerID: strings.TrimSpace(req.OwnerID), BootID: strings.TrimSpace(req.BootID),
		RuntimeInstanceID: strings.TrimSpace(req.RuntimeInstanceID), Backend: strings.TrimSpace(backend),
		RecordedAt: time.Now().UTC(),
	}
	return value, value.Validate()
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
	if previous.State != StateActive || next.State != StateReleased ||
		next.AuthorityID != previous.AuthorityID || next.OwnerID != previous.OwnerID ||
		next.BootID != previous.BootID || next.RuntimeInstanceID != previous.RuntimeInstanceID ||
		next.Backend != previous.Backend || next.TransitionOrdinal != previous.TransitionOrdinal+1 ||
		next.StateVersion != previous.StateVersion+1 || !next.RecordedAt.After(previous.RecordedAt) {
		return errors.New("process capability release transition is invalid")
	}
	return nil
}

type GrantTransitionRecorder interface {
	RecordGenerationGrantTransition(context.Context, *GrantEvidence, GrantEvidence) error
}

type SessionTerminalOwner interface {
	SelectedStoreSessionTerminal()
}

// RetainedSession is implemented only by private selected-store adapters. It
// never crosses process composition; callers receive ProcessCapability.
type RetainedSession interface {
	Authority() (Authority, error)
	ProveCurrent(context.Context) error
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
