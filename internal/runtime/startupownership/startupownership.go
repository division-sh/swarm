package startupownership

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
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
	if r.OwnerID == "" || r.OwnerID != strings.TrimSpace(r.OwnerID) {
		return errors.New("process capability owner_id must be nonempty and canonical")
	}
	for _, field := range []struct{ name, value string }{
		{"boot_id", r.BootID}, {"runtime_instance_id", r.RuntimeInstanceID},
	} {
		id, err := uuid.Parse(field.value)
		if err != nil || id == uuid.Nil || id.String() != field.value {
			return fmt.Errorf("process capability %s must be a canonical nonzero UUID", field.name)
		}
	}
	return nil
}

// Authority projects the current native lease's stamp into retained execution
// evidence. Shape validity never grants possession or mutation permission.
type Authority struct {
	AuthorityID         string `json:"authority_id"`
	AuthorityGeneration uint64 `json:"authority_generation"`
	OwnerID             string `json:"owner_id"`
	BootID              string `json:"boot_id"`
	RuntimeInstanceID   string `json:"runtime_instance_id"`
	Backend             string `json:"backend"`
}

func AuthorityFromLease(stamp processbinding.LeaseStamp) Authority {
	return Authority{
		AuthorityID: stamp.LeaseID, AuthorityGeneration: stamp.Generation,
		OwnerID: stamp.OwnerID, BootID: stamp.BootID,
		RuntimeInstanceID: stamp.RuntimeInstanceID, Backend: stamp.Backend,
	}
}

func (a Authority) LeaseStamp() processbinding.LeaseStamp {
	return processbinding.LeaseStamp{
		LeaseIdentity: processbinding.LeaseIdentity{
			LeaseID: a.AuthorityID, OwnerID: a.OwnerID, BootID: a.BootID,
			RuntimeInstanceID: a.RuntimeInstanceID, Backend: a.Backend,
		},
		Generation: a.AuthorityGeneration,
	}
}

func (a Authority) Validate() error { return a.LeaseStamp().Validate() }

type AuthorityInspectionStatus string

const (
	AuthorityInspectionEmpty AuthorityInspectionStatus = "empty"
	AuthorityInspectionValid AuthorityInspectionStatus = "valid"
)

// The last admitted stamp and the point-in-time native observation are distinct:
// a holder may still be bootstrapping before its generation is published.
type AuthorityInspection struct {
	Status     AuthorityInspectionStatus `json:"status"`
	Backend    string                    `json:"backend"`
	Authority  *Authority                `json:"last_admitted,omitempty"`
	Possession *PossessionObservation    `json:"possession,omitempty"`
}

func EmptyAuthorityInspection(backend string) (AuthorityInspection, error) {
	result := AuthorityInspection{Status: AuthorityInspectionEmpty, Backend: backend}
	return result, result.Validate()
}

func (i AuthorityInspection) Validate() error {
	if i.Backend != processbinding.PostgresLeaseBackend && i.Backend != processbinding.SQLiteLeaseBackend {
		return errors.New("authority inspection backend is invalid")
	}
	switch i.Status {
	case AuthorityInspectionEmpty:
		if i.Authority != nil {
			return errors.New("empty process inspection contains an admitted generation")
		}
	case AuthorityInspectionValid:
		if i.Authority == nil {
			return errors.New("process inspection lacks its admitted generation")
		}
		if err := i.Authority.Validate(); err != nil {
			return err
		}
		if i.Authority.Backend != i.Backend {
			return errors.New("process inspection generation backend differs")
		}
	default:
		return errors.New("authority inspection status is invalid")
	}
	if i.Possession != nil {
		if err := i.Possession.Validate(); err != nil {
			return err
		}
		if i.Possession.Backend != i.Backend {
			return errors.New("process inspection possession backend differs")
		}
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
	runtimedestructivereset.OperationStore
	// Inspection proves the full current source-set plan for ordinary evidence
	// in the same transaction as current-grant and run classification.
	InspectRunExecutionOwnership(context.Context, GrantEvidence, string) (runtimemanager.RunExecutionOwnership, error)
	ProveSelectedForkGenerationGrant(context.Context, GrantEvidence) error
	Authority() (Authority, error)
	ProveCurrent(context.Context) error
	MonitorProveCurrent(context.Context, time.Duration) error
	InstallTerminalOwner(SessionTerminalOwner, time.Duration) error
	GrantTransitionRecorder
	LoadSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error)
	CommitSourceSet(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error)
	CommitAgentLifecycleTransition(context.Context, runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error)
	RebindFlowReadinessSourceSet(context.Context, runtimemanager.FlowReadinessSourceSetRebindRequest, runtimemanager.ProcessExecutionBinding) (runtimemanager.FlowReadinessSourceSetRebindResult, error)
	Release(context.Context) error
}

type Store interface {
	AcquireProcessCapability(context.Context, AcquireRequest) (ProcessCapability, error)
}
