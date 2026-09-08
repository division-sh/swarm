package destructivereset

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

// OperationPhase records durable effects, not the availability of a runtime in
// any process. In particular, completed never substitutes for startup admission.
type OperationPhase string

const (
	PhaseAdmitted          OperationPhase = "admitted"
	PhasePlanned           OperationPhase = "planned"
	PhaseQuiesced          OperationPhase = "quiesced"
	PhaseCleanupCommitted  OperationPhase = "cleanup_committed"
	PhaseContainersSettled OperationPhase = "containers_settled"
	PhaseCompleted         OperationPhase = "completed"
)

var (
	ErrOperationConflict = errors.New("destructive reset request conflicts with admitted operation")
	ErrOperationChanged  = errors.New("destructive reset operation revision changed")
)

type OperationConflictError struct {
	OperationID            string
	OriginalRequestHash    string
	ConflictingRequestHash string
}

func (e *OperationConflictError) Error() string { return ErrOperationConflict.Error() }
func (e *OperationConflictError) Unwrap() error { return ErrOperationConflict }

type Operation struct {
	Request    Request                      `json:"request"`
	Revision   int64                        `json:"revision"`
	Phase      OperationPhase               `json:"phase"`
	SourceSet  *agenttopology.SourceSetPlan `json:"source_set"`
	Plan       *Result                      `json:"plan,omitempty"`
	Quiescence *QuiescenceResult            `json:"quiescence,omitempty"`
	Cleanup    *CleanupResult               `json:"cleanup,omitempty"`
	Containers *ContainerResetResult        `json:"containers,omitempty"`
	Response   *ExecutionResult             `json:"response,omitempty"`
}

// OperationStore is reset-family authority retained independently of the API
// completion cache. Admission returns the original operation for a keyed retry.
type OperationStore interface {
	LookupResetOperation(context.Context, Request) (*Operation, error)
	AdmitResetOperation(context.Context, Request) (Operation, error)
	ReadResetOperation(context.Context, string) (Operation, error)
	PendingResetOperations(context.Context) ([]Operation, error)
	AdvanceResetOperation(context.Context, Operation, Operation) error
}

func NewOperation(req Request) (Operation, error) {
	var err error
	req, err = req.normalize(req.RequestedAt)
	if err != nil {
		return Operation{}, err
	}
	if req.DryRun || req.RequestHash == "" || req.RequestedAt.IsZero() {
		return Operation{}, fmt.Errorf("%w: durable reset requires an apply request, request hash and timestamp", ErrInvalidRequest)
	}
	return Operation{Request: req, Revision: 1, Phase: PhaseAdmitted}, nil
}

func (o Operation) Matches(req Request) bool {
	return o.Request.ActorTokenID == req.ActorTokenID &&
		o.Request.IdempotencyKey == req.IdempotencyKey &&
		o.Request.RequestHash == req.RequestHash &&
		o.Request.DryRun == req.DryRun &&
		o.Request.IncludeSourceArtifacts == req.IncludeSourceArtifacts &&
		(req.IdempotencyKey != "" || o.Request.OperationID == req.OperationID)
}

func (o Operation) Validate() error {
	initial, err := NewOperation(o.Request)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(initial.Request, o.Request) || o.Revision < 1 {
		return errors.New("invalid reset operation identity or revision")
	}
	if o.SourceSet != nil {
		canonical, err := agenttopology.NewSourceSetPlan(o.SourceSet.Sources, o.SourceSet.Agents)
		if err != nil {
			return fmt.Errorf("invalid reset source-set snapshot: %w", err)
		}
		if !reflect.DeepEqual(&canonical, o.SourceSet) {
			return errors.New("reset source-set snapshot is not canonical")
		}
	}
	phase := map[OperationPhase]int{PhaseAdmitted: 0, PhasePlanned: 1, PhaseQuiesced: 2, PhaseCleanupCommitted: 3, PhaseContainersSettled: 4, PhaseCompleted: 5}
	step, ok := phase[o.Phase]
	if !ok {
		return fmt.Errorf("invalid reset operation phase %q", o.Phase)
	}
	if (o.Plan != nil) != (step >= 1) || (o.Quiescence != nil) != (step >= 2) ||
		(o.Cleanup != nil) != (step >= 3) || (o.Containers != nil) != (step >= 4) {
		return errors.New("reset operation phase does not match its durable evidence")
	}
	if o.Plan != nil && (o.Plan.DryRun || o.Plan.OperationName != DefaultOperationName ||
		o.Plan.IncludeSourceArtifacts != o.Request.IncludeSourceArtifacts ||
		o.Plan.Plan.IncludeSourceArtifacts != o.Request.IncludeSourceArtifacts ||
		!o.Plan.PlannedAt.Equal(o.Request.RequestedAt) || !o.Plan.Plan.CleanupRunSetKnown) {
		return errors.New("reset operation plan does not match admitted request")
	}
	if o.Plan != nil {
		seen := make(map[string]bool, len(o.Plan.Plan.ManagedContainers))
		for _, target := range o.Plan.Plan.ManagedContainers {
			identity := target.Identity()
			if target.Name == "" || target.RuntimeID == "" || seen[target.RuntimeID] || target.Action != ContainerActionStop ||
				identity.Validate() != nil || identity.BundleHash == "" || !identity.ResetEligibleManaged() {
				return errors.New("reset operation container intent requires unique immutable targets with complete reset-eligible source/projection identity")
			}
			seen[target.RuntimeID] = true
		}
	}
	if o.Quiescence != nil && (o.Quiescence.DryRun || o.Quiescence.OperationName != DefaultOperationName) {
		return errors.New("reset operation quiescence is invalid")
	}
	if o.Cleanup != nil && (o.Cleanup.DryRun || o.Cleanup.OperationName != DefaultOperationName || o.Cleanup.IncludeSourceArtifacts != o.Request.IncludeSourceArtifacts) {
		return errors.New("reset operation cleanup is invalid")
	}
	if o.Containers != nil && (o.Containers.DryRun || o.Containers.OperationName != DefaultOperationName || len(o.Containers.Failed) != 0) {
		return errors.New("reset operation resource settlement is incomplete")
	}
	if o.Containers != nil {
		if err := validateContainerSettlement(o.Plan.Plan.ManagedContainers, *o.Containers); err != nil {
			return err
		}
	}
	if o.Response != nil {
		if step < 3 || !reflect.DeepEqual(o.Plan, &o.Response.Plan) || !reflect.DeepEqual(o.Quiescence, &o.Response.Quiescence) ||
			!reflect.DeepEqual(o.Cleanup, &o.Response.Cleanup) || o.Response.Containers.OperationName != DefaultOperationName || o.Response.Containers.DryRun {
			return errors.New("reset response contradicts committed effect evidence")
		}
		if step < 5 && len(o.Response.Containers.Failed) == 0 {
			return errors.New("successful reset response requires process convergence")
		}
		if len(o.Response.Containers.Failed) == 0 && !reflect.DeepEqual(o.Containers, &o.Response.Containers) {
			return errors.New("successful reset response contradicts container settlement")
		}
	}
	if o.Phase == PhaseCompleted && o.Response == nil {
		return errors.New("completed reset requires its exact response")
	}
	return nil
}

// ValidateOperationTransition rejects evidence rewriting, skipped phases, and
// mutation of historical outcomes. Cleanup advancement additionally requires the
// private selected-store cleanup transaction; a process-local save cannot do it.
func ValidateOperationTransition(before, after Operation) error {
	if err := before.Validate(); err != nil {
		return err
	}
	if err := after.Validate(); err != nil {
		return err
	}
	next := map[OperationPhase]OperationPhase{
		PhaseAdmitted: PhasePlanned, PhasePlanned: PhaseQuiesced,
		PhaseQuiesced: PhaseCleanupCommitted, PhaseCleanupCommitted: PhaseContainersSettled,
		PhaseContainersSettled: PhaseCompleted,
	}
	recordPartialResponse := before.Phase == PhaseCleanupCommitted && after.Phase == before.Phase && before.Response == nil && after.Response != nil
	if (!recordPartialResponse && next[before.Phase] != after.Phase) || after.Revision != before.Revision+1 ||
		!reflect.DeepEqual(before.Request, after.Request) ||
		!reflect.DeepEqual(before.SourceSet, after.SourceSet) ||
		(before.Plan != nil && !reflect.DeepEqual(before.Plan, after.Plan)) ||
		(before.Quiescence != nil && !reflect.DeepEqual(before.Quiescence, after.Quiescence)) ||
		(before.Cleanup != nil && !reflect.DeepEqual(before.Cleanup, after.Cleanup)) ||
		(before.Containers != nil && !reflect.DeepEqual(before.Containers, after.Containers)) ||
		(before.Response != nil && !reflect.DeepEqual(before.Response, after.Response)) {
		return errors.New("invalid reset operation transition or rewritten evidence")
	}
	return nil
}

func (o Operation) Outcome() (ExecutionResult, error) {
	if err := o.Validate(); err != nil {
		return ExecutionResult{}, err
	}
	if o.Response == nil {
		return ExecutionResult{}, ErrOperationInProgress
	}
	return *o.Response, nil
}
