package startupownership

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

var ErrProcessNotCurrent = errors.New("prepared store operation requires current native process authority")

type PreparedOperationKind uint8

const (
	PreparedEffectCurrent PreparedOperationKind = iota + 1
	PreparedEffectAuthorize
	PreparedEffectLaunch
	PreparedForkMaterialize
	PreparedForkIssue
	PreparedForkClaim
	PreparedForkRecover
	PreparedForkStop
	PreparedManagedCapabilitySurface
	PreparedForkSourceEvents
	PreparedForkSettlement
	PreparedForkConstructionFailure
	PreparedForkDeliveryReconcile
	PreparedForkActivate
)

// PreparedProcessOperation is a closed synchronous store command. Only the
// typed constructors below can supply its invocation; it exposes no SQL or
// arbitrary callback. The original store owner still owns its transaction.
type PreparedProcessOperation struct {
	kind        PreparedOperationKind
	target      any
	coordinates processbinding.LeaseCoordinates
	generation  uint64
	stamp       *processbinding.LeaseStamp
	ownerID     string
	run         func(context.Context) error
}

type preparedOperationContextKey struct{}

type preparedOperationScope struct {
	kind   PreparedOperationKind
	target any
	life   context.Context
}

// PreparedOperationEntered permits only the exact synchronous semantic owner
// invoked by the process boundary to enter its transaction without recursing.
// Its independent lifetime ends before opMu is released, even if a consumer
// retains a WithoutCancel context. Caller cancellation is not scope retirement.
func PreparedOperationEntered(ctx context.Context, kind PreparedOperationKind, target any) bool {
	if ctx == nil {
		return false
	}
	scope, ok := ctx.Value(preparedOperationContextKey{}).(preparedOperationScope)
	return ok && scope.kind == kind && scope.target == target && scope.life.Err() == nil
}

func (p *processCapability) RunPreparedOperation(ctx context.Context, operation PreparedProcessOperation) error {
	if p == nil || ctx == nil || operation.run == nil || operation.target == nil {
		return ErrProcessNotCurrent
	}
	if _, resultOnly := ctx.Value(mutationAuthorityContextKey{}).(externalEffectResultContext); resultOnly {
		return ErrProcessNotCurrent
	}
	// A store invocation may enter only its original admitted owner. Reusing an
	// expired or foreign scope cannot recurse into opMu or start another command.
	if _, entered := ctx.Value(preparedOperationContextKey{}).(preparedOperationScope); entered {
		return ErrProcessNotCurrent
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if p.requireLive() != nil {
		return ErrProcessNotCurrent
	}
	if err := callerContextError(ctx); err != nil {
		return err
	}
	authority, err := p.session.Authority()
	if err != nil {
		return err
	}
	stamp := authority.LeaseStamp()
	if operation.stamp != nil && *operation.stamp != stamp ||
		operation.coordinates != (processbinding.LeaseCoordinates{}) && operation.coordinates != stamp.Coordinates() ||
		operation.generation != 0 && operation.generation != stamp.Generation ||
		operation.ownerID != "" && operation.ownerID != stamp.OwnerID {
		return ErrProcessNotCurrent
	}
	if err := p.proveCurrent(ctx); err != nil {
		return err
	}
	life, end := context.WithCancel(context.WithoutCancel(ctx))
	defer end()
	operationCtx := context.WithValue(ctx, preparedOperationContextKey{}, preparedOperationScope{operation.kind, operation.target, life})
	operationCtx = p.processMutationContext(operationCtx)
	err = operation.run(operationCtx)
	if err != nil {
		err = p.retireOnPossessionFailure(err)
	}
	return errors.Join(err, p.requireLive())
}

// PreparedManagedCapabilitySurfaceOperation owns only one synchronous save of
// an exact prospective actor's receipt. Probing and transport remain outside it.
func PreparedManagedCapabilitySurfaceOperation(ctx context.Context, process ProcessCapability, store managedcapabilities.Persistence, authority managedcapabilities.Authority, surface managedcapabilities.Surface) (PreparedProcessOperation, error) {
	p, ok := process.(*processCapability)
	if !ok || p == nil || ctx == nil || store == nil {
		return PreparedProcessOperation{}, ErrProcessNotCurrent
	}
	if authority.Kind != managedcapabilities.AuthorityStartupProbe || authority.ExecutionKind != managedcapabilities.ExecutionSelectedForkPreparation {
		return PreparedProcessOperation{}, errors.New("prepared surface requires non-executable startup authority")
	}
	if err := authority.Validate(); err != nil {
		return PreparedProcessOperation{}, err
	}
	surface = surface.Clone()
	if err := surface.Validate(); err != nil {
		return PreparedProcessOperation{}, err
	}
	actual := surface.Authority
	if actual.Preparation == nil || *actual.Preparation != *authority.Preparation {
		return PreparedProcessOperation{}, errors.New("prepared surface differs from its exact actor and catalog")
	}
	actual.Preparation = authority.Preparation
	if actual != authority {
		return PreparedProcessOperation{}, errors.New("prepared surface differs from its exact probe and preparation")
	}
	preparation := *authority.Preparation
	op := PreparedProcessOperation{kind: PreparedManagedCapabilitySurface, target: store,
		coordinates: processbinding.LeaseCoordinates{LeaseID: preparation.ProcessAuthorityID, OwnerID: preparation.ProcessOwnerID, BootID: preparation.ProcessBootID}}
	op.run = func(owned context.Context) error {
		// RunPreparedOperation has proved this process under opMu. Check the
		// caller's retained original before its derived process context is used.
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		return store.SaveManagedCapabilitySurface(owned, surface)
	}
	return op, nil
}

func (p *processCapability) requirePreparedOriginal(ctx context.Context) error {
	var grant *generationGrant
	var version uint64
	switch original := ctx.Value(mutationAuthorityContextKey{}).(type) {
	case nil:
		return nil
	case refusedMutationAuthority:
		return original.err
	case *processCapability:
		if original != p {
			return ErrProcessNotCurrent
		}
		return nil
	case *generationGrant:
		grant = original
	case MutationAuthority:
		if err := original.Validate(); err != nil {
			return err
		}
		if original.owner != p {
			return ErrProcessNotCurrent
		}
		if original.grant == nil {
			return nil
		}
		grant, version = original.grant, original.evidence.StateVersion
	default:
		return ErrMutationAuthorityMissing
	}
	if grant == nil || grant.owner != p {
		return ErrProcessNotCurrent
	}
	evidence, err := grant.Evidence()
	if err != nil {
		return err
	}
	if version != 0 && version != evidence.StateVersion {
		return errors.New("prepared surface has stale original custody")
	}
	return grant.requireExecutionAuthorityLocked(ctx, evidence)
}

func preparedSelectedForkOperation(ctx context.Context, process ProcessCapability, target any, binding runfork.SelectedForkPreparationBinding) (PreparedProcessOperation, *processCapability, error) {
	p, ok := process.(*processCapability)
	if ctx == nil || !ok || p == nil || target == nil {
		return PreparedProcessOperation{}, nil, ErrProcessNotCurrent
	}
	if err := binding.Validate(); err != nil {
		return PreparedProcessOperation{}, nil, err
	}
	op := preparedForkOperation(0, target, binding.SelectedForkPreparation)
	return op, p, nil
}

func validatePreparedSelectedExecution(binding runfork.SelectedForkPreparationBinding, authority effects.Authority) error {
	if !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork ||
		authority.ID != authority.SelectedFork.ExecutionID || authority.SelectedFork.ForkRunID != binding.ForkRunID {
		return errors.New("prepared operation requires its exact selected execution")
	}
	return nil
}

type PreparedForkSourceEventsResult struct {
	Events  []runfork.RunForkSelectedContractSourceEvent
	failure *preparedSourceEventsFailure
}

type preparedSourceEventsFailure struct{ cause error }

func (e *preparedSourceEventsFailure) Error() string { return e.cause.Error() }
func (e *preparedSourceEventsFailure) Unwrap() error { return e.cause }

// Preserve the exact native outcome only across this command's own transparent
// process join. A second error (including a possession failure) stays joined.
func (r PreparedForkSourceEventsResult) Outcome(err error) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok && r.failure != nil {
		causes := joined.Unwrap()
		if len(causes) == 1 && causes[0] == r.failure {
			err = r.failure.cause
		}
	}
	return r.Events, err
}

func PreparedForkSourceEventsOperation(ctx context.Context, process ProcessCapability, store interface {
	LoadRunForkSelectedContractSourceEvents(context.Context, string, string, []string, semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error)
}, binding runfork.SelectedForkPreparationBinding, authority effects.Authority, eventIDs []string, carriage semanticview.OriginalLoopCarriage, result *PreparedForkSourceEventsResult) (PreparedProcessOperation, error) {
	op, p, err := preparedSelectedForkOperation(ctx, process, store, binding)
	if err != nil {
		return PreparedProcessOperation{}, err
	}
	if err := validatePreparedSelectedExecution(binding, authority); err != nil {
		return PreparedProcessOperation{}, err
	}
	if result == nil {
		return PreparedProcessOperation{}, errors.New("prepared source events require their result")
	}
	ids := slices.Clone(eventIDs)
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id || seen[id] {
			return PreparedProcessOperation{}, errors.New("prepared source event membership is not exact")
		}
		seen[id] = true
	}
	op.kind = PreparedForkSourceEvents
	op.run = func(owned context.Context) error {
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		var err error
		result.Events, err = store.LoadRunForkSelectedContractSourceEvents(owned, binding.SourceRunID, binding.ForkRunID, ids, carriage)
		if err == nil {
			result.failure = nil
			return nil
		}
		result.failure = &preparedSourceEventsFailure{cause: err}
		return result.failure
	}
	return op, nil
}

type PreparedForkSettlementKind uint8

const (
	PreparedForkHeartbeat PreparedForkSettlementKind = iota + 1
	PreparedForkQuiesce
	PreparedForkFail
	PreparedForkClose
)

func PreparedForkSettlementOperation(ctx context.Context, process ProcessCapability, store interface {
	HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, effects.Authority, time.Duration) error
	QuiesceRunForkSelectedContractRuntimeExecution(context.Context, effects.Authority) error
	FailRunForkSelectedContractRuntimeExecution(context.Context, effects.Authority, json.RawMessage) error
	CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error
}, binding runfork.SelectedForkPreparationBinding, authority effects.Authority, kind PreparedForkSettlementKind, failure json.RawMessage, lease time.Duration) (PreparedProcessOperation, error) {
	op, p, err := preparedSelectedForkOperation(ctx, process, store, binding)
	if err != nil {
		return PreparedProcessOperation{}, err
	}
	if err := validatePreparedSelectedExecution(binding, authority); err != nil {
		return PreparedProcessOperation{}, err
	}
	if kind < PreparedForkHeartbeat || kind > PreparedForkClose {
		return PreparedProcessOperation{}, errors.New("prepared selected settlement kind is invalid")
	}
	failure = slices.Clone(failure)
	op.kind = PreparedForkSettlement
	op.run = func(owned context.Context) error {
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		switch kind {
		case PreparedForkHeartbeat:
			return store.HeartbeatRunForkSelectedContractRuntimeExecution(owned, authority, lease)
		case PreparedForkQuiesce:
			return store.QuiesceRunForkSelectedContractRuntimeExecution(owned, authority)
		case PreparedForkFail:
			return store.FailRunForkSelectedContractRuntimeExecution(owned, authority, failure)
		default:
			return store.CloseRunForkSelectedContractRuntimeExecution(owned, authority.ID)
		}
	}
	return op, nil
}

func PreparedForkConstructionFailureOperation(ctx context.Context, process ProcessCapability, store interface {
	FailMaterializedSelectedContractExecutionFork(context.Context, string, runfork.ForkOperationFailure) error
	DiscardMaterializedSelectedContractExecutionFork(context.Context, string) error
}, binding runfork.SelectedForkPreparationBinding, failure runfork.ForkOperationFailure, discard bool) (PreparedProcessOperation, error) {
	op, p, err := preparedSelectedForkOperation(ctx, process, store, binding)
	if err != nil {
		return PreparedProcessOperation{}, err
	}
	op.kind = PreparedForkConstructionFailure
	op.run = func(owned context.Context) error {
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		if discard {
			return store.DiscardMaterializedSelectedContractExecutionFork(owned, binding.ForkRunID)
		}
		return store.FailMaterializedSelectedContractExecutionFork(owned, binding.ForkRunID, failure)
	}
	return op, nil
}

func PreparedForkDeliveryReconcileOperation(ctx context.Context, process ProcessCapability, store interface {
	ReconcileSelectedSuccessorDeliveryAuthority(context.Context, string, deliverylifecycle.ExecutionAuthority) error
}, binding runfork.SelectedForkPreparationBinding, authority effects.Authority, predecessor string, successor deliverylifecycle.ExecutionAuthority) (PreparedProcessOperation, error) {
	op, p, err := preparedSelectedForkOperation(ctx, process, store, binding)
	if err != nil {
		return PreparedProcessOperation{}, err
	}
	if err := validatePreparedSelectedExecution(binding, authority); err != nil {
		return PreparedProcessOperation{}, err
	}
	if err := successor.Validate(); err != nil {
		return PreparedProcessOperation{}, err
	}
	if successor.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || successor.ExecutionID() != authority.ID ||
		successor.ForkRunID() != binding.ForkRunID || successor.Generation() != authority.SelectedFork.Generation ||
		successor.SourceArtifact().BundleHash() != binding.Coordinates.BundleHash || predecessor == "" || predecessor == authority.ID {
		return PreparedProcessOperation{}, errors.New("prepared delivery reconciliation differs from its selected successor")
	}
	op.kind = PreparedForkDeliveryReconcile
	op.run = func(owned context.Context) error {
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		return store.ReconcileSelectedSuccessorDeliveryAuthority(owned, predecessor, successor)
	}
	return op, nil
}

func PreparedForkActivateOperation(ctx context.Context, process ProcessCapability, store interface {
	ActivateRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
}, binding runfork.SelectedForkPreparationBinding, authority effects.Authority, request runfork.RunForkSelectedContractExecutionActivateRequest, result *runfork.RunForkActivation) (PreparedProcessOperation, error) {
	op, p, err := preparedSelectedForkOperation(ctx, process, store, binding)
	if err != nil {
		return PreparedProcessOperation{}, err
	}
	if err := validatePreparedSelectedExecution(binding, authority); err != nil {
		return PreparedProcessOperation{}, err
	}
	if result == nil || request.ForkRunID != binding.ForkRunID {
		return PreparedProcessOperation{}, errors.New("prepared activation requires its exact child and result")
	}
	request.AllowedSourceEventIDs = slices.Clone(request.AllowedSourceEventIDs)
	op.kind = PreparedForkActivate
	op.run = func(owned context.Context) error {
		if err := p.requirePreparedOriginal(ctx); err != nil {
			return err
		}
		var err error
		*result, err = store.ActivateRunForkForSelectedContractExecution(owned, request)
		return err
	}
	return op, nil
}

type PreparedEffectStore interface {
	IsExternalEffectAuthorityCurrent(context.Context, effects.Authority) (bool, error)
	AuthorizeExternalAttempt(context.Context, effects.Authority, effects.AuthorizeRequest) (effects.Attempt, error)
	MarkExternalAttemptLaunched(context.Context, effects.Attempt, time.Time) (effects.ExternalAttemptLaunch, error)
}

func preparedEffectOperation(kind PreparedOperationKind, store PreparedEffectStore, authority effects.Authority) PreparedProcessOperation {
	op := PreparedProcessOperation{kind: kind, target: store, ownerID: authority.ExecutionOwner}
	if preparation := authority.StartupProbe.Preparation; preparation != nil {
		op.coordinates = processbinding.LeaseCoordinates{LeaseID: preparation.ProcessAuthorityID, OwnerID: preparation.ProcessOwnerID, BootID: preparation.ProcessBootID}
		op.generation = authority.FenceGeneration
	}
	return op
}

func PreparedEffectCurrentOperation(store PreparedEffectStore, authority effects.Authority, current *bool) PreparedProcessOperation {
	if store == nil || current == nil {
		return PreparedProcessOperation{}
	}
	op := preparedEffectOperation(PreparedEffectCurrent, store, authority)
	op.run = func(ctx context.Context) (err error) {
		*current, err = store.IsExternalEffectAuthorityCurrent(ctx, authority)
		return err
	}
	return op
}

func PreparedEffectAuthorizeOperation(store PreparedEffectStore, authority effects.Authority, request effects.AuthorizeRequest, attempt *effects.Attempt) PreparedProcessOperation {
	if store == nil || attempt == nil {
		return PreparedProcessOperation{}
	}
	op := preparedEffectOperation(PreparedEffectAuthorize, store, authority)
	op.run = func(ctx context.Context) (err error) {
		*attempt, err = store.AuthorizeExternalAttempt(ctx, authority, request)
		return err
	}
	return op
}

func PreparedEffectLaunchOperation(store PreparedEffectStore, attempt effects.Attempt, now time.Time, launch *effects.ExternalAttemptLaunch) PreparedProcessOperation {
	if store == nil || launch == nil {
		return PreparedProcessOperation{}
	}
	op := preparedEffectOperation(PreparedEffectLaunch, store, attempt.Authority)
	op.run = func(ctx context.Context) (err error) {
		*launch, err = store.MarkExternalAttemptLaunched(ctx, attempt, now)
		return err
	}
	return op
}

type PreparedForkStore interface {
	IssueRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
	ClaimRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecution, string, time.Duration) (effects.Authority, error)
	RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
	StopSelectedFork(context.Context, runcontrol.SelectedStopRequest) (runcontrol.State, error)
}

func preparedForkOperation(kind PreparedOperationKind, store any, preparation runfork.SelectedForkPreparation) PreparedProcessOperation {
	return PreparedProcessOperation{kind: kind, target: store, generation: preparation.ProcessGeneration,
		coordinates: processbinding.LeaseCoordinates{LeaseID: preparation.Coordinates.ProcessAuthorityID, OwnerID: preparation.Coordinates.ProcessOwnerID, BootID: preparation.Coordinates.ProcessBootID}}
}

// R is the existing readiness-owned request, kept out of this foundational
// owner's import graph. The method and result remain exactly materialization;
// this is not an arbitrary operation or callback port.
func PreparedForkMaterializeOperation[R any](store interface {
	MaterializeRunForkForSelectedContractExecution(context.Context, R) (runfork.RunForkMaterialization, error)
}, request R, preparation runfork.SelectedForkPreparation, result *runfork.RunForkMaterialization) PreparedProcessOperation {
	if store == nil || result == nil {
		return PreparedProcessOperation{}
	}
	op := preparedForkOperation(PreparedForkMaterialize, store, preparation)
	op.run = func(ctx context.Context) (err error) {
		*result, err = store.MaterializeRunForkForSelectedContractExecution(ctx, request)
		return err
	}
	return op
}

func PreparedForkIssueOperation(store PreparedForkStore, request runfork.SelectedContractRuntimeExecutionIssueRequest, result *runfork.SelectedContractRuntimeExecution) PreparedProcessOperation {
	if store == nil || result == nil {
		return PreparedProcessOperation{}
	}
	op := preparedForkOperation(PreparedForkIssue, store, request.Preparation.SelectedForkPreparation)
	op.run = func(ctx context.Context) (err error) {
		*result, err = store.IssueRunForkSelectedContractRuntimeExecution(ctx, request)
		return err
	}
	return op
}

func PreparedForkClaimOperation(store PreparedForkStore, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration, result *effects.Authority) PreparedProcessOperation {
	if store == nil || result == nil {
		return PreparedProcessOperation{}
	}
	// Claim decodes and proves the exact persisted preparation inside its own
	// transaction. Admission still pins the actual current process for its whole
	// outcome; a matching historical binding cannot survive process Release.
	op := PreparedProcessOperation{kind: PreparedForkClaim, target: store}
	op.run = func(ctx context.Context) (err error) {
		*result, err = store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, owner, lease)
		return err
	}
	return op
}

func PreparedForkRecoverOperation(store PreparedForkStore, request runcontrol.SelectedForkRecoveryRequest, result *runfork.SelectedForkRecoveryResult) PreparedProcessOperation {
	if store == nil || result == nil {
		return PreparedProcessOperation{}
	}
	op := PreparedProcessOperation{kind: PreparedForkRecover, target: store, stamp: &request.Process}
	op.run = func(ctx context.Context) (err error) {
		*result, err = store.RecoverSelectedFork(ctx, request)
		return err
	}
	return op
}

func PreparedForkStopOperation(store PreparedForkStore, request runcontrol.SelectedStopRequest, result *runcontrol.State) PreparedProcessOperation {
	if store == nil || result == nil {
		return PreparedProcessOperation{}
	}
	op := PreparedProcessOperation{kind: PreparedForkStop, target: store, stamp: &request.Process}
	op.run = func(ctx context.Context) (err error) {
		*result, err = store.StopSelectedFork(ctx, request)
		return err
	}
	return op
}
