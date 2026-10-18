package startupownership

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
		if err := p.requirePreparedSurfaceOriginal(ctx); err != nil {
			return err
		}
		return store.SaveManagedCapabilitySurface(owned, surface)
	}
	return op, nil
}

func (p *processCapability) requirePreparedSurfaceOriginal(ctx context.Context) error {
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
