package eventreceiver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

type ExecutionKind string

type executionContextKey struct{}

const (
	ExecutionNormal               ExecutionKind = "normal"
	ExecutionSelectedContractFork ExecutionKind = "selected_contract_fork"
)

// ExecutionVariant is the closed receiver-owned execution state admitted at
// an EventBus handoff. It deliberately has no generic value or service map.
type ExecutionVariant struct {
	configured bool
	kind       ExecutionKind
	authority  runtimeeffects.Authority
	admission  managedexecution.Admission
	controller *runtimeeffects.Controller
	lineage    runtimecorrelation.RuntimeLineage
}

func NormalExecution() ExecutionVariant {
	return ExecutionVariant{configured: true, kind: ExecutionNormal}
}

func SelectedContractForkExecution(
	authority runtimeeffects.Authority,
	admission managedexecution.Admission,
	controller *runtimeeffects.Controller,
	lineage runtimecorrelation.RuntimeLineage,
) (ExecutionVariant, error) {
	variant := ExecutionVariant{
		configured: true,
		kind:       ExecutionSelectedContractFork,
		authority:  authority,
		admission:  admission,
		controller: controller,
		lineage:    lineage.Normalized(),
	}
	if err := variant.Validate(); err != nil {
		return ExecutionVariant{}, err
	}
	return variant, nil
}

func (v ExecutionVariant) Configured() bool {
	return v.configured
}

func (v ExecutionVariant) Kind() ExecutionKind {
	return v.kind
}

// WithSelectedAdmission finalizes the explicit selected owner after managed
// provider preflight, before any receiver loop starts.
func (v ExecutionVariant) WithSelectedAdmission(admission managedexecution.Admission) (ExecutionVariant, error) {
	if v.Kind() != ExecutionSelectedContractFork {
		return ExecutionVariant{}, errors.New("only selected receiver execution can finalize selected admission")
	}
	v.admission = admission
	if err := v.Validate(); err != nil {
		return ExecutionVariant{}, err
	}
	return v, nil
}

func (v ExecutionVariant) Validate() error {
	if !v.Configured() {
		return errors.New("receiver execution owner is not configured")
	}
	switch v.Kind() {
	case ExecutionNormal:
		if v.authority.Valid() || v.admission.Validate() == nil || v.controller != nil {
			return errors.New("normal receiver execution cannot carry selected execution state")
		}
		return nil
	case ExecutionSelectedContractFork:
		if !v.authority.Valid() || v.authority.Kind != runtimeeffects.AuthoritySelectedContractFork {
			return errors.New("selected receiver execution requires selected-contract authority")
		}
		if !v.admission.AuthorizesSelected(v.authority.SelectedFork.ExecutionID, v.authority.SelectedFork.ForkRunID, v.authority.SelectedFork.Generation) {
			return errors.New("selected receiver execution admission conflicts with authority")
		}
		if v.controller == nil || !v.controller.Enabled() {
			return errors.New("selected receiver execution requires an effect controller")
		}
		if !v.authority.ExecutionMode.Valid() {
			return errors.New("selected receiver execution requires an execution mode")
		}
		return nil
	default:
		return fmt.Errorf("receiver execution kind %q is invalid", v.kind)
	}
}

// Bind installs only receiver-owned state. The event's causal mode is always
// canonical; selected authority identity is fixed while its mode is projected
// from each admitted event.
func (v ExecutionVariant) Bind(ctx context.Context, mode executionmode.Mode) (context.Context, error) {
	if !v.Configured() {
		return nil, errors.New("receiver execution owner is not configured")
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if !mode.Valid() {
		return nil, errors.New("receiver execution requires typed causal execution mode")
	}
	ctx = runtimeeffects.WithExecutionMode(ctx, mode)
	ctx = context.WithValue(ctx, executionContextKey{}, v.Kind())
	if v.Kind() == ExecutionNormal {
		return ctx, nil
	}
	authority := v.authorityForMode(mode)
	if hasRuntimeLineage(v.lineage) {
		ctx = runtimecorrelation.WithRuntimeLineage(ctx, v.lineage)
	}
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	ctx = runtimeeffects.WithController(ctx, v.controller)
	ctx = managedexecution.WithAdmission(ctx, v.admission)
	return ctx, nil
}

func hasRuntimeLineage(lineage runtimecorrelation.RuntimeLineage) bool {
	lineage = lineage.Normalized()
	return lineage.Owner != "" ||
		lineage.RunID != "" ||
		lineage.SubjectEventID != "" ||
		lineage.SubjectEventType != "" ||
		lineage.ParentEventID != "" ||
		lineage.RowCategory != "" ||
		lineage.SelectedForkOwner != "" ||
		lineage.Classification != "" ||
		lineage.SelectedForkContext
}

func (v ExecutionVariant) ValidateBound(ctx context.Context, mode executionmode.Mode) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if kind, ok := ctx.Value(executionContextKey{}).(ExecutionKind); !ok || kind != v.Kind() {
		return errors.New("receiver execution context is not bound to its owner")
	}
	boundMode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok || boundMode != mode {
		return fmt.Errorf("receiver execution mode = %q, want causal mode %q", boundMode, mode)
	}
	if v.Kind() == ExecutionNormal {
		if _, ok := runtimeeffects.AuthorityFromContext(ctx); ok {
			return errors.New("normal receiver inherited execution authority")
		}
		if _, ok := managedexecution.FromContext(ctx); ok {
			return errors.New("normal receiver inherited managed execution admission")
		}
		if _, ok := runtimeeffects.ControllerFromContext(ctx); ok {
			return errors.New("normal receiver inherited effect controller")
		}
		return nil
	}
	authority, authorityOK := runtimeeffects.AuthorityFromContext(ctx)
	admission, admissionOK := managedexecution.FromContext(ctx)
	controller, controllerOK := runtimeeffects.ControllerFromContext(ctx)
	if !authorityOK || !reflect.DeepEqual(authority, v.authorityForMode(mode)) {
		return errors.New("selected receiver authority is missing or conflicts with its owner")
	}
	if !admissionOK || !reflect.DeepEqual(admission, v.admission) {
		return errors.New("selected receiver admission is missing or conflicts with its owner")
	}
	if !controllerOK || controller != v.controller {
		return errors.New("selected receiver effect controller is missing or conflicts with its owner")
	}
	return nil
}

func (v ExecutionVariant) authorityForMode(mode executionmode.Mode) runtimeeffects.Authority {
	authority := v.authority
	authority.ExecutionMode = mode
	return authority
}

func IsBound(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	kind, ok := ctx.Value(executionContextKey{}).(ExecutionKind)
	return ok && (kind == ExecutionNormal || kind == ExecutionSelectedContractFork)
}

// NewContext mirrors only lifetime cancellation/deadline onto an empty value
// tree. The returned cleanup releases the cancellation bridge.
func NewContext(lifetime context.Context) (context.Context, func()) {
	if lifetime == nil {
		lifetime = context.Background()
	}
	base := context.Background()
	deadlineCancel := func() {}
	if deadline, ok := lifetime.Deadline(); ok {
		var cancel context.CancelFunc
		base, cancel = context.WithDeadline(base, deadline)
		deadlineCancel = cancel
	}
	ctx, cancel := context.WithCancelCause(base)
	stop := func() bool { return false }
	if err := lifetime.Err(); err != nil {
		cancel(context.Cause(lifetime))
	} else {
		stop = context.AfterFunc(lifetime, func() {
			cancel(context.Cause(lifetime))
		})
	}
	cleanup := sync.OnceFunc(func() {
		stop()
		cancel(context.Canceled)
		deadlineCancel()
	})
	return ctx, cleanup
}
