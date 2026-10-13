package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// SelectedDecisionCardControl joins one control turn to the child's existing
// execution occurrence. Native consumers still reprove the grant in their TX.
type SelectedDecisionCardControl struct {
	ctx      context.Context
	pipeline *pipeline.PipelineCoordinator
	lease    *worklifetime.Lease
	cancel   func()
	once     sync.Once
	err      error
}

func (u *SelectedDecisionCardControl) WorkContext() context.Context { return u.ctx }

func (u *SelectedDecisionCardControl) Close() error {
	u.once.Do(func() {
		u.cancel()
		u.err = u.lease.Done()
	})
	return u.err
}

func (o SelectedContractExecutionOwner) AcquireSelectedDecisionCardControl(ctx context.Context, runID string, operation runfork.SelectedControl) (*SelectedDecisionCardControl, bool, error) {
	if ctx == nil {
		return nil, false, fmt.Errorf("decision control requires its caller")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !operation.IsDecisionCardControl() {
		return nil, false, fmt.Errorf("decision control requires a mailbox mutation")
	}
	ports, err := o.require()
	if err != nil {
		return nil, false, err
	}
	binding, exists, err := ports.fork.LoadRunForkSelectedContractBinding(ctx, runID)
	if err != nil {
		return nil, exists, err
	}
	if !exists {
		admission, _ := managedexecution.FromContext(ctx)
		return nil, false, runfork.RequireDecisionControlForBinding(runID, operation, binding, false, admission)
	}
	use, err := o.acquireDecisionControl(ctx, binding, operation)
	return use, true, err
}

// A control turn consumes the child's retained execution; source selection or
// a historical binding cannot create another executor or an approval.
func (o SelectedContractExecutionOwner) CommitSelectedDecisionCardMutation(ctx context.Context, runID string, request apiidempotency.Request, mutation pipeline.DecisionCardMutation) (response json.RawMessage, replayed, selected bool, finalErr error) {
	if err := mutation.ValidateRequest(request); err != nil {
		return nil, false, true, err
	}
	use, selected, err := o.AcquireSelectedDecisionCardControl(ctx, runID, runfork.SelectedControl(request.Method))
	if err != nil || !selected {
		return nil, false, selected, err
	}
	released := false
	defer func() {
		if !released {
			finalErr = errors.Join(finalErr, use.Close())
		}
	}()
	response, replayed, err = use.pipeline.CommitDecisionCardMutation(use.ctx, request, mutation)
	cleanupErr := use.Close()
	released = true
	return response, replayed, true, errors.Join(err, cleanupErr)
}

func (o SelectedContractExecutionOwner) acquireDecisionControl(caller context.Context, binding runfork.RunForkSelectedContractBinding, operation runfork.SelectedControl) (*SelectedDecisionCardControl, error) {
	contexts := o.ports.contexts
	if contexts == nil {
		return nil, fmt.Errorf("selected decision control lacks its reconciled execution owner")
	}
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || !contexts.recovered {
		return nil, worklifetime.ErrRetired
	}
	var runtime *selectedContractAgentRuntime
	found := false
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID != binding.ForkRunID {
			continue
		}
		if found || entry.retiring || entry.closed || entry.binding.BindingID != binding.BindingID || entry.retained == nil {
			return nil, fmt.Errorf("selected decision control requires one current retained fork")
		}
		found = true
		runtime = entry.retained.retainedRuntime
	}
	if runtime == nil {
		return nil, fmt.Errorf("selected decision control lacks its retained executor")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	owned, coordinator := runtime.controlContext, runtime.timerLifecycle
	if owned == nil || coordinator == nil {
		return nil, fmt.Errorf("selected decision control is not ready")
	}
	admission, _ := managedexecution.FromContext(owned)
	if err := runfork.RequireDecisionControlForBinding(binding.ForkRunID, operation, binding, true, admission); err != nil {
		return nil, err
	}
	occurrence, present := worklifetime.OccurrenceFromContext(owned)
	selectedOccurrence, selected := occurrence.(*worklifetime.SelectedForkOccurrence)
	if !present || !selected || selectedOccurrence.Identity() != (worklifetime.SelectedForkIdentity{
		ExecutionID: admission.ExecutionAuthorityID, RunID: admission.RunID, Generation: admission.Generation,
	}) {
		return nil, fmt.Errorf("selected decision control lacks its existing work occurrence")
	}
	ctx, releaseCaller := bindSelectedDecisionCaller(caller, owned)
	lease, err := occurrence.Begin(ctx)
	if err != nil {
		releaseCaller()
		return nil, err
	}
	return &SelectedDecisionCardControl{ctx: lease.Context(), pipeline: coordinator, lease: lease, cancel: releaseCaller}, nil
}

func bindSelectedDecisionCaller(caller, owned context.Context) (context.Context, func()) {
	ctx := owned
	releaseDeadline := func() {}
	if deadline, bounded := caller.Deadline(); bounded {
		ctx, releaseDeadline = context.WithDeadline(ctx, deadline)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	joined := make(chan struct{})
	stop := context.AfterFunc(caller, func() {
		defer close(joined)
		cancel(context.Cause(caller))
	})
	if err := caller.Err(); err != nil {
		cancel(context.Cause(caller))
	}
	return ctx, func() {
		if !stop() {
			<-joined
		}
		cancel(context.Canceled)
		releaseDeadline()
	}
}
