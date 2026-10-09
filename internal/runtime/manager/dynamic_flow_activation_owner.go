package manager

import (
	"context"
	"errors"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// AdoptSelectedFlowActivation transfers a committed selected attempt from the
// outer preparation owner to the Manager that will receive its deliveries.
func (am *AgentManager) AdoptSelectedFlowActivation(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, timersProjected bool) error {
	if am == nil || am.lifecycle == nil {
		return errors.New("selected flow activation requires its exact Manager owner")
	}
	if err := attempt.Validate(); err != nil {
		return err
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.RunID != attempt.RunID() || identity.Route.InstancePath != attempt.InstancePath() {
		return errors.New("selected flow identity differs from activation attempt")
	}
	am.lifecycle.mu.Lock()
	provider, ok := am.lifecycle.store.(processExecutionBindingProvider)
	am.lifecycle.mu.Unlock()
	if !ok {
		return errors.New("selected flow activation requires an exact generation grant")
	}
	binding, err := provider.ProcessExecutionBinding()
	if err != nil {
		return err
	}
	if !attempt.ProcessBinding().Equal(binding) {
		return errors.New("selected flow activation differs from Manager generation grant")
	}
	if am.workflowInstances == nil {
		return errors.New("selected flow activation requires its durable attempt owner")
	}
	if err := am.workflowInstances.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
		return err
	}
	key := dynamicFlowRuntimeReadinessKey{runID: attempt.RunID(), instancePath: attempt.InstancePath()}
	am.dynamicFlowReadinessMu.Lock()
	defer am.dynamicFlowReadinessMu.Unlock()
	if am.dynamicFlowActiveAttempts == nil {
		am.dynamicFlowActiveAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowActiveAttempt)
	}
	if am.dynamicFlowActiveAttempts[key] != nil {
		return errors.New("selected flow activation already has a Manager owner")
	}
	am.dynamicFlowActiveAttempts[key] = &dynamicFlowActiveAttempt{
		receipt: attempt, identity: identity, timersProjected: timersProjected, complete: true,
	}
	return nil
}

type flowActivationRetirementDisposition uint8

const (
	flowActivationProcessRetirement flowActivationRetirementDisposition = iota + 1
	flowActivationTerminalRetirement
	flowActivationFailedRetirement
)

func (am *AgentManager) retireCurrentDynamicFlowActiveAttempt(ctx context.Context, key dynamicFlowRuntimeReadinessKey, retirement *preparedFlowTopologyRetirement, disposition flowActivationRetirementDisposition) error {
	am.dynamicFlowReadinessMu.Lock()
	active := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if active == nil {
		return nil
	}
	return am.settleDynamicFlowActiveAttempt(ctx, key, active, retirement, disposition)
}

func (am *AgentManager) beginDynamicFlowActiveAttempt(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	revision uint64,
	disposition string,
) (*dynamicFlowActiveAttempt, bool, error) {
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
	if err != nil {
		return nil, false, err
	}
	if identity.RunID != key.runID || identity.Route.InstancePath != key.instancePath {
		return nil, false, errors.New("flow activation identity differs from admitted key")
	}
	am.lifecycle.mu.Lock()
	provider, ok := am.lifecycle.store.(processExecutionBindingProvider)
	am.lifecycle.mu.Unlock()
	if !ok {
		return nil, false, errors.New("dynamic flow activation requires a selected-store process binding")
	}
	binding, err := provider.ProcessExecutionBinding()
	if err != nil {
		return nil, false, fmt.Errorf("dynamic flow activation process binding: %w", err)
	}
	if err := binding.Validate(); err != nil {
		return nil, false, err
	}
	if binding.BundleHash != plan.BundleHash {
		return nil, false, errors.New("dynamic flow activation plan differs from selected generation source")
	}
	am.dynamicFlowReadinessMu.Lock()
	previous := am.dynamicFlowActiveAttempts[key]
	previousDisposition := flowActivationRetirementDisposition(0)
	if previous != nil {
		previousDisposition = previous.retirementKind
		if previous.retiring {
			am.dynamicFlowReadinessMu.Unlock()
			return nil, false, errors.New("dynamic flow activation predecessor retirement is incomplete")
		}
		if previous.pending == nil && previous.retirementKind == 0 && previous.receipt.Ordinal() == revision && previous.receipt.ProcessBinding().Equal(binding) {
			am.dynamicFlowReadinessMu.Unlock()
			if err := am.workflowInstances.VerifyDynamicFlowRuntimeActivationAttempt(ctx, previous.receipt); err == nil {
				return previous, false, nil
			} else if !errors.Is(err, runtimepipeline.ErrFlowAttachmentStale) {
				return nil, false, err
			}
		} else {
			am.dynamicFlowReadinessMu.Unlock()
		}
	} else {
		am.dynamicFlowReadinessMu.Unlock()
	}
	if previous != nil {
		disposition := flowActivationProcessRetirement
		if previousDisposition != 0 {
			disposition = previousDisposition
		}
		lease, err := am.beginWork(ctx, "flow activation predecessor retirement")
		if err != nil {
			return nil, false, err
		}
		retirement := &preparedFlowTopologyRetirement{manager: am, lease: lease}
		if err := am.settleDynamicFlowActiveAttempt(ctx, key, previous, retirement, disposition); err != nil {
			return nil, false, fmt.Errorf("settle flow activation predecessor: %w", err)
		}
	}
	am.dynamicFlowReadinessMu.Lock()
	if previous != nil && previous.receipt.Validate() == nil {
		// Exact acknowledged settlement supersedes the earlier observation.
		disposition = "retired"
		if previous.retirementKind == flowActivationFailedRetirement {
			disposition = "aborted"
		}
	}
	am.dynamicFlowReadinessMu.Unlock()
	request := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan, revision, disposition, binding)
	if err := request.Validate(); err != nil {
		return nil, false, err
	}
	hash, err := plan.Hash()
	if err != nil {
		return nil, false, err
	}
	active := &dynamicFlowActiveAttempt{pending: &request, admissionDone: make(chan struct{}), planHash: hash, identity: identity, retirementKind: flowActivationFailedRetirement}
	am.dynamicFlowReadinessMu.Lock()
	if am.dynamicFlowActiveAttempts == nil {
		am.dynamicFlowActiveAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowActiveAttempt)
	}
	if am.dynamicFlowActiveAttempts[key] != nil {
		am.dynamicFlowReadinessMu.Unlock()
		return nil, false, errors.New("flow activation local owner changed before durable admission")
	}
	am.dynamicFlowActiveAttempts[key] = active
	am.dynamicFlowReadinessMu.Unlock()
	defer close(active.admissionDone)
	admitted, commitErr := am.workflowInstances.BeginDynamicFlowRuntimeActivation(ctx, request)
	if !admitted.Acknowledged {
		resolved, resolveErr := am.workflowInstances.ResolveDynamicFlowRuntimeActivation(ctx, request)
		commitErr = errors.Join(commitErr, resolveErr)
		if err := request.ValidateResolution(resolved); err != nil {
			return nil, false, errors.Join(commitErr, err)
		}
		switch resolved.Disposition {
		case runtimepipeline.FlowActivationAdmitted:
			admitted.Attempt = resolved.Attempt
		case runtimepipeline.FlowActivationUnadmitted, runtimepipeline.FlowActivationForeign:
			am.dynamicFlowReadinessMu.Lock()
			if am.dynamicFlowActiveAttempts[key] == active {
				delete(am.dynamicFlowActiveAttempts, key)
			}
			am.dynamicFlowReadinessMu.Unlock()
			return nil, false, errors.Join(commitErr, errors.New("flow activation request did not own a current admission"))
		default:
			return nil, false, errors.Join(commitErr, errors.New("flow activation admission remains unresolved"))
		}
	}
	if err := admitted.Attempt.Validate(); err != nil {
		return nil, false, errors.Join(commitErr, err)
	}
	if admitted.Attempt.RunID() != key.runID || admitted.Attempt.InstancePath() != key.instancePath || !admitted.Attempt.ProcessBinding().Equal(binding) {
		return nil, false, errors.Join(commitErr, errors.New("flow activation acknowledgment differs from its exact request"))
	}
	am.dynamicFlowReadinessMu.Lock()
	active.receipt, active.pending, active.retirementKind = admitted.Attempt, nil, 0
	am.dynamicFlowReadinessMu.Unlock()
	return active, true, commitErr
}

func (am *AgentManager) resolvePendingDynamicFlowActivation(ctx context.Context, key dynamicFlowRuntimeReadinessKey, active *dynamicFlowActiveAttempt) (bool, error) {
	am.dynamicFlowReadinessMu.Lock()
	pending, done := active.pending, active.admissionDone
	am.dynamicFlowReadinessMu.Unlock()
	if pending == nil {
		return true, nil
	}
	select {
	case <-done:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	resolved, err := am.workflowInstances.ResolveDynamicFlowRuntimeActivation(ctx, *pending)
	if validateErr := pending.ValidateResolution(resolved); validateErr != nil {
		return false, errors.Join(err, validateErr)
	}
	if resolved.Disposition == runtimepipeline.FlowActivationUnresolved {
		return false, errors.Join(err, errors.New("flow activation request remains unresolved"))
	}
	am.dynamicFlowReadinessMu.Lock()
	defer am.dynamicFlowReadinessMu.Unlock()
	if am.dynamicFlowActiveAttempts[key] != active {
		return false, errors.Join(err, errors.New("flow activation request lost its retained owner"))
	}
	if resolved.Disposition == runtimepipeline.FlowActivationAdmitted {
		if validateErr := resolved.Attempt.Validate(); validateErr != nil {
			return false, errors.Join(err, validateErr)
		}
		active.receipt, active.pending = resolved.Attempt, nil
		return true, err
	}
	delete(am.dynamicFlowActiveAttempts, key)
	return false, err
}

func (am *AgentManager) settleDynamicFlowActiveAttempt(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	active *dynamicFlowActiveAttempt,
	retirement *preparedFlowTopologyRetirement,
	disposition flowActivationRetirementDisposition,
) (result error) {
	if active == nil || retirement == nil {
		if retirement != nil {
			return errors.Join(errors.New("flow activation settlement requires exact local owner"), retirement.abort())
		}
		return errors.New("flow activation settlement requires exact local owner")
	}
	owned, resolveErr := am.resolvePendingDynamicFlowActivation(ctx, key, active)
	if resolveErr != nil || !owned {
		return errors.Join(resolveErr, retirement.abort())
	}
	am.dynamicFlowReadinessMu.Lock()
	if am.dynamicFlowActiveAttempts[key] != active || active.retiring {
		am.dynamicFlowReadinessMu.Unlock()
		return errors.Join(errors.New("flow activation settlement lost its exact local owner"), retirement.abort())
	}
	if active.retirementKind != 0 && active.retirementKind != disposition {
		am.dynamicFlowReadinessMu.Unlock()
		return errors.Join(errors.New("flow activation retirement disposition changed before settlement"), retirement.abort())
	}
	active.retiring = true
	active.retirementKind = disposition
	previousSet := active.retirementSet
	locallyRetired := active.locallyRetired
	timersRetired := active.timersRetired
	timersProjected := active.timersProjected
	am.dynamicFlowReadinessMu.Unlock()
	defer func() {
		result = errors.Join(result, retirement.abort())
		am.dynamicFlowReadinessMu.Lock()
		if am.dynamicFlowActiveAttempts[key] == active {
			active.retiring = false
		}
		am.dynamicFlowReadinessMu.Unlock()
	}()
	identity := active.identity
	if err := identity.Validate(); err != nil {
		return err
	}
	if !locallyRetired {
		if previousSet == nil {
			var retireErr error
			switch disposition {
			case flowActivationProcessRetirement, flowActivationFailedRetirement:
				retireErr = retirement.retire(identity)
			case flowActivationTerminalRetirement:
				retireErr = retirement.retireTerminal(identity)
			default:
				return errors.New("flow activation retirement requires an exact disposition")
			}
			retireErr = errors.Join(retireErr, retirement.wait(), retirement.abort())
			am.dynamicFlowReadinessMu.Lock()
			if retirement.set != nil {
				active.retirementSet = retirement.set
			}
			am.dynamicFlowReadinessMu.Unlock()
			if retireErr != nil {
				return retireErr
			}
		} else {
			if disposition != flowActivationProcessRetirement && disposition != flowActivationFailedRetirement {
				return errors.New("failed terminal flow retirement requires process replacement")
			}
			if err := am.lifecycle.retryProcessFlowRetirement(previousSet); err != nil {
				return fmt.Errorf("retry exact flow agent retirement: %w", err)
			}
		}
		if err := retirement.abort(); err != nil {
			return err
		}
		am.dynamicFlowReadinessMu.Lock()
		active.locallyRetired = true
		am.dynamicFlowReadinessMu.Unlock()
	}
	if !timersRetired {
		if timersProjected {
			if err := am.workflowInstances.RetireInitialEntryTimerWakeups(ctx, identity); err != nil {
				return fmt.Errorf("retire flow activation timer projections: %w", err)
			}
		}
		am.dynamicFlowReadinessMu.Lock()
		active.timersRetired = true
		am.dynamicFlowReadinessMu.Unlock()
	}
	if err := am.settleDynamicFlowAttemptDurably(ctx, active); err != nil {
		return fmt.Errorf("retire durable flow activation attempt: %w", err)
	}
	am.dynamicFlowReadinessMu.Lock()
	defer am.dynamicFlowReadinessMu.Unlock()
	if am.dynamicFlowActiveAttempts[key] != active {
		return errors.New("flow activation local owner changed during settlement")
	}
	delete(am.dynamicFlowActiveAttempts, key)
	return nil
}

func (am *AgentManager) settleDynamicFlowAttemptDurably(ctx context.Context, active *dynamicFlowActiveAttempt) error {
	if active.retirementKind == flowActivationFailedRetirement {
		return am.workflowInstances.AbandonDynamicFlowRuntimeActivationAttempt(ctx, active.receipt)
	}
	return am.workflowInstances.RetireDynamicFlowRuntimeActivationAttempt(ctx, active.receipt)
}

type joinedFlowRetirement struct {
	key    dynamicFlowRuntimeReadinessKey
	active *dynamicFlowActiveAttempt
}

// A joined manager generation no longer owns executable flow work. Release
// process projections before making its exact durable attempt reusable.
func (am *AgentManager) retireDynamicFlowAttemptsAfterJoin(ctx context.Context) error {
	am.dynamicFlowRetirementMu.Lock()
	defer am.dynamicFlowRetirementMu.Unlock()
	am.dynamicFlowReadinessMu.Lock()
	attempts := make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowActiveAttempt, len(am.dynamicFlowActiveAttempts))
	for key, active := range am.dynamicFlowActiveAttempts {
		attempts[key] = active
	}
	am.dynamicFlowReadinessMu.Unlock()
	var result error
	batch := make([]joinedFlowRetirement, 0, runtimepipeline.DynamicFlowRuntimeRetirementBatchLimit)
	finish := func(entries []joinedFlowRetirement, committed bool) {
		am.dynamicFlowReadinessMu.Lock()
		defer am.dynamicFlowReadinessMu.Unlock()
		for _, entry := range entries {
			if am.dynamicFlowActiveAttempts[entry.key] != entry.active {
				continue
			}
			if committed {
				delete(am.dynamicFlowActiveAttempts, entry.key)
			} else {
				entry.active.retiring = false
			}
		}
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		retireErr := am.retireJoinedDynamicFlowAttemptBatch(ctx, batch)
		result = errors.Join(result, retireErr)
		finish(batch, retireErr == nil)
		batch = batch[:0]
	}
	for key, active := range attempts {
		owned, resolveErr := am.resolvePendingDynamicFlowActivation(ctx, key, active)
		result = errors.Join(result, resolveErr)
		if resolveErr != nil || !owned {
			continue
		}
		am.dynamicFlowReadinessMu.Lock()
		current := am.dynamicFlowActiveAttempts[key] == active
		incomplete := active.retiring || (active.retirementKind == flowActivationTerminalRetirement && !active.locallyRetired)
		if current && !incomplete {
			active.retiring = true
			if active.retirementKind == 0 {
				active.retirementKind = flowActivationProcessRetirement
			}
		}
		am.dynamicFlowReadinessMu.Unlock()
		if !current {
			result = errors.Join(result, fmt.Errorf("flow activation attempt %s changed before joined retirement", key.instancePath))
			continue
		}
		if incomplete {
			result = errors.Join(result, fmt.Errorf("flow activation attempt %s retains unsettled local retirement", key.instancePath))
			continue
		}
		deferDurable := active.retirementKind != flowActivationFailedRetirement
		settleErr := am.settleDynamicFlowAttemptAfterJoin(ctx, key, active, deferDurable)
		result = errors.Join(result, settleErr)
		if settleErr == nil && deferDurable {
			batch = append(batch, joinedFlowRetirement{key: key, active: active})
			if len(batch) == runtimepipeline.DynamicFlowRuntimeRetirementBatchLimit {
				flush()
			}
			continue
		}
		finish([]joinedFlowRetirement{{key: key, active: active}}, settleErr == nil)
	}
	flush()
	return result
}

func (am *AgentManager) retireJoinedDynamicFlowAttemptBatch(ctx context.Context, entries []joinedFlowRetirement) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("retire joined flow activation batch: %v", recovered)
		}
	}()
	attempts := make([]runtimepipeline.DynamicFlowRuntimeActivationAttempt, 0, len(entries))
	for _, entry := range entries {
		attempts = append(attempts, entry.active.receipt)
	}
	return am.workflowInstances.RetireDynamicFlowRuntimeActivationAttempts(ctx, attempts)
}

func (am *AgentManager) settleDynamicFlowAttemptAfterJoin(ctx context.Context, key dynamicFlowRuntimeReadinessKey, active *dynamicFlowActiveAttempt, deferDurable bool) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("retire joined flow activation attempt %s: %v", key.instancePath, recovered)
		}
	}()
	if !active.locallyRetired {
		if active.retirementSet != nil {
			if err := am.lifecycle.retryProcessFlowRetirement(active.retirementSet); err != nil {
				return fmt.Errorf("retry flow agent retirement %s: %w", key.instancePath, err)
			}
		}
		am.dynamicFlowReadinessMu.Lock()
		active.locallyRetired = true
		am.dynamicFlowReadinessMu.Unlock()
	}
	if !active.timersRetired {
		if active.timersProjected {
			identity := active.identity
			if err := identity.Validate(); err != nil {
				return err
			}
			if err := am.workflowInstances.RetireInitialEntryTimerWakeups(ctx, identity); err != nil {
				return fmt.Errorf("retire flow timer wakeups %s: %w", key.instancePath, err)
			}
		}
		am.dynamicFlowReadinessMu.Lock()
		active.timersRetired = true
		am.dynamicFlowReadinessMu.Unlock()
	}
	if deferDurable {
		return nil
	}
	if err := am.settleDynamicFlowAttemptDurably(ctx, active); err != nil {
		return fmt.Errorf("retire flow activation attempt %s: %w", key.instancePath, err)
	}
	return nil
}
