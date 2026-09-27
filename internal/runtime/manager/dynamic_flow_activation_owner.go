package manager

import (
	"context"
	"errors"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// AdoptSelectedFlowActivation transfers a committed selected attempt from the
// outer preparation owner to the Manager that will receive its deliveries.
func (am *AgentManager) AdoptSelectedFlowActivation(attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, publication runtimebus.FlowRoutePublicationHandle, timersProjected bool) error {
	if am == nil || am.lifecycle == nil || publication == nil {
		return errors.New("selected flow activation requires a manager and exact route publication")
	}
	if err := attempt.Validate(); err != nil {
		return err
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
		receipt: attempt, publication: publication, timersProjected: timersProjected, complete: true,
	}
	return nil
}

type flowActivationRetirementDisposition uint8

const (
	flowActivationProcessRetirement flowActivationRetirementDisposition = iota + 1
	flowActivationTerminalRetirement
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
) (*dynamicFlowActiveAttempt, bool, error) {
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
	if previous != nil {
		if previous.retiring {
			am.dynamicFlowReadinessMu.Unlock()
			return nil, false, errors.New("dynamic flow activation predecessor retirement is incomplete")
		}
		if previous.retirementKind == 0 && previous.receipt.PlanRevision() == revision && previous.receipt.ProcessBinding().Equal(binding) {
			am.dynamicFlowReadinessMu.Unlock()
			return previous, false, nil
		}
	}
	am.dynamicFlowReadinessMu.Unlock()
	if previous != nil {
		lease, err := am.beginWork(ctx, "flow activation predecessor retirement")
		if err != nil {
			return nil, false, err
		}
		retirement := &preparedFlowTopologyRetirement{manager: am, lease: lease, attempt: previous.receipt}
		if err := am.settleDynamicFlowActiveAttempt(ctx, key, previous, retirement, flowActivationProcessRetirement); err != nil {
			return nil, false, fmt.Errorf("settle flow activation predecessor: %w", err)
		}
	}
	admitted, commitErr := am.workflowInstances.BeginDynamicFlowRuntimeActivation(ctx, plan, revision, binding)
	if !admitted.Acknowledged {
		return nil, false, errors.Join(commitErr, errors.New("flow activation admission was not acknowledged"))
	}
	if err := admitted.Attempt.Validate(); err != nil {
		return nil, false, errors.Join(commitErr, err)
	}
	if admitted.Reused {
		return nil, false, errors.Join(commitErr, errors.New("committed flow activation attempt has no retained process owner"))
	}
	active := &dynamicFlowActiveAttempt{receipt: admitted.Attempt}
	am.dynamicFlowReadinessMu.Lock()
	if am.dynamicFlowActiveAttempts == nil {
		am.dynamicFlowActiveAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowActiveAttempt)
	}
	if am.dynamicFlowActiveAttempts[key] != nil {
		am.dynamicFlowReadinessMu.Unlock()
		return nil, false, errors.Join(commitErr, errors.New("flow activation local owner changed after durable admission"))
	}
	am.dynamicFlowActiveAttempts[key] = active
	am.dynamicFlowReadinessMu.Unlock()
	return active, true, commitErr
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
	retirement.publication = active.publication
	retirement.attempt = active.receipt
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
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return err
	}
	if !locallyRetired {
		if previousSet == nil {
			var retireErr error
			switch disposition {
			case flowActivationProcessRetirement:
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
			if disposition != flowActivationProcessRetirement {
				return errors.New("failed terminal flow retirement requires process replacement")
			}
			if err := am.retireFlowRouteAttempt(active.receipt, active.publication); err != nil {
				return fmt.Errorf("retry exact flow route retirement: %w", err)
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
	if err := am.workflowInstances.RetireDynamicFlowRuntimeActivationAttempt(ctx, active.receipt); err != nil {
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

// A joined manager generation no longer owns executable flow work. Release
// process projections before making its exact durable attempt reusable.
func (am *AgentManager) retireDynamicFlowAttemptsAfterJoin(ctx context.Context) error {
	am.dynamicFlowReadinessMu.Lock()
	attempts := make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowActiveAttempt, len(am.dynamicFlowActiveAttempts))
	for key, active := range am.dynamicFlowActiveAttempts {
		attempts[key] = active
	}
	am.dynamicFlowReadinessMu.Unlock()
	var result error
	for key, active := range attempts {
		am.dynamicFlowReadinessMu.Lock()
		incomplete := active.retiring || (active.retirementKind != 0 && !active.locallyRetired)
		am.dynamicFlowReadinessMu.Unlock()
		if incomplete {
			result = errors.Join(result, fmt.Errorf("flow activation attempt %s retains unsettled local retirement", key.instancePath))
			continue
		}
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if err := am.retireFlowRouteAttempt(active.receipt, active.publication); err != nil {
			result = errors.Join(result, fmt.Errorf("retire flow route publication %s: %w", key.instancePath, err))
			continue
		}
		if active.timersProjected {
			if err := am.workflowInstances.RetireInitialEntryTimerWakeups(ctx, identity); err != nil {
				result = errors.Join(result, fmt.Errorf("retire flow timer wakeups %s: %w", key.instancePath, err))
				continue
			}
		}
		if err := am.workflowInstances.RetireDynamicFlowRuntimeActivationAttempt(ctx, active.receipt); err != nil {
			result = errors.Join(result, fmt.Errorf("retire flow activation attempt %s: %w", key.instancePath, err))
			continue
		}
		am.dynamicFlowReadinessMu.Lock()
		if am.dynamicFlowActiveAttempts[key] == active {
			delete(am.dynamicFlowActiveAttempts, key)
		}
		am.dynamicFlowReadinessMu.Unlock()
	}
	return result
}
