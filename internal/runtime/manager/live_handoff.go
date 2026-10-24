package manager

import (
	"context"
	"errors"
	"sort"
)

// WithParkedRun joins process-local attempts without terminalizing the retained
// instances. The existing attachment exclusion spans the caller's transaction.
func (am *AgentManager) WithParkedRun(ctx context.Context, runID string, commit func() error) (result error) {
	if am == nil || runID == "" || commit == nil {
		return errors.New("run handoff requires its Manager, exact run and commit owner")
	}
	am.dynamicFlowRetirementMu.Lock()
	defer am.dynamicFlowRetirementMu.Unlock()
	am.dynamicFlowAttachmentMu.Lock()
	defer am.dynamicFlowAttachmentMu.Unlock()
	am.dynamicFlowReadinessMu.Lock()
	var keys []dynamicFlowRuntimeReadinessKey
	for key := range am.dynamicFlowActiveAttempts {
		if key.runID == runID {
			keys = append(keys, key)
		}
	}
	am.dynamicFlowReadinessMu.Unlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].instancePath < keys[j].instancePath })
	for _, key := range keys {
		lease, err := am.beginWork(ctx, "park live flow for handoff")
		if err != nil {
			return err
		}
		retirement := &preparedFlowTopologyRetirement{manager: am, lease: lease}
		if err := am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement); err != nil {
			return errors.Join(err, retirement.abort())
		}
	}
	return commit()
}

func (am *AgentManager) ReconcileLiveRun(ctx context.Context, runID string) error {
	if am == nil || runID == "" {
		return errors.New("live reconciliation requires its exact Manager and run")
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	projection, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, source.fact)
	if err != nil {
		return err
	}
	var result error
	for _, item := range append(projection.CurrentPending, projection.CurrentCompleted...) {
		if item.Plan.RunID == runID && item.Eligible() {
			result = errors.Join(result, am.reconcileDynamicFlowRuntimeReadiness(ctx, runID, item.InstancePath))
		}
	}
	return result
}
