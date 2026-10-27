package manager

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// ParkLiveRun joins process-local attempts without terminalizing the retained
// instances. Its release ends the existing attachment exclusion.
func (am *AgentManager) ParkLiveRun(ctx context.Context, runID string) (_ func(), result error) {
	if am == nil || runID == "" {
		return nil, errors.New("run handoff requires its Manager and exact run")
	}
	am.dynamicFlowRetirementMu.Lock()
	am.dynamicFlowAttachmentMu.Lock()
	release := sync.OnceFunc(func() {
		am.dynamicFlowAttachmentMu.Unlock()
		am.dynamicFlowRetirementMu.Unlock()
	})
	defer func() {
		if result != nil {
			release()
		}
	}()
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
			return nil, err
		}
		retirement := &preparedFlowTopologyRetirement{manager: am, lease: lease}
		if err := am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement); err != nil {
			return nil, errors.Join(err, retirement.abort())
		}
	}
	return release, nil
}

func (am *AgentManager) ReconcileLiveRun(ctx context.Context, runID string) error {
	if am == nil || runID == "" {
		return errors.New("live reconciliation requires its exact Manager and run")
	}
	ctx = correlation.WithRunID(ctx, runID)
	if err := am.ReconcileDynamicFlowRuntimeReadinessPlansForRun(ctx, time.Now().UTC()); err != nil {
		return err
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
