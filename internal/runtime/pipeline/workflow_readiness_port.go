package pipeline

import (
	"context"
	"fmt"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func (s *workflowInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlans(ctx context.Context, requests []DynamicFlowRuntimeReadinessPlanReconciliation, observedAt time.Time) ([]DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	if s == nil || s.readiness == nil {
		return nil, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, observedAt)
}

func (s *workflowInstanceStore) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func (s *workflowInstanceStore) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.BundleSourceFact) (DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("dynamic flow runtime readiness persistence is required")
	}
	return s.readiness.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
}

func (s *workflowInstanceStore) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source runtimecorrelation.BundleSourceFact) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.readiness == nil {
		return nil, fmt.Errorf("dynamic flow runtime readiness persistence is required")
	}
	return s.readiness.InspectDynamicFlowRuntimeReadinessForRun(ctx, runID, source)
}

func (s *workflowInstanceStore) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, readyAt)
}
