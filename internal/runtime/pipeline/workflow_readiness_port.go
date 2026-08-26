package pipeline

import (
	"context"
	"fmt"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func (s *workflowInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	if s == nil || s.readiness == nil {
		return false, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, plan, observedAt)
}

func (s *workflowInstanceStore) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func (s *workflowInstanceStore) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.readiness == nil {
		return nil, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.ListDynamicFlowRuntimeReadiness(ctx)
}

func (s *workflowInstanceStore) InspectDynamicFlowRuntimeStartupProjection(ctx context.Context, source runtimecorrelation.BundleSourceFact) (DynamicFlowRuntimeStartupProjection, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeStartupProjection{}, fmt.Errorf("dynamic flow runtime readiness persistence is required")
	}
	return s.readiness.InspectDynamicFlowRuntimeStartupProjection(ctx, source)
}

func (s *workflowInstanceStore) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, readyAt)
}
