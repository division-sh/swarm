package pipeline

import (
	"context"
	"fmt"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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

func (s *workflowInstanceStore) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]DynamicFlowRuntimeReadinessKey, error) {
	if s == nil || s.readiness == nil {
		return nil, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.ListDynamicFlowRuntimeReadinessKeys(ctx)
}

func (s *workflowInstanceStore) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, readyAt)
}
