package pipeline

import (
	"context"
	"fmt"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
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

func (s *workflowInstanceStore) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.SourceArtifactFact) (DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("dynamic flow runtime readiness persistence is required")
	}
	return s.readiness.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
}

func (s *workflowInstanceStore) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.readiness == nil {
		return nil, fmt.Errorf("dynamic flow runtime readiness persistence is required")
	}
	return s.readiness.InspectDynamicFlowRuntimeReadinessForRun(ctx, runID, source)
}

func (s *workflowInstanceStore) BeginDynamicFlowRuntimeActivation(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, revision uint64, binding runtimeprocessbinding.Binding) (DynamicFlowRuntimeActivationAdmissionResult, error) {
	if s == nil || s.readiness == nil {
		return DynamicFlowRuntimeActivationAdmissionResult{}, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.BeginDynamicFlowRuntimeActivation(ctx, plan, revision, binding)
}

func (s *workflowInstanceStore) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt DynamicFlowRuntimeActivationAttempt) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt)
}

func (s *workflowInstanceStore) AdvanceFlowAttachment(ctx context.Context, attempt DynamicFlowRuntimeActivationAttempt, previous FlowAttachmentPhase, at time.Time) (FlowAttachmentAdvanceResult, error) {
	if s == nil || s.readiness == nil {
		return FlowAttachmentAdvanceResult{}, fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.AdvanceFlowAttachment(ctx, attempt, previous, at)
}

func (s *workflowInstanceStore) RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt DynamicFlowRuntimeActivationAttempt) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.RetireDynamicFlowRuntimeActivationAttempt(ctx, attempt)
}

func (s *workflowInstanceStore) RetireDynamicFlowRuntimeActivationAttempts(ctx context.Context, attempts []DynamicFlowRuntimeActivationAttempt) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.RetireDynamicFlowRuntimeActivationAttempts(ctx, attempts)
}

func (s *workflowInstanceStore) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt DynamicFlowRuntimeActivationAttempt) error {
	if s == nil || s.readiness == nil {
		return fmt.Errorf("dynamic flow runtime readiness owner is required")
	}
	return s.readiness.AbandonDynamicFlowRuntimeActivationAttempt(ctx, attempt)
}
