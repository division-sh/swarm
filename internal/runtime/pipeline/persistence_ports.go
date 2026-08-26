package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type FlowInstanceTerminationRequest struct {
	Route        runtimeflowidentity.Route
	EntityID     identity.EntityID
	RunID        string
	TerminatedAt time.Time
}

type FlowInstanceTermination struct {
	Instance WorkflowInstance
	Route    runtimeflowidentity.Route
}

type StandingFlowInstanceOwner interface {
	ReconcileDynamicFlowRuntimeReadinessPlansForRun(context.Context, time.Time) error
	EnsureFlowInstance(context.Context, FlowInstanceActivationRequest) (bool, error)
}

type StandingTargetMutation struct {
	Candidate  StandingServiceCandidate
	Activation FlowInstanceActivationRequest
}

type StandingTargetMutationRequest struct {
	Previous   []StandingServiceCandidate
	Targets    []StandingTargetMutation
	Replace    bool
	ObservedAt time.Time
}

type StandingTargetMutationResult struct {
	Reconciliation      StandingServiceReconciliation
	Created             bool
	PublicationSequence int64
}

// CommitDynamicFlowRuntimeReadinessReconciliation owns the selected
// transaction for one run-scoped readiness reconciliation.
func (pc *PipelineCoordinator) CommitDynamicFlowRuntimeReadinessReconciliation(
	ctx context.Context,
	observedAt time.Time,
	owner StandingFlowInstanceOwner,
) error {
	if pc == nil || pc.workflowStore == nil {
		return fmt.Errorf("dynamic flow readiness reconciliation requires workflow persistence")
	}
	if owner == nil {
		return fmt.Errorf("dynamic flow readiness reconciliation requires flow instance owner")
	}
	return owner.ReconcileDynamicFlowRuntimeReadinessPlansForRun(ctx, observedAt)
}

func (pc *PipelineCoordinator) CommitStandingTargets(ctx context.Context, req StandingTargetMutationRequest, owner StandingFlowInstanceOwner) ([]StandingTargetMutationResult, error) {
	if pc == nil || pc.workflowStore == nil {
		return nil, fmt.Errorf("standing target mutation requires workflow persistence")
	}
	if owner == nil {
		return nil, fmt.Errorf("standing target mutation requires flow instance owner")
	}
	observedAt := req.ObservedAt.UTC()
	if observedAt.IsZero() {
		return nil, fmt.Errorf("standing target mutation requires observed_at")
	}
	if req.Replace {
		candidates := make([]StandingServiceCandidate, 0, len(req.Targets))
		for _, target := range req.Targets {
			candidates = append(candidates, target.Candidate)
		}
		if _, err := pc.workflowStore.ReconcileStandingServiceReplacement(ctx, req.Previous, candidates); err != nil {
			return nil, err
		}
	}
	results := make([]StandingTargetMutationResult, 0, len(req.Targets))
	for _, target := range req.Targets {
		reconciliation, found, err := pc.workflowStore.LoadReconciledStandingService(ctx, target.Candidate)
		if err != nil {
			return nil, err
		}
		if !found {
			reconciliation, err = pc.workflowStore.ReconcileStandingService(ctx, target.Candidate)
			if err != nil {
				return nil, err
			}
		}
		result := StandingTargetMutationResult{Reconciliation: reconciliation, PublicationSequence: reconciliation.PublicationSequence}
		if reconciliation.EffectiveState == "active" {
			if err := pc.workflowStore.AdmitStandingServiceRun(ctx, reconciliation.RunID, pc.executionPosture); err != nil {
				return nil, err
			}
			operationCtx := runtimecorrelation.WithRunID(ctx, reconciliation.RunID)
			operationCtx = runtimecorrelation.WithBundleSourceFact(operationCtx, target.Candidate.Source)
			operationCtx = runtimeeffects.WithExecutionMode(operationCtx, runtimeeffects.ExecutionMode(pc.executionPosture.RootMode()))
			if err := owner.ReconcileDynamicFlowRuntimeReadinessPlansForRun(operationCtx, observedAt); err != nil {
				return nil, err
			}
			activation := target.Activation
			activation.StandingGenerationReplacement = reconciliation.Generation > 1
			created, err := owner.EnsureFlowInstance(operationCtx, activation)
			if err != nil {
				return nil, err
			}
			result.Created = created
			result.PublicationSequence, err = pc.workflowStore.PublishStandingService(operationCtx, reconciliation.ServiceID, reconciliation.RunID, reconciliation.Generation)
			if err != nil {
				return nil, err
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func (pc *PipelineCoordinator) CommitFlowInstanceTermination(ctx context.Context, req FlowInstanceTerminationRequest) (FlowInstanceTermination, error) {
	if pc == nil || pc.workflowStore == nil {
		return FlowInstanceTermination{}, fmt.Errorf("flow instance termination requires workflow persistence")
	}
	route := runtimeflowidentity.StoredRoute(req.Route.ScopeKey, req.Route.InstanceID, req.Route.InstancePath)
	runID := strings.TrimSpace(req.RunID)
	entityID := identity.NormalizeEntityID(req.EntityID.String())
	if !route.Valid() || entityID.IsZero() || runID == "" {
		return FlowInstanceTermination{}, fmt.Errorf("flow instance termination requires an exact route, entity_id, and run_id")
	}
	instance, err := pc.commitWorkflowTermination(runtimecorrelation.WithRunID(ctx, runID), route, entityID, req.TerminatedAt, true)
	if err != nil {
		return FlowInstanceTermination{}, err
	}
	canonicalRef := strings.TrimSpace(instance.StorageRef)
	canonicalRoute := runtimeflowidentity.RouteForInstancePath(canonicalRef)
	if !canonicalRoute.Valid() {
		return FlowInstanceTermination{}, fmt.Errorf("derive canonical route identity for flow path %s", canonicalRef)
	}
	return FlowInstanceTermination{Instance: instance, Route: canonicalRoute}, nil
}

func (pc *PipelineCoordinator) Load(ctx context.Context, route runtimeflowidentity.Route) (WorkflowInstance, bool, error) {
	return pc.workflowStore.Load(ctx, route)
}

func (pc *PipelineCoordinator) ListWorkflowInstances(ctx context.Context) ([]WorkflowInstance, error) {
	return pc.workflowStore.list(ctx)
}

func (pc *PipelineCoordinator) StartActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	return pc.workflowStore.StartActivityAttempt(ctx, record)
}

func (pc *PipelineCoordinator) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	return pc.workflowStore.ClaimActivityAttemptForLoopGeneration(ctx, record)
}

func (pc *PipelineCoordinator) CompleteActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	return pc.workflowStore.CompleteActivityAttempt(ctx, record)
}

func (pc *PipelineCoordinator) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	return pc.workflowStore.MarkActivityAttemptUncertain(ctx, record)
}

func (pc *PipelineCoordinator) LoadActivityAttempt(ctx context.Context, requestEventID string) (ActivityAttemptRecord, bool, error) {
	return pc.workflowStore.LoadActivityAttempt(ctx, requestEventID)
}

func (pc *PipelineCoordinator) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	return pc.workflowStore.RequireGateRouteAdmitted(ctx, runID)
}

func (pc *PipelineCoordinator) LoadRouteRecoveryProjection(ctx context.Context, route runtimeflowidentity.Route) (WorkflowInstanceRouteRecoveryProjection, error) {
	return pc.workflowStore.LoadRouteRecoveryProjection(ctx, route)
}

func (pc *PipelineCoordinator) MaterializeInitialEntry(ctx context.Context, instance WorkflowInstance, occurredAt time.Time) (WorkflowInitialMaterializationResult, error) {
	return pc.workflowStore.MaterializeInitialEntry(ctx, instance, occurredAt)
}

func (pc *PipelineCoordinator) PrepareInitialEntryLifecycle(ctx context.Context, instance WorkflowInstance, occurredAt time.Time) (WorkflowInstance, WorkflowLifecycleMutationPlan, error) {
	if pc == nil || pc.workflowStore == nil {
		return WorkflowInstance{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow instance lifecycle store is required")
	}
	normalized, _, lifecycle, err := pc.workflowStore.prepareInitialEntryLifecycle(ctx, instance, occurredAt)
	return normalized, lifecycle, err
}

func (pc *PipelineCoordinator) FinalizeInitialEntryLifecycle(ctx context.Context, committed CommittedWorkflowLifecycleMutation) error {
	if pc == nil || pc.workflowStore == nil {
		return fmt.Errorf("workflow instance lifecycle store is required")
	}
	return pc.workflowStore.finalizeInitialEntryLifecycle(ctx, committed)
}

func (pc *PipelineCoordinator) ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.ArmInitialEntryTimers(ctx, route)
}

func (pc *PipelineCoordinator) ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.ReconcileInitialEntryTimers(ctx, route)
}

func (pc *PipelineCoordinator) RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.RetireInitialEntryTimerWakeups(ctx, route)
}

func (pc *PipelineCoordinator) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, observed DynamicFlowRuntimeReadiness, plan DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	return pc.workflowStore.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, observed, plan, observedAt)
}

func (pc *PipelineCoordinator) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (DynamicFlowRuntimeReadiness, bool, error) {
	return pc.workflowStore.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func (pc *PipelineCoordinator) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.BundleSourceFact) (DynamicFlowRuntimeReadinessProjection, error) {
	return pc.workflowStore.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
}

func (pc *PipelineCoordinator) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source runtimecorrelation.BundleSourceFact) ([]DynamicFlowRuntimeReadiness, error) {
	return pc.workflowStore.InspectDynamicFlowRuntimeReadinessForRun(ctx, runID, source)
}

func (pc *PipelineCoordinator) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	return pc.workflowStore.MarkDynamicFlowRuntimeTopologyReady(ctx, expected, readyAt)
}

func (pc *PipelineCoordinator) MarkTerminated(ctx context.Context, route runtimeflowidentity.Route, entityID identity.EntityID, terminatedAt time.Time) error {
	_, err := pc.commitWorkflowTermination(ctx, route, entityID, terminatedAt, false)
	return err
}

func (pc *PipelineCoordinator) CommitDecision(ctx context.Context, card decisioncard.Card, decision string, decidedAt time.Time) error {
	return pc.workflowStore.CommitDecision(ctx, card, decision, decidedAt)
}

func (pc *PipelineCoordinator) ReconcileStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	result, err := pc.workflowStore.ReconcileStandingService(ctx, candidate)
	if err == nil {
		err = pc.reconcileTimerCancellations(ctx, result.TimerCancellations)
	}
	return result, err
}

func (pc *PipelineCoordinator) LoadReconciledStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, bool, error) {
	return pc.workflowStore.LoadReconciledStandingService(ctx, candidate)
}

func (pc *PipelineCoordinator) ReconcileStandingServiceSet(ctx context.Context, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	results, err := pc.workflowStore.ReconcileStandingServiceSet(ctx, candidates)
	if err == nil {
		for _, result := range results {
			if err = pc.reconcileTimerCancellations(ctx, result.TimerCancellations); err != nil {
				break
			}
		}
	}
	return results, err
}

func (pc *PipelineCoordinator) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	results, err := pc.workflowStore.ReconcileStandingServiceReplacement(ctx, previous, candidates)
	if err == nil {
		for _, result := range results {
			if err = pc.reconcileTimerCancellations(ctx, result.TimerCancellations); err != nil {
				break
			}
		}
	}
	return results, err
}

func (pc *PipelineCoordinator) SuspendStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	result, err := pc.workflowStore.SuspendStandingService(ctx, op)
	if err == nil {
		err = pc.reconcileTimerCancellations(ctx, result.TimerCancellations)
	}
	return result, err
}

func (pc *PipelineCoordinator) ResumeStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	op.ExecutionPosture = pc.executionPosture
	return pc.workflowStore.ResumeStandingService(ctx, op)
}

func (pc *PipelineCoordinator) ResetStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	op.ExecutionPosture = pc.executionPosture
	result, err := pc.workflowStore.ResetStandingService(ctx, op)
	if err == nil {
		err = pc.reconcileTimerCancellations(ctx, result.TimerCancellations)
	}
	return result, err
}

func (pc *PipelineCoordinator) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	return pc.workflowStore.PublishStandingService(ctx, serviceID, runID, generation)
}

func (pc *PipelineCoordinator) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	return pc.workflowStore.StandingRunUsesIntrinsicRecovery(ctx, runID)
}

func (pc *PipelineCoordinator) ListStandingServiceStatuses(ctx context.Context) ([]StandingServiceStatus, error) {
	return pc.workflowStore.ListStandingServiceStatuses(ctx)
}

func (pc *PipelineCoordinator) RegisterDeliveryContinuationSignal(authority runtimedelivery.ExecutionAuthority, signal func()) (*DeliveryContinuationSignalRegistration, error) {
	return pc.workflowStore.RegisterDeliveryContinuationSignal(authority, signal)
}

func (pc *PipelineCoordinator) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	return pc.workflowStore.ReadTimerObligations(ctx, scope, observedAt)
}
