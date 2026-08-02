package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type FlowInstanceTerminationRequest struct {
	StorageRef   string
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
	return pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
		return owner.ReconcileDynamicFlowRuntimeReadinessPlansForRun(txctx, observedAt)
	})
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
	results := make([]StandingTargetMutationResult, 0, len(req.Targets))
	err := pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
		if req.Replace {
			candidates := make([]StandingServiceCandidate, 0, len(req.Targets))
			for _, target := range req.Targets {
				candidates = append(candidates, target.Candidate)
			}
			if _, err := pc.workflowStore.ReconcileStandingServiceReplacement(txctx, req.Previous, candidates); err != nil {
				return err
			}
		}
		for _, target := range req.Targets {
			reconciliation, found, err := pc.workflowStore.LoadReconciledStandingService(txctx, target.Candidate)
			if err != nil {
				return err
			}
			if !found {
				reconciliation, err = pc.workflowStore.ReconcileStandingService(txctx, target.Candidate)
				if err != nil {
					return err
				}
			}
			result := StandingTargetMutationResult{Reconciliation: reconciliation, PublicationSequence: reconciliation.PublicationSequence}
			if reconciliation.EffectiveState == "active" {
				operationCtx := runtimecorrelation.WithRunID(txctx, reconciliation.RunID)
				operationCtx = runtimecorrelation.WithBundleSourceFact(operationCtx, target.Candidate.Source)
				operationCtx = runtimeeffects.WithExecutionMode(operationCtx, executionmode.Live)
				if reconciliation.Generation > 1 {
					operationCtx = WithStandingGenerationRebind(operationCtx)
				}
				if err := owner.ReconcileDynamicFlowRuntimeReadinessPlansForRun(operationCtx, observedAt); err != nil {
					return err
				}
				created, err := owner.EnsureFlowInstance(operationCtx, target.Activation)
				if err != nil {
					return err
				}
				result.Created = created
				result.PublicationSequence, err = pc.workflowStore.PublishStandingService(operationCtx, reconciliation.ServiceID, reconciliation.RunID, reconciliation.Generation)
				if err != nil {
					return err
				}
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

func (pc *PipelineCoordinator) CommitFlowInstanceTermination(ctx context.Context, req FlowInstanceTerminationRequest) (FlowInstanceTermination, error) {
	if pc == nil || pc.workflowStore == nil {
		return FlowInstanceTermination{}, fmt.Errorf("flow instance termination requires workflow persistence")
	}
	storageRef := strings.TrimSpace(req.StorageRef)
	runID := strings.TrimSpace(req.RunID)
	if storageRef == "" || runID == "" {
		return FlowInstanceTermination{}, fmt.Errorf("flow instance termination requires storage_ref and run_id")
	}
	var result FlowInstanceTermination
	err := pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
		if err := pc.workflowStore.MarkTerminated(txctx, storageRef, req.TerminatedAt); err != nil {
			return err
		}
		route, err := workflowInstanceRouteForPath(storageRef)
		if err != nil {
			return err
		}
		instance, ok, err := pc.workflowStore.Load(txctx, route)
		if err != nil {
			return err
		}
		if !ok || strings.TrimSpace(instance.Status) != "terminated" || instance.TerminatedAt.IsZero() {
			return fmt.Errorf("canonical terminal flow instance %s was not persisted", storageRef)
		}
		canonicalRef := strings.TrimSpace(instance.StorageRef)
		canonicalRoute := runtimeflowidentity.RouteForInstancePath(canonicalRef)
		if !canonicalRoute.Valid() {
			return fmt.Errorf("derive canonical route identity for flow path %s", canonicalRef)
		}
		if pc.flowRoutes == nil {
			return fmt.Errorf("flow instance termination requires route topology owner")
		}
		txctx = runtimecorrelation.WithRunID(txctx, runID)
		if err := pc.flowRoutes.RemoveFlowInstanceRouteContext(txctx, canonicalRoute); err != nil {
			return err
		}
		result = FlowInstanceTermination{Instance: instance, Route: canonicalRoute}
		return nil
	})
	return result, err
}

func (pc *PipelineCoordinator) Load(ctx context.Context, route runtimeflowidentity.Route) (WorkflowInstance, bool, error) {
	return pc.workflowStore.Load(ctx, route)
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

func (pc *PipelineCoordinator) ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.ArmInitialEntryTimers(ctx, route)
}

func (pc *PipelineCoordinator) ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.ReconcileInitialEntryTimers(ctx, route)
}

func (pc *PipelineCoordinator) RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error {
	return pc.workflowStore.RetireInitialEntryTimerWakeups(ctx, route)
}

func (pc *PipelineCoordinator) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, plan DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	return pc.workflowStore.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, plan, observedAt)
}

func (pc *PipelineCoordinator) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID, instanceID string) (DynamicFlowRuntimeReadiness, bool, error) {
	return pc.workflowStore.LoadDynamicFlowRuntimeReadiness(ctx, runID, instanceID)
}

func (pc *PipelineCoordinator) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	return pc.workflowStore.ListDynamicFlowRuntimeReadiness(ctx)
}

func (pc *PipelineCoordinator) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]DynamicFlowRuntimeReadinessKey, error) {
	return pc.workflowStore.ListDynamicFlowRuntimeReadinessKeys(ctx)
}

func (pc *PipelineCoordinator) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	return pc.workflowStore.MarkDynamicFlowRuntimeTopologyReady(ctx, expected, readyAt)
}

func (pc *PipelineCoordinator) CommitDynamicFlowRuntimeCreationOccurrence(ctx context.Context, req DynamicFlowRuntimeCreationOccurrenceRequest, publisher DynamicFlowRuntimeCreationOccurrencePublisher) error {
	return pc.workflowStore.CommitDynamicFlowRuntimeCreationOccurrence(ctx, req, publisher)
}

func (pc *PipelineCoordinator) MarkTerminated(ctx context.Context, storageRef string, terminatedAt time.Time) error {
	return pc.workflowStore.MarkTerminated(ctx, storageRef, terminatedAt)
}

func (pc *PipelineCoordinator) CommitDecision(ctx context.Context, card decisioncard.Card, decision string, decidedAt time.Time) error {
	return pc.workflowStore.CommitDecision(ctx, card, decision, decidedAt)
}

func (pc *PipelineCoordinator) ReconcileStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	return pc.workflowStore.ReconcileStandingService(ctx, candidate)
}

func (pc *PipelineCoordinator) LoadReconciledStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, bool, error) {
	return pc.workflowStore.LoadReconciledStandingService(ctx, candidate)
}

func (pc *PipelineCoordinator) ReconcileStandingServiceSet(ctx context.Context, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	return pc.workflowStore.ReconcileStandingServiceSet(ctx, candidates)
}

func (pc *PipelineCoordinator) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	return pc.workflowStore.ReconcileStandingServiceReplacement(ctx, previous, candidates)
}

func (pc *PipelineCoordinator) SuspendStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	return pc.workflowStore.SuspendStandingService(ctx, op)
}

func (pc *PipelineCoordinator) ResumeStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	return pc.workflowStore.ResumeStandingService(ctx, op)
}

func (pc *PipelineCoordinator) ResetStandingService(ctx context.Context, op StandingServiceOperation) (StandingServiceReconciliation, error) {
	return pc.workflowStore.ResetStandingService(ctx, op)
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
