package manager

// The selected-store row owns phase progress. This reconciler consumes exact attempt receipts; process resources are reconciled by their physical owners.

import (
	"context"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
	"time"
)

type dynamicFlowRuntimeReadinessFinalizationError struct {
	cause error
}

func (e *dynamicFlowRuntimeReadinessFinalizationError) Error() string {
	if e == nil || e.cause == nil {
		return "dynamic flow runtime readiness finalization failed"
	}
	return "dynamic flow runtime readiness finalization failed: " + e.cause.Error()
}

func (e *dynamicFlowRuntimeReadinessFinalizationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func IsDynamicFlowRuntimeReadinessFinalizationError(err error) bool {
	var target *dynamicFlowRuntimeReadinessFinalizationError
	return errors.As(err, &target)
}

type dynamicFlowRuntimeReadinessAdmission struct {
	ctx                 context.Context
	key                 dynamicFlowRuntimeReadinessKey
	plan                runtimepipeline.DynamicFlowRuntimeReadinessPlan
	planHash            string
	attemptOrdinal      uint64
	observed            *runtimepipeline.DynamicFlowRuntimeReadiness
	source              dynamicFlowRuntimeReadinessSource
	processOnly         bool
	preAdmission        bool
	admittedPreRun      bool
	previouslyCompleted bool
	processPrepared     bool
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessOnce(
	admission dynamicFlowRuntimeReadinessAdmission,
) (retErr error) {
	am.dynamicFlowAttachmentMu.RLock()
	defer am.dynamicFlowAttachmentMu.RUnlock()
	ctx := admission.ctx
	var acknowledgedCleanupErr error
	var active *dynamicFlowActiveAttempt
	var retirement *preparedFlowTopologyRetirement
	activationAccepted, shutdownCancellation := false, false
	checkForward := func() error {
		err := ctx.Err()
		if err != nil {
			shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
		}
		return err
	}
	defer func() {
		if shutdownCancellation && active == nil && retirement != nil {
			am.dynamicFlowReadinessMu.Lock()
			active = am.dynamicFlowActiveAttempts[admission.key]
			am.dynamicFlowReadinessMu.Unlock()
		}
		var cleanupErr error
		if active != nil {
			am.dynamicFlowReadinessMu.Lock()
			complete := active.complete
			am.dynamicFlowReadinessMu.Unlock()
			if shutdownCancellation || (!activationAccepted && !complete) {
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dynamicFlowRuntimeReadinessCleanupTimeout)
				cleanupErr = am.settleDynamicFlowActiveAttempt(cleanupCtx, admission.key, active, retirement, flowActivationFailedRetirement)
				cancel()
			}
		}
		if retirement != nil {
			cleanupErr = errors.Join(cleanupErr, retirement.abort())
		}
		retErr = errors.Join(retErr, acknowledgedCleanupErr, cleanupErr)
		// Cancellation alone never proves completion: the exact admitted attempt
		// must have joined retirement, or forward admission must not have begun.
		if shutdownCancellation && acknowledgedCleanupErr == nil && cleanupErr == nil && am.ownsReadinessShutdownCancellation(ctx) {
			retErr = &readinessShutdownCancellation{cause: retErr, key: admission.key, ordinal: admission.attemptOrdinal, planHash: admission.planHash}
		}
	}()
	if err := am.requireRunExecutionOwnership(ctx, admission.key.runID); err != nil {
		return err
	}
	lease, err := am.beginWork(ctx, "readiness topology retirement")
	if err != nil {
		shutdownCancellation = am.cleanReadinessShutdownAdmissionRefusal(ctx, err)
		return err
	}
	retirement = &preparedFlowTopologyRetirement{manager: am, lease: lease}
	if err := checkForward(); err != nil {
		return err
	}
	key := admission.key
	source := admission.source.source
	if source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(
		key.runID,
		admission.plan.Identity.Route(),
	)
	if err != nil {
		return fmt.Errorf("resolve dynamic flow runtime identity %s: %w", key.instancePath, err)
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, flowIdentity.Route)
	if err != nil {
		shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
		return err
	}
	if !found {
		return errors.Join(fmt.Errorf("dynamic flow runtime readiness not found for %s", key.instancePath), am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement))
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if err := am.executionPosture.Admit(plan.ExecutionMode, "dynamic flow runtime readiness topology reconciliation"); err != nil {
		return err
	}
	if readiness.AttemptOrdinal != admission.attemptOrdinal || readiness.AttemptState != admission.observed.AttemptState {
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	if readiness.PlanHash != admission.planHash {
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, admission.source); err != nil {
		return err
	}
	if !readiness.Eligible() {
		disposition := flowActivationProcessRetirement
		if readiness.Terminal() {
			disposition = flowActivationTerminalRetirement
		}
		return am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, disposition)
	}
	if strings.TrimSpace(source.WorkflowVersion()) != plan.WorkflowVersion {
		return errors.Join(fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			readiness.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.WorkflowVersion()),
		), am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement))
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, flowIdentity)
	if err != nil {
		shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return errors.Join(fmt.Errorf("dynamic flow runtime readiness %s persisted identity changed", readiness.InstancePath), am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement))
	}
	if err := checkForward(); err != nil {
		return err
	}
	scope, ok := semanticview.FlowScopeByID(source, plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow contract view not found: %s", plan.Identity.TemplateID)
	}
	schema, ok := source.FlowSchemaByID(plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow schema not found: %s", plan.Identity.TemplateID)
	}
	req := runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		Instance:       projection.Identity,
	}
	records, err := am.flowInstanceAgentRecords(plan.RunID, req, schema, scope)
	if err != nil {
		return err
	}
	records, selectedRootReadiness, err := am.flowReadinessOwnedAgentRecords(plan, source, records)
	if err != nil {
		return err
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Agents); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	if admission.preAdmission {
		if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
			return fmt.Errorf("standing flow topology preparation requires a stopped manager")
		}
		topologyAuthority, err := DynamicFlowAgentTopologyAdmission(plan)
		if err != nil {
			return err
		}
		topologyAuthority, err = topologyAuthority.WithFlowPreparationAttempt(admission.attemptOrdinal)
		if err != nil {
			return err
		}
		persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, flowIdentity)
		if err != nil {
			return fmt.Errorf("load standing flow agents for %s: %w", readiness.InstancePath, err)
		}
		if err := am.reconcileDynamicFlowAgentSet(ctx, source, flowIdentity, records, persistedAgents, topologyAuthority, false, selectedRootReadiness); err != nil {
			return fmt.Errorf("prepare standing flow agents for %s: %w", readiness.InstancePath, err)
		}
		if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
			return fmt.Errorf("standing flow topology preparation crossed manager run admission")
		}
		return nil
	}
	if err := checkForward(); err != nil {
		return err
	}
	var created bool
	var beginErr error
	active, created, beginErr = am.beginDynamicFlowActiveAttempt(ctx, key, plan, admission.attemptOrdinal, readiness.AttemptState)
	if active == nil {
		shutdownCancellation = am.cleanReadinessShutdownAdmissionRefusal(ctx, beginErr)
		return beginErr
	}
	phase := readiness.Phase
	if created {
		phase = runtimepipeline.FlowAttachmentPlanned
	}
	if beginErr != nil {
		if !created && am.cleanReadinessShutdownReadCancellation(ctx, beginErr) {
			shutdownCancellation = true
			return beginErr
		}
		acknowledgedCleanupErr = errors.Join(acknowledgedCleanupErr, fmt.Errorf("admit dynamic flow activation %s: %w", readiness.InstancePath, beginErr))
	}
	advance := func(previous runtimepipeline.FlowAttachmentPhase) (bool, error) {
		if err := checkForward(); err != nil {
			return false, err
		}
		result, err := am.workflowInstances.AdvanceFlowAttachment(ctx, active.receipt, previous, time.Now().UTC())
		if !result.Acknowledged {
			if am.cleanReadinessShutdownCancellation(ctx, result, err) {
				shutdownCancellation = true
				return false, err
			}
			return false, errors.Join(err, errors.New("attachment progress was not acknowledged"))
		}
		if !result.Admitted() {
			disposition := flowActivationFailedRetirement
			if result.Terminal {
				disposition = flowActivationTerminalRetirement
			}
			cleanupErr := am.settleDynamicFlowActiveAttempt(context.WithoutCancel(ctx), key, active, retirement, disposition)
			activationAccepted = true
			if result.Progress == runtimepipeline.FlowAttachmentStale {
				err = errors.Join(err, errDynamicFlowRuntimeReadinessPlanStale)
			}
			return false, errors.Join(err, cleanupErr)
		}
		phase = result.Phase
		if err != nil {
			acknowledgedCleanupErr = errors.Join(acknowledgedCleanupErr, err)
		}
		return true, nil
	}
	advancePending := func(previous runtimepipeline.FlowAttachmentPhase) (bool, error) {
		if err := checkForward(); err != nil {
			return false, err
		}
		next, err := previous.Next()
		if err != nil {
			return false, err
		}
		if phase.Includes(next) {
			return true, nil
		}
		return advance(previous)
	}
	if err := checkForward(); err != nil {
		return err
	}
	topologyAuthority, err := DynamicFlowAgentTopologyAdmission(plan)
	if err != nil {
		return fmt.Errorf("authorize dynamic flow agent topology for %s: %w", readiness.InstancePath, err)
	}
	topologyAuthority, err = topologyAuthority.WithFlowActivationAttempt(active.receipt.ID())
	if err != nil {
		return fmt.Errorf("bind dynamic flow agent topology to activation attempt for %s: %w", readiness.InstancePath, err)
	}
	if !admission.processPrepared && (admission.admittedPreRun || admission.processOnly || !phase.Includes(runtimepipeline.FlowAttachmentAgentsRegistered)) {
		persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, flowIdentity)
		if err != nil {
			shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
			return fmt.Errorf("load dynamic flow agents for %s: %w", readiness.InstancePath, err)
		}
		if err := checkForward(); err != nil {
			return err
		}
		if err := am.reconcileDynamicFlowAgentSet(ctx, source, flowIdentity, records, persistedAgents, topologyAuthority, true, selectedRootReadiness); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent set for %s: %w", readiness.InstancePath, err)
		}
	}
	if err := checkForward(); err != nil {
		return err
	}
	if err := am.verifyDynamicFlowAgents(ctx, flowIdentity, records, topologyAuthority, selectedRootReadiness); err != nil {
		shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
		return fmt.Errorf("verify dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	if eligible, err := advancePending(runtimepipeline.FlowAttachmentPlanned); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if admission.admittedPreRun {
		if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
			return errors.New("admitted flow preparation crossed manager run admission")
		}
		am.dynamicFlowReadinessMu.Lock()
		active.admittedPreRun = true
		active.preRunPredecessorOrdinal = admission.observed.AttemptOrdinal
		am.dynamicFlowReadinessMu.Unlock()
		activationAccepted = true
		return nil
	}
	// route_installed records attachment progress, not mutable route membership.
	if err := checkForward(); err != nil {
		return err
	}
	if err := am.workflowInstances.VerifyDynamicFlowRuntimeActivationAttempt(ctx, active.receipt); err != nil {
		shutdownCancellation = am.cleanReadinessShutdownReadCancellation(ctx, err)
		return err
	}
	if eligible, err := advancePending(runtimepipeline.FlowAttachmentAgentsRegistered); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if admission.processOnly {
		activationAccepted = true
		return nil
	}
	if !phase.Includes(runtimepipeline.FlowAttachmentTimersArmed) {
		if err := checkForward(); err != nil {
			return err
		}
		am.dynamicFlowReadinessMu.Lock()
		active.timersProjected = true
		am.dynamicFlowReadinessMu.Unlock()
		if err := am.workflowInstances.ReconcileInitialEntryTimersForAttempt(ctx, flowIdentity, active.receipt, plan); err != nil {
			shutdownCancellation = am.cleanReadinessShutdownForwardCancellation(ctx, err)
			return fmt.Errorf("reconcile initial workflow timers for %s: %w", readiness.InstancePath, err)
		}
	}
	if eligible, err := advancePending(runtimepipeline.FlowAttachmentRouteInstalled); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if eligible, err := advancePending(runtimepipeline.FlowAttachmentTimersArmed); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if plan.CreationEvent == nil || !readiness.CreationEventEmittedAt.IsZero() {
		activationAccepted = true
		am.dynamicFlowReadinessMu.Lock()
		active.complete = true
		am.dynamicFlowReadinessMu.Unlock()
		return nil
	}
	if err := checkForward(); err != nil {
		return err
	}
	evt, err := dynamicFlowRuntimeCreationEvent(source, plan)
	if err != nil {
		return err
	}
	publisher := am.roles.CreationPublisher
	if publisher == nil {
		return fmt.Errorf("dynamic flow creation occurrence requires transactional event publisher")
	}
	creationCtx := events.WithDeliveryContext(ctx, plan.CreationEvent.DeliveryContext)
	dispatchMode := runtimepipeline.DynamicFlowRuntimeCreationDispatchAsync
	if admission.processPrepared {
		// Only authorized startup-pending finalization uses this phase. Let
		// recovery dispatch after this readiness attempt has finished.
		dispatchMode = runtimepipeline.DynamicFlowRuntimeCreationDispatchStartupRecovery
	}
	if err := publisher.CommitDynamicFlowRuntimeCreationOccurrence(
		creationCtx,
		runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest{
			RunID:        plan.RunID,
			InstancePath: readiness.InstancePath,
			Plan:         plan,
			Attempt:      active.receipt,
			Event:        evt,
			OccurredAt:   time.Now().UTC(),
			DispatchMode: dispatchMode,
		},
	); err != nil {
		if eligible, progressErr := advance(runtimepipeline.FlowAttachmentTimersArmed); progressErr != nil {
			// Progress cancellation cannot attest the publication's COMMIT outcome.
			shutdownCancellation = false
			return errors.Join(err, progressErr)
		} else if !eligible {
			return nil
		}
		return fmt.Errorf("commit dynamic flow creation occurrence %s: %w", readiness.InstancePath, err)
	}
	activationAccepted = true
	am.dynamicFlowReadinessMu.Lock()
	active.complete = true
	am.dynamicFlowReadinessMu.Unlock()
	return nil
}
