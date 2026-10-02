package manager

// The selected-store row owns phase progress. This reconciler consumes exact attempt receipts; process resources are reconciled by their physical owners.

import (
	"context"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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
	topologyDurable     bool
	processOnly         bool
	preAdmission        bool
	admittedPreRun      bool
	previouslyCompleted bool
	processPrepared     bool
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessOnce(
	admission dynamicFlowRuntimeReadinessAdmission,
) (retErr error) {
	ctx := admission.ctx
	var acknowledgedCleanupErr error
	var cleanupCancels []context.CancelFunc
	defer func() {
		for _, cancel := range cleanupCancels {
			cancel()
		}
	}()
	defer func() { retErr = errors.Join(retErr, acknowledgedCleanupErr) }()
	if err := am.requireRunExecutionOwnership(ctx, admission.key.runID); err != nil {
		return err
	}
	lease, err := am.beginWork(ctx, "readiness topology retirement")
	if err != nil {
		return err
	}
	retirement := &preparedFlowTopologyRetirement{manager: am, lease: lease}
	defer func() { retErr = errors.Join(retErr, retirement.abort()) }()
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
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return errors.Join(fmt.Errorf("dynamic flow runtime readiness %s persisted identity changed", readiness.InstancePath), am.retireCurrentDynamicFlowActiveAttempt(ctx, key, retirement, flowActivationProcessRetirement))
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
		Config:         projection.Config,
	}
	records, err := am.flowInstanceAgentRecords(plan.RunID, req, schema, scope)
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
		if err := am.reconcileDynamicFlowAgentSet(ctx, source, flowIdentity, records, persistedAgents, topologyAuthority, false); err != nil {
			return fmt.Errorf("prepare standing flow agents for %s: %w", readiness.InstancePath, err)
		}
		if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
			return fmt.Errorf("standing flow topology preparation crossed manager run admission")
		}
		return nil
	}
	active, created, beginErr := am.beginDynamicFlowActiveAttempt(ctx, key, plan, admission.attemptOrdinal, readiness.AttemptState)
	if active == nil {
		return beginErr
	}
	if err := ctx.Err(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dynamicFlowRuntimeReadinessCleanupTimeout)
		defer cancel()
		return errors.Join(beginErr, err, am.settleDynamicFlowActiveAttempt(cleanupCtx, key, active, retirement, flowActivationFailedRetirement))
	}
	phase := readiness.Phase
	if created {
		phase = runtimepipeline.FlowAttachmentPlanned
	}
	if beginErr != nil {
		acknowledgedCleanupErr = errors.Join(acknowledgedCleanupErr, fmt.Errorf("admit dynamic flow activation %s: %w", readiness.InstancePath, beginErr))
		followupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dynamicFlowRuntimeReadinessCleanupTimeout)
		defer cancel()
		ctx = followupCtx
	}
	activationAccepted := false
	advance := func(previous runtimepipeline.FlowAttachmentPhase) (bool, error) {
		result, err := am.workflowInstances.AdvanceFlowAttachment(ctx, active.receipt, previous, time.Now().UTC())
		if !result.Acknowledged {
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
			followupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dynamicFlowRuntimeReadinessCleanupTimeout)
			cleanupCancels = append(cleanupCancels, cancel)
			ctx = followupCtx
		}
		return true, nil
	}
	advancePending := func(previous runtimepipeline.FlowAttachmentPhase) (bool, error) {
		next, err := previous.Next()
		if err != nil {
			return false, err
		}
		if phase.Includes(next) {
			return true, nil
		}
		return advance(previous)
	}
	defer func() {
		am.dynamicFlowReadinessMu.Lock()
		complete := active.complete
		am.dynamicFlowReadinessMu.Unlock()
		if activationAccepted || complete {
			return
		}
		retErr = errors.Join(retErr, am.settleDynamicFlowActiveAttempt(context.WithoutCancel(ctx), key, active, retirement, flowActivationFailedRetirement))
	}()
	topologyAuthority, err := DynamicFlowAgentTopologyAdmission(plan)
	if err != nil {
		return fmt.Errorf("authorize dynamic flow agent topology for %s: %w", readiness.InstancePath, err)
	}
	topologyAuthority, err = topologyAuthority.WithFlowActivationAttempt(active.receipt.ID())
	if err != nil {
		return fmt.Errorf("bind dynamic flow agent topology to activation attempt for %s: %w", readiness.InstancePath, err)
	}
	if !admission.processPrepared && !phase.Includes(runtimepipeline.FlowAttachmentAgentsRegistered) {
		persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, flowIdentity)
		if err != nil {
			return fmt.Errorf("load dynamic flow agents for %s: %w", readiness.InstancePath, err)
		}
		if err := am.reconcileDynamicFlowAgentSet(ctx, source, flowIdentity, records, persistedAgents, topologyAuthority, true); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent set for %s: %w", readiness.InstancePath, err)
		}
	}
	if err := am.verifyDynamicFlowAgents(ctx, flowIdentity, records, topologyAuthority); err != nil {
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
	if !admission.processPrepared && !phase.Includes(runtimepipeline.FlowAttachmentRouteInstalled) {
		flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, req.Instance.Route())
		if err != nil {
			return fmt.Errorf("resolve dynamic flow route identity %s: %w", readiness.InstancePath, err)
		}
		if !admission.topologyDurable {
			committed, commitErr := am.installFlowInstanceRoute(ctx, flowIdentity, req)
			if !committed.Acknowledged {
				return fmt.Errorf("persist dynamic flow route %s: %w", readiness.InstancePath, errors.Join(commitErr, errors.New("flow-instance route topology commit was not acknowledged")))
			}
			if commitErr != nil {
				acknowledgedCleanupErr = errors.Join(acknowledgedCleanupErr, fmt.Errorf("persist dynamic flow route %s: %w", readiness.InstancePath, commitErr))
				followupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dynamicFlowRuntimeReadinessCleanupTimeout)
				defer cancel()
				ctx = followupCtx
			}
		}
		publication, err := am.publishPersistedDynamicFlowRoute(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
			Identity:            flowIdentity,
			ActivationVariables: flowActivationVars(req),
		}, active.receipt)
		if err != nil {
			return fmt.Errorf("publish dynamic flow route %s: %w", readiness.InstancePath, err)
		}
		am.dynamicFlowReadinessMu.Lock()
		active.publication = publication
		am.dynamicFlowReadinessMu.Unlock()
	}
	if err := am.verifyDynamicFlowRoute(ctx, flowIdentity); err != nil {
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
		am.dynamicFlowReadinessMu.Lock()
		active.timersProjected = true
		am.dynamicFlowReadinessMu.Unlock()
		if err := am.workflowInstances.ReconcileInitialEntryTimersForAttempt(ctx, flowIdentity, active.receipt, plan); err != nil {
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
	evt, err := dynamicFlowRuntimeCreationEvent(plan)
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
