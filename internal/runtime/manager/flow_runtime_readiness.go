package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

type dynamicFlowRuntimeReadinessKey struct {
	runID        string
	instancePath string
}

type dynamicFlowRuntimeReadinessAttempt struct {
	done                chan struct{}
	err                 error
	planCoordinate      string
	successorRequired   bool
	successorContext    context.Context
	successorSource     semanticview.Source
	successorCoordinate string
}

var errDynamicFlowRuntimeReadinessPlanStale = errors.New(
	"dynamic flow runtime readiness declared plan is stale",
)

func newDynamicFlowRuntimeReadinessKey(runID, instancePath string) (dynamicFlowRuntimeReadinessKey, error) {
	key := dynamicFlowRuntimeReadinessKey{
		runID:        strings.TrimSpace(runID),
		instancePath: strings.Trim(strings.TrimSpace(instancePath), "/"),
	}
	if key.runID == "" || key.instancePath == "" {
		return dynamicFlowRuntimeReadinessKey{}, fmt.Errorf("dynamic flow runtime readiness requires exact run_id and instance_id")
	}
	return key, nil
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessForStartup(ctx context.Context) error {
	if am == nil || am.workflowInstances == nil {
		return nil
	}
	active, err := am.workflowInstances.ListDynamicFlowRuntimeReadiness(ctx)
	if err != nil {
		return err
	}
	activePaths := make(map[string]struct{}, len(active))
	for _, item := range active {
		activePaths[item.InstancePath] = struct{}{}
		if err := am.reconcileDynamicFlowRuntimeReadiness(ctx, item.Plan.RunID, item.InstancePath, am.semanticSource); err != nil {
			return fmt.Errorf("finalize dynamic flow runtime readiness %s: %w", item.InstancePath, err)
		}
	}
	keys, err := am.workflowInstances.ListDynamicFlowRuntimeReadinessKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, hasActiveSuccessor := activePaths[key.InstancePath]; hasActiveSuccessor {
			continue
		}
		if err := am.reconcileDynamicFlowRuntimeReadiness(ctx, key.RunID, key.InstancePath, am.semanticSource); err != nil {
			return fmt.Errorf("finalize dynamic flow runtime readiness %s: %w", key.InstancePath, err)
		}
	}
	return nil
}

func (am *AgentManager) reconcilePendingDynamicFlowRuntimeReadiness(
	ctx context.Context,
	source semanticview.Source,
) error {
	if am == nil || am.workflowInstances == nil {
		return nil
	}
	items, err := am.workflowInstances.ListDynamicFlowRuntimeReadiness(ctx)
	if err != nil {
		return err
	}
	var reconcileErrs []error
	for _, item := range items {
		if !item.Pending() {
			continue
		}
		if err := am.reconcileDynamicFlowRuntimeReadiness(ctx, item.Plan.RunID, item.InstancePath, source); err != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("%s: %w", item.InstancePath, err))
		}
	}
	return errors.Join(reconcileErrs...)
}

func (am *AgentManager) reconcileEnsuredDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
	runID string,
) (runtimepipeline.DynamicFlowRuntimeReadinessPlan, error) {
	templateID := strings.TrimSpace(req.Instance.TemplateID)
	scope, ok := semanticview.FlowScopeByID(req.ContractBundle, templateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow contract view not found: %s", templateID)
	}
	schema, ok := req.ContractBundle.FlowSchemaByID(templateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow schema not found: %s", templateID)
	}
	agentRecords, err := am.flowInstanceAgentRecords(req, schema, scope)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	occurredAt := req.OccurredAt.UTC()
	if !req.TriggerEvent.CreatedAt().IsZero() {
		occurredAt = req.TriggerEvent.CreatedAt().UTC()
	}
	if occurredAt.IsZero() {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow readiness reconciliation requires an exact occurrence time")
	}
	current, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, runID, req.Instance.InstancePath)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	if !found {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness not found for %s", req.Instance.InstancePath)
	}
	expected := current.Plan
	expected.Identity = req.Instance
	expected.WorkflowVersion = strings.TrimSpace(req.ContractBundle.WorkflowVersion())
	expected.Agents = make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(agentRecords))
	for _, record := range agentRecords {
		revision, err := lifecycleConfigRevision(record)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent revision %s: %w", record.Config.ID, err)
		}
		expected.Agents = append(expected.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			AgentID: record.Config.ID, ConfigRevision: revision,
		})
	}
	expected.CreationEvent, err = rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		current.Plan.CreationEvent,
		!current.CreationEventEmittedAt.IsZero(),
		req.ContractBundle,
		schema,
		req.Instance,
		req.Config,
	)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("rebuild dynamic flow creation plan %s: %w", req.Instance.InstancePath, err)
	}
	if _, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, expected, occurredAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("reconcile dynamic flow runtime readiness plan %s: %w", req.Instance.InstancePath, err)
	}
	return expected, nil
}

// ReconcileDynamicFlowRuntimeReadinessPlansForRun advances every active
// readiness owner in the selected run before revised routes are derived.
func (am *AgentManager) ReconcileDynamicFlowRuntimeReadinessPlansForRun(
	ctx context.Context,
	source semanticview.Source,
	observedAt time.Time,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	if source == nil || strings.TrimSpace(source.WorkflowVersion()) == "" {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact run_id")
	}
	observedAt = observedAt.UTC()
	if observedAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact occurrence time")
	}
	if _, transactional := runtimepipeline.PipelineSQLTxFromContext(ctx); !transactional {
		return fmt.Errorf("run-scoped dynamic flow runtime readiness reconciliation requires selected mutation")
	}
	items, err := am.workflowInstances.ListDynamicFlowRuntimeReadiness(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.Plan.RunID != runID {
			continue
		}
		projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, item.Plan.Identity.Route())
		if err != nil {
			return fmt.Errorf("load dynamic flow readiness projection %s: %w", item.InstancePath, err)
		}
		scope, ok := semanticview.FlowScopeByID(source, projection.Identity.TemplateID)
		if !ok {
			return fmt.Errorf("flow contract view not found: %s", projection.Identity.TemplateID)
		}
		schema, ok := source.FlowSchemaByID(projection.Identity.TemplateID)
		if !ok {
			return fmt.Errorf("flow schema not found: %s", projection.Identity.TemplateID)
		}
		records, err := am.flowInstanceAgentRecords(runtimepipeline.FlowInstanceActivationRequest{
			ContractBundle: source,
			Instance:       projection.Identity,
			Config:         projection.Config,
		}, schema, scope)
		if err != nil {
			return fmt.Errorf("derive dynamic flow readiness agents %s: %w", item.InstancePath, err)
		}
		expected := item.Plan
		expected.Identity = projection.Identity
		expected.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
		expected.Agents = make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(records))
		for _, record := range records {
			revision, err := lifecycleConfigRevision(record)
			if err != nil {
				return fmt.Errorf("derive dynamic flow agent revision %s: %w", record.Config.ID, err)
			}
			expected.Agents = append(expected.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
				AgentID: record.Config.ID, ConfigRevision: revision,
			})
		}
		expected.CreationEvent, err = rebuildPendingDynamicFlowRuntimeCreationEventPlan(
			item.Plan.CreationEvent,
			!item.CreationEventEmittedAt.IsZero(),
			source,
			schema,
			projection.Identity,
			projection.Config,
		)
		if err != nil {
			return fmt.Errorf("rebuild dynamic flow creation plan %s: %w", item.InstancePath, err)
		}
		changed, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, expected, observedAt)
		if err != nil {
			return fmt.Errorf("reconcile dynamic flow runtime readiness plan %s: %w", item.InstancePath, err)
		}
		if !changed {
			continue
		}
		reconcile := func(actionCtx context.Context) {
			postCommitCtx := runtimepipeline.WithoutPipelineSQLConnContext(
				runtimepipeline.WithoutPipelineSQLTxContext(actionCtx),
			)
			if err := am.reconcileDynamicFlowRuntimeReadinessPlan(postCommitCtx, expected, source); err != nil {
				am.signalDynamicFlowRuntimeReadiness()
			}
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, reconcile) {
			return fmt.Errorf("dynamic flow runtime readiness %s requires post-commit reconciliation owner", item.InstancePath)
		}
	}
	return nil
}

func rebuildPendingDynamicFlowRuntimeCreationEventPlan(
	current *runtimepipeline.DynamicFlowRuntimeCreationEventPlan,
	emitted bool,
	source semanticview.Source,
	schema runtimecontracts.FlowSchemaDocument,
	identity runtimeflowidentity.Instance,
	config map[string]any,
) (*runtimepipeline.DynamicFlowRuntimeCreationEventPlan, error) {
	if emitted {
		return current, nil
	}
	autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event)
	if current == nil {
		if autoEmit != "" {
			return nil, fmt.Errorf(
				"cannot introduce auto-emit %s without persisted trigger lineage",
				autoEmit,
			)
		}
		return nil, nil
	}
	return buildDynamicFlowRuntimeCreationEventPlan(
		source,
		schema,
		identity.TemplateID,
		identity.InstancePath,
		identity.EntityID,
		events.EventLineage{
			RunID:         current.RunID,
			ParentEventID: current.ParentEventID,
			ExecutionMode: current.ExecutionMode,
		},
		config,
		current.DeliveryContext,
		current.CreatedAt,
	)
}

func (am *AgentManager) signalDynamicFlowRuntimeReadiness() {
	if am == nil || am.dynamicFlowReadinessSignal == nil {
		return
	}
	select {
	case am.dynamicFlowReadinessSignal <- struct{}{}:
	default:
	}
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	instancePath string,
	source semanticview.Source,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	if source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	key, err := newDynamicFlowRuntimeReadinessKey(runID, instancePath)
	if err != nil {
		return err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
	if err != nil {
		return err
	}
	planCoordinate := "missing"
	if found {
		planCoordinate, err = dynamicFlowRuntimeReadinessPlanCoordinate(readiness.Plan)
		if err != nil {
			return err
		}
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(ctx, key, planCoordinate, source)
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	source semanticview.Source,
) error {
	normalized, err := plan.Normalized()
	if err != nil {
		return err
	}
	key, err := newDynamicFlowRuntimeReadinessKey(normalized.RunID, normalized.Identity.InstancePath)
	if err != nil {
		return err
	}
	planCoordinate, err := dynamicFlowRuntimeReadinessPlanCoordinate(normalized)
	if err != nil {
		return err
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(ctx, key, planCoordinate, source)
}

func (am *AgentManager) reconcileDeclaredDynamicFlowRuntimeReadiness(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	planCoordinate string,
	source semanticview.Source,
) error {
	am.dynamicFlowReadinessMu.Lock()
	currentCoordinate, err := am.loadDynamicFlowRuntimeReadinessPlanCoordinate(ctx, key)
	if err != nil {
		am.dynamicFlowReadinessMu.Unlock()
		return err
	}
	if currentCoordinate != planCoordinate {
		am.dynamicFlowReadinessMu.Unlock()
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	if am.dynamicFlowReadinessAttempts == nil {
		am.dynamicFlowReadinessAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt)
	}
	if attempt := am.dynamicFlowReadinessAttempts[key]; attempt != nil {
		if planCoordinate != attempt.planCoordinate {
			attempt.successorRequired = true
			attempt.successorContext = ctx
			attempt.successorSource = source
			attempt.successorCoordinate = planCoordinate
		}
		am.dynamicFlowReadinessMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &dynamicFlowRuntimeReadinessAttempt{
		done:           make(chan struct{}),
		planCoordinate: planCoordinate,
	}
	am.dynamicFlowReadinessAttempts[key] = attempt
	am.dynamicFlowReadinessMu.Unlock()

	currentContext := ctx
	currentSource := source
	for {
		attemptErr := am.reconcileDynamicFlowRuntimeReadinessOnce(currentContext, key, currentSource)
		am.dynamicFlowReadinessMu.Lock()
		attempt.err = attemptErr
		if attempt.successorRequired {
			currentContext = attempt.successorContext
			currentSource = attempt.successorSource
			attempt.planCoordinate = attempt.successorCoordinate
			attempt.successorRequired = false
			attempt.successorContext = nil
			attempt.successorSource = nil
			attempt.successorCoordinate = ""
			am.dynamicFlowReadinessMu.Unlock()
			continue
		}
		delete(am.dynamicFlowReadinessAttempts, key)
		close(attempt.done)
		am.dynamicFlowReadinessMu.Unlock()
		return attemptErr
	}
}

func (am *AgentManager) loadDynamicFlowRuntimeReadinessPlanCoordinate(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
) (string, error) {
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
	if err != nil {
		return "", err
	}
	if !found {
		return "missing", nil
	}
	return dynamicFlowRuntimeReadinessPlanCoordinate(readiness.Plan)
}

func dynamicFlowRuntimeReadinessPlanCoordinate(
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
) (string, error) {
	normalized, err := plan.Normalized()
	if err != nil {
		return "", err
	}
	encoded, err := canonicaljson.Bytes(normalized)
	if err != nil {
		return "", fmt.Errorf(
			"encode dynamic flow runtime readiness plan coordinate %s: %w",
			normalized.Identity.InstancePath,
			err,
		)
	}
	return string(encoded), nil
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessOnce(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	source semanticview.Source,
) (retErr error) {
	if source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
	if err != nil {
		return err
	}
	if !found {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return fmt.Errorf("dynamic flow runtime readiness not found for %s", key.instancePath)
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if err := am.retirePublishedDynamicFlowRoute(plan.Identity.Route()); err != nil {
		return err
	}
	if !readiness.Eligible() {
		return am.retireDynamicFlowProcessTopology(key.instancePath)
	}
	if strings.TrimSpace(source.WorkflowVersion()) != plan.WorkflowVersion {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			readiness.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.WorkflowVersion()),
		)
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, plan.Identity.Route())
	if err != nil {
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return fmt.Errorf("dynamic flow runtime readiness %s persisted identity changed", readiness.InstancePath)
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
	records, err := am.flowInstanceAgentRecords(req, schema, scope)
	if err != nil {
		return err
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Agents); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, readiness.InstancePath)
	if err != nil {
		return fmt.Errorf("load dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	topologyAuthority, err := dynamicFlowAgentTopologyAuthority(plan)
	if err != nil {
		return fmt.Errorf("authorize dynamic flow agent topology for %s: %w", readiness.InstancePath, err)
	}
	if err := am.reconcileDynamicFlowAgentSet(ctx, source, readiness.InstancePath, records, persistedAgents, topologyAuthority); err != nil {
		return fmt.Errorf("reconcile dynamic flow agent set for %s: %w", readiness.InstancePath, err)
	}
	if err := am.verifyDynamicFlowAgents(ctx, readiness.InstancePath, records); err != nil {
		return fmt.Errorf("verify dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if err := am.installFlowInstanceRoute(ctx, req); err != nil {
		return fmt.Errorf("persist dynamic flow route %s: %w", readiness.InstancePath, err)
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if err := am.publishPersistedDynamicFlowRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity:            req.Instance.Route(),
		ActivationVariables: flowActivationVars(req),
	}); err != nil {
		return fmt.Errorf("publish dynamic flow route %s: %w", readiness.InstancePath, err)
	}
	published := true
	defer func() {
		if published && retErr != nil {
			_ = am.retirePublishedDynamicFlowRoute(plan.Identity.Route())
		}
	}()
	if err := am.verifyDynamicFlowRoute(ctx, plan.Identity.Route()); err != nil {
		return err
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	if err := am.workflowInstances.ArmInitialEntryTimers(ctx, readiness.InstancePath); err != nil {
		return fmt.Errorf("arm initial workflow timers for %s: %w", readiness.InstancePath, err)
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	now := time.Now().UTC()
	if err := am.workflowInstances.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, now); err != nil {
		return fmt.Errorf("record dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	fresh, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
	if err != nil {
		return err
	}
	if !found || !fresh.Eligible() {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return nil
	}
	current, err := dynamicFlowRuntimeReadinessPlanMatches(fresh.Plan, plan)
	if err != nil {
		return fmt.Errorf("verify completed dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	if !current || fresh.TopologyReadyAt.IsZero() {
		retireErr := am.retireDynamicFlowProcessTopology(key.instancePath)
		return errors.Join(
			fmt.Errorf("dynamic flow runtime readiness changed after topology completion for %s", readiness.InstancePath),
			retireErr,
		)
	}
	if plan.CreationEvent == nil || !fresh.CreationEventEmittedAt.IsZero() {
		published = false
		return nil
	}
	evt, err := dynamicFlowRuntimeCreationEvent(plan)
	if err != nil {
		return err
	}
	publisher, ok := am.bus.(runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher)
	if !ok || publisher == nil {
		return fmt.Errorf("dynamic flow creation occurrence requires transactional event publisher")
	}
	creationCtx := events.WithDeliveryContext(ctx, plan.CreationEvent.DeliveryContext)
	if err := am.workflowInstances.CommitDynamicFlowRuntimeCreationOccurrence(
		creationCtx,
		runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest{
			RunID:        plan.RunID,
			InstancePath: readiness.InstancePath,
			Plan:         plan,
			Event:        evt,
			OccurredAt:   time.Now().UTC(),
		},
		publisher,
	); err != nil {
		fresh, found, loadErr := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
		if loadErr == nil && (!found || !fresh.Eligible()) {
			if retireErr := am.retireDynamicFlowProcessTopology(key.instancePath); retireErr != nil {
				return errors.Join(
					fmt.Errorf("commit dynamic flow creation occurrence %s: %w", readiness.InstancePath, err),
					fmt.Errorf("retire terminal dynamic flow process topology %s: %w", readiness.InstancePath, retireErr),
				)
			}
			return nil
		}
		if loadErr != nil {
			return errors.Join(
				fmt.Errorf("commit dynamic flow creation occurrence %s: %w", readiness.InstancePath, err),
				fmt.Errorf("reload dynamic flow creation eligibility %s: %w", readiness.InstancePath, loadErr),
			)
		}
		return fmt.Errorf("commit dynamic flow creation occurrence %s: %w", readiness.InstancePath, err)
	}
	published = false
	return nil
}

func (am *AgentManager) dynamicFlowRuntimeReadinessStillEligible(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
) (bool, error) {
	fresh, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, key.instancePath)
	if err != nil {
		return false, err
	}
	if !found || !fresh.Eligible() {
		return false, am.retireDynamicFlowProcessTopology(key.instancePath)
	}
	if fresh.Plan.RunID != key.runID || fresh.Plan.Identity.InstancePath != key.instancePath {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return false, fmt.Errorf("dynamic flow runtime readiness identity changed for %s", key.instancePath)
	}
	current, err := dynamicFlowRuntimeReadinessPlanMatches(fresh.Plan, expected)
	if err != nil {
		return false, fmt.Errorf("compare dynamic flow runtime readiness plan %s: %w", key.instancePath, err)
	}
	if !current {
		_ = am.retireDynamicFlowProcessTopology(key.instancePath)
		return false, fmt.Errorf("dynamic flow runtime readiness plan changed for %s", key.instancePath)
	}
	return true, nil
}

func dynamicFlowRuntimeReadinessPlanMatches(
	actual runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
) (bool, error) {
	actualJSON, err := canonicaljson.Bytes(actual)
	if err != nil {
		return false, fmt.Errorf("encode current plan: %w", err)
	}
	expectedJSON, err := canonicaljson.Bytes(expected)
	if err != nil {
		return false, fmt.Errorf("encode expected plan: %w", err)
	}
	return string(actualJSON) == string(expectedJSON), nil
}

func (am *AgentManager) publishPersistedDynamicFlowRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	publisher, ok := am.bus.(persistedFlowInstanceRouteRestorer)
	if !ok || publisher == nil {
		return fmt.Errorf("event bus does not support process publication for persisted flow-instance route %s", req.Identity.InstancePath)
	}
	return publisher.PublishPersistedFlowInstanceRoute(req)
}

func (am *AgentManager) retirePublishedDynamicFlowRoute(route runtimeflowidentity.Route) error {
	retirer, ok := am.bus.(publishedFlowInstanceRouteRetirer)
	if !ok || retirer == nil {
		return fmt.Errorf("event bus does not support process retirement for flow-instance route %s", route.InstancePath)
	}
	return retirer.RetirePublishedFlowInstanceRoute(route)
}

func (am *AgentManager) retireDynamicFlowProcessTopology(instancePath string) error {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	var retireErrs []error
	if err := am.retirePublishedDynamicFlowRoute(runtimeflowidentity.StoredRoute("", "", instancePath)); err != nil {
		retireErrs = append(retireErrs, err)
	}
	for _, cfg := range am.ListAgentConfigs() {
		if strings.Trim(strings.TrimSpace(cfg.FlowPath), "/") != instancePath {
			continue
		}
		if err := am.TeardownAgent(cfg.ID); err != nil && !errors.Is(err, ErrAgentNotFound) {
			retireErrs = append(retireErrs, fmt.Errorf("retire dynamic flow process agent %s: %w", cfg.ID, err))
		}
	}
	return errors.Join(retireErrs...)
}

func (am *AgentManager) loadDynamicFlowPersistedAgents(ctx context.Context, instancePath string) (map[string]PersistedAgent, error) {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	persistedByID := map[string]PersistedAgent{}
	if am.store == nil {
		return persistedByID, nil
	}
	persisted, err := am.store.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range persisted {
		if strings.Trim(strings.TrimSpace(rec.Config.FlowPath), "/") == instancePath {
			persistedByID[strings.TrimSpace(rec.Config.ID)] = rec
		}
	}
	return persistedByID, nil
}

func (am *AgentManager) reconcileDynamicFlowAgentSet(
	ctx context.Context,
	source semanticview.Source,
	instancePath string,
	expected []PersistedAgent,
	persisted map[string]PersistedAgent,
	topologyAuthority DynamicAgentTopologyMutation,
) error {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	expectedIDs := make(map[string]struct{}, len(expected))
	for _, rec := range expected {
		expectedIDs[strings.TrimSpace(rec.Config.ID)] = struct{}{}
	}
	removedIDs := make(map[string]struct{})
	for id := range persisted {
		if _, ok := expectedIDs[id]; !ok {
			removedIDs[id] = struct{}{}
		}
	}
	for _, cfg := range am.ListAgentConfigs() {
		if strings.Trim(strings.TrimSpace(cfg.FlowPath), "/") != instancePath {
			continue
		}
		if _, ok := expectedIDs[strings.TrimSpace(cfg.ID)]; !ok {
			removedIDs[strings.TrimSpace(cfg.ID)] = struct{}{}
		}
	}
	orderedRemoved := make([]string, 0, len(removedIDs))
	for id := range removedIDs {
		orderedRemoved = append(orderedRemoved, id)
	}
	sort.Strings(orderedRemoved)
	for _, id := range orderedRemoved {
		if _, live := am.GetAgentConfig(id); !live {
			stored, found := persisted[id]
			if !found {
				return fmt.Errorf("removed dynamic flow agent %s has neither process nor durable lifecycle owner", id)
			}
			if err := am.adoptPersistedAgentForLifecycle(ctx, source, stored); err != nil {
				return fmt.Errorf("adopt removed dynamic flow agent %s: %w", id, err)
			}
		}
		removeAuthority := topologyAuthority
		removeAuthority.desiredPresent = false
		if err := am.teardownAgentWithTopology(ctx, id, "teardown", &removeAuthority); err != nil {
			return fmt.Errorf("retire removed dynamic flow agent %s: %w", id, err)
		}
		delete(persisted, id)
	}
	for _, rec := range expected {
		presentAuthority := topologyAuthority
		presentAuthority.desiredPresent = true
		if err := am.reconcileDynamicFlowAgent(ctx, source, rec, persisted[strings.TrimSpace(rec.Config.ID)], &presentAuthority); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent %s: %w", rec.Config.ID, err)
		}
	}
	return nil
}

func (am *AgentManager) reconcileDynamicFlowAgent(
	ctx context.Context,
	source semanticview.Source,
	rec PersistedAgent,
	persisted PersistedAgent,
	topology *DynamicAgentTopologyMutation,
) error {
	expectedRevision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	existing, live := am.GetAgentConfig(rec.Config.ID)
	actualRevision := ""
	if live {
		actualRevision, err = lifecycleConfigRevision(PersistedAgent{Config: existing})
		if err != nil {
			return err
		}
	}
	persistedID := strings.TrimSpace(persisted.Config.ID)
	persistedRevision := ""
	if persistedID != "" {
		persistedRevision, err = lifecycleConfigRevision(persisted)
		if err != nil {
			return err
		}
		if persistedID != strings.TrimSpace(rec.Config.ID) {
			return fmt.Errorf("agent %s persisted identity changed", rec.Config.ID)
		}
	}
	if live && persistedID == "" {
		return fmt.Errorf("agent %s is process-ready without durable registration", rec.Config.ID)
	}
	if live && actualRevision != persistedRevision {
		return fmt.Errorf("agent %s process and durable revisions disagree", rec.Config.ID)
	}
	if !live && persistedID != "" {
		if err := am.adoptPersistedAgentForLifecycle(ctx, source, persisted); err != nil {
			return fmt.Errorf("adopt persisted agent %s: %w", rec.Config.ID, err)
		}
		live = true
		actualRevision = persistedRevision
	}
	if live {
		if actualRevision == expectedRevision {
			return nil
		}
		return am.reconfigureAgentExactWithTopology(ctx, source, rec.Config.ID, rec.Config, topology)
	}
	return am.spawnAgentInternalForSourceWithTopology(ctx, rec, true, source, topology)
}

func dynamicFlowAgentTopologyAuthority(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (DynamicAgentTopologyMutation, error) {
	fingerprint, err := canonicaljson.Hash(plan)
	if err != nil {
		return DynamicAgentTopologyMutation{}, err
	}
	return DynamicAgentTopologyMutation{
		runID:           strings.TrimSpace(plan.RunID),
		instancePath:    strings.Trim(strings.TrimSpace(plan.Identity.InstancePath), "/"),
		planFingerprint: fingerprint,
	}, nil
}

func (am *AgentManager) verifyDynamicFlowAgents(ctx context.Context, instancePath string, expected []PersistedAgent) error {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	expectedByID := make(map[string]PersistedAgent, len(expected))
	for _, rec := range expected {
		expectedByID[strings.TrimSpace(rec.Config.ID)] = rec
	}
	persistedByID := make(map[string]PersistedAgent, len(expected))
	if am.store == nil {
		if len(expectedByID) != 0 {
			return fmt.Errorf("declared agents require durable manager persistence")
		}
	} else {
		persisted, err := am.store.LoadAgents(ctx)
		if err != nil {
			return err
		}
		for _, rec := range persisted {
			if strings.Trim(strings.TrimSpace(rec.Config.FlowPath), "/") == instancePath {
				persistedByID[strings.TrimSpace(rec.Config.ID)] = rec
			}
		}
	}
	liveByID := make(map[string]models.AgentConfig, len(expected))
	for _, cfg := range am.ListAgentConfigs() {
		if strings.Trim(strings.TrimSpace(cfg.FlowPath), "/") == instancePath {
			liveByID[strings.TrimSpace(cfg.ID)] = cfg
		}
	}
	if len(persistedByID) != len(expectedByID) || len(liveByID) != len(expectedByID) {
		return fmt.Errorf(
			"declared agent set mismatch: expected=%d persisted=%d process=%d",
			len(expectedByID),
			len(persistedByID),
			len(liveByID),
		)
	}
	for _, rec := range expected {
		expectedRevision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		live, ok := liveByID[strings.TrimSpace(rec.Config.ID)]
		if !ok {
			return fmt.Errorf("declared agent %s is not process-ready", rec.Config.ID)
		}
		liveRevision, err := lifecycleConfigRevision(PersistedAgent{Config: live})
		if err != nil {
			return err
		}
		stored, ok := persistedByID[strings.TrimSpace(rec.Config.ID)]
		if !ok {
			return fmt.Errorf("declared agent %s is not durably registered", rec.Config.ID)
		}
		storedRevision, err := lifecycleConfigRevision(stored)
		if err != nil {
			return err
		}
		if liveRevision != expectedRevision || storedRevision != expectedRevision {
			return fmt.Errorf("declared agent %s readiness revision mismatch", rec.Config.ID)
		}
	}
	return nil
}

func verifyDynamicFlowAgentExpectations(actual []PersistedAgent, expected []runtimepipeline.DynamicFlowRuntimeAgentExpectation) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("declared agent count changed: expected=%d actual=%d", len(expected), len(actual))
	}
	for idx, rec := range actual {
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		if strings.TrimSpace(rec.Config.ID) != expected[idx].AgentID || revision != expected[idx].ConfigRevision {
			return fmt.Errorf("declared agent topology changed at %s", rec.Config.ID)
		}
	}
	return nil
}

func (am *AgentManager) verifyDynamicFlowRoute(ctx context.Context, route runtimeflowidentity.Route) error {
	verifier, ok := am.bus.(flowInstanceRouteContextVerifier)
	if !ok || verifier == nil || !verifier.HasFlowInstanceRoute(route) {
		return fmt.Errorf("dynamic flow route %s is not process-ready", route.InstancePath)
	}
	if err := verifier.VerifyFlowInstanceRoute(ctx, route); err != nil {
		return fmt.Errorf("verify dynamic flow route %s: %w", route.InstancePath, err)
	}
	return nil
}

func dynamicFlowRuntimeCreationEvent(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (events.Event, error) {
	var empty events.Event
	creation := plan.CreationEvent
	if creation == nil {
		return empty, fmt.Errorf("dynamic flow creation event plan is required")
	}
	routingSource, err := events.NewRuntimeRoutingSource(plan.Identity.TemplateID, plan.Identity.InstancePath, plan.Identity.EntityID)
	if err != nil {
		return empty, err
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{
		EntityID:     plan.Identity.EntityID,
		FlowInstance: plan.Identity.InstancePath,
	}, events.RouteIdentity{
		FlowID:       plan.Identity.TemplateID,
		FlowInstance: plan.Identity.InstancePath,
		EntityID:     plan.Identity.EntityID,
	})
	return events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: creation.EventID, Type: events.EventType(creation.EventType),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "flow-instance-activator"},
			Payload:  creation.Payload, Envelope: envelope, RoutingSource: routingSource,
			CreatedAt: creation.CreatedAt, ExecutionMode: creation.ExecutionMode,
		},
		Lineage: events.EventLineage{
			RunID: creation.RunID, ParentEventID: creation.ParentEventID, ExecutionMode: creation.ExecutionMode,
		},
	})
}
