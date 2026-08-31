package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
	done              chan struct{}
	err               error
	planCoordinate    string
	successorRequired bool
	successor         *dynamicFlowRuntimeReadinessAdmission
}

type dynamicFlowRuntimeReadinessSource struct {
	fact   runtimecorrelation.BundleSourceFact
	source semanticview.Source
}

type dynamicFlowRuntimeReadinessAdmission struct {
	ctx             context.Context
	key             dynamicFlowRuntimeReadinessKey
	plan            runtimepipeline.DynamicFlowRuntimeReadinessPlan
	planCoordinate  string
	source          dynamicFlowRuntimeReadinessSource
	topologyDurable bool
	processOnly     bool
	processPrepared bool
}

// DynamicFlowRuntimeStartupReadiness is the one-startup-attempt authority for
// pending rows admitted by exact source transition or explicit recovery.
type DynamicFlowRuntimeStartupReadiness struct {
	sourceFact        runtimecorrelation.BundleSourceFact
	replayAllowed     bool
	authorizedPending map[dynamicFlowRuntimeReadinessKey]struct{}
	empty             bool
}

var errDynamicFlowRuntimeReadinessPlanStale = errors.New(
	"dynamic flow runtime readiness declared plan is stale",
)

var errDynamicFlowRuntimeReadinessSourceStale = errors.New(
	"dynamic flow runtime readiness callback source is stale",
)

const (
	dynamicFlowRuntimeReadinessCleanupTimeout = 10 * time.Second
	defaultDynamicFlowReadinessRetryInterval  = 5 * time.Second
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

func (am *AgentManager) reconcilePendingDynamicFlowRuntimeReadiness(ctx context.Context) error {
	if am == nil || am.workflowInstances == nil {
		return nil
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	projection, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, source.fact)
	if err != nil {
		return err
	}
	var reconcileErrs []error
	for _, item := range projection.CurrentPending {
		if err := am.reconcileDynamicFlowRuntimeReadiness(ctx, item.Plan.RunID, item.InstancePath); err != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("%s: %w", item.InstancePath, err))
		}
	}
	return errors.Join(reconcileErrs...)
}

func (am *AgentManager) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if am == nil || am.workflowInstances == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, nil
	}
	projection, err := am.workflowInstances.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if am.roles.StandingRestarts == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, errors.New("dynamic flow runtime readiness requires standing restart disposition reader")
	}
	cache := make(map[string]runtimepipeline.StandingRestartDisposition)
	filter := func(items []runtimepipeline.DynamicFlowRuntimeReadiness) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
		filtered := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(items))
		for _, item := range items {
			runID := strings.TrimSpace(item.Plan.RunID)
			disposition, ok := cache[runID]
			if !ok {
				disposition, err = am.roles.StandingRestarts.StandingRunRestartDisposition(ctx, runID)
				if err != nil {
					return nil, fmt.Errorf("classify dynamic flow runtime readiness run %s: %w", runID, err)
				}
				cache[runID] = disposition
			}
			if disposition.UsesGenericRecovery() || disposition.Executable() {
				filtered = append(filtered, item)
			}
		}
		return filtered, nil
	}
	if projection.CurrentCompleted, err = filter(projection.CurrentCompleted); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if projection.CurrentPending, err = filter(projection.CurrentPending); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if projection.SourceTransitionRequired, err = filter(projection.SourceTransitionRequired); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	return projection, nil
}

func (am *AgentManager) CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx context.Context, sourceFact runtimecorrelation.BundleSourceFact, replayAllowed bool) (DynamicFlowRuntimeStartupReadiness, error) {
	startup := DynamicFlowRuntimeStartupReadiness{
		sourceFact: sourceFact, replayAllowed: replayAllowed,
		authorizedPending: make(map[dynamicFlowRuntimeReadinessKey]struct{}),
	}
	projection, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceFact)
	if err != nil {
		return DynamicFlowRuntimeStartupReadiness{}, err
	}
	if len(projection.CurrentCompleted) == 0 && len(projection.CurrentPending) == 0 && len(projection.SourceTransitionRequired) == 0 {
		startup.empty = true
		return startup, nil
	}
	ownedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return DynamicFlowRuntimeStartupReadiness{}, err
	}
	if !ownedSource.fact.Matches(sourceFact) {
		return DynamicFlowRuntimeStartupReadiness{}, fmt.Errorf("dynamic flow startup readiness source is not manager-owned")
	}
	transitionRequests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, len(projection.SourceTransitionRequired))
	for _, item := range projection.SourceTransitionRequired {
		if item.Pending() && !replayAllowed {
			return DynamicFlowRuntimeStartupReadiness{}, &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf(
				"source transition for %s retains incomplete predecessor readiness and requires recovery",
				item.InstancePath,
			)}
		}
		expected, err := am.deriveCurrentDynamicFlowRuntimeReadinessPlan(ctx, item, ownedSource)
		if err != nil {
			return DynamicFlowRuntimeStartupReadiness{}, &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("derive current-source dynamic flow readiness %s: %w", item.InstancePath, err)}
		}
		transitionRequests = append(transitionRequests, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{Observed: item, Expected: expected})
		key, err := newDynamicFlowRuntimeReadinessKey(expected.RunID, expected.Identity.InstancePath)
		if err != nil {
			return DynamicFlowRuntimeStartupReadiness{}, err
		}
		startup.authorizedPending[key] = struct{}{}
	}
	if len(transitionRequests) != 0 {
		if _, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, transitionRequests, time.Now().UTC()); err != nil {
			return DynamicFlowRuntimeStartupReadiness{}, &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("canonicalize current-source dynamic flow readiness set: %w", err)}
		}
	}
	projection, err = am.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceFact)
	if err != nil {
		return DynamicFlowRuntimeStartupReadiness{}, err
	}
	if len(projection.SourceTransitionRequired) != 0 {
		return DynamicFlowRuntimeStartupReadiness{}, fmt.Errorf("dynamic topology startup retains %d unresolved source transition(s)", len(projection.SourceTransitionRequired))
	}
	for _, item := range projection.CurrentPending {
		key, keyErr := newDynamicFlowRuntimeReadinessKey(item.Plan.RunID, item.InstancePath)
		if keyErr != nil {
			return DynamicFlowRuntimeStartupReadiness{}, keyErr
		}
		if replayAllowed {
			startup.authorizedPending[key] = struct{}{}
		}
		if _, authorized := startup.authorizedPending[key]; !authorized {
			return DynamicFlowRuntimeStartupReadiness{}, fmt.Errorf("dynamic topology startup requires recovery for incomplete source-owned instance %s", item.InstancePath)
		}
	}
	am.dynamicFlowReadinessMu.Lock()
	am.dynamicFlowStartupTopologyPending = true
	am.dynamicFlowReadinessMu.Unlock()
	return startup, nil
}

func (am *AgentManager) CompleteDynamicFlowRuntimeStartupTopology(ctx context.Context, startup DynamicFlowRuntimeStartupReadiness) error {
	if startup.empty {
		return nil
	}
	if err := startup.sourceFact.Validate(); err != nil {
		return fmt.Errorf("dynamic flow startup topology authority: %w", err)
	}
	projection, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, startup.sourceFact)
	if err != nil {
		return err
	}
	if len(projection.SourceTransitionRequired) != 0 {
		return fmt.Errorf(
			"dynamic flow startup topology retains %d source transition(s)",
			len(projection.SourceTransitionRequired),
		)
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	prepare := append([]runtimepipeline.DynamicFlowRuntimeReadiness{}, projection.CurrentCompleted...)
	prepare = append(prepare, projection.CurrentPending...)
	sort.Slice(prepare, func(i, j int) bool {
		if prepare[i].Plan.RunID != prepare[j].Plan.RunID {
			return prepare[i].Plan.RunID < prepare[j].Plan.RunID
		}
		return prepare[i].InstancePath < prepare[j].InstancePath
	})
	for _, item := range projection.CurrentPending {
		key, keyErr := newDynamicFlowRuntimeReadinessKey(item.Plan.RunID, item.InstancePath)
		if keyErr != nil {
			return keyErr
		}
		if _, authorized := startup.authorizedPending[key]; !authorized {
			return fmt.Errorf("dynamic flow startup topology lacks pending authorization for %s", item.InstancePath)
		}
	}
	for _, item := range prepare {
		if err := am.reconcileDynamicFlowRuntimeReadinessItem(ctx, item, source, true, false); err != nil {
			return &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("prepare dynamic flow process topology %s: %w", item.InstancePath, err)}
		}
	}
	for _, item := range projection.CurrentPending {
		if err := am.reconcileDynamicFlowRuntimeReadinessItem(ctx, item, source, false, true); err != nil {
			return &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("finalize dynamic flow runtime readiness %s: %w", item.InstancePath, err)}
		}
	}
	fresh, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, startup.sourceFact)
	if err != nil {
		return err
	}
	if len(fresh.CurrentPending) != 0 || len(fresh.SourceTransitionRequired) != 0 {
		return fmt.Errorf("dynamic flow startup topology remains incomplete: current_pending=%d source_transition_required=%d", len(fresh.CurrentPending), len(fresh.SourceTransitionRequired))
	}
	for _, item := range fresh.CurrentCompleted {
		if err := am.verifyDynamicFlowRuntimeProcessTopology(ctx, item, source); err != nil {
			return &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("verify dynamic flow startup topology %s: %w", item.InstancePath, err)}
		}
	}
	am.dynamicFlowReadinessMu.Lock()
	am.dynamicFlowStartupTopologyPending = false
	am.dynamicFlowReadinessMu.Unlock()
	return nil
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessItem(ctx context.Context, item runtimepipeline.DynamicFlowRuntimeReadiness, source dynamicFlowRuntimeReadinessSource, processOnly, processPrepared bool) error {
	plan, err := item.Plan.Normalized()
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, source); err != nil {
		return err
	}
	key, err := newDynamicFlowRuntimeReadinessKey(plan.RunID, item.InstancePath)
	if err != nil {
		return err
	}
	coordinate, err := dynamicFlowRuntimeReadinessPlanCoordinate(plan)
	if err != nil {
		return err
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: plan, planCoordinate: coordinate, source: source,
		processOnly: processOnly, processPrepared: processPrepared,
	})
}

func (am *AgentManager) verifyDynamicFlowRuntimeProcessTopology(ctx context.Context, item runtimepipeline.DynamicFlowRuntimeReadiness, source dynamicFlowRuntimeReadinessSource) error {
	if !item.Eligible() || item.Pending() {
		return fmt.Errorf("dynamic flow runtime readiness %s is not complete", item.InstancePath)
	}
	plan, err := item.Plan.Normalized()
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, source); err != nil {
		return err
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
	if err != nil {
		return err
	}
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, flowIdentity)
	if err != nil {
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return fmt.Errorf("dynamic flow runtime readiness %s persisted identity changed", item.InstancePath)
	}
	scope, ok := semanticview.FlowScopeByID(source.source, plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow contract view not found: %s", plan.Identity.TemplateID)
	}
	schema, ok := source.source.FlowSchemaByID(plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow schema not found: %s", plan.Identity.TemplateID)
	}
	records, err := am.flowInstanceAgentRecords(plan.RunID, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source.source,
		Instance:       projection.Identity,
		Config:         projection.Config,
	}, schema, scope)
	if err != nil {
		return err
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Agents); err != nil {
		return err
	}
	topologyAuthority, err := dynamicFlowAgentTopologyAuthority(plan)
	if err != nil {
		return err
	}
	if err := am.verifyDynamicFlowAgents(ctx, flowIdentity, records, topologyAuthority); err != nil {
		return err
	}
	return am.verifyDynamicFlowRoute(ctx, flowIdentity)
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
	agentRecords, err := am.flowInstanceAgentRecords(runID, req, schema, scope)
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
	current, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, runID, req.Instance.Route())
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	if !found {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness not found for %s", req.Instance.InstancePath)
	}
	expected := current.Plan
	expected.Identity = req.Instance
	expected.BundleHash, expected.BundleSource, err = dynamicFlowRuntimeReadinessSourceCoordinate(ctx)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	expected.WorkflowVersion = strings.TrimSpace(req.ContractBundle.WorkflowVersion())
	expected.Agents = make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(agentRecords))
	for _, record := range agentRecords {
		identity, err := record.Config.ConcreteIdentity()
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf(
				"derive dynamic flow agent identity %s: %w",
				record.Config.ID,
				err,
			)
		}
		revision, err := lifecycleConfigRevision(record)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent revision %s: %w", record.Config.ID, err)
		}
		expected.Agents = append(expected.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			Identity: identity, ConfigRevision: revision,
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
	if err := am.executionPosture.Admit(expected.ExecutionMode, "dynamic flow runtime readiness plan reconciliation"); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	if _, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: current, Expected: expected}}, occurredAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("reconcile dynamic flow runtime readiness plan %s: %w", req.Instance.InstancePath, err)
	}
	return expected, nil
}

// ReconcileDynamicFlowRuntimeReadinessPlansForRun advances every active
// readiness owner in the selected run before revised routes are derived.
func (am *AgentManager) ReconcileDynamicFlowRuntimeReadinessPlansForRun(
	ctx context.Context,
	observedAt time.Time,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	source := admittedSource.source
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact run_id")
	}
	observedAt = observedAt.UTC()
	if observedAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact occurrence time")
	}
	items, err := am.workflowInstances.InspectDynamicFlowRuntimeReadinessForRun(ctx, runID, admittedSource.fact)
	if err != nil {
		return err
	}
	requests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, len(items))
	plansByKey := make(map[runtimepipeline.DynamicFlowRuntimeReadinessKey]runtimepipeline.DynamicFlowRuntimeReadinessPlan, len(items))
	for _, item := range items {
		expected, err := am.deriveCurrentDynamicFlowRuntimeReadinessPlan(ctx, item, admittedSource)
		if err != nil {
			return err
		}
		requests = append(requests, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{Observed: item, Expected: expected})
		plansByKey[runtimepipeline.DynamicFlowRuntimeReadinessKey{RunID: expected.RunID, InstancePath: expected.Identity.InstancePath}] = expected
	}
	results, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, observedAt)
	if err != nil {
		return fmt.Errorf("reconcile dynamic flow runtime readiness plan set: %w", err)
	}
	for _, result := range results {
		if !result.Changed {
			continue
		}
		expected := plansByKey[runtimepipeline.DynamicFlowRuntimeReadinessKey{RunID: result.RunID, InstancePath: result.InstancePath}]
		if err := am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, expected, source); err != nil {
			am.signalDynamicFlowRuntimeReadiness()
			return err
		}
	}
	return nil
}

func (am *AgentManager) deriveCurrentDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	item runtimepipeline.DynamicFlowRuntimeReadiness,
	admittedSource dynamicFlowRuntimeReadinessSource,
) (runtimepipeline.DynamicFlowRuntimeReadinessPlan, error) {
	if !item.OwningRunSource.Matches(admittedSource.fact) {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow readiness %s is not owned by the admitted source", item.InstancePath)
	}
	plan, err := item.Plan.Normalized()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, flowIdentity)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("load dynamic flow readiness projection %s: %w", item.InstancePath, err)
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow readiness %s persisted identity changed", item.InstancePath)
	}
	source := admittedSource.source
	scope, ok := semanticview.FlowScopeByID(source, projection.Identity.TemplateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow contract view not found: %s", projection.Identity.TemplateID)
	}
	schema, ok := source.FlowSchemaByID(projection.Identity.TemplateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow schema not found: %s", projection.Identity.TemplateID)
	}
	records, err := am.flowInstanceAgentRecords(plan.RunID, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		Instance:       projection.Identity,
		Config:         projection.Config,
	}, schema, scope)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow readiness agents %s: %w", item.InstancePath, err)
	}
	bundleHash, bundleSource := admittedSource.fact.StorageValues()
	expected := plan
	expected.Identity = projection.Identity
	expected.BundleHash = bundleHash
	expected.BundleSource = bundleSource
	expected.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	expected.Agents = make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(records))
	for _, record := range records {
		identity, err := record.Config.ConcreteIdentity()
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent identity %s: %w", record.Config.ID, err)
		}
		revision, err := lifecycleConfigRevision(record)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent revision %s: %w", record.Config.ID, err)
		}
		expected.Agents = append(expected.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			Identity: identity, ConfigRevision: revision,
		})
	}
	expected.CreationEvent, err = rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		plan.CreationEvent,
		!item.CreationEventEmittedAt.IsZero(),
		source,
		schema,
		projection.Identity,
		projection.Config,
	)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("rebuild dynamic flow creation plan %s: %w", item.InstancePath, err)
	}
	if err := am.executionPosture.Admit(expected.ExecutionMode, "dynamic flow runtime readiness plan reconciliation"); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	return expected.Normalized()
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
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	key, err := newDynamicFlowRuntimeReadinessKey(runID, instancePath)
	if err != nil {
		return err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return err
	}
	planCoordinate := "missing"
	var plan runtimepipeline.DynamicFlowRuntimeReadinessPlan
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	if found {
		plan, err = readiness.Plan.Normalized()
		if err != nil {
			return err
		}
		if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, admittedSource); err != nil {
			return err
		}
		planCoordinate, err = dynamicFlowRuntimeReadinessPlanCoordinate(plan)
		if err != nil {
			return err
		}
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: plan, planCoordinate: planCoordinate, source: admittedSource,
	})
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
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, source)
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(normalized, admittedSource); err != nil {
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
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: normalized, planCoordinate: planCoordinate, source: admittedSource,
	})
}

func (am *AgentManager) reconcileCommittedDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	source semanticview.Source,
) error {
	normalized, err := plan.Normalized()
	if err != nil {
		return err
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, source)
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(normalized, admittedSource); err != nil {
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
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: normalized, planCoordinate: planCoordinate, source: admittedSource,
		topologyDurable: true,
	})
}

// PreparePersistedDynamicFlowRuntimeProcessTopology consumes the exact durable
// plan for one live flow owner without claiming durable readiness completion.
// It is used by bounded runtimes that execute before their run is activated.
func (am *AgentManager) PreparePersistedDynamicFlowRuntimeProcessTopology(
	ctx context.Context,
	owner runtimeflowidentity.RunScopedFlowInstance,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness finalizer requires manager and workflow store")
	}
	owner = owner.Normalize()
	if err := owner.Validate(); err != nil {
		return err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, owner.RunID, owner.Route)
	if err != nil {
		return fmt.Errorf("load committed dynamic flow runtime readiness %s: %w", owner.Route.InstancePath, err)
	}
	if !found {
		return fmt.Errorf("committed dynamic flow runtime readiness not found for %s", owner.Route.InstancePath)
	}
	if readiness.Plan.RunID != owner.RunID || readiness.Plan.Identity.Route() != owner.Route {
		return fmt.Errorf("committed dynamic flow runtime readiness identity does not match %s", owner.Route.InstancePath)
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	return am.reconcileDynamicFlowRuntimeReadinessItem(ctx, readiness, source, true, false)
}

func (am *AgentManager) dynamicFlowRuntimeReadinessSource(
	ctx context.Context,
	candidates ...semanticview.Source,
) (dynamicFlowRuntimeReadinessSource, error) {
	if am == nil {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires manager",
		)
	}
	owned := am.semanticReadinessSource
	if owned.source == nil || !sameLoadedDynamicFlowSemanticSource(am.semanticSource, owned.source) {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires the manager-owned semantic source",
		)
	}
	if len(candidates) > 0 && !sameLoadedDynamicFlowSemanticSource(owned.source, candidates[0]) {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"%w: callback semantic source is not the manager-owned loaded source",
			errDynamicFlowRuntimeReadinessSourceStale,
		)
	}
	if err := owned.fact.Validate(); err != nil {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness manager source fact: %w",
			err,
		)
	}
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires exact bundle source fact",
		)
	}
	if !sourceFact.Matches(owned.fact) {
		declaredHash, declaredSource := owned.fact.StorageValues()
		activeHash, activeSource := sourceFact.StorageValues()
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"%w: declared=%s/%s active=%s/%s",
			errDynamicFlowRuntimeReadinessSourceStale,
			declaredHash,
			declaredSource,
			activeHash,
			activeSource,
		)
	}
	if strings.TrimSpace(owned.source.WorkflowVersion()) == "" {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness manager source requires workflow version",
		)
	}
	return owned, nil
}

func sameLoadedDynamicFlowSemanticSource(left, right semanticview.Source) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftBundle, leftOK := semanticview.Bundle(left)
	rightBundle, rightOK := semanticview.Bundle(right)
	return leftOK && rightOK && leftBundle == rightBundle
}

func dynamicFlowRuntimeReadinessSourceCoordinate(ctx context.Context) (string, string, error) {
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("dynamic flow runtime readiness requires exact bundle source fact")
	}
	if err := sourceFact.Validate(); err != nil {
		return "", "", fmt.Errorf("dynamic flow runtime readiness bundle source fact: %w", err)
	}
	bundleHash, bundleSource := sourceFact.StorageValues()
	return bundleHash, bundleSource, nil
}

func validateDynamicFlowRuntimeReadinessCallbackSource(
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	source dynamicFlowRuntimeReadinessSource,
) error {
	bundleHash, bundleSource := source.fact.StorageValues()
	if bundleHash != plan.BundleHash || bundleSource != plan.BundleSource {
		return fmt.Errorf(
			"%w: declared=%s/%s active=%s/%s",
			errDynamicFlowRuntimeReadinessSourceStale,
			plan.BundleHash,
			plan.BundleSource,
			bundleHash,
			bundleSource,
		)
	}
	if source.source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	if strings.TrimSpace(source.source.WorkflowVersion()) != plan.WorkflowVersion {
		return fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			plan.Identity.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.source.WorkflowVersion()),
		)
	}
	return nil
}

func (am *AgentManager) reconcileDeclaredDynamicFlowRuntimeReadiness(
	admission dynamicFlowRuntimeReadinessAdmission,
) error {
	am.dynamicFlowReadinessMu.Lock()
	currentCoordinate, err := am.loadDynamicFlowRuntimeReadinessPlanCoordinate(admission.ctx, admission.key)
	if err != nil {
		am.dynamicFlowReadinessMu.Unlock()
		return err
	}
	if currentCoordinate != admission.planCoordinate {
		am.dynamicFlowReadinessMu.Unlock()
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	if am.dynamicFlowReadinessAttempts == nil {
		am.dynamicFlowReadinessAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt)
	}
	if attempt := am.dynamicFlowReadinessAttempts[admission.key]; attempt != nil {
		if admission.planCoordinate != attempt.planCoordinate {
			attempt.successorRequired = true
			successor := admission
			attempt.successor = &successor
		}
		am.dynamicFlowReadinessMu.Unlock()
		select {
		case <-admission.ctx.Done():
			return admission.ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &dynamicFlowRuntimeReadinessAttempt{
		done:           make(chan struct{}),
		planCoordinate: admission.planCoordinate,
	}
	am.dynamicFlowReadinessAttempts[admission.key] = attempt
	am.dynamicFlowReadinessMu.Unlock()

	if am.testAfterDynamicFlowReadinessAdmission != nil {
		am.testAfterDynamicFlowReadinessAdmission()
	}
	current := admission
	for {
		attemptErr := am.reconcileDynamicFlowRuntimeReadinessOnce(current)
		am.dynamicFlowReadinessMu.Lock()
		attempt.err = attemptErr
		if attempt.successorRequired {
			current = *attempt.successor
			attempt.planCoordinate = current.planCoordinate
			attempt.successorRequired = false
			attempt.successor = nil
			am.dynamicFlowReadinessMu.Unlock()
			continue
		}
		delete(am.dynamicFlowReadinessAttempts, admission.key)
		close(attempt.done)
		am.dynamicFlowReadinessMu.Unlock()
		return attemptErr
	}
}

func (am *AgentManager) loadDynamicFlowRuntimeReadinessPlanCoordinate(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
) (string, error) {
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
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
	admission dynamicFlowRuntimeReadinessAdmission,
) (retErr error) {
	ctx := admission.ctx
	key := admission.key
	source := admission.source.source
	if source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(
		key.runID,
		runtimeflowidentity.RouteForInstancePath(key.instancePath),
	)
	if err != nil {
		return fmt.Errorf("resolve dynamic flow runtime identity %s: %w", key.instancePath, err)
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return err
	}
	if !found {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
		return fmt.Errorf("dynamic flow runtime readiness not found for %s", key.instancePath)
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if err := am.executionPosture.Admit(plan.ExecutionMode, "dynamic flow runtime readiness topology reconciliation"); err != nil {
		return err
	}
	currentCoordinate, err := dynamicFlowRuntimeReadinessPlanCoordinate(plan)
	if err != nil {
		return err
	}
	if currentCoordinate != admission.planCoordinate {
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, admission.source); err != nil {
		return err
	}
	if !admission.processPrepared {
		if err := am.retirePublishedDynamicFlowRoute(flowIdentity); err != nil {
			return err
		}
	}
	if !readiness.Eligible() {
		return am.retireDynamicFlowProcessTopology(flowIdentity)
	}
	if strings.TrimSpace(source.WorkflowVersion()) != plan.WorkflowVersion {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
		return fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			readiness.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.WorkflowVersion()),
		)
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, flowIdentity)
	if err != nil {
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
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
	records, err := am.flowInstanceAgentRecords(plan.RunID, req, schema, scope)
	if err != nil {
		return err
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Agents); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	topologyAuthority, err := dynamicFlowAgentTopologyAuthority(plan)
	if err != nil {
		return fmt.Errorf("authorize dynamic flow agent topology for %s: %w", readiness.InstancePath, err)
	}
	if !admission.processPrepared {
		persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, flowIdentity)
		if err != nil {
			return fmt.Errorf("load dynamic flow agents for %s: %w", readiness.InstancePath, err)
		}
		if err := am.reconcileDynamicFlowAgentSet(ctx, source, flowIdentity, records, persistedAgents, topologyAuthority); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent set for %s: %w", readiness.InstancePath, err)
		}
	}
	if err := am.verifyDynamicFlowAgents(ctx, flowIdentity, records, topologyAuthority); err != nil {
		return fmt.Errorf("verify dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	published := false
	if !admission.processPrepared {
		flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, req.Instance.Route())
		if err != nil {
			return fmt.Errorf("resolve dynamic flow route identity %s: %w", readiness.InstancePath, err)
		}
		if !admission.topologyDurable {
			if err := am.installFlowInstanceRoute(ctx, flowIdentity, req); err != nil {
				return fmt.Errorf("persist dynamic flow route %s: %w", readiness.InstancePath, err)
			}
		}
		if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
			return err
		} else if !eligible {
			return nil
		}
		if err := am.publishPersistedDynamicFlowRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
			Identity:            flowIdentity,
			ActivationVariables: flowActivationVars(req),
		}); err != nil {
			return fmt.Errorf("publish dynamic flow route %s: %w", readiness.InstancePath, err)
		}
		published = true
	}
	wakeupsArmed := false
	readinessAccepted := false
	defer func() {
		if wakeupsArmed && !readinessAccepted {
			cleanupCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				dynamicFlowRuntimeReadinessCleanupTimeout,
			)
			retireErr := am.workflowInstances.RetireInitialEntryTimerWakeups(cleanupCtx, flowIdentity)
			cancel()
			if retireErr != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf(
						"retire incomplete dynamic flow workflow timers %s: %w",
						readiness.InstancePath,
						retireErr,
					),
				)
			}
		}
		if published && retErr != nil {
			if retireErr := am.retirePublishedDynamicFlowRoute(flowIdentity); retireErr != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("retire incomplete dynamic flow route %s: %w", readiness.InstancePath, retireErr),
				)
			}
		}
	}()
	if err := am.verifyDynamicFlowRoute(ctx, flowIdentity); err != nil {
		return err
	}
	if admission.processOnly {
		published = false
		return nil
	}
	if eligible, err := am.dynamicFlowRuntimeReadinessStillEligible(ctx, key, plan); err != nil {
		return err
	} else if !eligible {
		return nil
	}
	wakeupsArmed = true
	if err := am.workflowInstances.ReconcileInitialEntryTimers(ctx, flowIdentity); err != nil {
		return fmt.Errorf("reconcile initial workflow timers for %s: %w", readiness.InstancePath, err)
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
	fresh, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return err
	}
	if !found || !fresh.Eligible() {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
		return nil
	}
	current, err := dynamicFlowRuntimeReadinessPlanMatches(fresh.Plan, plan)
	if err != nil {
		return fmt.Errorf("verify completed dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	if !current || fresh.TopologyReadyAt.IsZero() {
		retireErr := am.retireDynamicFlowProcessTopology(flowIdentity)
		return errors.Join(
			fmt.Errorf("dynamic flow runtime readiness changed after topology completion for %s", readiness.InstancePath),
			retireErr,
		)
	}
	if plan.CreationEvent == nil || !fresh.CreationEventEmittedAt.IsZero() {
		readinessAccepted = true
		published = false
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
	if err := publisher.CommitDynamicFlowRuntimeCreationOccurrence(
		creationCtx,
		runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest{
			RunID:        plan.RunID,
			InstancePath: readiness.InstancePath,
			Plan:         plan,
			Event:        evt,
			OccurredAt:   time.Now().UTC(),
		},
	); err != nil {
		fresh, found, loadErr := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
		if loadErr == nil && (!found || !fresh.Eligible()) {
			if retireErr := am.retireDynamicFlowProcessTopology(flowIdentity); retireErr != nil {
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
	readinessAccepted = true
	published = false
	return nil
}

func (am *AgentManager) dynamicFlowRuntimeReadinessStillEligible(
	ctx context.Context,
	key dynamicFlowRuntimeReadinessKey,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
) (bool, error) {
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(
		key.runID,
		runtimeflowidentity.RouteForInstancePath(key.instancePath),
	)
	if err != nil {
		return false, fmt.Errorf("resolve dynamic flow runtime identity %s: %w", key.instancePath, err)
	}
	fresh, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return false, err
	}
	if !found || !fresh.Eligible() {
		return false, am.retireDynamicFlowProcessTopology(flowIdentity)
	}
	if fresh.Plan.RunID != key.runID || fresh.Plan.Identity.InstancePath != key.instancePath {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
		return false, fmt.Errorf("dynamic flow runtime readiness identity changed for %s", key.instancePath)
	}
	current, err := dynamicFlowRuntimeReadinessPlanMatches(fresh.Plan, expected)
	if err != nil {
		return false, fmt.Errorf("compare dynamic flow runtime readiness plan %s: %w", key.instancePath, err)
	}
	if !current {
		_ = am.retireDynamicFlowProcessTopology(flowIdentity)
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
	publisher := am.roles.RouteRestorer
	if publisher == nil {
		return fmt.Errorf("event bus does not support process publication for persisted flow-instance route %s", req.Identity.Route.InstancePath)
	}
	return publisher.PublishPersistedFlowInstanceRoute(req)
}

func (am *AgentManager) retirePublishedDynamicFlowRoute(identity runtimeflowidentity.RunScopedFlowInstance) error {
	retirer := am.roles.RouteRetirer
	if retirer == nil {
		return fmt.Errorf("event bus does not support process retirement for flow-instance route %s", identity.Key())
	}
	return retirer.RetirePublishedFlowInstanceRoute(identity)
}

func (am *AgentManager) retireDynamicFlowProcessTopology(flowIdentity runtimeflowidentity.RunScopedFlowInstance) error {
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return err
	}
	var retireErrs []error
	if err := am.retirePublishedDynamicFlowRoute(flowIdentity); err != nil {
		retireErrs = append(retireErrs, err)
	}
	for _, cfg := range am.ListAgentConfigs() {
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			retireErrs = append(retireErrs, err)
			continue
		}
		if identity.RunID != flowIdentity.RunID || identity.FlowInstance() != flowIdentity.Route.InstancePath {
			continue
		}
		if err := am.teardownIdentity(am.runtimeContext(), identity, "teardown"); err != nil && !errors.Is(err, ErrAgentNotFound) {
			retireErrs = append(retireErrs, fmt.Errorf("retire dynamic flow process agent %s: %w", identity.Description(), err))
		}
	}
	return errors.Join(retireErrs...)
}

func (am *AgentManager) loadDynamicFlowPersistedAgents(
	ctx context.Context,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
) (map[runtimeagentidentity.Identity]PersistedAgent, error) {
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return nil, err
	}
	persistedByIdentity := map[runtimeagentidentity.Identity]PersistedAgent{}
	if am.store == nil {
		return persistedByIdentity, nil
	}
	persisted, err := am.store.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range persisted {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		if identity.RunID == flowIdentity.RunID && identity.FlowInstance() == flowIdentity.Route.InstancePath {
			if _, exists := persistedByIdentity[identity]; exists {
				return nil, fmt.Errorf("duplicate persisted agent identity %s", identity.Description())
			}
			persistedByIdentity[identity] = rec
		}
	}
	return persistedByIdentity, nil
}

func (am *AgentManager) reconcileDynamicFlowAgentSet(
	ctx context.Context,
	source semanticview.Source,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
	expected []PersistedAgent,
	persisted map[runtimeagentidentity.Identity]PersistedAgent,
	topologyAuthority runtimeagenttopology.Admission,
) error {
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return err
	}
	expectedIdentities := make(map[runtimeagentidentity.Identity]struct{}, len(expected))
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		expectedIdentities[identity] = struct{}{}
	}
	removedIdentities := make(map[runtimeagentidentity.Identity]struct{})
	for identity := range persisted {
		if _, ok := expectedIdentities[identity]; !ok {
			removedIdentities[identity] = struct{}{}
		}
	}
	for _, cfg := range am.ListAgentConfigs() {
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			return err
		}
		if identity.RunID != flowIdentity.RunID || identity.FlowInstance() != flowIdentity.Route.InstancePath {
			continue
		}
		if _, ok := expectedIdentities[identity]; !ok {
			removedIdentities[identity] = struct{}{}
		}
	}
	orderedRemoved := make([]runtimeagentidentity.Identity, 0, len(removedIdentities))
	for identity := range removedIdentities {
		orderedRemoved = append(orderedRemoved, identity)
	}
	sort.Slice(orderedRemoved, func(i, j int) bool {
		return runtimeagentidentity.Less(orderedRemoved[i], orderedRemoved[j])
	})
	for _, identity := range orderedRemoved {
		if _, live := am.getAgentConfigIdentity(identity); !live {
			stored, found := persisted[identity]
			if !found {
				return fmt.Errorf(
					"removed dynamic flow agent %s has neither process nor durable lifecycle owner",
					identity.Description(),
				)
			}
			if err := am.adoptPersistedAgentLifecycleOnly(ctx, stored); err != nil {
				return fmt.Errorf("adopt removed dynamic flow agent %s: %w", identity.Description(), err)
			}
		}
		if err := am.teardownIdentityWithTopology(ctx, identity, "teardown", &topologyAuthority); err != nil {
			return fmt.Errorf("retire removed dynamic flow agent %s: %w", identity.Description(), err)
		}
		delete(persisted, identity)
	}
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		rec.Topology = topologyAuthority
		if err := am.reconcileDynamicFlowAgent(ctx, source, rec, persisted[identity], &topologyAuthority); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent %s: %w", identity.Description(), err)
		}
	}
	return nil
}

func (am *AgentManager) reconcileDynamicFlowAgent(
	ctx context.Context,
	source semanticview.Source,
	rec PersistedAgent,
	persisted PersistedAgent,
	topology *runtimeagenttopology.Admission,
) error {
	if topology == nil || !rec.Topology.Equal(*topology) {
		return errors.New("dynamic flow agent requires the exact readiness topology admission")
	}
	if err := rec.Topology.Validate(); err != nil {
		return fmt.Errorf("expected dynamic flow agent topology: %w", err)
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	expectedRevision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	existing, present := am.getAgentConfigIdentity(identity)
	actualRevision := ""
	actualTopology := runtimeagenttopology.Admission{}
	if present {
		actualRevision, err = lifecycleConfigRevision(PersistedAgent{Config: existing})
		if err != nil {
			return err
		}
		state, found := am.lifecycle.stateByIdentity(identity)
		if !found {
			return fmt.Errorf("agent %s is process-ready without lifecycle state", identity.Description())
		}
		actualTopology = state.Topology
	}
	persistedIdentity := runtimeagentidentity.Identity{}
	persistedRevision := ""
	if strings.TrimSpace(persisted.Config.ID) != "" {
		persistedIdentity, err = persisted.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if err := persisted.Topology.Validate(); err != nil {
			return fmt.Errorf("persisted dynamic flow agent %s topology: %w", identity.Description(), err)
		}
		persistedRevision, err = lifecycleConfigRevision(persisted)
		if err != nil {
			return err
		}
		if persistedIdentity != identity {
			return fmt.Errorf("agent %s persisted identity changed", identity.Description())
		}
	}
	if present && persistedIdentity.IsZero() {
		return fmt.Errorf("agent %s is process-ready without durable registration", identity.Description())
	}
	if present && (actualRevision != persistedRevision || !actualTopology.Equal(persisted.Topology)) {
		return fmt.Errorf("agent %s process and durable lifecycle facts disagree", identity.Description())
	}
	if !present && !persistedIdentity.IsZero() {
		adopt := am.adoptPersistedAgentForLifecycle
		if persistedRevision != expectedRevision || !persisted.Topology.Equal(rec.Topology) {
			adopt = func(ctx context.Context, _ semanticview.Source, rec PersistedAgent) error {
				return am.adoptPersistedAgentLifecycleOnly(ctx, rec)
			}
		}
		if err := adopt(ctx, source, persisted); err != nil {
			return fmt.Errorf("adopt persisted agent %s: %w", identity.Description(), err)
		}
		present = true
		actualRevision = persistedRevision
		actualTopology = persisted.Topology
	}
	if present {
		if actualRevision == expectedRevision && actualTopology.Equal(rec.Topology) {
			_, err := am.ensureExecutableAgentLifecycle(ctx, identity)
			return err
		}
		if err := am.reconfigureAgentIdentityExactWithTopology(ctx, source, identity, rec.Config, topology); err != nil {
			return err
		}
		_, err := am.ensureExecutableAgentLifecycle(ctx, identity)
		return err
	}
	return am.spawnAgentInternalForSourceWithTopology(ctx, rec, true, source, topology)
}

func dynamicFlowAgentTopologyAuthority(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (runtimeagenttopology.Admission, error) {
	fingerprint, err := canonicaljson.Hash(plan)
	if err != nil {
		return runtimeagenttopology.Admission{}, err
	}
	return runtimeagenttopology.FlowReadinessAdmission(
		strings.TrimSpace(plan.RunID),
		strings.Trim(strings.TrimSpace(plan.Identity.InstancePath), "/"),
		fingerprint,
	)
}

func (am *AgentManager) verifyDynamicFlowAgents(
	ctx context.Context,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
	expected []PersistedAgent,
	topology runtimeagenttopology.Admission,
) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("verify dynamic flow topology admission: %w", err)
	}
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return err
	}
	expectedByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(expected))
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		expectedByIdentity[identity] = rec
	}
	persistedByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(expected))
	if am.store == nil {
		if len(expectedByIdentity) != 0 {
			return fmt.Errorf("declared agents require durable manager persistence")
		}
	} else {
		persisted, err := am.store.LoadAgents(ctx)
		if err != nil {
			return err
		}
		for _, rec := range persisted {
			identity, err := rec.Config.ConcreteIdentity()
			if err != nil {
				return err
			}
			if identity.RunID == flowIdentity.RunID && identity.FlowInstance() == flowIdentity.Route.InstancePath {
				persistedByIdentity[identity] = rec
			}
		}
	}
	processByIdentity := make(map[runtimeagentidentity.Identity]models.AgentConfig, len(expected))
	for _, cfg := range am.ListAgentConfigs() {
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			return err
		}
		if identity.RunID == flowIdentity.RunID && identity.FlowInstance() == flowIdentity.Route.InstancePath {
			processByIdentity[identity] = cfg
		}
	}
	if len(persistedByIdentity) != len(expectedByIdentity) || len(processByIdentity) != len(expectedByIdentity) {
		return fmt.Errorf(
			"declared agent set mismatch: expected=%d persisted=%d process=%d",
			len(expectedByIdentity),
			len(persistedByIdentity),
			len(processByIdentity),
		)
	}
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		expectedRevision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		process, ok := processByIdentity[identity]
		if !ok {
			return fmt.Errorf("declared agent %s is not process-ready", identity.Description())
		}
		processRevision, err := lifecycleConfigRevision(PersistedAgent{Config: process})
		if err != nil {
			return err
		}
		stored, ok := persistedByIdentity[identity]
		if !ok {
			return fmt.Errorf("declared agent %s is not durably registered", identity.Description())
		}
		storedRevision, err := lifecycleConfigRevision(stored)
		if err != nil {
			return err
		}
		readiness, err := am.lifecycle.executableReadinessByIdentity(identity)
		if err != nil {
			return fmt.Errorf("declared agent %s executable readiness: %w", identity.Description(), err)
		}
		if processRevision != expectedRevision || storedRevision != expectedRevision ||
			!readiness.State.Topology.Equal(topology) || !stored.Topology.Equal(topology) {
			return fmt.Errorf(
				"declared agent %s readiness facts mismatch: expected_revision=%s process_revision=%s stored_revision=%s process_topology_equal=%t stored_topology_equal=%t",
				identity.Description(), expectedRevision, processRevision, storedRevision,
				readiness.State.Topology.Equal(topology), stored.Topology.Equal(topology),
			)
		}
	}
	return nil
}

func verifyDynamicFlowAgentExpectations(actual []PersistedAgent, expected []runtimepipeline.DynamicFlowRuntimeAgentExpectation) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("declared agent count changed: expected=%d actual=%d", len(expected), len(actual))
	}
	actualByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(actual))
	for _, rec := range actual {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if _, exists := actualByIdentity[identity]; exists {
			return fmt.Errorf("duplicate declared agent %s", identity.Description())
		}
		actualByIdentity[identity] = rec
	}
	for _, item := range expected {
		rec, ok := actualByIdentity[item.Identity]
		if !ok {
			return fmt.Errorf("declared agent topology missing %s", item.Identity.Description())
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		if revision != item.ConfigRevision {
			return fmt.Errorf("declared agent topology changed at %s: expected_revision=%s actual_revision=%s", item.Identity.Description(), item.ConfigRevision, revision)
		}
	}
	return nil
}

func (am *AgentManager) verifyDynamicFlowRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	verifier := am.roles.RouteVerifier
	if verifier == nil || !verifier.HasFlowInstanceRoute(identity) {
		return fmt.Errorf("dynamic flow route %s is not process-ready", identity.Key())
	}
	if err := verifier.VerifyFlowInstanceRoute(ctx, identity); err != nil {
		return fmt.Errorf("verify dynamic flow route %s: %w", identity.Key(), err)
	}
	return nil
}

func dynamicFlowRuntimeCreationEvent(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (events.Event, error) {
	var empty events.Event
	creation := plan.CreationEvent
	if creation == nil {
		return empty, fmt.Errorf("dynamic flow creation event plan is required")
	}
	routingSource, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID,
	})
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
