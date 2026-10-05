package manager

// Startup admits persisted readiness inventory before execution and verifies the installed topology; it does not infer phase progress.

import (
	"context"
	"errors"
	"fmt"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"sort"
	"strings"
	"time"
)

// DynamicFlowRuntimeStartupReadiness is the one-startup-attempt authority for
// pending rows admitted by exact source transition or explicit recovery.
type DynamicFlowRuntimeStartupReadiness struct {
	sourceFact         runtimecorrelation.SourceArtifactFact
	replayAllowed      bool
	authorizedPending  map[dynamicFlowRuntimeReadinessKey]struct{}
	completedBeforeRun map[dynamicFlowRuntimeReadinessKey]uint64
	empty              bool
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

func (am *AgentManager) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if am == nil || am.workflowInstances == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, nil
	}
	projection, err := am.workflowInstances.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	return FilterDynamicFlowRuntimeReadiness(ctx, projection, am.roles.StandingRestarts, func(ctx context.Context, runID string) (bool, error) {
		ownership, err := am.inspectRunExecutionOwnership(ctx, runID)
		return ownership == RunExecutionOwned, err
	})
}

// FilterDynamicFlowRuntimeReadiness consumes source applicability at inspection,
// and actual grant ownership at boot. Its result never supplies either grant.
func FilterDynamicFlowRuntimeReadiness(ctx context.Context, projection runtimepipeline.DynamicFlowRuntimeReadinessProjection, restarts runtimestanding.StandingRestartDispositionReader, applicable func(context.Context, string) (bool, error)) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if restarts == nil || applicable == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, errors.New("dynamic flow runtime readiness requires standing restart and source applicability readers")
	}
	cache := make(map[string]runtimestanding.StandingRestartDisposition)
	// This read-only inspection shares observations, never mutation authority.
	ownershipCache := make(map[string]bool)
	filter := func(items []runtimepipeline.DynamicFlowRuntimeReadiness) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
		filtered := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(items))
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			runID := strings.TrimSpace(item.Plan.RunID)
			ownership, ok := ownershipCache[runID]
			if !ok {
				var err error
				ownership, err = applicable(ctx, runID)
				if err != nil {
					return nil, fmt.Errorf("admit dynamic flow readiness run %s: %w", runID, err)
				}
				ownershipCache[runID] = ownership
			}
			if !ownership {
				continue
			}
			disposition, ok := cache[runID]
			if !ok {
				var err error
				disposition, err = restarts.StandingRunRestartDisposition(ctx, runID)
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
	var err error
	if projection.CurrentCompleted, err = filter(projection.CurrentCompleted); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if projection.CurrentPending, err = filter(projection.CurrentPending); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if projection.SourceTransitionRequired, err = filter(projection.SourceTransitionRequired); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	if err := ctx.Err(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	return projection, nil
}

func (am *AgentManager) CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx context.Context, sourceFact runtimecorrelation.SourceArtifactFact, replayAllowed bool) (DynamicFlowRuntimeStartupReadiness, error) {
	startup := DynamicFlowRuntimeStartupReadiness{
		sourceFact: sourceFact, replayAllowed: replayAllowed,
		authorizedPending:  make(map[dynamicFlowRuntimeReadinessKey]struct{}),
		completedBeforeRun: make(map[dynamicFlowRuntimeReadinessKey]uint64),
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
	for _, item := range projection.CurrentCompleted {
		key, keyErr := newDynamicFlowRuntimeReadinessKey(item.Plan.RunID, item.InstancePath)
		if keyErr != nil {
			return DynamicFlowRuntimeStartupReadiness{}, keyErr
		}
		startup.completedBeforeRun[key] = item.AttemptOrdinal
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

// PrepareAdmittedDynamicFlowAgentsForStart transfers pre-run declarations to
// the exact admitted attempt before Manager.Run upgrades their lifecycle cells.
// Route publication and durable topology completion remain post-Run work.
func (am *AgentManager) PrepareAdmittedDynamicFlowAgentsForStart(ctx context.Context) error {
	if am == nil || am.lifecycle == nil {
		return errors.New("admitted dynamic flow preparation requires manager lifecycle")
	}
	if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
		return errors.New("admitted dynamic flow preparation requires a stopped manager")
	}
	prepared := make(map[dynamicFlowRuntimeReadinessKey]uint64)
	preparedAgents := make(map[dynamicFlowRuntimeReadinessKey][]runtimeagentidentity.Identity)
	for _, identity := range am.lifecycle.executionIdentities() {
		state, found := am.lifecycle.stateByIdentity(identity)
		if !found {
			return fmt.Errorf("prepared dynamic flow agent %s has no lifecycle cell", identity.Description())
		}
		authority := state.Topology.Authority
		if authority.Kind != runtimeagenttopology.AuthorityFlowReadinessPlan || authority.Readiness == nil || !authority.Readiness.Preparation {
			continue
		}
		if authority.Readiness.RunID != identity.RunID ||
			state.Phase != AgentLifecycleRegistered || state.RunMode != AgentRunModeStopped {
			return fmt.Errorf("dynamic flow agent %s is not an exact stopped preparation", identity.Description())
		}
		key, err := newDynamicFlowRuntimeReadinessKey(authority.Readiness.RunID, authority.Readiness.InstancePath)
		if err != nil {
			return err
		}
		ordinal, err := runtimeflowidentity.ParseActivationAttemptID(authority.Readiness.AttemptID)
		if err != nil {
			return err
		}
		if previous, exists := prepared[key]; exists && previous != ordinal {
			return fmt.Errorf("dynamic flow %s has mixed preparation attempts", key.instancePath)
		}
		prepared[key] = ordinal
		preparedAgents[key] = append(preparedAgents[key], identity)
	}
	if len(prepared) == 0 {
		return nil
	}
	if am.workflowInstances == nil {
		return errors.New("admitted dynamic flow preparation requires workflow store")
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	keys := make([]dynamicFlowRuntimeReadinessKey, 0, len(prepared))
	for key := range prepared {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].runID != keys[j].runID {
			return keys[i].runID < keys[j].runID
		}
		return keys[i].instancePath < keys[j].instancePath
	})
	for _, key := range keys {
		readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
		if err != nil {
			return err
		}
		if !found || !readiness.Eligible() || readiness.AttemptOrdinal != prepared[key] {
			return fmt.Errorf("dynamic flow %s preparation is no longer current", key.instancePath)
		}
		plan, err := readiness.Plan.Normalized()
		if err != nil {
			return err
		}
		if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, source); err != nil {
			return err
		}
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
		if err != nil {
			return err
		}
		for _, identity := range preparedAgents[key] {
			if !owner.MatchesAgentRoute(identity) {
				return fmt.Errorf("dynamic flow agent %s does not match its preparation owner", identity.Description())
			}
			declared := false
			for _, expected := range plan.Agents {
				if expected.Identity == identity {
					declared = true
					break
				}
			}
			if !declared {
				return fmt.Errorf("dynamic flow agent %s is absent from its current preparation plan", identity.Description())
			}
		}
		if err := am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
			ctx: ctx, key: key, plan: plan, attemptOrdinal: readiness.AttemptOrdinal, observed: &readiness, source: source,
			processOnly: true, admittedPreRun: true,
		}); err != nil {
			return fmt.Errorf("admit prepared dynamic flow %s before manager run: %w", key.instancePath, err)
		}
	}
	if am.lifecycle.phaseSnapshot() != runtimeLifecycleStopped {
		return errors.New("admitted dynamic flow preparation crossed manager run admission")
	}
	return nil
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
			am.dynamicFlowReadinessMu.Lock()
			active := am.dynamicFlowActiveAttempts[key]
			preRun := active != nil && active.admittedPreRun && active.receipt.Ordinal() == item.AttemptOrdinal
			predecessor := uint64(0)
			if preRun {
				predecessor = active.preRunPredecessorOrdinal
			}
			am.dynamicFlowReadinessMu.Unlock()
			// The acknowledged admission may have issued a successor after exact
			// predecessor retirement; the startup snapshot cannot name that successor.
			if predecessor == 0 || startup.completedBeforeRun[key] != predecessor || !preRun {
				return fmt.Errorf("dynamic flow startup topology lacks pending authorization for %s", item.InstancePath)
			}
		}
	}
	for _, item := range prepare {
		if err := am.reconcileDynamicFlowRuntimeReadinessItem(ctx, item, source, true, false); err != nil {
			return &dynamicFlowRuntimeReadinessFinalizationError{cause: fmt.Errorf("prepare dynamic flow process topology %s: %w", item.InstancePath, err)}
		}
	}
	for _, item := range prepare {
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
	ordinal := item.AttemptOrdinal
	observed := &item
	if processPrepared {
		am.dynamicFlowReadinessMu.Lock()
		active := am.dynamicFlowActiveAttempts[key]
		if active == nil || active.retiring || active.publication == nil {
			am.dynamicFlowReadinessMu.Unlock()
			return fmt.Errorf("dynamic flow %s has no retained prepared topology", item.InstancePath)
		}
		ordinal = active.receipt.Ordinal()
		am.dynamicFlowReadinessMu.Unlock()
		// Preparation can accept a planned attempt or issue a successor after
		// predecessor cleanup. Finalization observes that exact retained receipt,
		// not the pre-preparation snapshot; admission still verifies the store row.
		observed = nil
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: plan, attemptOrdinal: ordinal, observed: observed, source: source,
		processOnly: processOnly, previouslyCompleted: (item.Phase == runtimepipeline.FlowAttachmentReady), processPrepared: processPrepared,
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
	topologyAuthority, err := DynamicFlowAgentTopologyAdmission(plan)
	if err != nil {
		return err
	}
	activeKey := dynamicFlowRuntimeReadinessKey{runID: plan.RunID, instancePath: plan.Identity.InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	active := am.dynamicFlowActiveAttempts[activeKey]
	am.dynamicFlowReadinessMu.Unlock()
	if active == nil {
		return errors.New("completed dynamic flow has no current process activation attempt")
	}
	topologyAuthority, err = topologyAuthority.WithFlowActivationAttempt(active.receipt.ID())
	if err != nil {
		return err
	}
	if err := am.verifyDynamicFlowAgents(ctx, flowIdentity, records, topologyAuthority); err != nil {
		return err
	}
	return am.verifyDynamicFlowRoute(ctx, flowIdentity)
}
