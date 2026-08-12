package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type workflowTimerCauseKind string

const (
	workflowTimerCauseInitial    workflowTimerCauseKind = "initial_stage"
	workflowTimerCauseEvent      workflowTimerCauseKind = "event"
	workflowTimerCauseTransition workflowTimerCauseKind = "transition"
)

type workflowTimerCause struct {
	Kind          workflowTimerCauseKind
	EventID       string
	EventType     string
	OccurredAt    time.Time
	TransitionID  string
	FromState     string
	ToState       string
	ExecutionMode executionmode.Mode
}

func (c workflowTimerCause) normalized() workflowTimerCause {
	c.Kind = workflowTimerCauseKind(strings.TrimSpace(string(c.Kind)))
	c.EventID = strings.TrimSpace(c.EventID)
	c.EventType = strings.TrimSpace(c.EventType)
	c.OccurredAt = canonicalWorkflowTimerTime(c.OccurredAt)
	c.TransitionID = strings.TrimSpace(c.TransitionID)
	c.FromState = strings.TrimSpace(c.FromState)
	c.ToState = strings.TrimSpace(c.ToState)
	c.ExecutionMode = executionmode.Mode(strings.TrimSpace(string(c.ExecutionMode)))
	return c
}

func (c workflowTimerCause) validateForActivation() error {
	c = c.normalized()
	if c.OccurredAt.IsZero() {
		return fmt.Errorf("workflow timer activation requires an exact causal time")
	}
	if !c.ExecutionMode.Valid() {
		return fmt.Errorf("workflow timer activation requires an exact causal execution mode")
	}
	switch c.Kind {
	case workflowTimerCauseInitial:
		if c.ToState == "" {
			return fmt.Errorf("initial workflow timer activation requires the initial state")
		}
	case workflowTimerCauseEvent:
		if c.EventID == "" || c.EventType == "" {
			return fmt.Errorf("event workflow timer activation requires exact event identity")
		}
	case workflowTimerCauseTransition:
		if c.EventID == "" || c.TransitionID == "" || c.ToState == "" {
			return fmt.Errorf("transition workflow timer activation requires event and transition identity")
		}
	default:
		return fmt.Errorf("workflow timer activation has unsupported cause %q", c.Kind)
	}
	return nil
}

type WorkflowTimerFireOutcome string

const (
	WorkflowTimerFireCommitted WorkflowTimerFireOutcome = "committed"
	WorkflowTimerFireRetry     WorkflowTimerFireOutcome = "retry"
	WorkflowTimerFireTerminal  WorkflowTimerFireOutcome = "terminal"
)

// WorkflowTimerLifecycle owns workflow activation identity, row transitions,
// scheduler projection, fire publication, restore, and handler authorization.
type WorkflowTimerLifecycle struct {
	storeOwner   *workflowInstanceStore
	source       semanticview.Source
	logger       systemNodeRuntimeLogger
	workOwner    worklifetime.Occurrence
	scheduler    *Scheduler
	publication  EnginePublicationPlanner
	dispatcher   runtimeengine.PostCommitDispatcher
	posture      executionposture.Posture
	recoveryCtx  context.Context
	cancel       context.CancelFunc
	projectionMu sync.Mutex
	recoveryMu   sync.Mutex
	recovering   map[string]chan struct{}
	stopped      bool

	wakeupCallbackTimeout time.Duration
	testAfterWakeupLoad   func()
}

func newWorkflowTimerLifecycle(store *workflowInstanceStore, source semanticview.Source, bus Bus, workOwner worklifetime.Occurrence, scheduler *Scheduler, posture executionposture.Posture) *WorkflowTimerLifecycle {
	if store == nil {
		return nil
	}
	if !posture.Valid() {
		panic("pipeline: workflow timer lifecycle requires a valid execution posture")
	}
	recoveryCtx, cancel := context.WithCancel(context.Background())
	lifecycle := &WorkflowTimerLifecycle{
		storeOwner:            store,
		source:                source,
		logger:                bus,
		workOwner:             workOwner,
		posture:               posture,
		recoveryCtx:           recoveryCtx,
		cancel:                cancel,
		recovering:            make(map[string]chan struct{}),
		wakeupCallbackTimeout: workflowTimerWakeupCallbackTimeout,
	}
	lifecycle.publication, _ = bus.(EnginePublicationPlanner)
	if provider, ok := bus.(interface {
		EngineDispatcher() runtimeengine.PostCommitDispatcher
	}); ok {
		lifecycle.dispatcher = provider.EngineDispatcher()
	}
	if scheduler != nil {
		if err := lifecycle.bindScheduler(scheduler); err != nil {
			panic(fmt.Sprintf("pipeline: bind workflow timer lifecycle: %v", err))
		}
	}
	return lifecycle
}

func (pc *PipelineCoordinator) RestoreWorkflowTimers(ctx context.Context) error {
	if pc == nil || pc.workflowTimers == nil {
		return nil
	}
	return pc.workflowTimers.Restore(ctx)
}

func (pc *PipelineCoordinator) StopWorkflowTimerLifecycle(ctx context.Context) error {
	if pc == nil || pc.workflowTimers == nil {
		return nil
	}
	return pc.workflowTimers.stop(ctx)
}

func (l *WorkflowTimerLifecycle) store() *workflowInstanceStore {
	if l == nil {
		return nil
	}
	return l.storeOwner
}

func (l *WorkflowTimerLifecycle) ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	active, err := l.initialEntryTimerActivations(ctx, route)
	if err != nil {
		return err
	}
	for _, activation := range active {
		if err := l.queueWakeupReconcile(ctx, activation.Ref); err != nil {
			return err
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) reconcileInitialEntryDeclarations(ctx context.Context, route runtimeflowidentity.Route) error {
	store := l.store()
	if store == nil || !store.enabled() {
		return nil
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return fmt.Errorf("workflow initial timer reconciliation requires instance identity")
	}
	instance, found, err := store.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("workflow initial timer reconciliation instance %s is missing", route.InstancePath)
	}
	source := l.source
	if source == nil {
		return fmt.Errorf("workflow initial timer reconciliation requires semantic source")
	}
	entityID, err := workflowInstancePersistedEntityID(instance)
	if err != nil {
		return fmt.Errorf("validate workflow initial timer reconciliation owner: %w", err)
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, instance); err != nil {
		return fmt.Errorf("validate workflow initial timer reconciliation owner: %w", err)
	}
	runID := workflowTimerRunID(ctx, instance)
	if entityID.IsZero() || runID == "" {
		return fmt.Errorf("workflow initial timer reconciliation requires exact run and entity identity")
	}
	active, err := store.listPersistedWorkflowTimerActivations(ctx, runID, entityID.String(), true)
	if err != nil {
		return err
	}
	var readinessMode executionmode.Mode
	if store.readiness != nil {
		readiness, found, err := store.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
		if err != nil {
			return err
		}
		if found {
			readinessMode = readiness.Plan.ExecutionMode
		}
	}
	currentState := strings.TrimSpace(instance.CurrentState)
	initialState := strings.TrimSpace(workflowInitialStateForFlow(source, instance.WorkflowName))
	if initialState == "" {
		initialState = strings.TrimSpace(source.WorkflowInitialStage())
	}
	initialEntryOpen := currentState == initialState
	desired := map[string]WorkflowTimerActivation{}
	if initialEntryOpen {
		generation, _, err := workflowLoopGenerationForStage(source, &instance, currentState)
		if err != nil {
			return err
		}
		var initialMode executionmode.Mode
		for _, declaration := range workflowTimerDeclarationsForInstance(source, instance) {
			if !workflowTimerShouldStartOnTransition(declaration, "", currentState, "state:"+currentState) {
				continue
			}
			if !initialMode.Valid() {
				initialMode, err = initialWorkflowTimerExecutionMode(ctx, readinessMode, active)
				if err != nil {
					return err
				}
			}
			cause := workflowTimerCause{
				Kind:          workflowTimerCauseInitial,
				ExecutionMode: initialMode,
				EventType:     "state:" + currentState,
				OccurredAt:    instance.CreatedAt,
				ToState:       currentState,
			}
			if err := cause.validateForActivation(); err != nil {
				return err
			}
			if err := l.posture.Admit(cause.ExecutionMode, "initial workflow timer activation reconciliation"); err != nil {
				return err
			}
			if err := validateWorkflowTimerTopology(source, declaration); err != nil {
				return err
			}
			interval := workflowTimerDuration(declaration, workflowTimerPolicy(source, declaration.FlowID))
			if interval <= 0 {
				return fmt.Errorf("workflow timer %s has no executable positive delay", declaration.ID)
			}
			activation, err := workflowTimerActivationForCause(
				source,
				runID,
				entityID.String(),
				route,
				declaration,
				generation,
				cause,
				interval,
			)
			if err != nil {
				return err
			}
			desired[activation.Ref.ActivationID] = activation
		}
	}

	plan := WorkflowLifecycleMutationPlan{}
	unchanged := make([]timeridentity.WorkflowTimerActivationRef, 0, len(active))
	for _, activation := range active {
		if activation.Ref.Cause != timeridentity.WorkflowTimerActivationCauseInitial {
			continue
		}
		expected, keep := desired[activation.Ref.ActivationID]
		if !keep && !initialEntryOpen {
			// Source revisions are prospective after the initial-entry edge has passed.
			declaration, found := workflowTimerDeclarationForInstance(
				source,
				instance,
				activation.Ref.Declaration,
			)
			if found &&
				workflowTimerShouldStartOnTransition(
					declaration,
					"",
					initialState,
					"state:"+initialState,
				) {
				if err := validateWorkflowTimerTopology(source, declaration); err != nil {
					return err
				}
				interval := workflowTimerDuration(
					declaration,
					workflowTimerPolicy(source, declaration.FlowID),
				)
				if interval <= 0 {
					return fmt.Errorf(
						"workflow timer %s has no executable positive delay",
						declaration.ID,
					)
				}
				expected, err = workflowTimerActivationForCause(
					source,
					runID,
					entityID.String(),
					route,
					declaration,
					activation.Ref.Generation,
					workflowTimerCause{
						Kind:          workflowTimerCauseInitial,
						ExecutionMode: activation.ExecutionMode,
						EventType:     "state:" + initialState,
						OccurredAt:    activation.CreatedAt,
						ToState:       initialState,
					},
					interval,
				)
				if err != nil {
					return err
				}
				keep = expected.Ref == activation.Ref
			}
		}
		if keep {
			if err := requireSameWorkflowTimerActivationFacts(activation, expected); err != nil {
				return err
			}
			delete(desired, activation.Ref.ActivationID)
			unchanged = append(unchanged, activation.Ref)
			continue
		}
		plan.Timers = append(plan.Timers, WorkflowTimerMutation{Kind: WorkflowTimerMutationCancel, Activation: activation})
		plan.RequestCompletionCandidate = true
	}

	activationIDs := make([]string, 0, len(desired))
	for activationID := range desired {
		activationIDs = append(activationIDs, activationID)
	}
	sort.Strings(activationIDs)
	for _, activationID := range activationIDs {
		plan.Timers = append(plan.Timers, WorkflowTimerMutation{Kind: WorkflowTimerMutationInsert, Activation: desired[activationID]})
	}
	committed := CommittedWorkflowLifecycleMutation{}
	if len(plan.Timers) != 0 {
		if store.timerActivations == nil {
			return fmt.Errorf("workflow timer reconciliation owner is required")
		}
		committed, err = store.timerActivations.CommitWorkflowTimerReconciliation(ctx, WorkflowTimerReconciliationCommand{
			RunID: runID, Route: route, EntityID: entityID.String(), Plan: plan,
		})
		if err != nil {
			return err
		}
	}
	for _, ref := range append(append(unchanged, committed.Wakeups...), committed.Cancellations...) {
		if err := l.queueWakeupReconcile(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

func initialWorkflowTimerExecutionMode(ctx context.Context, readinessMode executionmode.Mode, active []WorkflowTimerActivation) (executionmode.Mode, error) {
	if readinessMode != "" && !readinessMode.Valid() {
		return "", fmt.Errorf("workflow initial timer readiness has invalid execution mode")
	}
	var persisted executionmode.Mode
	for _, activation := range active {
		if activation.Ref.Cause != timeridentity.WorkflowTimerActivationCauseInitial {
			continue
		}
		mode := activation.ExecutionMode
		if !mode.Valid() {
			return "", fmt.Errorf("workflow initial timer %s has invalid execution mode", activation.Ref.ActivationID)
		}
		if persisted.Valid() && persisted != mode {
			return "", fmt.Errorf("workflow initial timers disagree on execution mode")
		}
		persisted = mode
	}
	if readinessMode.Valid() {
		if persisted.Valid() && persisted != readinessMode {
			return "", fmt.Errorf("workflow initial timers disagree with durable readiness execution mode")
		}
		return readinessMode, nil
	}
	if persisted.Valid() {
		return persisted, nil
	}
	if mode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok && mode.Valid() {
		return mode, nil
	}
	return "", fmt.Errorf("workflow initial timer reconciliation requires typed execution mode authority")
}

func (l *WorkflowTimerLifecycle) RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error {
	active, err := l.initialEntryTimerActivations(ctx, route)
	if err != nil {
		return err
	}
	refs := make([]timeridentity.WorkflowTimerActivationRef, 0, len(active))
	for _, activation := range active {
		refs = append(refs, activation.Ref)
	}
	if len(refs) == 0 {
		return nil
	}
	if l.scheduler == nil {
		return errWorkflowTimerSchedulerRequired
	}
	return l.scheduler.retireWorkflowTimerWakeups(ctx, refs)
}

func (l *WorkflowTimerLifecycle) initialEntryTimerActivations(ctx context.Context, route runtimeflowidentity.Route) ([]WorkflowTimerActivation, error) {
	store := l.store()
	if store == nil || !store.enabled() {
		return nil, nil
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return nil, fmt.Errorf("workflow initial timer activation requires instance identity")
	}
	instance, found, err := store.Load(ctx, route)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("workflow initial timer activation instance %s is missing", route.InstancePath)
	}
	entityID, err := workflowInstancePersistedEntityID(instance)
	if err != nil {
		return nil, fmt.Errorf("validate workflow initial timer activation owner: %w", err)
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, instance); err != nil {
		return nil, fmt.Errorf("validate workflow initial timer activation owner: %w", err)
	}
	runID := workflowTimerRunID(ctx, instance)
	if entityID.IsZero() || runID == "" {
		return nil, fmt.Errorf("workflow initial timer activation requires exact run and entity identity")
	}
	active, err := store.listPersistedWorkflowTimerActivations(ctx, runID, entityID.String(), true)
	if err != nil {
		return nil, err
	}
	initial := make([]WorkflowTimerActivation, 0, len(active))
	for _, activation := range active {
		if activation.Ref.Cause == timeridentity.WorkflowTimerActivationCauseInitial {
			initial = append(initial, activation)
		}
	}
	return initial, nil
}

func validateWorkflowTimerTopology(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract) error {
	if timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
		return fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
	}
	if workflowTimerLeavesBoundedLoop(source, timer) {
		return fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
	}
	return nil
}

func workflowTimerGenerationKey(declaration string, generation attemptgeneration.Generation) string {
	return strings.TrimSpace(declaration) + "\x00" + generation.Normalize().KeySuffix()
}

func workflowTimerActivationForCause(
	source semanticview.Source,
	runID, entityID string,
	route runtimeflowidentity.Route,
	declaration runtimecontracts.WorkflowTimerContract,
	generation attemptgeneration.Generation,
	cause workflowTimerCause,
	interval time.Duration,
) (WorkflowTimerActivation, error) {
	cause = cause.normalized()
	generation = generation.Normalize()
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer activation requires an exact route")
	}
	declarationRevision, err := workflowTimerDeclarationRevision(source, declaration)
	if err != nil {
		return WorkflowTimerActivation{}, err
	}
	activationCause := timeridentity.WorkflowTimerActivationCause(strings.TrimSpace(string(cause.Kind)))
	routingSource, admittedEvent, err := workflowTimerDeclarationSourceEvent(
		source, entityID, route.InstancePath, declaration,
	)
	if err != nil {
		return WorkflowTimerActivation{}, err
	}
	activationID := timeridentity.WorkflowTimerActivationID(
		runID,
		entityID,
		route.InstancePath,
		declaration.ID,
		declarationRevision,
		string(activationCause),
		generation.KeySuffix(),
		cause.EventID,
		cause.EventType,
		cause.TransitionID,
		cause.FromState,
		cause.ToState,
	)
	return WorkflowTimerActivation{
		Ref: timeridentity.WorkflowTimerActivationRef{
			ActivationID:        activationID,
			Declaration:         strings.TrimSpace(declaration.ID),
			DeclarationRevision: declarationRevision,
			Cause:               activationCause,
			Generation:          generation,
		},
		RunID:         strings.TrimSpace(runID),
		EntityID:      strings.TrimSpace(entityID),
		Route:         route,
		RoutingSource: routingSource,
		OwnerAgent:    strings.TrimSpace(declaration.Owner),
		EventType:     string(admittedEvent),
		ExecutionMode: cause.ExecutionMode,
		Payload:       []byte("{}"),
		FireAt:        canonicalWorkflowTimerTime(cause.OccurredAt.Add(interval)),
		Recurring:     declaration.Recurring,
		RecurrenceInterval: func() time.Duration {
			if declaration.Recurring {
				return interval
			}
			return 0
		}(),
		Status:    workflowTimerStatusActive,
		CreatedAt: cause.OccurredAt,
	}.normalized(), nil
}

func workflowTimerDeclarationRevision(
	source semanticview.Source,
	declaration runtimecontracts.WorkflowTimerContract,
) (string, error) {
	if source == nil {
		return "", fmt.Errorf("workflow timer declaration revision requires semantic source")
	}
	renderedDelay := workflowTimerRenderedDelay(
		declaration.Delay,
		workflowTimerPolicy(source, declaration.FlowID),
	)
	revision, err := canonicaljson.Hash(struct {
		ID           string `json:"id"`
		Stage        string `json:"stage"`
		Event        string `json:"event"`
		Owner        string `json:"owner"`
		FlowID       string `json:"flow_id"`
		NodeID       string `json:"node_id"`
		StageOwned   bool   `json:"stage_owned"`
		AdvancesTo   string `json:"advances_to"`
		Action       string `json:"action"`
		Cancellation string `json:"cancellation"`
		Delay        string `json:"delay"`
		StartOn      string `json:"start_on"`
		CancelOn     string `json:"cancel_on"`
		Recurring    bool   `json:"recurring"`
	}{
		ID:           strings.TrimSpace(declaration.ID),
		Stage:        strings.TrimSpace(declaration.Stage),
		Event:        strings.TrimSpace(declaration.Event),
		Owner:        strings.TrimSpace(declaration.Owner),
		FlowID:       strings.TrimSpace(declaration.FlowID),
		NodeID:       strings.TrimSpace(declaration.NodeID),
		StageOwned:   declaration.StageOwned,
		AdvancesTo:   strings.TrimSpace(declaration.AdvancesTo),
		Action:       strings.TrimSpace(declaration.Action),
		Cancellation: strings.TrimSpace(declaration.Cancellation),
		Delay:        strings.TrimSpace(renderedDelay),
		StartOn:      strings.TrimSpace(declaration.StartOn),
		CancelOn:     strings.TrimSpace(declaration.CancelOn),
		Recurring:    declaration.Recurring,
	})
	if err != nil {
		return "", fmt.Errorf("derive workflow timer declaration revision %s: %w", declaration.ID, err)
	}
	return revision, nil
}

func workflowTimerGenerationPresent(items []attemptgeneration.Generation, target attemptgeneration.Generation) bool {
	for _, item := range items {
		if item.Equal(target) {
			return true
		}
	}
	return false
}

func (l *WorkflowTimerLifecycle) bindScheduler(scheduler *Scheduler) error {
	if l == nil {
		return errors.New("workflow timer lifecycle is required")
	}
	if scheduler == nil {
		return errors.New("workflow timer scheduler is required")
	}
	l.projectionMu.Lock()
	defer l.projectionMu.Unlock()
	if l.stopped {
		return errors.New("workflow timer lifecycle is stopped")
	}
	if l.scheduler != nil {
		return errors.New("workflow timer lifecycle already has a scheduler")
	}
	if err := scheduler.bindWorkflowTimerLifecycle(l.handleWakeup); err != nil {
		return err
	}
	l.scheduler = scheduler
	return nil
}

// ReconcileWakeup is the sole durable-to-process projection boundary. Every
// caller reloads canonical state and either installs its exact active occurrence
// or retires the local key under the same lifecycle fence.
func (l *WorkflowTimerLifecycle) ReconcileWakeup(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
	if l == nil {
		return nil
	}
	ref = ref.Normalize()
	if !ref.Valid() {
		return errors.New("workflow timer wakeup reconciliation requires exact activation identity")
	}
	l.projectionMu.Lock()
	defer l.projectionMu.Unlock()
	if l.stopped {
		return l.retireWakeup(ref)
	}
	store := l.store()
	if store == nil || !store.enabled() {
		return nil
	}
	activation, found, err := store.loadPersistedWorkflowTimerActivation(ctx, ref.ActivationID)
	if err != nil {
		return err
	}
	if l.testAfterWakeupLoad != nil {
		l.testAfterWakeupLoad()
	}
	if !found || activation.Ref != ref || activation.Status != workflowTimerStatusActive {
		return l.retireWakeup(ref)
	}
	if err := l.posture.Admit(activation.ExecutionMode, "workflow timer wakeup projection"); err != nil {
		return err
	}
	current, err := l.workflowTimerActivationDeclarationCurrent(ctx, activation)
	if err != nil {
		return err
	}
	if !current {
		return l.retireWakeup(ref)
	}
	wakeup, err := newWorkflowTimerWakeup(activation)
	if err != nil {
		return err
	}
	return l.registerWakeup(ctx, wakeup)
}

// ReconcileWakeupWithRecovery attempts the exact process projection once and,
// on failure, enters the lifecycle's existing coalesced recovery loop.
func (l *WorkflowTimerLifecycle) ReconcileWakeupWithRecovery(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) (bool, error) {
	if err := l.ReconcileWakeup(ctx, ref); err != nil {
		return l.startWakeupRecovery(ref), err
	}
	return false, nil
}

func (l *WorkflowTimerLifecycle) reconcileWakeupImmediately(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(attempt) * 20 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := l.ReconcileWakeup(ctx, ref); err != nil {
			last = err
			if errors.Is(err, errWorkflowTimerSchedulerRequired) {
				return err
			}
			continue
		}
		return nil
	}
	return last
}

func (l *WorkflowTimerLifecycle) queueWakeupReconcile(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
	if l == nil {
		return nil
	}
	if err := l.reconcileWakeupImmediately(ctx, ref); err != nil {
		l.logFailure(ctx, "workflow_timer_reconcile_failed", ref, err)
		if !errors.Is(err, errWorkflowTimerSchedulerRequired) {
			l.startWakeupRecovery(ref)
		}
		return nil
	}
	return nil
}

func (l *WorkflowTimerLifecycle) queueCancellation(ctx context.Context, activation WorkflowTimerActivation) error {
	return l.queueWakeupReconcile(ctx, activation.Ref)
}

func (l *WorkflowTimerLifecycle) fireWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) (WorkflowTimerFireOutcome, bool, error) {
	store := l.store()
	if store == nil || !store.enabled() {
		return WorkflowTimerFireTerminal, false, fmt.Errorf("workflow timer lifecycle store is unavailable")
	}
	if store.timerOccurrences == nil {
		return WorkflowTimerFireTerminal, false, fmt.Errorf("workflow timer occurrence owner is unavailable")
	}
	if l.publication == nil || l.dispatcher == nil {
		return WorkflowTimerFireTerminal, false, fmt.Errorf("workflow timer publication owners are unavailable")
	}
	if err := wakeup.validate(); err != nil {
		return WorkflowTimerFireTerminal, false, err
	}
	occurrence := wakeup.Occurrence()
	hint, found, err := store.loadPersistedWorkflowTimerActivation(ctx, occurrence.Activation.ActivationID)
	if err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if !found {
		return WorkflowTimerFireTerminal, false, nil
	}
	if hint.Status != workflowTimerStatusActive {
		return WorkflowTimerFireTerminal, false, nil
	}
	if hint.Ref != occurrence.Activation {
		return WorkflowTimerFireTerminal, false, fmt.Errorf("workflow timer wakeup does not match the persisted active coordinate")
	}
	if !hint.FireAt.Equal(occurrence.DueAt) {
		return WorkflowTimerFireTerminal, false, nil
	}
	if err := l.posture.Admit(hint.ExecutionMode, "workflow timer occurrence preparation"); err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	current, err := l.workflowTimerActivationDeclarationCurrent(ctx, hint)
	if err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if !current {
		if retireErr := l.retireWakeup(occurrence.Activation); retireErr != nil {
			return WorkflowTimerFireRetry, false, retireErr
		}
		return WorkflowTimerFireTerminal, false, nil
	}
	ctx = runtimecorrelation.WithRunID(ctx, hint.RunID)
	firedAt := canonicalWorkflowTimerTime(time.Now())
	if firedAt.Before(occurrence.DueAt) {
		return WorkflowTimerFireRetry, false, fmt.Errorf(
			"workflow timer wakeup for %s arrived before its due coordinate",
			occurrence.Activation.ActivationID,
		)
	}
	eventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
	evt, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		Facts: events.EventFacts{
			ID: eventID, Type: events.EventType(hint.EventType),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime.workflow_timer"},
			TaskID:   occurrence.TaskID(), Payload: json.RawMessage(append([]byte(nil), hint.Payload...)),
			Envelope:      events.EventEnvelope{EntityID: hint.EntityID, FlowInstance: hint.Route.InstancePath},
			RoutingSource: hint.RoutingSource, CreatedAt: firedAt, ExecutionMode: hint.ExecutionMode,
		},
		RunID: hint.RunID,
	})
	if err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	intent := runtimeengine.EmitIntent{Event: evt}
	plans, err := l.publication.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{intent})
	if err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if len(plans) != 1 {
		releaseErr := l.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return WorkflowTimerFireRetry, false, errors.Join(fmt.Errorf("workflow timer publication planner returned %d plans", len(plans)), releaseErr)
	}
	committed, err := store.timerOccurrences.CommitWorkflowTimerOccurrence(ctx, WorkflowTimerOccurrenceCommand{
		Activation:  hint,
		Occurrence:  occurrence,
		FiredAt:     firedAt,
		Publication: plans[0],
	})
	if err != nil {
		err = errors.Join(err, l.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
		recoveryCtx := ctx
		if registerErr := l.reconcileWakeupImmediately(recoveryCtx, occurrence.Activation); registerErr != nil {
			l.logFailure(recoveryCtx, "workflow_timer_register_failed", occurrence.Activation, registerErr)
			l.startWakeupRecovery(occurrence.Activation)
			return WorkflowTimerFireRetry, false, errors.Join(err, fmt.Errorf("re-register workflow timer: %w", registerErr))
		}
		return WorkflowTimerFireRetry, false, err
	}
	if committed.Outcome == WorkflowTimerOccurrenceTerminal {
		if err := l.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), plans); err != nil {
			return WorkflowTimerFireRetry, false, err
		}
		return WorkflowTimerFireTerminal, false, nil
	}
	if err := l.publication.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{committed.Publication}); err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if err := l.dispatcher.DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if committed.Next.Status != workflowTimerStatusActive {
		return WorkflowTimerFireCommitted, false, nil
	}
	return WorkflowTimerFireCommitted, true, nil
}

func (l *WorkflowTimerLifecycle) AuthorizeAcceptedEvent(ctx context.Context, evt events.Event) (WorkflowTimerActivation, timeridentity.WorkflowTimerOccurrenceRef, bool, error) {
	exactProducer := evt.ProducerType() == events.EventProducerPlatform && evt.SourceAgent() == "runtime.workflow_timer"
	if !exactProducer {
		return WorkflowTimerActivation{}, timeridentity.WorkflowTimerOccurrenceRef{}, false, nil
	}
	occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(evt.TaskID())
	if !ok {
		return WorkflowTimerActivation{}, timeridentity.WorkflowTimerOccurrenceRef{}, true, fmt.Errorf("workflow timer producer requires a valid typed occurrence")
	}
	store := l.store()
	if store == nil || !store.enabled() {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("workflow timer authorization store is unavailable")
	}
	activation, found, err := store.loadPersistedWorkflowTimerActivation(ctx, occurrence.Activation.ActivationID)
	if err != nil {
		return WorkflowTimerActivation{}, occurrence, true, err
	}
	if !found || activation.Ref != occurrence.Activation {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("accepted workflow timer activation is missing or mismatched")
	}
	current, err := l.workflowTimerActivationDeclarationCurrent(ctx, activation)
	if err != nil {
		return WorkflowTimerActivation{}, occurrence, true, err
	}
	if !current {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf(
			"accepted workflow timer declaration revision is stale",
		)
	}
	if evt.ID() != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) ||
		evt.RunID() != activation.RunID ||
		strings.TrimSpace(string(evt.Type())) != activation.EventType ||
		!workflowTimerJSONEqual(evt.Payload(), activation.Payload) {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf(
			"accepted workflow timer event does not match canonical activation: event_id=%q/%q run_id=%q/%q type=%q/%q payload_equal=%t",
			evt.ID(), timeridentity.WorkflowTimerOccurrenceEventID(occurrence), evt.RunID(), activation.RunID,
			strings.TrimSpace(string(evt.Type())), activation.EventType, workflowTimerJSONEqual(evt.Payload(), activation.Payload),
		)
	}
	if source := evt.RoutingSource(); source != activation.RoutingSource {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf(
			"accepted workflow timer routing source %s/%#v does not match canonical activation %s/%#v",
			source.Kind().StorageCode(), source.Route(), activation.RoutingSource.Kind().StorageCode(), activation.RoutingSource.Route(),
		)
	}
	if !workflowTimerOccurrenceAccepted(activation, occurrence) {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("workflow timer occurrence was not durably accepted")
	}
	return activation, occurrence, true, nil
}

func (l *WorkflowTimerLifecycle) workflowTimerActivationDeclarationCurrent(
	ctx context.Context,
	activation WorkflowTimerActivation,
) (bool, error) {
	declaration, found, err := l.workflowTimerDeclarationForActivation(ctx, activation)
	if err != nil || !found {
		return false, err
	}
	revision, err := workflowTimerDeclarationRevision(l.source, declaration)
	if err != nil {
		return false, err
	}
	if revision != activation.Ref.DeclarationRevision {
		return false, nil
	}
	expectedSource, expectedEvent, err := workflowTimerDeclarationSourceEvent(
		l.source, activation.EntityID, activation.Route.InstancePath, declaration,
	)
	if err != nil {
		return false, err
	}
	if activation.RoutingSource != expectedSource || activation.EventType != string(expectedEvent) {
		return false, fmt.Errorf(
			"workflow timer activation %s source/event does not match declaration %s",
			activation.Ref.ActivationID, activation.Ref.Declaration,
		)
	}
	return true, nil
}

func workflowTimerDeclarationSourceEvent(
	source semanticview.Source,
	entityID, flowInstance string,
	declaration runtimecontracts.WorkflowTimerContract,
) (events.RoutingSource, events.EventType, error) {
	flowID := strings.TrimSpace(declaration.FlowID)
	var (
		routingSource events.RoutingSource
		err           error
	)
	if flowID == "" {
		routingSource, err = events.NewRootRoutingSource(entityID)
	} else {
		routingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
			FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
		})
	}
	if err != nil {
		return events.RoutingSource{}, "", fmt.Errorf("admit workflow timer owner source: %w", err)
	}
	admittedEvent, err := runtimepinrouting.AdmitRuntimeControlSourceEvent(
		source, flowID, events.EventType(strings.TrimSpace(declaration.Event)), routingSource,
	)
	if err != nil {
		return events.RoutingSource{}, "", fmt.Errorf("admit workflow timer event identity: %w", err)
	}
	return routingSource, admittedEvent, nil
}

func (l *WorkflowTimerLifecycle) workflowTimerDeclarationForActivation(
	ctx context.Context,
	activation WorkflowTimerActivation,
) (runtimecontracts.WorkflowTimerContract, bool, error) {
	if l == nil || l.source == nil {
		return runtimecontracts.WorkflowTimerContract{}, false, fmt.Errorf(
			"workflow timer declaration validation requires semantic source",
		)
	}
	store := l.store()
	if store == nil || !store.enabled() {
		return runtimecontracts.WorkflowTimerContract{}, false, fmt.Errorf(
			"workflow timer declaration validation requires workflow store",
		)
	}
	if !activation.Route.Valid() {
		return runtimecontracts.WorkflowTimerContract{}, false, fmt.Errorf("workflow timer activation is missing its instance route")
	}
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(activation.RunID))
	instance, found, err := store.Load(ctx, activation.Route)
	if err != nil || !found {
		return runtimecontracts.WorkflowTimerContract{}, false, err
	}
	declaration, found := workflowTimerDeclarationForInstance(l.source, instance, activation.Ref.Declaration)
	if !found {
		return runtimecontracts.WorkflowTimerContract{}, false, nil
	}
	return declaration, true, nil
}

func workflowTimerDeclarationsForInstance(
	source semanticview.Source,
	instance WorkflowInstance,
) []runtimecontracts.WorkflowTimerContract {
	if source == nil {
		return nil
	}
	workflowName := strings.TrimSpace(instance.WorkflowName)
	declarations := make([]runtimecontracts.WorkflowTimerContract, 0)
	for _, declaration := range source.WorkflowTimers() {
		if workflowTimerDeclarationOwnedByInstance(source, workflowName, declaration.FlowID) {
			declarations = append(declarations, declaration)
		}
	}
	return declarations
}

func workflowTimerDeclarationOwnedByInstance(
	source semanticview.Source,
	workflowName string,
	declarationFlowID string,
) bool {
	if source == nil {
		return false
	}
	workflowName = strings.TrimSpace(workflowName)
	declarationFlowID = strings.TrimSpace(declarationFlowID)
	if declarationFlowID == workflowName {
		return true
	}
	return declarationFlowID == "" && workflowName == strings.TrimSpace(source.WorkflowName())
}

func workflowTimerDeclarationForInstance(
	source semanticview.Source,
	instance WorkflowInstance,
	declarationID string,
) (runtimecontracts.WorkflowTimerContract, bool) {
	declarationID = strings.TrimSpace(declarationID)
	for _, declaration := range workflowTimerDeclarationsForInstance(source, instance) {
		if strings.TrimSpace(declaration.ID) == declarationID {
			return declaration, true
		}
	}
	return runtimecontracts.WorkflowTimerContract{}, false
}

func workflowTimerOccurrenceAccepted(activation WorkflowTimerActivation, occurrence timeridentity.WorkflowTimerOccurrenceRef) bool {
	activation = activation.normalized()
	occurrence = occurrence.Normalize()
	if !activation.Recurring {
		return activation.Status == workflowTimerStatusFired && activation.FireAt.Equal(occurrence.DueAt)
	}
	if activation.RecurrenceInterval <= 0 || !occurrence.DueAt.Before(activation.FireAt) {
		return false
	}
	firstDue := canonicalWorkflowTimerTime(activation.CreatedAt.Add(activation.RecurrenceInterval))
	if occurrence.DueAt.Before(firstDue) {
		return false
	}
	delta := activation.FireAt.Sub(occurrence.DueAt)
	return delta > 0 && delta%activation.RecurrenceInterval == 0
}

func (l *WorkflowTimerLifecycle) Restore(ctx context.Context) error {
	store := l.store()
	if store == nil || !store.enabled() {
		return nil
	}
	runID := runtimecorrelation.RunIDFromContext(ctx)
	activations, err := store.listPersistedWorkflowTimerActivations(ctx, runID, "", true)
	if err != nil {
		return err
	}
	if runID == "" {
		// Standing adoption restores these rows with an exact run only after its
		// durable generation and process-visible routes are installed.
		runtimeOwned := activations[:0]
		for _, activation := range activations {
			standingOwned, err := store.StandingRunUsesIntrinsicRecovery(ctx, activation.RunID)
			if err != nil {
				return fmt.Errorf("classify workflow timer %s startup owner: %w", activation.Ref.ActivationID, err)
			}
			if !standingOwned {
				runtimeOwned = append(runtimeOwned, activation)
			}
		}
		activations = runtimeOwned
	}
	if len(activations) > 0 {
		l.projectionMu.Lock()
		schedulerMissing := l.scheduler == nil
		l.projectionMu.Unlock()
		if schedulerMissing {
			return fmt.Errorf("restore workflow timers with %d active activation(s): %w", len(activations), errWorkflowTimerSchedulerRequired)
		}
	}
	for _, activation := range activations {
		if err := l.reconcileWakeupImmediately(ctx, activation.Ref); err != nil {
			l.logFailure(ctx, "workflow_timer_restore_register_failed", activation.Ref, err)
			if !l.startWakeupRecovery(activation.Ref) {
				return fmt.Errorf("restore workflow timer %s: %w", activation.Ref.ActivationID, err)
			}
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) startWakeupRecovery(ref timeridentity.WorkflowTimerActivationRef) bool {
	ref = ref.Normalize()
	return l.startRecovery("reconcile\x00"+ref.ActivationID, func(ctx context.Context) error {
		return l.ReconcileWakeup(ctx, ref)
	}, func(ctx context.Context, err error) {
		l.logFailure(ctx, "workflow_timer_reconcile_retry_failed", ref, err)
	})
}

func (l *WorkflowTimerLifecycle) startRecovery(key string, operation func(context.Context) error, onFailure func(context.Context, error)) bool {
	if l == nil || operation == nil {
		return false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	l.projectionMu.Lock()
	if l.stopped {
		l.projectionMu.Unlock()
		return false
	}
	l.recoveryMu.Lock()
	if _, exists := l.recovering[key]; exists {
		l.recoveryMu.Unlock()
		l.projectionMu.Unlock()
		return true
	}
	if l.workOwner == nil {
		l.recoveryMu.Unlock()
		l.projectionMu.Unlock()
		return false
	}
	lease, err := l.workOwner.Begin(l.recoveryCtx)
	if err != nil {
		l.recoveryMu.Unlock()
		l.projectionMu.Unlock()
		return false
	}
	done := make(chan struct{})
	l.recovering[key] = done
	l.recoveryMu.Unlock()
	l.projectionMu.Unlock()

	go func() {
		defer func() {
			l.recoveryMu.Lock()
			delete(l.recovering, key)
			l.recoveryMu.Unlock()
			_ = lease.Done()
			close(done)
		}()
		for attempt := 0; ; attempt++ {
			timer := time.NewTimer(workflowTimerRecoveryDelay(attempt))
			select {
			case <-lease.Context().Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := operation(lease.Context()); err != nil {
				if lease.Context().Err() != nil {
					return
				}
				if onFailure != nil {
					onFailure(lease.Context(), err)
				}
				continue
			}
			return
		}
	}()
	return true
}

func workflowTimerRecoveryDelay(attempt int) time.Duration {
	delay := 20 * time.Millisecond
	for attempt > 0 && delay < 500*time.Millisecond {
		delay *= 2
		attempt--
	}
	if delay > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return delay
}

func (l *WorkflowTimerLifecycle) stop(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.projectionMu.Lock()
	if !l.stopped {
		l.stopped = true
		l.cancel()
	}
	l.recoveryMu.Lock()
	done := make([]<-chan struct{}, 0, len(l.recovering))
	for _, recoveryDone := range l.recovering {
		done = append(done, recoveryDone)
	}
	l.recoveryMu.Unlock()
	l.projectionMu.Unlock()
	if err := l.stopWakeups(ctx); err != nil {
		return err
	}
	for _, recoveryDone := range done {
		select {
		case <-recoveryDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) logFailure(ctx context.Context, action string, ref timeridentity.WorkflowTimerActivationRef, err error) {
	if l == nil || l.logger == nil || err == nil {
		return
	}
	_ = l.logger.LogRuntime(ctx, RuntimeLogEntry{
		Level: "error", Message: "Workflow timer lifecycle operation failed", Component: runtimeWorkflowID,
		Action: action, Detail: map[string]any{"activation_id": ref.ActivationID, "declaration": ref.Declaration},
	})
}
