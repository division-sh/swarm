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
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type workflowTimerCauseKind string

const (
	workflowTimerCauseInitial    workflowTimerCauseKind = "initial_stage"
	workflowTimerCauseEvent      workflowTimerCauseKind = "event"
	workflowTimerCauseTransition workflowTimerCauseKind = "transition"
)

type workflowTimerCause struct {
	Kind         workflowTimerCauseKind
	EventID      string
	EventType    string
	OccurredAt   time.Time
	TransitionID string
	FromState    string
	ToState      string
}

func (c workflowTimerCause) normalized() workflowTimerCause {
	c.Kind = workflowTimerCauseKind(strings.TrimSpace(string(c.Kind)))
	c.EventID = strings.TrimSpace(c.EventID)
	c.EventType = strings.TrimSpace(c.EventType)
	c.OccurredAt = canonicalWorkflowTimerTime(c.OccurredAt)
	c.TransitionID = strings.TrimSpace(c.TransitionID)
	c.FromState = strings.TrimSpace(c.FromState)
	c.ToState = strings.TrimSpace(c.ToState)
	return c
}

func (c workflowTimerCause) validateForActivation() error {
	c = c.normalized()
	if c.OccurredAt.IsZero() {
		return fmt.Errorf("workflow timer activation requires an exact causal time")
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
	storeOwner   *WorkflowInstanceStore
	source       semanticview.Source
	publisher    workflowGateMutationPublisher
	logger       systemNodeRuntimeLogger
	workOwner    worklifetime.Occurrence
	scheduler    *Scheduler
	recoveryCtx  context.Context
	cancel       context.CancelFunc
	projectionMu sync.Mutex
	recoveryMu   sync.Mutex
	recovering   map[string]chan struct{}
	stopped      bool

	wakeupCallbackTimeout time.Duration
	testAfterWakeupLoad   func()
}

func newWorkflowTimerLifecycle(store *WorkflowInstanceStore, source semanticview.Source, bus Bus, workOwner worklifetime.Occurrence, scheduler *Scheduler) *WorkflowTimerLifecycle {
	if store == nil {
		return nil
	}
	recoveryCtx, cancel := context.WithCancel(context.Background())
	publisher, _ := bus.(workflowGateMutationPublisher)
	lifecycle := &WorkflowTimerLifecycle{
		storeOwner:            store,
		source:                source,
		publisher:             publisher,
		logger:                bus,
		workOwner:             workOwner,
		recoveryCtx:           recoveryCtx,
		cancel:                cancel,
		recovering:            make(map[string]chan struct{}),
		wakeupCallbackTimeout: workflowTimerWakeupCallbackTimeout,
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

func (l *WorkflowTimerLifecycle) store() *WorkflowInstanceStore {
	if l == nil {
		return nil
	}
	return l.storeOwner
}

func (l *WorkflowTimerLifecycle) Reconcile(ctx context.Context, entityID, currentState, nextState string, cause workflowTimerCause) error {
	return l.reconcile(ctx, entityID, currentState, nextState, cause, true)
}

func (l *WorkflowTimerLifecycle) reconcileInitialEntry(ctx context.Context, entityID, initialState string, cause workflowTimerCause) error {
	return l.reconcile(ctx, entityID, "", initialState, cause, false)
}

func (l *WorkflowTimerLifecycle) reconcile(ctx context.Context, entityID, currentState, nextState string, cause workflowTimerCause, armWakeups bool) error {
	store := l.store()
	if store == nil || !store.Enabled() {
		return nil
	}
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return fmt.Errorf("workflow timer reconciliation requires the selected workflow mutation")
	}
	entityID = strings.TrimSpace(entityID)
	currentState = strings.TrimSpace(currentState)
	nextState = strings.TrimSpace(nextState)
	cause = cause.normalized()
	if entityID == "" {
		return nil
	}
	instance, ok, err := store.Load(ctx, entityID)
	if err != nil || !ok {
		return err
	}
	entityID = workflowTimerCanonicalEntityID(instance, entityID)
	if entityID == "" {
		return fmt.Errorf("workflow timer lifecycle requires canonical entity identity")
	}
	source := l.source
	if source == nil {
		return fmt.Errorf("workflow timer lifecycle requires semantic source")
	}
	runID := workflowTimerRunID(ctx, instance)
	if runID == "" {
		return fmt.Errorf("workflow timer lifecycle requires run identity")
	}
	active, err := store.listWorkflowTimerActivations(ctx, runID, entityID, true)
	if err != nil {
		return err
	}
	activeByDeclaration := map[string]WorkflowTimerActivation{}
	for _, activation := range active {
		declaration, found := workflowTimerDeclarationForInstance(source, instance, activation.Ref.Declaration)
		if !found {
			return fmt.Errorf("active workflow timer %s references unknown declaration %s", activation.Ref.ActivationID, activation.Ref.Declaration)
		}
		if err := validateWorkflowTimerTopology(source, declaration); err != nil {
			return err
		}
		if workflowTimerShouldCancelOnTransition(declaration, currentState, nextState, cause.EventType) {
			cancelled, changed, err := store.cancelWorkflowTimerActivation(ctx, activation.Ref)
			if err != nil {
				return err
			}
			if changed {
				if err := l.queueCancellation(ctx, cancelled); err != nil {
					return err
				}
			}
			continue
		}
		activeByDeclaration[workflowTimerGenerationKey(activation.Ref.Declaration, activation.Ref.Generation)] = activation
	}

	generationStage := nextState
	if generationStage == "" {
		generationStage = currentState
	}
	if generationStage == "" {
		generationStage = strings.TrimSpace(instance.CurrentState)
	}
	generation, _, err := workflowLoopGenerationForStage(source, &instance, generationStage)
	if err != nil {
		return err
	}
	for _, declaration := range workflowTimerDeclarationsForInstance(source, instance) {
		if !workflowTimerShouldStartOnTransition(declaration, currentState, nextState, cause.EventType) {
			continue
		}
		if err := validateWorkflowTimerTopology(source, declaration); err != nil {
			return err
		}
		if err := cause.validateForActivation(); err != nil {
			return err
		}
		interval := workflowTimerDuration(declaration, workflowTimerPolicy(source, declaration.FlowID))
		if interval <= 0 {
			return fmt.Errorf("workflow timer %s has no executable positive delay", declaration.ID)
		}
		activation, err := workflowTimerActivationForCause(
			source,
			runID,
			entityID,
			instance.StorageRef,
			declaration,
			generation,
			cause,
			interval,
		)
		if err != nil {
			return err
		}
		key := workflowTimerGenerationKey(declaration.ID, generation)
		if existing, found := activeByDeclaration[key]; found {
			if armWakeups && existing.Ref == activation.Ref {
				if err := l.queueWakeupReconcile(ctx, existing.Ref); err != nil {
					return err
				}
			}
			continue
		}
		persisted, _, err := store.insertWorkflowTimerActivation(ctx, activation)
		if err != nil {
			return err
		}
		if persisted.Status == workflowTimerStatusActive {
			activeByDeclaration[key] = persisted
			if armWakeups {
				if err := l.queueWakeupReconcile(ctx, persisted.Ref); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) ArmInitialEntryTimers(ctx context.Context, instanceID string) error {
	active, err := l.initialEntryTimerActivations(ctx, instanceID)
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

func (l *WorkflowTimerLifecycle) reconcileInitialEntryDeclarations(ctx context.Context, instanceID string) error {
	store := l.store()
	if store == nil || !store.Enabled() {
		return nil
	}
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return fmt.Errorf("workflow initial timer reconciliation requires the selected workflow mutation")
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return fmt.Errorf("workflow initial timer reconciliation requires instance identity")
	}
	instance, found, err := store.Load(ctx, instanceID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("workflow initial timer reconciliation instance %s is missing", instanceID)
	}
	source := l.source
	if source == nil {
		return fmt.Errorf("workflow initial timer reconciliation requires semantic source")
	}
	entityID := workflowTimerCanonicalEntityID(instance, instanceID)
	runID := workflowTimerRunID(ctx, instance)
	if entityID == "" || runID == "" {
		return fmt.Errorf("workflow initial timer reconciliation requires exact run and entity identity")
	}
	active, err := store.listWorkflowTimerActivations(ctx, runID, entityID, true)
	if err != nil {
		return err
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
		cause := workflowTimerCause{
			Kind:       workflowTimerCauseInitial,
			EventType:  "state:" + currentState,
			OccurredAt: instance.CreatedAt,
			ToState:    currentState,
		}
		if err := cause.validateForActivation(); err != nil {
			return err
		}
		for _, declaration := range workflowTimerDeclarationsForInstance(source, instance) {
			if !workflowTimerShouldStartOnTransition(declaration, "", currentState, cause.EventType) {
				continue
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
				entityID,
				instance.StorageRef,
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
					entityID,
					instance.StorageRef,
					declaration,
					activation.Ref.Generation,
					workflowTimerCause{
						Kind:       workflowTimerCauseInitial,
						EventType:  "state:" + initialState,
						OccurredAt: activation.CreatedAt,
						ToState:    initialState,
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
			if err := l.queueWakeupReconcile(ctx, activation.Ref); err != nil {
				return err
			}
			continue
		}
		cancelled, changed, err := store.cancelWorkflowTimerActivation(ctx, activation.Ref)
		if err != nil {
			return err
		}
		if changed {
			if err := l.queueCancellation(ctx, cancelled); err != nil {
				return err
			}
		}
	}

	activationIDs := make([]string, 0, len(desired))
	for activationID := range desired {
		activationIDs = append(activationIDs, activationID)
	}
	sort.Strings(activationIDs)
	for _, activationID := range activationIDs {
		persisted, _, err := store.insertWorkflowTimerActivation(ctx, desired[activationID])
		if err != nil {
			return err
		}
		if persisted.Status == workflowTimerStatusActive {
			if err := l.queueWakeupReconcile(ctx, persisted.Ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) RetireInitialEntryTimerWakeups(ctx context.Context, instanceID string) error {
	active, err := l.initialEntryTimerActivations(ctx, instanceID)
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

func (l *WorkflowTimerLifecycle) initialEntryTimerActivations(ctx context.Context, instanceID string) ([]WorkflowTimerActivation, error) {
	store := l.store()
	if store == nil || !store.Enabled() {
		return nil, nil
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil, fmt.Errorf("workflow initial timer activation requires instance identity")
	}
	instance, found, err := store.Load(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("workflow initial timer activation instance %s is missing", instanceID)
	}
	entityID := workflowTimerCanonicalEntityID(instance, instanceID)
	runID := workflowTimerRunID(ctx, instance)
	if entityID == "" || runID == "" {
		return nil, fmt.Errorf("workflow initial timer activation requires exact run and entity identity")
	}
	active, err := store.listWorkflowTimerActivations(ctx, runID, entityID, true)
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
	runID, entityID, flowInstance string,
	declaration runtimecontracts.WorkflowTimerContract,
	generation attemptgeneration.Generation,
	cause workflowTimerCause,
	interval time.Duration,
) (WorkflowTimerActivation, error) {
	cause = cause.normalized()
	generation = generation.Normalize()
	declarationRevision, err := workflowTimerDeclarationRevision(source, declaration)
	if err != nil {
		return WorkflowTimerActivation{}, err
	}
	activationCause := timeridentity.WorkflowTimerActivationCause(strings.TrimSpace(string(cause.Kind)))
	activationID := timeridentity.WorkflowTimerActivationID(
		runID,
		entityID,
		flowInstance,
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
		RunID:        strings.TrimSpace(runID),
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		OwnerAgent:   strings.TrimSpace(declaration.Owner),
		EventType:    strings.TrimSpace(declaration.Event),
		Payload:      []byte("{}"),
		FireAt:       canonicalWorkflowTimerTime(cause.OccurredAt.Add(interval)),
		Recurring:    declaration.Recurring,
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

func (l *WorkflowTimerLifecycle) CancelSupersededGenerations(ctx context.Context, entityID string, current []attemptgeneration.Generation) error {
	store := l.store()
	if store == nil || !store.Enabled() {
		return nil
	}
	instance, ok, err := store.Load(ctx, strings.TrimSpace(entityID))
	if err != nil || !ok {
		return err
	}
	entityID = workflowTimerCanonicalEntityID(instance, entityID)
	if entityID == "" {
		return fmt.Errorf("workflow timer lifecycle requires canonical entity identity")
	}
	active, err := store.listWorkflowTimerActivations(ctx, workflowTimerRunID(ctx, instance), entityID, true)
	if err != nil {
		return err
	}
	for _, activation := range active {
		if !activation.Ref.Generation.Valid() || workflowTimerGenerationPresent(current, activation.Ref.Generation) {
			continue
		}
		cancelled, changed, err := store.cancelWorkflowTimerActivation(ctx, activation.Ref)
		if err != nil {
			return err
		}
		if changed {
			if err := l.queueCancellation(ctx, cancelled); err != nil {
				return err
			}
		}
	}
	return nil
}

func workflowTimerCanonicalEntityID(instance WorkflowInstance, fallback string) string {
	ref := firstNonEmptyString(
		strings.TrimSpace(instance.StorageRef),
		strings.TrimSpace(asString(instance.Metadata["entity_id"])),
		strings.TrimSpace(instance.InstanceID),
		strings.TrimSpace(fallback),
	)
	if ref == "" {
		return ""
	}
	return workflowInstanceRowID(ref)
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
	if store == nil || !store.Enabled() {
		return nil
	}
	activation, found, err := store.loadWorkflowTimerActivation(ctx, ref.ActivationID, false)
	if err != nil {
		return err
	}
	if l.testAfterWakeupLoad != nil {
		l.testAfterWakeupLoad()
	}
	if !found || activation.Ref != ref || activation.Status != workflowTimerStatusActive {
		return l.retireWakeup(ref)
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
	action := func(actionCtx context.Context) {
		postCommitCtx := withoutSQLTxContext(actionCtx)
		if err := l.reconcileWakeupImmediately(postCommitCtx, ref); err != nil {
			l.logFailure(postCommitCtx, "workflow_timer_reconcile_failed", ref, err)
			if errors.Is(err, errWorkflowTimerSchedulerRequired) {
				return
			}
			l.startWakeupRecovery(ref)
		}
	}
	if _, inMutation := PipelineSQLTxFromContext(ctx); inMutation {
		if !queuePipelinePostCommitAction(ctx, action) {
			return fmt.Errorf("workflow timer wakeup reconciliation requires post-commit ownership")
		}
		return nil
	}
	return l.reconcileWakeupImmediately(withoutSQLTxContext(ctx), ref)
}

func (l *WorkflowTimerLifecycle) queueCancellation(ctx context.Context, activation WorkflowTimerActivation) error {
	return l.queueWakeupReconcile(ctx, activation.Ref)
}

func (l *WorkflowTimerLifecycle) fireWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) (WorkflowTimerFireOutcome, bool, error) {
	store := l.store()
	if store == nil || !store.Enabled() {
		return WorkflowTimerFireTerminal, false, fmt.Errorf("workflow timer lifecycle store is unavailable")
	}
	if err := wakeup.validate(); err != nil {
		return WorkflowTimerFireTerminal, false, err
	}
	occurrence := wakeup.Occurrence()
	hint, found, err := store.loadWorkflowTimerActivation(ctx, occurrence.Activation.ActivationID, false)
	if err != nil {
		return WorkflowTimerFireRetry, false, err
	}
	if !found {
		return WorkflowTimerFireTerminal, false, nil
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
	var (
		activation  WorkflowTimerActivation
		next        WorkflowTimerActivation
		terminal    bool
		terminalErr error
	)
	err = store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("workflow timer fire requires the selected transaction")
		}
		activeRunID, err := store.requireActiveWorkflowRun(txctx, tx)
		if err != nil {
			if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
				terminal = true
				terminalErr = err
				return nil
			}
			return err
		}
		if activeRunID != hint.RunID {
			terminal = true
			terminalErr = fmt.Errorf("workflow timer fire run mismatch")
			return nil
		}
		loaded, found, err := store.loadWorkflowTimerActivation(txctx, occurrence.Activation.ActivationID, true)
		if err != nil {
			return err
		}
		if !found {
			terminal = true
			return nil
		}
		activation = loaded
		if activation.Ref != occurrence.Activation {
			terminal = true
			terminalErr = fmt.Errorf("workflow timer callback activation mismatch")
			return nil
		}
		if activation.Status != workflowTimerStatusActive || !activation.FireAt.Equal(occurrence.DueAt) {
			terminal = true
			return nil
		}
		if canonicalWorkflowTimerTime(time.Now()).Before(occurrence.DueAt) {
			return fmt.Errorf(
				"workflow timer wakeup for %s arrived before its due coordinate",
				occurrence.Activation.ActivationID,
			)
		}
		if l.publisher == nil {
			return fmt.Errorf("workflow timer fire requires transactional event publication")
		}
		firedAt := canonicalWorkflowTimerTime(time.Now())
		eventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
		evt, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
			Facts: events.EventFacts{
				ID: eventID, Type: events.EventType(activation.EventType),
				Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime.workflow_timer"},
				TaskID:   occurrence.TaskID(), Payload: json.RawMessage(append([]byte(nil), activation.Payload...)),
				Envelope:  events.EventEnvelope{EntityID: activation.EntityID, FlowInstance: activation.FlowInstance},
				CreatedAt: firedAt, ExecutionMode: executionmode.Live,
			},
			RunID: activation.RunID,
		})
		if err != nil {
			return err
		}
		if err := l.publisher.PublishInMutation(txctx, evt); err != nil {
			return err
		}
		next, err = store.completeWorkflowTimerOccurrence(txctx, activation, occurrence, firedAt)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		recoveryCtx := withoutSQLTxContext(ctx)
		if registerErr := l.reconcileWakeupImmediately(recoveryCtx, occurrence.Activation); registerErr != nil {
			l.logFailure(recoveryCtx, "workflow_timer_register_failed", occurrence.Activation, registerErr)
			l.startWakeupRecovery(occurrence.Activation)
			return WorkflowTimerFireRetry, false, errors.Join(err, fmt.Errorf("re-register workflow timer: %w", registerErr))
		}
		return WorkflowTimerFireRetry, false, err
	}
	if terminal {
		return WorkflowTimerFireTerminal, false, terminalErr
	}
	if next.Status != workflowTimerStatusActive {
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
	if store == nil || !store.Enabled() {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("workflow timer authorization store is unavailable")
	}
	activation, found, err := store.loadWorkflowTimerActivation(ctx, occurrence.Activation.ActivationID, false)
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
		evt.RunID() != activation.RunID || workflowEventEntityID(evt) != activation.EntityID ||
		strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != activation.FlowInstance ||
		strings.TrimSpace(string(evt.Type())) != activation.EventType ||
		!workflowTimerJSONEqual(evt.Payload(), activation.Payload) {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("accepted workflow timer event does not match canonical activation")
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
	return revision == activation.Ref.DeclarationRevision, nil
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
	if store == nil || !store.Enabled() {
		return runtimecontracts.WorkflowTimerContract{}, false, fmt.Errorf(
			"workflow timer declaration validation requires workflow store",
		)
	}
	instanceID := strings.Trim(strings.TrimSpace(activation.FlowInstance), "/")
	if instanceID == "" {
		instanceID = strings.TrimSpace(activation.EntityID)
	}
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(activation.RunID))
	instance, found, err := store.Load(ctx, instanceID)
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
	if store == nil || !store.Enabled() {
		return nil
	}
	runID := runtimecorrelation.RunIDFromContext(ctx)
	activations, err := store.listWorkflowTimerActivations(ctx, runID, "", true)
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
