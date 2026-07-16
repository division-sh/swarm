package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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
	coordinator *PipelineCoordinator
	recoveryCtx context.Context
	cancel      context.CancelFunc
	recoveryMu  sync.Mutex
	recoveryWG  sync.WaitGroup
	recovering  map[string]struct{}
	stopped     bool
}

func newWorkflowTimerLifecycle(coordinator *PipelineCoordinator) *WorkflowTimerLifecycle {
	if coordinator == nil {
		return nil
	}
	recoveryCtx, cancel := context.WithCancel(context.Background())
	return &WorkflowTimerLifecycle{
		coordinator: coordinator,
		recoveryCtx: recoveryCtx,
		cancel:      cancel,
		recovering:  make(map[string]struct{}),
	}
}

func (pc *PipelineCoordinator) IsWorkflowTimerSchedule(schedule Schedule) bool {
	return pc != nil && pc.workflowTimers != nil && schedule.CanonicalWorkflowTimer()
}

func (pc *PipelineCoordinator) FireWorkflowTimer(ctx context.Context, schedule Schedule) (WorkflowTimerFireOutcome, error) {
	if pc == nil || pc.workflowTimers == nil {
		return WorkflowTimerFireTerminal, fmt.Errorf("workflow timer lifecycle is unavailable")
	}
	return pc.workflowTimers.Fire(ctx, schedule)
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
	if l == nil || l.coordinator == nil {
		return nil
	}
	return l.coordinator.workflowStore
}

func (l *WorkflowTimerLifecycle) Reconcile(ctx context.Context, entityID, currentState, nextState string, cause workflowTimerCause) error {
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
	source := l.coordinator.SemanticSource()
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
		declaration, found := source.WorkflowTimerByID(activation.Ref.Declaration)
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

	generation, _, err := workflowLoopGenerationForStage(source, &instance, nextState)
	if err != nil {
		return err
	}
	for _, declaration := range source.WorkflowTimers() {
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
		activation := workflowTimerActivationForCause(runID, entityID, instance.StorageRef, declaration, generation, cause, interval)
		key := workflowTimerGenerationKey(declaration.ID, generation)
		if existing, found := activeByDeclaration[key]; found {
			if existing.Ref == activation.Ref {
				if err := l.queueEnsureRegistered(ctx, existing.Ref); err != nil {
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
			if err := l.queueEnsureRegistered(ctx, persisted.Ref); err != nil {
				return err
			}
		}
	}
	return nil
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
	runID, entityID, flowInstance string,
	declaration runtimecontracts.WorkflowTimerContract,
	generation attemptgeneration.Generation,
	cause workflowTimerCause,
	interval time.Duration,
) WorkflowTimerActivation {
	cause = cause.normalized()
	generation = generation.Normalize()
	activationID := timeridentity.WorkflowTimerActivationID(
		runID,
		entityID,
		flowInstance,
		declaration.ID,
		generation.KeySuffix(),
		string(cause.Kind),
		cause.EventID,
		cause.EventType,
		cause.TransitionID,
		cause.FromState,
		cause.ToState,
	)
	return WorkflowTimerActivation{
		Ref: timeridentity.WorkflowTimerActivationRef{
			ActivationID: activationID,
			Declaration:  strings.TrimSpace(declaration.ID),
			Generation:   generation,
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
	}.normalized()
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

func (l *WorkflowTimerLifecycle) EnsureRegistered(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
	store := l.store()
	if store == nil || !store.Enabled() || l.coordinator.timerScheduler == nil {
		return nil
	}
	activation, found, err := store.loadWorkflowTimerActivation(ctx, ref.ActivationID, false)
	if err != nil {
		return err
	}
	if !found || activation.Ref != ref || activation.Status != workflowTimerStatusActive {
		return nil
	}
	claimed, err := ClaimAndRegisterSchedule(ctx, l.coordinator.timerScheduleStore, l.coordinator.timerScheduler, activation.schedule())
	if err != nil {
		return err
	}
	if !claimed && l.coordinator.timerScheduleStore != nil {
		return fmt.Errorf("workflow timer activation %s is not yet claimed for registration", ref.ActivationID)
	}
	return nil
}

func (l *WorkflowTimerLifecycle) ensureRegisteredImmediately(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
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
		if err := l.EnsureRegistered(ctx, ref); err != nil {
			last = err
			continue
		}
		return nil
	}
	return last
}

func (l *WorkflowTimerLifecycle) queueEnsureRegistered(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) error {
	if l == nil || l.coordinator == nil || l.coordinator.timerScheduler == nil {
		return nil
	}
	action := func() {
		postCommitCtx := withoutSQLTxContext(context.WithoutCancel(ctx))
		if err := l.ensureRegisteredImmediately(postCommitCtx, ref); err != nil {
			l.logFailure(postCommitCtx, "workflow_timer_register_failed", ref, err)
			l.startRegistrationRecovery(ref)
		}
	}
	if _, inMutation := PipelineSQLTxFromContext(ctx); inMutation {
		if !queuePipelinePostCommitAction(ctx, action) {
			return fmt.Errorf("workflow timer activation requires post-commit registration ownership")
		}
		return nil
	}
	action()
	return nil
}

func (l *WorkflowTimerLifecycle) queueCancellation(ctx context.Context, activation WorkflowTimerActivation) error {
	if l == nil || l.coordinator == nil {
		return nil
	}
	schedule := activation.schedule()
	action := func() {
		postCommitCtx := withoutSQLTxContext(context.WithoutCancel(ctx))
		if l.coordinator.timerScheduler != nil {
			if err := l.coordinator.timerScheduler.CancelExact(schedule); err != nil {
				l.logFailure(postCommitCtx, "workflow_timer_cancel_failed", activation.Ref, err)
			}
		}
		if l.coordinator.timerScheduleStore != nil {
			if err := l.coordinator.timerScheduleStore.ReleaseSchedule(postCommitCtx, schedule); err != nil {
				l.logFailure(postCommitCtx, "workflow_timer_claim_release_failed", activation.Ref, err)
				l.startReleaseRecovery(schedule, activation.Ref)
			}
		}
	}
	if _, inMutation := PipelineSQLTxFromContext(ctx); inMutation {
		if !queuePipelinePostCommitAction(ctx, action) {
			return fmt.Errorf("workflow timer cancellation requires post-commit scheduler ownership")
		}
		return nil
	}
	action()
	return nil
}

func (l *WorkflowTimerLifecycle) Fire(ctx context.Context, schedule Schedule) (WorkflowTimerFireOutcome, error) {
	store := l.store()
	if store == nil || !store.Enabled() {
		return WorkflowTimerFireTerminal, fmt.Errorf("workflow timer lifecycle store is unavailable")
	}
	occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(schedule.TaskID)
	if !ok || schedule.EffectiveTimerID() != occurrence.Activation.ActivationID {
		return WorkflowTimerFireTerminal, fmt.Errorf("workflow timer callback identity is invalid")
	}
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(schedule.RunID))
	var (
		activation  WorkflowTimerActivation
		next        WorkflowTimerActivation
		terminal    bool
		terminalErr error
	)
	err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("workflow timer fire requires the selected transaction")
		}
		activeRunID, err := store.requireActiveWorkflowRun(txctx, tx)
		if err != nil {
			if errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				terminal = true
				terminalErr = err
				return nil
			}
			return err
		}
		if activeRunID != strings.TrimSpace(schedule.RunID) {
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
		if !workflowTimerScheduleMatchesActivation(schedule, activation) {
			terminal = true
			terminalErr = fmt.Errorf("workflow timer callback does not match canonical activation")
			return nil
		}
		publisher, ok := l.coordinator.bus.(workflowGateMutationPublisher)
		if !ok || publisher == nil {
			return fmt.Errorf("workflow timer fire requires transactional event publication")
		}
		firedAt := canonicalWorkflowTimerTime(time.Now())
		eventID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
		evt := events.NewRuntimeControlEvent(
			eventID,
			events.EventType(activation.EventType),
			events.AgentProducer(activation.OwnerAgent),
			occurrence.TaskID(),
			json.RawMessage(append([]byte(nil), activation.Payload...)),
			0,
			activation.RunID,
			"",
			events.EventEnvelope{EntityID: activation.EntityID, FlowInstance: activation.FlowInstance},
			firedAt,
		)
		if err := publisher.PublishInMutation(txctx, evt); err != nil {
			return err
		}
		next, err = store.completeWorkflowTimerOccurrence(txctx, activation, occurrence, firedAt)
		if err != nil {
			return err
		}
		return l.queueAfterFire(txctx, activation, next)
	})
	if err != nil {
		recoveryCtx := withoutSQLTxContext(context.WithoutCancel(ctx))
		if registerErr := l.ensureRegisteredImmediately(recoveryCtx, occurrence.Activation); registerErr != nil {
			l.logFailure(recoveryCtx, "workflow_timer_register_failed", occurrence.Activation, registerErr)
			l.startRegistrationRecovery(occurrence.Activation)
			return WorkflowTimerFireRetry, errors.Join(err, fmt.Errorf("re-register workflow timer: %w", registerErr))
		}
		return WorkflowTimerFireRetry, err
	}
	if terminal {
		if l.coordinator.timerScheduleStore != nil {
			if err := l.coordinator.timerScheduleStore.ReleaseSchedule(withoutSQLTxContext(ctx), schedule); err != nil {
				l.startReleaseRecovery(schedule, occurrence.Activation)
				return WorkflowTimerFireTerminal, errors.Join(terminalErr, err)
			}
		}
		return WorkflowTimerFireTerminal, terminalErr
	}
	return WorkflowTimerFireCommitted, nil
}

func workflowTimerScheduleMatchesActivation(schedule Schedule, activation WorkflowTimerActivation) bool {
	want := activation.schedule()
	schedule.NormalizeRunID()
	schedule.NormalizeEntityID()
	schedule.NormalizeFlowInstance()
	schedule.NormalizeTimerID()
	if strings.TrimSpace(schedule.Mode) == "" {
		schedule.Mode = "once"
	}
	return schedule.RunID == want.RunID && schedule.AgentID == want.AgentID &&
		schedule.EventType == want.EventType && schedule.Mode == want.Mode &&
		strings.TrimSpace(schedule.Cron) == "" && canonicalWorkflowTimerTime(schedule.At).Equal(want.At) &&
		schedule.EntityID == want.EntityID && schedule.FlowInstance == want.FlowInstance &&
		schedule.TaskID == want.TaskID && schedule.TimerID == want.TimerID && schedule.Context.Empty() &&
		workflowTimerJSONEqual(schedule.Payload, want.Payload)
}

func (l *WorkflowTimerLifecycle) queueAfterFire(ctx context.Context, previous, next WorkflowTimerActivation) error {
	previousSchedule := previous.schedule()
	action := func() {
		postCommitCtx := withoutSQLTxContext(context.WithoutCancel(ctx))
		if l.coordinator.timerScheduleStore != nil {
			if err := l.coordinator.timerScheduleStore.ReleaseSchedule(postCommitCtx, previousSchedule); err != nil {
				l.logFailure(postCommitCtx, "workflow_timer_claim_release_failed", previous.Ref, err)
				l.startReleaseRecovery(previousSchedule, previous.Ref)
			}
		}
		if next.Status == workflowTimerStatusActive {
			if err := l.ensureRegisteredImmediately(postCommitCtx, next.Ref); err != nil {
				l.logFailure(postCommitCtx, "workflow_timer_recurrence_register_failed", next.Ref, err)
				l.startRegistrationRecovery(next.Ref)
			}
		}
	}
	if !queuePipelinePostCommitAction(ctx, action) {
		return fmt.Errorf("workflow timer fire requires post-commit claim ownership")
	}
	return nil
}

func (l *WorkflowTimerLifecycle) AuthorizeAcceptedEvent(ctx context.Context, evt events.Event) (WorkflowTimerActivation, timeridentity.WorkflowTimerOccurrenceRef, bool, error) {
	occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(evt.TaskID())
	if !ok {
		return WorkflowTimerActivation{}, timeridentity.WorkflowTimerOccurrenceRef{}, false, nil
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
	if evt.ID() != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) ||
		evt.RunID() != activation.RunID || workflowEventEntityID(evt) != activation.EntityID ||
		strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != activation.FlowInstance ||
		strings.TrimSpace(string(evt.Type())) != activation.EventType ||
		!evt.Producer().Equal(events.AgentProducer(activation.OwnerAgent)) ||
		!workflowTimerJSONEqual(evt.Payload(), activation.Payload) {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("accepted workflow timer event does not match canonical activation")
	}
	if !workflowTimerOccurrenceAccepted(activation, occurrence) {
		return WorkflowTimerActivation{}, occurrence, true, fmt.Errorf("workflow timer occurrence was not durably accepted")
	}
	return activation, occurrence, true, nil
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
	if err := store.rejectObsoleteWorkflowTimerRows(ctx); err != nil {
		return err
	}
	activations, err := store.listWorkflowTimerActivations(ctx, runtimecorrelation.RunIDFromContext(ctx), "", true)
	if err != nil {
		return err
	}
	for _, activation := range activations {
		if err := l.ensureRegisteredImmediately(ctx, activation.Ref); err != nil {
			l.logFailure(ctx, "workflow_timer_restore_register_failed", activation.Ref, err)
			if !l.startRegistrationRecovery(activation.Ref) {
				return fmt.Errorf("restore workflow timer %s: %w", activation.Ref.ActivationID, err)
			}
		}
	}
	return nil
}

func (l *WorkflowTimerLifecycle) startRegistrationRecovery(ref timeridentity.WorkflowTimerActivationRef) bool {
	ref = ref.Normalize()
	return l.startRecovery("register\x00"+ref.ActivationID, func(ctx context.Context) error {
		return l.EnsureRegistered(ctx, ref)
	}, func(ctx context.Context, err error) {
		l.logFailure(ctx, "workflow_timer_register_retry_failed", ref, err)
	})
}

func (l *WorkflowTimerLifecycle) startReleaseRecovery(schedule Schedule, ref timeridentity.WorkflowTimerActivationRef) bool {
	ref = ref.Normalize()
	return l.startRecovery("release\x00"+scheduleKey(schedule), func(ctx context.Context) error {
		if l.coordinator == nil || l.coordinator.timerScheduleStore == nil {
			return nil
		}
		return l.coordinator.timerScheduleStore.ReleaseSchedule(ctx, schedule)
	}, func(ctx context.Context, err error) {
		l.logFailure(ctx, "workflow_timer_claim_release_retry_failed", ref, err)
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
	l.recoveryMu.Lock()
	if l.stopped {
		l.recoveryMu.Unlock()
		return false
	}
	if _, exists := l.recovering[key]; exists {
		l.recoveryMu.Unlock()
		return true
	}
	l.recovering[key] = struct{}{}
	l.recoveryWG.Add(1)
	l.recoveryMu.Unlock()

	go func() {
		defer func() {
			l.recoveryMu.Lock()
			delete(l.recovering, key)
			l.recoveryMu.Unlock()
			l.recoveryWG.Done()
		}()
		for attempt := 0; ; attempt++ {
			timer := time.NewTimer(workflowTimerRecoveryDelay(attempt))
			select {
			case <-l.recoveryCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := operation(l.recoveryCtx); err != nil {
				if l.recoveryCtx.Err() != nil {
					return
				}
				if onFailure != nil {
					onFailure(l.recoveryCtx, err)
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
	l.recoveryMu.Lock()
	if !l.stopped {
		l.stopped = true
		l.cancel()
	}
	l.recoveryMu.Unlock()
	done := make(chan struct{})
	go func() {
		l.recoveryWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *WorkflowTimerLifecycle) logFailure(ctx context.Context, action string, ref timeridentity.WorkflowTimerActivationRef, err error) {
	if l == nil || l.coordinator == nil || l.coordinator.bus == nil || err == nil {
		return
	}
	_ = l.coordinator.bus.LogRuntime(ctx, RuntimeLogEntry{
		Level: "error", Message: "Workflow timer lifecycle operation failed", Component: runtimeWorkflowID,
		Action: action, Detail: map[string]any{"activation_id": ref.ActivationID, "declaration": ref.Declaration},
	})
}
