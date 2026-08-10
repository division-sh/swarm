package genericschedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

const wakeupCallbackTimeout = 10 * time.Second

const catchupWarningThreshold = 1000

type Store interface {
	AdmitGenericSchedule(context.Context, AdmissionCommand) (AdmissionResult, error)
	LoadGenericScheduleActivation(context.Context, string) (Activation, bool, error)
	ListActiveGenericScheduleActivations(context.Context) ([]Activation, error)
	PrepareGenericScheduleOccurrence(context.Context, Wakeup) (PreparedOccurrence, error)
	CommitGenericScheduleOccurrence(context.Context, CommitCommand) (CommitResult, error)
	CancelGenericSchedule(context.Context, CancelCommand) (CancelResult, error)
	ClaimGenericScheduleWakeup(context.Context, Wakeup) (bool, error)
	ReleaseGenericScheduleWakeup(context.Context, Wakeup) error
	ReleaseGenericScheduleClaims(context.Context) error
}

type Scheduler interface {
	BindGenericScheduleLifecycle(func(context.Context, Wakeup)) error
	RegisterGenericScheduleWakeup(context.Context, Wakeup) error
	RetireGenericScheduleWakeup(Wakeup) error
	StopGenericScheduleWakeups(context.Context) error
}

type PublicationPlanner interface {
	PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error)
	ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error
	FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error
}

type CommitCommand struct {
	Activation  Activation
	Occurrence  Occurrence
	AcceptedAt  time.Time
	Publication runtimeengine.DurablePublicationPlan
}

func (c CommitCommand) Validate() error {
	return c.validate(true)
}

func (c CommitCommand) ValidatePrepared() error {
	return c.validate(false)
}

func (c CommitCommand) validate(requireAcceptedAt bool) error {
	c.Activation = c.Activation.Canonical()
	c.Occurrence = c.Occurrence.Canonical()
	c.AcceptedAt = canonicalTime(c.AcceptedAt)
	if err := c.Activation.Validate(); err != nil {
		return err
	}
	if c.Activation.Status != StatusActive {
		return errors.New("generic schedule occurrence commit requires active activation")
	}
	if err := c.Occurrence.Validate(); err != nil {
		return err
	}
	if c.Occurrence.ActivationID != c.Activation.ID || !c.Occurrence.DueAt.Equal(c.Activation.CurrentDueAt) ||
		c.Occurrence.EventID != c.Activation.CurrentEventID || !c.Occurrence.AdmittedAt.Equal(c.Activation.CurrentEventAdmittedAt) {
		return errors.New("generic schedule occurrence commit does not match prepared activation")
	}
	if requireAcceptedAt && (c.AcceptedAt.IsZero() || c.AcceptedAt.Before(c.Occurrence.AdmittedAt)) {
		return errors.New("generic schedule occurrence commit requires accepted_at after admission")
	}
	if !c.AcceptedAt.IsZero() && c.AcceptedAt.Before(c.Occurrence.AdmittedAt) {
		return errors.New("generic schedule occurrence accepted_at precedes admission")
	}
	if c.Publication == nil {
		return errors.New("generic schedule occurrence commit requires durable publication plan")
	}
	if err := c.Publication.ValidateDurablePublicationPlan(); err != nil {
		return err
	}
	if c.Publication.DurablePublicationEventID() != c.Occurrence.EventID {
		return errors.New("generic schedule occurrence publication identity mismatch")
	}
	return nil
}

type CommitResult struct {
	Outcome                     CommitOutcome
	Next                        Activation
	Publication                 runtimeengine.CommittedDurablePublication
	PublicationAlreadyCommitted bool
}

func (r CommitResult) Validate() error {
	switch r.Outcome {
	case CommitRetry:
		if r.Publication != nil || r.Next.ID != "" || r.PublicationAlreadyCommitted {
			return errors.New("retry generic schedule result cannot carry committed evidence")
		}
		return nil
	case CommitTerminal, CommitStaleCancelled:
		if r.Publication != nil || r.PublicationAlreadyCommitted {
			return errors.New("terminal generic schedule result cannot carry publication evidence")
		}
		if r.Next.ID != "" {
			return r.Next.Validate()
		}
		return nil
	case CommitCommitted:
		if err := r.Next.Validate(); err != nil {
			return err
		}
		if r.Next.Status != StatusActive && r.Next.Status != StatusFired {
			return errors.New("committed generic schedule occurrence has invalid successor state")
		}
		if r.Publication == nil {
			return errors.New("committed generic schedule occurrence requires publication evidence")
		}
		return r.Publication.ValidateCommittedDurablePublication()
	default:
		return fmt.Errorf("generic schedule commit outcome %q is invalid", r.Outcome)
	}
}

type Logger interface {
	GenericScheduleFailure(context.Context, string, string, error)
	GenericScheduleCatchupWarning(context.Context, string, int)
}

// Lifecycle owns durable-to-process projection, occurrence execution, and
// bounded recovery. Scheduler only owns inert clocks.
type Lifecycle struct {
	store      Store
	scheduler  Scheduler
	planner    PublicationPlanner
	dispatcher runtimeengine.PostCommitDispatcher
	logger     Logger

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup
	retry  map[string]struct{}
	stop   bool
}

func NewLifecycle(store Store, scheduler Scheduler, planner PublicationPlanner, dispatcher runtimeengine.PostCommitDispatcher, logger Logger) (*Lifecycle, error) {
	if store == nil || scheduler == nil || planner == nil || dispatcher == nil {
		return nil, errors.New("generic schedule lifecycle requires store, scheduler, publication planner, and dispatcher")
	}
	ctx, cancel := context.WithCancel(context.Background())
	lifecycle := &Lifecycle{
		store: store, scheduler: scheduler, planner: planner, dispatcher: dispatcher, logger: logger,
		ctx: ctx, cancel: cancel, retry: make(map[string]struct{}),
	}
	if err := scheduler.BindGenericScheduleLifecycle(lifecycle.handleWakeup); err != nil {
		cancel()
		return nil, err
	}
	return lifecycle, nil
}

func (l *Lifecycle) Admit(ctx context.Context, command AdmissionCommand) (AdmissionResult, error) {
	if l == nil {
		return AdmissionResult{}, errors.New("generic schedule lifecycle is required")
	}
	result, err := l.store.AdmitGenericSchedule(ctx, command)
	if err != nil {
		return AdmissionResult{}, err
	}
	if err := result.Validate(); err != nil {
		return AdmissionResult{}, err
	}
	if err := l.reconcileImmediately(ctx, result.Activation.ID); err != nil {
		l.log(ctx, "reconcile_after_admission", result.Activation.ID, err)
		l.startRecovery(result.Activation.ID)
	}
	return result, nil
}

func (l *Lifecycle) Cancel(ctx context.Context, command CancelCommand) (CancelResult, error) {
	if l == nil {
		return CancelResult{}, errors.New("generic schedule lifecycle is required")
	}
	result, err := l.store.CancelGenericSchedule(ctx, command)
	if err != nil {
		return CancelResult{}, err
	}
	if result.Activation.ID != "" {
		if err := l.ReconcileWakeup(ctx, result.Activation.ID); err != nil {
			l.log(ctx, "reconcile_after_cancel", result.Activation.ID, err)
			l.startRecovery(result.Activation.ID)
		}
	}
	return result, nil
}

func (l *Lifecycle) Restore(ctx context.Context) (int, error) {
	if l == nil {
		return 0, nil
	}
	activations, err := l.store.ListActiveGenericScheduleActivations(ctx)
	if err != nil {
		return 0, err
	}
	reconciled := 0
	for _, activation := range activations {
		if depth, err := catchupDepth(activation, time.Now()); err != nil {
			return 0, err
		} else if depth > catchupWarningThreshold && l.logger != nil {
			l.logger.GenericScheduleCatchupWarning(ctx, activation.ID, depth)
		}
		if err := l.ReconcileWakeup(ctx, activation.ID); err != nil {
			return 0, err
		}
		reconciled++
	}
	return reconciled, nil
}

func catchupDepth(activation Activation, now time.Time) (int, error) {
	activation = activation.Canonical()
	now = canonicalTime(now)
	if err := activation.Validate(); err != nil {
		return 0, err
	}
	if !activation.Command.Due.Recurring() || activation.CurrentDueAt.After(now) {
		return 0, nil
	}
	if activation.Command.Due.Kind == DueEvery {
		return int(now.Sub(activation.CurrentDueAt)/activation.Command.Due.Every) + 1, nil
	}
	depth := 1
	coordinate := activation.CurrentDueAt
	for {
		next, err := activation.Command.Due.Next(coordinate)
		if err != nil {
			return 0, err
		}
		if next.After(now) {
			return depth, nil
		}
		depth++
		coordinate = next
	}
}

func (l *Lifecycle) ReconcileWakeup(ctx context.Context, activationID string) error {
	if l == nil {
		return nil
	}
	activationID = stringsTrim(activationID)
	if activationID == "" {
		return errors.New("generic schedule reconciliation requires activation_id")
	}
	l.mu.Lock()
	stopped := l.stop
	l.mu.Unlock()
	activation, found, err := l.store.LoadGenericScheduleActivation(ctx, activationID)
	if err != nil {
		return err
	}
	if !found || stopped || activation.Status != StatusActive {
		if found {
			wakeup, wakeErr := NewWakeup(activation.ID, activation.CurrentDueAt)
			if wakeErr != nil {
				return wakeErr
			}
			return l.retireExactWakeup(context.WithoutCancel(ctx), wakeup)
		}
		return nil
	}
	wakeup, err := activation.Wakeup()
	if err != nil {
		return err
	}
	claimed, err := l.store.ClaimGenericScheduleWakeup(ctx, wakeup)
	if err != nil || !claimed {
		return err
	}
	if err := l.scheduler.RegisterGenericScheduleWakeup(ctx, wakeup); err != nil {
		_ = l.store.ReleaseGenericScheduleWakeup(context.WithoutCancel(ctx), wakeup)
		return err
	}
	return nil
}

func (l *Lifecycle) reconcileImmediately(ctx context.Context, activationID string) error {
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
		if err := l.ReconcileWakeup(ctx, activationID); err != nil {
			last = err
			continue
		}
		return nil
	}
	return last
}

func (l *Lifecycle) handleWakeup(ctx context.Context, wakeup Wakeup) {
	callbackCtx, cancel := context.WithTimeout(ctx, wakeupCallbackTimeout)
	defer cancel()
	result, err := l.fire(callbackCtx, wakeup)
	if err != nil {
		l.log(callbackCtx, "fire", wakeup.ActivationID(), err)
	}
	if err != nil || result.Outcome == CommitRetry {
		l.startRecovery(wakeup.ActivationID())
		return
	}
	if result.Next.ID != "" {
		if reconcileErr := l.ReconcileWakeup(context.WithoutCancel(callbackCtx), result.Next.ID); reconcileErr != nil {
			l.log(callbackCtx, "reconcile_after_fire", result.Next.ID, reconcileErr)
			l.startRecovery(result.Next.ID)
		}
		return
	}
	if result.Outcome == CommitTerminal || result.Outcome == CommitStaleCancelled {
		if retireErr := l.retireExactWakeup(context.WithoutCancel(callbackCtx), wakeup); retireErr != nil {
			l.log(callbackCtx, "retire_terminal_wakeup", wakeup.ActivationID(), retireErr)
			l.startTerminalRetirementRecovery(wakeup)
		}
	}
}

func (l *Lifecycle) fire(ctx context.Context, wakeup Wakeup) (CommitResult, error) {
	prepared, err := l.store.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil {
		return CommitResult{Outcome: CommitRetry}, err
	}
	if err := prepared.Validate(); err != nil {
		return CommitResult{Outcome: CommitRetry}, err
	}
	switch prepared.Outcome {
	case PrepareStaleCancelled:
		return CommitResult{Outcome: CommitStaleCancelled, Next: prepared.Activation}, nil
	case PrepareTerminal:
		return CommitResult{Outcome: CommitTerminal, Next: prepared.Activation}, nil
	}
	activation := prepared.Activation
	occurrence := prepared.Occurrence
	payload, err := canonicaljson.Encode(activation.Command.Payload)
	if err != nil {
		return CommitResult{Outcome: CommitRetry}, err
	}
	event, err := occurrenceEvent(activation, occurrence, payload)
	if err != nil {
		return CommitResult{Outcome: CommitRetry}, err
	}
	deliveryContext := events.DeliveryContext{}
	if activation.Command.ReplyContext != "" {
		deliveryContext.Reply = &events.ReplyContextRef{ID: activation.Command.ReplyContext}
	}
	ctx = events.WithDeliveryContext(ctx, deliveryContext)
	intent := runtimeengine.EmitIntent{Event: event}
	plans, err := l.planner.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{intent})
	if err != nil {
		return CommitResult{Outcome: CommitRetry}, err
	}
	if len(plans) != 1 {
		releaseErr := l.planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return CommitResult{Outcome: CommitRetry}, errors.Join(fmt.Errorf("generic schedule publication planner returned %d plans", len(plans)), releaseErr)
	}
	result, err := l.store.CommitGenericScheduleOccurrence(ctx, CommitCommand{
		Activation: activation, Occurrence: occurrence, Publication: plans[0],
	})
	if err != nil {
		return CommitResult{Outcome: CommitRetry}, errors.Join(err, l.planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	if err := result.Validate(); err != nil {
		return CommitResult{Outcome: CommitRetry}, errors.Join(err, l.planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	if result.Outcome != CommitCommitted {
		return result, l.planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
	}
	if result.PublicationAlreadyCommitted {
		return result, l.planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
	}
	// Publication is already durable. Finalization/dispatch failures are logged
	// for downstream recovery and never cause a synchronous occurrence resend.
	if err := l.planner.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{result.Publication}); err != nil {
		l.log(ctx, "finalize_committed_publication", activation.ID, err)
	}
	if err := l.dispatcher.DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		l.log(ctx, "dispatch_committed_publication", activation.ID, err)
	}
	return result, nil
}

func occurrenceEvent(activation Activation, occurrence Occurrence, payload []byte) (events.Event, error) {
	facts := events.EventFacts{
		ID: occurrence.EventID, Type: events.EventType(activation.Command.EventType),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: occurrenceProducerID},
		TaskID:   activation.Command.TaskID, Payload: json.RawMessage(append([]byte(nil), payload...)),
		Envelope:      events.EventEnvelope{EntityID: activation.Command.EntityID, FlowInstance: activation.Command.FlowInstance},
		RoutingSource: activation.Command.RoutingSource, CreatedAt: occurrence.DueAt, ExecutionMode: executionmode.Live,
	}
	if activation.Command.RunID == "" {
		return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: facts})
	}
	return events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: activation.Command.RunID})
}

func (l *Lifecycle) startRecovery(activationID string) {
	if l == nil {
		return
	}
	activationID = stringsTrim(activationID)
	if activationID == "" {
		return
	}
	l.mu.Lock()
	if l.stop {
		l.mu.Unlock()
		return
	}
	if _, exists := l.retry[activationID]; exists {
		l.mu.Unlock()
		return
	}
	l.retry[activationID] = struct{}{}
	l.wg.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.wg.Done()
		defer func() {
			l.mu.Lock()
			delete(l.retry, activationID)
			l.mu.Unlock()
		}()
		delay := 50 * time.Millisecond
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(delay):
			}
			if err := l.ReconcileWakeup(l.ctx, activationID); err == nil {
				return
			} else {
				l.log(l.ctx, "recovery", activationID, err)
			}
			if delay < time.Second {
				delay *= 2
			}
		}
	}()
}

func (l *Lifecycle) retireExactWakeup(ctx context.Context, wakeup Wakeup) error {
	if err := wakeup.Validate(); err != nil {
		return err
	}
	if err := l.scheduler.RetireGenericScheduleWakeup(wakeup); err != nil {
		return err
	}
	return l.store.ReleaseGenericScheduleWakeup(ctx, wakeup)
}

func (l *Lifecycle) startTerminalRetirementRecovery(wakeup Wakeup) {
	if l == nil || wakeup.Validate() != nil {
		return
	}
	recoveryKey := "terminal:" + wakeup.ActivationID() + ":" + formatTime(wakeup.DueAt())
	l.mu.Lock()
	if l.stop {
		l.mu.Unlock()
		return
	}
	if _, exists := l.retry[recoveryKey]; exists {
		l.mu.Unlock()
		return
	}
	l.retry[recoveryKey] = struct{}{}
	l.wg.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.wg.Done()
		defer func() {
			l.mu.Lock()
			delete(l.retry, recoveryKey)
			l.mu.Unlock()
		}()
		delay := 50 * time.Millisecond
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(delay):
			}
			if err := l.retireExactWakeup(l.ctx, wakeup); err == nil {
				return
			} else {
				l.log(l.ctx, "terminal_retirement_recovery", wakeup.ActivationID(), err)
			}
			if delay < time.Second {
				delay *= 2
			}
		}
	}()
}

func (l *Lifecycle) Stop(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if !l.stop {
		l.stop = true
		l.cancel()
	}
	l.mu.Unlock()
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	if err := l.scheduler.StopGenericScheduleWakeups(ctx); err != nil {
		return err
	}
	return l.store.ReleaseGenericScheduleClaims(ctx)
}

func (l *Lifecycle) log(ctx context.Context, action, activationID string, err error) {
	if l != nil && l.logger != nil && err != nil {
		l.logger.GenericScheduleFailure(ctx, action, activationID, err)
	}
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
