package genericschedule

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func testGlobalActivation(t *testing.T, due DueBasis, admittedAt, currentDue time.Time) Activation {
	t.Helper()
	command := AdmissionCommand{
		ScheduleKey: "global-proof", OwnerKind: OwnerSystem, OwnerID: "runtime",
		EventType: "platform.generic_schedule_proof", Payload: semanticvalue.EmptyObject(),
		RoutingSource: events.NewPlatformControlRoutingSource(), Due: due, TaskID: "global-proof",
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	initial, err := due.FirstDue(admittedAt)
	if err != nil {
		t.Fatal(err)
	}
	activation := Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: admittedAt,
		InitialDueAt: initial, CurrentDueAt: currentDue, Status: StatusActive,
	}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation
}

func TestOccurrenceEventUsesPersistedDueCoordinateAsSemanticTime(t *testing.T) {
	dueAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	admittedAt := dueAt.Add(3 * time.Hour)
	activation := testGlobalActivation(t, AbsoluteDue(dueAt), dueAt.Add(-time.Hour), dueAt)
	occurrence := Occurrence{
		ActivationID: activation.ID, DueAt: dueAt,
		EventID: OccurrenceEventID(activation.ID, dueAt), AdmittedAt: admittedAt,
	}
	event, err := occurrenceEvent(activation, occurrence, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !event.CreatedAt().Equal(dueAt) {
		t.Fatalf("event semantic time = %s, want persisted due coordinate %s", event.CreatedAt(), dueAt)
	}
	if occurrence.AdmittedAt.Equal(event.CreatedAt()) {
		t.Fatal("occurrence admission bookkeeping was conflated with semantic occurrence time")
	}
}

func TestCatchupDepthUsesPersistedCoordinatesWithoutSkipping(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	activation := testGlobalActivation(t, EveryDue(time.Minute), now.Add(-2001*time.Minute), now.Add(-2000*time.Minute))
	depth, err := catchupDepth(activation, now)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 2001 {
		t.Fatalf("catch-up depth = %d, want 2001 persisted occurrences", depth)
	}
}

type restoreStore struct{ activation Activation }

func (s *restoreStore) AdmitGenericSchedule(context.Context, AdmissionCommand) (AdmissionResult, error) {
	return AdmissionResult{}, nil
}
func (s *restoreStore) LoadGenericScheduleActivation(context.Context, string) (Activation, bool, error) {
	return s.activation, true, nil
}
func (s *restoreStore) ListActiveGenericScheduleActivations(context.Context) ([]Activation, error) {
	return []Activation{s.activation}, nil
}
func (*restoreStore) PrepareGenericScheduleOccurrence(context.Context, Wakeup) (PreparedOccurrence, error) {
	return PreparedOccurrence{}, nil
}
func (*restoreStore) CommitGenericScheduleOccurrence(context.Context, CommitCommand) (CommitResult, error) {
	return CommitResult{}, nil
}
func (*restoreStore) CancelGenericSchedule(context.Context, CancelCommand) (CancelResult, error) {
	return CancelResult{}, nil
}
func (*restoreStore) ClaimGenericScheduleWakeup(context.Context, Wakeup) (bool, error) {
	return true, nil
}
func (*restoreStore) ReleaseGenericScheduleWakeup(context.Context, Wakeup) error { return nil }
func (*restoreStore) ReleaseGenericScheduleClaims(context.Context) error         { return nil }

type restoreScheduler struct {
	callback   func(context.Context, Wakeup)
	registered []Wakeup
}

func (s *restoreScheduler) BindGenericScheduleLifecycle(callback func(context.Context, Wakeup)) error {
	s.callback = callback
	return nil
}
func (s *restoreScheduler) RegisterGenericScheduleWakeup(_ context.Context, wakeup Wakeup) error {
	s.registered = append(s.registered, wakeup)
	return nil
}
func (*restoreScheduler) RetireGenericScheduleWakeup(Wakeup) error         { return nil }
func (*restoreScheduler) StopGenericScheduleWakeups(context.Context) error { return nil }

type restorePlanner struct{}

func (restorePlanner) PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	return nil, nil
}
func (restorePlanner) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	return nil
}
func (restorePlanner) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	return nil
}

type restoreDispatcher struct{}

func (restoreDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type restoreLogger struct {
	activationID string
	depth        int
}

func (*restoreLogger) GenericScheduleFailure(context.Context, string, string, error) {}
func (l *restoreLogger) GenericScheduleCatchupWarning(_ context.Context, activationID string, depth int) {
	l.activationID, l.depth = activationID, depth
}

func TestLifecycleRestoreEmitsDeepCatchupWarningBeforeRegisteringWakeup(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	activation := testGlobalActivation(t, EveryDue(time.Millisecond), now.Add(-2001*time.Millisecond), now.Add(-1500*time.Millisecond))
	store := &restoreStore{activation: activation}
	scheduler := &restoreScheduler{}
	logger := &restoreLogger{}
	lifecycle, err := NewLifecycle(store, scheduler, restorePlanner{}, restoreDispatcher{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := lifecycle.Restore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reconciled != 1 {
		t.Fatalf("reconciled activations = %d, want 1", reconciled)
	}
	if logger.activationID != activation.ID || logger.depth <= catchupWarningThreshold {
		t.Fatalf("catch-up warning = activation:%q depth:%d", logger.activationID, logger.depth)
	}
	if len(scheduler.registered) != 1 || scheduler.registered[0].ActivationID() != activation.ID {
		t.Fatalf("registered wakeups = %#v", scheduler.registered)
	}
}

type lifecycleProofPlan struct{ intent runtimeengine.EmitIntent }

func (p lifecycleProofPlan) DurablePublicationEventID() string { return p.intent.Event.ID() }
func (p lifecycleProofPlan) ValidateDurablePublicationPlan() error {
	if strings.TrimSpace(p.DurablePublicationEventID()) == "" {
		return errors.New("lifecycle proof publication requires event identity")
	}
	return nil
}

type lifecycleProofCommit struct{ plan lifecycleProofPlan }

func (p lifecycleProofCommit) CommittedDurablePublicationEventID() string {
	return p.plan.DurablePublicationEventID()
}
func (p lifecycleProofCommit) CommittedDurablePublicationIntent() runtimeengine.EmitIntent {
	return p.plan.intent
}
func (p lifecycleProofCommit) ValidateCommittedDurablePublication() error {
	return p.plan.ValidateDurablePublicationPlan()
}

type lifecycleProofStore struct {
	activation  Activation
	prepared    PreparedOccurrence
	commit      func(CommitCommand) (CommitResult, error)
	commitCalls int
	released    []Wakeup
	order       *[]string
}

func (s *lifecycleProofStore) AdmitGenericSchedule(context.Context, AdmissionCommand) (AdmissionResult, error) {
	if s.order != nil {
		*s.order = append(*s.order, "persist")
	}
	return AdmissionResult{Outcome: AdmissionCreated, Activation: s.activation}, nil
}
func (s *lifecycleProofStore) LoadGenericScheduleActivation(context.Context, string) (Activation, bool, error) {
	if s.order != nil {
		*s.order = append(*s.order, "load")
	}
	return s.activation, true, nil
}
func (s *lifecycleProofStore) ListActiveGenericScheduleActivations(context.Context) ([]Activation, error) {
	return []Activation{s.activation}, nil
}
func (s *lifecycleProofStore) PrepareGenericScheduleOccurrence(context.Context, Wakeup) (PreparedOccurrence, error) {
	return s.prepared, nil
}
func (s *lifecycleProofStore) CommitGenericScheduleOccurrence(_ context.Context, command CommitCommand) (CommitResult, error) {
	s.commitCalls++
	if s.commit == nil {
		return CommitResult{}, errors.New("unexpected generic schedule commit")
	}
	return s.commit(command)
}
func (*lifecycleProofStore) CancelGenericSchedule(context.Context, CancelCommand) (CancelResult, error) {
	return CancelResult{}, nil
}
func (s *lifecycleProofStore) ClaimGenericScheduleWakeup(context.Context, Wakeup) (bool, error) {
	if s.order != nil {
		*s.order = append(*s.order, "claim")
	}
	return true, nil
}
func (s *lifecycleProofStore) ReleaseGenericScheduleWakeup(_ context.Context, wakeup Wakeup) error {
	s.released = append(s.released, wakeup)
	if s.order != nil {
		*s.order = append(*s.order, "release")
	}
	return nil
}
func (*lifecycleProofStore) ReleaseGenericScheduleClaims(context.Context) error { return nil }

type lifecycleProofScheduler struct {
	restoreScheduler
	order     *[]string
	retired   []Wakeup
	retireErr error
}

func (s *lifecycleProofScheduler) RegisterGenericScheduleWakeup(ctx context.Context, wakeup Wakeup) error {
	if s.order != nil {
		*s.order = append(*s.order, "register")
	}
	return s.restoreScheduler.RegisterGenericScheduleWakeup(ctx, wakeup)
}
func (s *lifecycleProofScheduler) RetireGenericScheduleWakeup(wakeup Wakeup) error {
	s.retired = append(s.retired, wakeup)
	if s.order != nil {
		*s.order = append(*s.order, "retire")
	}
	return s.retireErr
}

type lifecycleProofPlanner struct {
	prepareErr error
	releases   int
	finalizes  int
}

func (p *lifecycleProofPlanner) PrepareEnginePublications(_ context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	result := make([]runtimeengine.DurablePublicationPlan, 0, len(intents))
	for _, intent := range intents {
		result = append(result, lifecycleProofPlan{intent: intent})
	}
	return result, nil
}
func (p *lifecycleProofPlanner) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	p.releases++
	return nil
}
func (p *lifecycleProofPlanner) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	p.finalizes++
	return nil
}

type lifecycleProofDispatcher struct{ calls int }

func (d *lifecycleProofDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	d.calls++
	return nil
}

func lifecyclePreparedOccurrence(t *testing.T) (Activation, Occurrence, Wakeup) {
	t.Helper()
	dueAt := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	activation := testGlobalActivation(t, AbsoluteDue(dueAt), dueAt.Add(-time.Hour), dueAt)
	activation.CurrentEventID = OccurrenceEventID(activation.ID, dueAt)
	activation.CurrentEventAdmittedAt = dueAt.Add(time.Second)
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	occurrence := Occurrence{
		ActivationID: activation.ID,
		DueAt:        dueAt,
		EventID:      activation.CurrentEventID,
		AdmittedAt:   activation.CurrentEventAdmittedAt,
	}
	wakeup, err := activation.Wakeup()
	if err != nil {
		t.Fatal(err)
	}
	return activation, occurrence, wakeup
}

func stopLifecycleProof(t *testing.T, lifecycle *Lifecycle) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lifecycle.Stop(ctx); err != nil {
		t.Fatalf("stop generic schedule lifecycle proof: %v", err)
	}
}

func TestLifecyclePersistsActivationBeforeRegisteringWakeup(t *testing.T) {
	dueAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	activation := testGlobalActivation(t, AbsoluteDue(dueAt), dueAt.Add(-time.Minute), dueAt)
	order := []string{}
	store := &lifecycleProofStore{activation: activation, order: &order}
	scheduler := &lifecycleProofScheduler{order: &order}
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)
	if _, err := lifecycle.Admit(context.Background(), activation.Command); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "persist,load,claim,register" {
		t.Fatalf("generic schedule admission order = %q", got)
	}
}

func TestLifecycleTerminalWakeupsRetireBeforeReleasingClaims(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, test := range []struct {
		name   string
		status Status
		apply  func(*Activation)
	}{
		{
			name: "fired_one_shot", status: StatusFired,
			apply: func(activation *Activation) {
				activation.AcceptedAt = now
				activation.FiredAt = now
			},
		},
		{
			name: "cancelled", status: StatusCancelled,
			apply: func(activation *Activation) {
				activation.CancelCause = "operator_cancelled"
				activation.CancelledAt = now
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			activation := testGlobalActivation(t, AbsoluteDue(now.Add(time.Hour)), now, now.Add(time.Hour))
			activation.Status = test.status
			test.apply(&activation)
			if err := activation.Validate(); err != nil {
				t.Fatal(err)
			}
			order := []string{}
			store := &lifecycleProofStore{activation: activation, order: &order}
			scheduler := &lifecycleProofScheduler{order: &order}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer stopLifecycleProof(t, lifecycle)

			if err := lifecycle.ReconcileWakeup(context.Background(), activation.ID); err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(order, ","); got != "load,retire,release" {
				t.Fatalf("terminal reconciliation order = %q, want load,retire,release", got)
			}
			if len(scheduler.retired) != 1 || len(store.released) != 1 || scheduler.retired[0] != store.released[0] {
				t.Fatalf("terminal wakeup retirement/release = retired:%#v released:%#v", scheduler.retired, store.released)
			}
		})
	}
}

func TestLifecycleActiveRecurringWakeupRetainsClaim(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	activation := testGlobalActivation(t, EveryDue(time.Hour), now, now.Add(time.Hour))
	order := []string{}
	store := &lifecycleProofStore{activation: activation, order: &order}
	scheduler := &lifecycleProofScheduler{order: &order}
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)

	if err := lifecycle.ReconcileWakeup(context.Background(), activation.ID); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "load,claim,register" {
		t.Fatalf("active recurring reconciliation order = %q, want load,claim,register", got)
	}
	if len(scheduler.retired) != 0 || len(store.released) != 0 {
		t.Fatalf("active recurring wakeup lost claim: retired:%#v released:%#v", scheduler.retired, store.released)
	}
}

func TestLifecycleRetirementFailureRetainsClaim(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	activation := testGlobalActivation(t, AbsoluteDue(now.Add(time.Hour)), now, now.Add(time.Hour))
	activation.Status = StatusCancelled
	activation.CancelCause = "operator_cancelled"
	activation.CancelledAt = now
	retireErr := errors.New("scheduler retirement failed")
	order := []string{}
	store := &lifecycleProofStore{activation: activation, order: &order}
	scheduler := &lifecycleProofScheduler{order: &order, retireErr: retireErr}
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)

	err = lifecycle.ReconcileWakeup(context.Background(), activation.ID)
	if !errors.Is(err, retireErr) {
		t.Fatalf("retirement error = %v, want %v", err, retireErr)
	}
	if got := strings.Join(order, ","); got != "load,retire" {
		t.Fatalf("failed retirement order = %q, want load,retire", got)
	}
	if len(store.released) != 0 {
		t.Fatalf("failed retirement released claim: %#v", store.released)
	}
}

func TestLifecyclePlannerFailureCannotReachOccurrenceSettlement(t *testing.T) {
	activation, occurrence, wakeup := lifecyclePreparedOccurrence(t)
	store := &lifecycleProofStore{
		activation: activation,
		prepared:   PreparedOccurrence{Outcome: PrepareReady, Activation: activation, Occurrence: occurrence},
	}
	plannerErr := errors.New("publication plan rejected")
	planner := &lifecycleProofPlanner{prepareErr: plannerErr}
	lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, planner, &lifecycleProofDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)
	result, err := lifecycle.fire(context.Background(), wakeup)
	if !errors.Is(err, plannerErr) || result.Outcome != CommitRetry {
		t.Fatalf("planner failure result = %#v, %v", result, err)
	}
	if store.commitCalls != 0 {
		t.Fatalf("planner failure reached occurrence settlement %d time(s)", store.commitCalls)
	}
}

func TestLifecycleCommittedReplayDoesNotFinalizeOrDispatchAgain(t *testing.T) {
	activation, occurrence, wakeup := lifecyclePreparedOccurrence(t)
	next := activation
	next.Status = StatusFired
	next.AcceptedAt = occurrence.AdmittedAt.Add(time.Millisecond)
	next.FiredAt = next.AcceptedAt
	store := &lifecycleProofStore{
		activation: activation,
		prepared:   PreparedOccurrence{Outcome: PrepareReady, Activation: activation, Occurrence: occurrence},
		commit: func(command CommitCommand) (CommitResult, error) {
			plan := command.Publication.(lifecycleProofPlan)
			return CommitResult{
				Outcome: CommitCommitted, Next: next, Publication: lifecycleProofCommit{plan: plan},
				PublicationAlreadyCommitted: true,
			}, nil
		},
	}
	planner := &lifecycleProofPlanner{}
	dispatcher := &lifecycleProofDispatcher{}
	lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, planner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)
	result, err := lifecycle.fire(context.Background(), wakeup)
	if err != nil || result.Outcome != CommitCommitted || !result.PublicationAlreadyCommitted {
		t.Fatalf("committed replay result = %#v, %v", result, err)
	}
	if planner.releases != 1 || planner.finalizes != 0 || dispatcher.calls != 0 {
		t.Fatalf("committed replay side effects = releases:%d finalizes:%d dispatches:%d", planner.releases, planner.finalizes, dispatcher.calls)
	}
}
