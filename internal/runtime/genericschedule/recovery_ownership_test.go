package genericschedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

type recoveryRegistration struct {
	owner  worklifetime.Occurrence
	err    error
	wakeup Wakeup
}

type recoveryOwnershipScheduler struct {
	restoreScheduler
	mu       sync.Mutex
	failures int
	seen     chan recoveryRegistration
}

func (s *recoveryOwnershipScheduler) RegisterGenericScheduleWakeup(ctx context.Context, wakeup Wakeup) error {
	owner, _ := worklifetime.OccurrenceFromContext(ctx)
	s.mu.Lock()
	fail := s.failures > 0
	if fail {
		s.failures--
	}
	s.mu.Unlock()
	s.seen <- recoveryRegistration{owner: owner, err: ctx.Err(), wakeup: wakeup}
	if fail {
		return errors.New("injected registration failure")
	}
	return s.restoreScheduler.RegisterGenericScheduleWakeup(ctx, wakeup)
}

type recoveryOwnershipStore struct {
	*lifecycleProofStore
	mu            sync.Mutex
	claimFailures int
	claims        int
	admissions    int
	loads         int
	holdRecovery  chan struct{}
	recoveryRead  chan context.Context
}

func (s *recoveryOwnershipStore) AdmitGenericScheduleOutcome(ctx context.Context, command AdmissionCommand) (AdmissionCommit, error) {
	s.mu.Lock()
	s.admissions++
	s.mu.Unlock()
	return s.lifecycleProofStore.AdmitGenericScheduleOutcome(ctx, command)
}

func (s *recoveryOwnershipStore) LoadGenericScheduleActivation(ctx context.Context, id string) (Activation, bool, error) {
	s.mu.Lock()
	s.loads++
	hold, seen := s.holdRecovery, s.recoveryRead
	blocked := hold != nil && s.loads > 1
	s.mu.Unlock()
	if blocked {
		seen <- ctx
		select {
		case <-hold:
		case <-ctx.Done():
			return Activation{}, false, ctx.Err()
		}
	}
	return s.lifecycleProofStore.LoadGenericScheduleActivation(ctx, id)
}

func (s *recoveryOwnershipStore) ClaimGenericScheduleWakeup(ctx context.Context, wakeup Wakeup) (bool, error) {
	s.mu.Lock()
	s.claims++
	fail := s.claimFailures > 0
	if fail {
		s.claimFailures--
	}
	s.mu.Unlock()
	if fail {
		return false, errors.New("injected claim failure")
	}
	return s.lifecycleProofStore.ClaimGenericScheduleWakeup(ctx, wakeup)
}

func instanceRecoveryActivation(t *testing.T) Activation {
	t.Helper()
	command := instanceScheduleCommand(t, ".")
	admitted := time.Now().UTC().Truncate(time.Microsecond)
	due, err := command.Due.FirstDue(admitted)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	return Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
		AdmittedAt: admitted, InitialDueAt: due, CurrentDueAt: due, Status: StatusActive}
}

func instanceRecoveryWorkOwner(t *testing.T, runID string) (*worklifetime.StandingOccurrence, *worklifetime.RuntimeOccurrence) {
	t.Helper()
	process := worklifetime.NewProcess()
	runtime, err := process.NewRuntime(t.Context(), worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: "clock-bundle"})
	if err != nil {
		t.Fatal(err)
	}
	standing, err := runtime.NewStanding(t.Context(), worklifetime.StandingIdentity{ServiceID: "clock-service", RunID: runID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := standing.RetireAndWait(ctx); err != nil {
			t.Error(err)
		}
		if _, err := runtime.RetireAndWait(ctx); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	return standing, runtime
}

func requireRecoveryRegistration(t *testing.T, scheduler *recoveryOwnershipScheduler, owner worklifetime.Occurrence) recoveryRegistration {
	t.Helper()
	select {
	case got := <-scheduler.seen:
		if got.owner != owner {
			t.Fatalf("registration lost exact execution owner: got %T %p, want %T %p", got.owner, got.owner, owner, owner)
		}
		return got
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach registration")
		return recoveryRegistration{}
	}
}

func TestInstanceClockRecoveryRetainsExactStandingOwner(t *testing.T) {
	for _, entry := range []string{"admission", "claim", "occurrence", "reconcile_after_fire"} {
		t.Run(entry, func(t *testing.T) {
			activation := instanceRecoveryActivation(t)
			standing, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
			store := &recoveryOwnershipStore{lifecycleProofStore: &lifecycleProofStore{activation: activation}}
			scheduler := &recoveryOwnershipScheduler{seen: make(chan recoveryRegistration, 8)}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
			lease, err := standing.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := lease.Done(); err != nil && !errors.Is(err, worklifetime.ErrAlreadySettled) {
					t.Error(err)
				}
			}()
			switch entry {
			case "admission":
				scheduler.failures = 3
				if _, err := lifecycle.Admit(lease.Context(), activation.Command); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 3; i++ {
					requireRecoveryRegistration(t, scheduler, standing)
				}
			case "claim":
				store.claimFailures = 1
				queued, err := lifecycle.ReconcileWakeupWithRecovery(lease.Context(), activation.ID)
				if !queued || err == nil {
					t.Fatalf("claim retry: queued=%t err=%v", queued, err)
				}
			case "occurrence":
				store.prepareReject = true
				wakeup, err := activation.Wakeup()
				if err != nil {
					t.Fatal(err)
				}
				lifecycle.handleWakeup(lease.Context(), wakeup)
			case "reconcile_after_fire":
				occurrence := Occurrence{ActivationID: activation.ID, DueAt: activation.CurrentDueAt,
					EventID: OccurrenceEventID(activation.ID, activation.CurrentDueAt), AdmittedAt: activation.CurrentDueAt}
				activation.CurrentEventID, activation.CurrentEventAdmittedAt = occurrence.EventID, occurrence.AdmittedAt
				store.activation = activation
				store.prepared = PreparedOccurrence{Outcome: PrepareReady, Activation: activation, Occurrence: occurrence}
				store.commit = func(command CommitCommand) (CommitResult, error) {
					next := activation
					next.CurrentDueAt, err = activation.Command.Due.Next(activation.CurrentDueAt)
					if err != nil {
						return CommitResult{}, err
					}
					next.CurrentEventID, next.CurrentEventAdmittedAt = "", time.Time{}
					store.activation = next
					return CommitResult{Outcome: CommitCommitted, Next: next, Publication: lifecycleProofCommit{plan: command.Publication.(lifecycleProofPlan)}}, nil
				}
				scheduler.failures = 1
				wakeup, err := activation.Wakeup()
				if err != nil {
					t.Fatal(err)
				}
				lifecycle.handleWakeup(lease.Context(), wakeup)
				requireRecoveryRegistration(t, scheduler, standing)
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			recovered := requireRecoveryRegistration(t, scheduler, standing)
			if recovered.err != nil {
				t.Fatalf("recovery borrowed its completed initiating lease: %v", recovered.err)
			}
		})
	}
}

func TestInstanceClockRecoveryRejectsMissingWrongAndClosedOwners(t *testing.T) {
	for _, kind := range []string{"missing", "runtime", "wrong_run", "fenced", "retired"} {
		t.Run(kind, func(t *testing.T) {
			activation := instanceRecoveryActivation(t)
			runID := activation.Command.RunID
			if kind == "wrong_run" {
				runID = uuid.NewString()
			}
			standing, runtime := instanceRecoveryWorkOwner(t, runID)
			ctx := worklifetime.WithOccurrence(t.Context(), standing)
			switch kind {
			case "missing":
				ctx = t.Context()
			case "runtime":
				ctx = worklifetime.WithOccurrence(t.Context(), runtime)
			case "fenced":
				if err := standing.Fence(); err != nil {
					t.Fatal(err)
				}
			case "retired":
				standing.Retire()
			}
			store := &recoveryOwnershipStore{lifecycleProofStore: &lifecycleProofStore{activation: activation}}
			scheduler := &recoveryOwnershipScheduler{seen: make(chan recoveryRegistration, 8)}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
			queued, err := lifecycle.ReconcileWakeupWithRecovery(ctx, activation.ID)
			if queued || err == nil {
				t.Fatalf("%s execution owner accepted: queued=%t err=%v", kind, queued, err)
			}
			select {
			case registration := <-scheduler.seen:
				t.Fatalf("invalid execution owner reached scheduler: %+v", registration)
			default:
			}
			if _, err := lifecycle.Admit(ctx, activation.Command); err == nil {
				t.Fatal("invalid owner reached schedule admission")
			}
			store.mu.Lock()
			claims, admissions := store.claims, store.admissions
			store.mu.Unlock()
			if claims != 0 || admissions != 0 {
				t.Fatalf("invalid owner mutated schedule: claims=%d admissions=%d", claims, admissions)
			}
		})
	}
}

func TestInstanceClockPendingRecoveryJoinsStandingAndShutdown(t *testing.T) {
	for _, stop := range []string{"standing_retire", "standing_fence", "runtime_shutdown"} {
		t.Run(stop, func(t *testing.T) {
			activation := instanceRecoveryActivation(t)
			standing, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
			store := &recoveryOwnershipStore{lifecycleProofStore: &lifecycleProofStore{activation: activation},
				holdRecovery: make(chan struct{}), recoveryRead: make(chan context.Context, 1)}
			scheduler := &recoveryOwnershipScheduler{failures: 1, seen: make(chan recoveryRegistration, 8)}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
			queued, err := lifecycle.ReconcileWakeupWithRecovery(worklifetime.WithOccurrence(t.Context(), standing), activation.ID)
			if !queued || err == nil {
				t.Fatalf("initial registration: queued=%t err=%v", queued, err)
			}
			requireRecoveryRegistration(t, scheduler, standing)
			var recovering context.Context
			select {
			case recovering = <-store.recoveryRead:
			case <-time.After(time.Second):
				t.Fatal("recovery did not reach held store read")
			}
			owner, _ := worklifetime.OccurrenceFromContext(recovering)
			if owner != standing {
				t.Fatalf("pending recovery detached from standing owner: %T", owner)
			}
			cancelled, cancelWait := context.WithCancel(context.Background())
			cancelWait()
			if err := standing.Wait(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("held recovery is not accounted by its standing owner: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch stop {
			case "standing_retire":
				if err := standing.RetireAndWait(ctx); err != nil {
					t.Fatal(err)
				}
			case "standing_fence":
				if err := standing.Fence(); err != nil {
					t.Fatal(err)
				}
				close(store.holdRecovery)
				if err := standing.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			case "runtime_shutdown":
				if err := lifecycle.Stop(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := standing.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := lifecycle.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case registration := <-scheduler.seen:
				t.Fatalf("closed owner registered an orphan wakeup: %+v", registration)
			default:
			}
		})
	}
}

func TestInstanceClockRecoveryDoesNotBorrowSuccessor(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	standing, runtime := instanceRecoveryWorkOwner(t, activation.Command.RunID)
	successor, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
	store := &recoveryOwnershipStore{lifecycleProofStore: &lifecycleProofStore{activation: activation},
		holdRecovery: make(chan struct{}), recoveryRead: make(chan context.Context, 1)}
	scheduler := &recoveryOwnershipScheduler{failures: 1, seen: make(chan recoveryRegistration, 8)}
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
	ctx := worklifetime.WithOccurrence(t.Context(), standing)
	queued, err := lifecycle.ReconcileWakeupWithRecovery(ctx, activation.ID)
	if !queued || err == nil {
		t.Fatalf("initial recovery: queued=%t err=%v", queued, err)
	}
	requireRecoveryRegistration(t, scheduler, standing)
	select {
	case <-store.recoveryRead:
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach held store read")
	}
	for _, replacement := range []worklifetime.Occurrence{nil, runtime, successor} {
		queued, err := lifecycle.startRecovery(worklifetime.WithOccurrence(t.Context(), replacement), activation.ID)
		if queued || err == nil {
			t.Fatalf("recovery adopted %T: queued=%t err=%v", replacement, queued, err)
		}
	}
	if queued, err := lifecycle.startRecovery(ctx, activation.ID); !queued || err != nil {
		t.Fatalf("exact-owner recovery failed to coalesce: queued=%t err=%v", queued, err)
	}
	standing.Retire()
	if err := lifecycle.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case registration := <-scheduler.seen:
		t.Fatalf("recovery escaped to successor: %+v", registration)
	default:
	}
}
