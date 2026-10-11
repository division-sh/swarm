package worklifetime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPublicCompletionAdmissionSharedProcessBound(t *testing.T) {
	process := NewProcess()
	var owners []*RuntimeOccurrence
	for index := range 2 {
		owner, err := process.NewRuntime(t.Context(), RuntimeIdentity{RuntimeInstanceID: fmt.Sprint(index), BundleHash: "bundle"})
		if err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner)
	}
	var leases []*Lease
	t.Cleanup(func() {
		for _, lease := range leases {
			if lease != nil {
				_ = lease.Done()
			}
		}
		for _, owner := range owners {
			_, _ = owner.RetireAndWait(context.Background())
		}
		_, _ = process.Join(context.Background())
	})
	for index := range PublicCompletionLimit {
		lease, err := BeginPublicCompletion(t.Context(), owners[index%2])
		if err != nil {
			t.Fatalf("reservation %d: %v", index, err)
		}
		leases = append(leases, lease)
	}
	if _, err := BeginPublicCompletion(t.Context(), owners[1]); !errors.Is(err, ErrPublicCompletionCapacity) {
		t.Fatalf("N+1 admission = %v", err)
	}
	if err := leases[0].Done(); err != nil {
		t.Fatal(err)
	}
	if err := leases[0].Done(); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("duplicate disposition = %v", err)
	}
	leases[0] = nil
	lease, err := BeginPublicCompletion(t.Context(), owners[1])
	if err != nil {
		t.Fatalf("release-one/readmit: %v", err)
	}
	leases = append(leases, lease)
	for index, lease := range leases {
		if lease != nil {
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			leases[index] = nil
		}
	}
	if process.occurrence.gate.publicCompletions != 0 {
		t.Fatalf("retained slots = %d", process.occurrence.gate.publicCompletions)
	}
}

func TestPublicCompletionAdmissionClosedOccurrenceProjections(t *testing.T) {
	process, foreign := NewProcess(), NewProcess()
	runtime, err := process.NewRuntime(t.Context(), RuntimeIdentity{RuntimeInstanceID: "runtime", BundleHash: "bundle"})
	if err != nil {
		t.Fatal(err)
	}
	standing, err := runtime.NewStanding(t.Context(), StandingIdentity{ServiceID: "service", RunID: "run", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := process.NewSelectedFork(t.Context(), SelectedForkIdentity{ExecutionID: "fork", RunID: "fork-run", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerRunOccurrence(t.Context(), runtime, ManagerRunIdentity{Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = manager.RetireAndWait(context.Background())
		_ = standing.RetireAndWait(context.Background())
		_ = selected.RetireAndWait(context.Background())
		_, _ = runtime.RetireAndWait(context.Background())
		_, _ = process.Join(context.Background())
		_, _ = foreign.Join(context.Background())
	})
	for name, owner := range map[string]Occurrence{
		"runtime": runtime, "standing": standing, "selected": selected,
		"manager-standing": &ManagerWorkOccurrence{manager: manager, companion: standing},
		"manager-selected": &ManagerWorkOccurrence{manager: manager, companion: selected},
	} {
		t.Run(name, func(t *testing.T) {
			lease, err := BeginPublicCompletion(WithProcess(t.Context(), foreign), owner)
			if err != nil {
				t.Fatal(err)
			}
			actual, ok := ProcessFromContext(lease.Context())
			if !ok || actual != process || process.PublicCompletionCount() != 1 || foreign.PublicCompletionCount() != 0 {
				t.Fatal("ambient process replaced exact occurrence authority")
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
		})
	}
	foreignSelected, err := foreign.NewSelectedFork(t.Context(), SelectedForkIdentity{ExecutionID: "foreign", RunID: "foreign-run", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreignSelected.RetireAndWait(context.Background()) }()
	if _, err := BeginPublicCompletion(t.Context(), &ManagerWorkOccurrence{manager: manager, companion: foreignSelected}); err == nil {
		t.Fatal("accepted foreign Manager companion")
	}
	if process.PublicCompletionCount() != 0 || foreign.PublicCompletionCount() != 0 {
		t.Fatal("rejected companion leaked capacity")
	}
}

func TestPublicCompletionAdmissionOwnedJoinAndReplacement(t *testing.T) {
	process := NewProcess()
	predecessor, err := process.NewRuntime(t.Context(), RuntimeIdentity{RuntimeInstanceID: "old", BundleHash: "bundle"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := BeginPublicCompletion(t.Context(), predecessor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Done() }()
	predecessor.Retire()
	<-lease.Context().Done()
	cut, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := predecessor.RetireAndWait(cut); !errors.Is(err, context.Canceled) {
		t.Fatalf("predecessor joined before accepted disposition: %v", err)
	}
	successor, err := process.NewRuntime(t.Context(), RuntimeIdentity{RuntimeInstanceID: "new", BundleHash: "bundle"})
	if err != nil {
		t.Fatal(err)
	}
	successorLease, err := BeginPublicCompletion(t.Context(), successor)
	if err != nil {
		t.Fatal(err)
	}
	if process.PublicCompletionCount() != 2 || successorLease.Context().Err() != nil {
		t.Fatal("replacement lost predecessor responsibility or canceled successor")
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := predecessor.RetireAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	process.Retire()
	<-successorLease.Context().Done()
	if _, err := process.Join(cut); !errors.Is(err, context.Canceled) {
		t.Fatalf("process joined before successor disposition: %v", err)
	}
	if _, err := BeginPublicCompletion(t.Context(), successor); !errors.Is(err, ErrRetired) {
		t.Fatalf("retirement-wins admission=%v", err)
	}
	if err := successorLease.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := successor.RetireAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancelJoin := context.WithTimeout(t.Context(), time.Second)
	defer cancelJoin()
	if _, err := process.Join(ctx); err != nil || process.PublicCompletionCount() != 0 {
		t.Fatalf("final process join: count=%d err=%v", process.PublicCompletionCount(), err)
	}
}

func TestPublicCompletionAdmissionCancellationAndRetirement(t *testing.T) {
	process := NewProcess()
	owner, err := process.NewRuntime(t.Context(), RuntimeIdentity{RuntimeInstanceID: "runtime", BundleHash: "bundle"})
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(t.Context())
	lease, err := BeginPublicCompletion(caller, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = lease.Done()
		_, _ = owner.RetireAndWait(context.Background())
		_, _ = process.Join(context.Background())
	})
	cancel()
	if err := lease.Context().Err(); err != nil {
		t.Fatalf("accepted lifetime inherited caller cancellation: %v", err)
	}
	if _, err := BeginPublicCompletion(caller, owner); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled precommit admission = %v", err)
	}
	if err := owner.Fence(); err != nil {
		t.Fatal(err)
	}
	if _, err := BeginPublicCompletion(t.Context(), owner); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("fence-wins admission = %v", err)
	}
	if process.occurrence.gate.publicCompletions != 1 {
		t.Fatalf("partial admission leaked capacity: %d", process.occurrence.gate.publicCompletions)
	}
	owner.Retire()
	<-lease.Context().Done()
	if !errors.Is(context.Cause(lease.Context()), ErrRetired) {
		t.Fatalf("retirement cause = %v", context.Cause(lease.Context()))
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.RetireAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Join(t.Context()); err != nil {
		t.Fatal(err)
	}
}
