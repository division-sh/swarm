package worklifetime

import (
	"context"
	"errors"
	"fmt"
	"testing"
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
