package serveapp

import (
	"fmt"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"

	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func retainedJoinOutcomesComplete(joins []joinruntime.Activation, expected []timeridentity.JoinRef) (bool, error) {
	if len(expected) == 0 || len(joins) != len(expected) {
		return false, fmt.Errorf("retained join catalog has %d entries, want %d exact generations", len(joins), len(expected))
	}
	for _, join := range joins {
		if err := join.Validate(); err != nil {
			return false, err
		}
	}
	complete := true
	for index, ref := range expected {
		if !ref.Valid() || !ref.Generation().Valid() {
			return false, fmt.Errorf("retained join requires a valid captured generation")
		}
		for _, previous := range expected[:index] {
			if ref.Equal(previous) {
				return false, fmt.Errorf("retained join contains a duplicate expected generation")
			}
		}
		found := 0
		for _, join := range joins {
			if !join.JoinRef().Equal(ref) {
				continue
			}
			found++
			if join.Status == joinruntime.StatusClosed && join.CloseReason != joinruntime.CloseReasonComplete {
				return false, fmt.Errorf("retained join closed with %q instead of completed outcome", join.CloseReason)
			}
			if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete || !join.TimerCancelled || join.OutcomePending || !join.OutcomeFired {
				complete = false
			}
		}
		if found != 1 {
			return false, fmt.Errorf("retained join generation has %d matches, want exactly one", found)
		}
	}
	return complete, nil
}

func TestRetainedJoinOutcomeFenceDoesNotBorrowRunReadiness(t *testing.T) {
	at := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	loop, err := loopruntime.New("run", "run", "", ".", "revision", "start", "working", 3, at)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := timeridentity.NewJoinRef(identitytest.RootNode(t, "controller"), "review.requested", "working", "reviews")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(timeridentity.StageEntryRef{RunID: "run", FlowScope: ".", InstanceID: "run", InstancePath: "run", EntityID: "run", Stage: "working", Cause: "construction"}, loop.Generation())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := joinruntime.NewActivation(ref, []string{"member-one"}, nil, at, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if disposition, err := pending.Add("member-one", "member-one"); err != nil || disposition != joinruntime.AddAccepted {
		t.Fatalf("seed genuine pending outcome: disposition=%s err=%v", disposition, err)
	}
	if !pending.Close(joinruntime.CloseReasonComplete, true, false) || !pending.FireAt.After(at) {
		t.Fatal("control must be a complete join with a future pending publication")
	}
	ready := operatorread.RunTestQuiescence{Ready: true}
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{pending}, []timeridentity.JoinRef{ref}); !ready.Ready || err != nil || complete {
		t.Fatalf("ready-but-future-pending observation released the fence: ready=%+v complete=%t err=%v", ready, complete, err)
	}
	completed := pending
	completed.TimerCancelled, completed.OutcomePending, completed.OutcomeFired = true, false, true
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{completed}, []timeridentity.JoinRef{ref}); err != nil || !complete {
		t.Fatalf("exact completed outcome did not release the fence: complete=%t err=%v", complete, err)
	}
	foreign, err := loopruntime.New("other", "other", "", ".", "revision", "start", "working", 3, at)
	if err != nil {
		t.Fatal(err)
	}
	foreignRef, err := ref.WithGeneration(foreign.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{completed}, []timeridentity.JoinRef{foreignRef}); err == nil || complete {
		t.Fatalf("another completed generation released the fence: complete=%t err=%v", complete, err)
	}
	if _, err := loop.Repeat("working", "repeat", at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	secondRef, err := ref.WithGeneration(loop.Generation())
	if err != nil {
		t.Fatal(err)
	}
	second, err := joinruntime.NewActivation(secondRef, []string{"member-one"}, nil, at, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if disposition, err := second.Add("member-one", "member-one"); err != nil || disposition != joinruntime.AddAccepted {
		t.Fatalf("seed second pending outcome: disposition=%s err=%v", disposition, err)
	}
	if !second.Close(joinruntime.CloseReasonComplete, true, false) {
		t.Fatal("second control must have a genuine pending completion")
	}
	expected := []timeridentity.JoinRef{ref, secondRef}
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{completed, second}, expected); err != nil || complete {
		t.Fatalf("one completed generation released a two-generation fence: complete=%t err=%v", complete, err)
	}
	second.TimerCancelled, second.OutcomePending, second.OutcomeFired = true, false, true
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{second, completed}, expected); err != nil || !complete {
		t.Fatalf("exact completed generations were order-dependent: complete=%t err=%v", complete, err)
	}
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{completed, second}, []timeridentity.JoinRef{ref, ref}); err == nil || complete {
		t.Fatalf("duplicated expectation hid another generation: complete=%t err=%v", complete, err)
	}
	completed.CloseReason = joinruntime.CloseReasonDeadline
	if complete, err := retainedJoinOutcomesComplete([]joinruntime.Activation{completed}, []timeridentity.JoinRef{ref}); err == nil || complete {
		t.Fatalf("wrong closed outcome was treated as retryable completion: complete=%t err=%v", complete, err)
	}
}
