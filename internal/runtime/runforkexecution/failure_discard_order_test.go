package runforkexecution

import (
	"context"
	"errors"
	"testing"
)

type failureDiscardProbe struct {
	SelectedContractForkLifecycle
	discard func(context.Context, string) error
}

func (p failureDiscardProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, runID string) error {
	return p.discard(ctx, runID)
}

func TestFailedForkDiscardRetainsPreparationUntilSettlement(t *testing.T) {
	for _, phase := range []string{"success", "retirement_fail_once", "retirement_persistent", "discard_fail_once", "discard_persistent"} {
		t.Run(phase, func(t *testing.T) {
			owner, process, ctx := contextLifetimeOwner(t)
			prepared := contextLifetimePreparation(t, owner, ctx)
			bindContextLifetimePreparation(t, owner, prepared)
			retirementCalls, discardCalls := 0, 0
			repaired, retired := false, false
			defer func() { repaired = true }()
			failure := errors.New("injected disposition failure")
			prepared.loadedSource.Cleanup = func() error {
				retirementCalls++
				if !repaired && (phase == "retirement_persistent" || (phase == "retirement_fail_once" && retirementCalls == 1)) {
					return failure
				}
				retired = true
				return nil
			}
			runID := prepared.operation.selected.Identity().RunID
			store := failureDiscardProbe{discard: func(discardCtx context.Context, got string) error {
				discardCalls++
				if !retired || got != runID || process.ActiveCount() == 0 || discardCtx.Err() != nil {
					t.Fatalf("discard escaped its joined preparation: retired=%v run=%s active=%d err=%v", retired, got, process.ActiveCount(), discardCtx.Err())
				}
				if !repaired && (phase == "discard_persistent" || (phase == "discard_fail_once" && discardCalls == 1)) {
					return failure
				}
				return nil
			}}
			cause := errors.New("execution rejected")
			if err := prepared.cleanupExecutionFailure(ctx, store, runID, nil, cause); !errors.Is(err, cause) || discardCalls != 0 {
				t.Fatalf("failure deleted before preparation retirement: calls=%d err=%v", discardCalls, err)
			}
			err := owner.completePreparation(prepared)
			if phase == "success" {
				if err != nil || retirementCalls != 1 || discardCalls != 1 || process.ActiveCount() != 0 {
					t.Fatalf("settlement: retire=%d discard=%d active=%d err=%v", retirementCalls, discardCalls, process.ActiveCount(), err)
				}
				return
			}
			if !errors.Is(err, failure) || process.ActiveCount() == 0 || prepared.pendingDiscard == nil {
				t.Fatalf("failed disposition lost owner: active=%d pending=%v err=%v", process.ActiveCount(), prepared.pendingDiscard != nil, err)
			}
			if !retired && discardCalls != 0 {
				t.Fatal("discard overtook failed retirement")
			}
			if phase == "retirement_persistent" || phase == "discard_persistent" {
				if err := prepared.Close(); !errors.Is(err, failure) || process.ActiveCount() == 0 {
					t.Fatalf("persistent failure released owner: %v", err)
				}
			}
			repaired = true
			if err := prepared.Close(); err != nil || process.ActiveCount() != 0 || prepared.pendingDiscard != nil {
				t.Fatalf("retry did not settle exact discard: active=%d pending=%v err=%v", process.ActiveCount(), prepared.pendingDiscard != nil, err)
			}
			calls := discardCalls
			if err := prepared.Close(); err != nil || discardCalls != calls {
				t.Fatalf("settled discard repeated: calls=%d err=%v", discardCalls, err)
			}
		})
	}
}

func TestFailedForkDiscardPreventsProcessRelease(t *testing.T) {
	owner, process, ctx := contextLifetimeOwner(t)
	prepared := contextLifetimePreparation(t, owner, ctx)
	bindContextLifetimePreparation(t, owner, prepared)
	entered, release := make(chan struct{}), make(chan struct{})
	store := failureDiscardProbe{discard: func(context.Context, string) error {
		close(entered)
		<-release
		return nil
	}}
	if err := prepared.cleanupExecutionFailure(ctx, store, prepared.operation.selected.Identity().RunID, nil, errors.New("rejected")); err == nil {
		t.Fatal("lost execution error")
	}
	completed := make(chan error, 1)
	go func() { completed <- owner.completePreparation(prepared) }()
	<-entered
	process.Retire()
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := process.Join(expired); receipt != nil || err == nil || process.ActiveCount() == 0 {
		t.Error("process/store release overtook active discard")
	}
	if err := owner.RequireResetPredecessor(expired, process, nil); err == nil {
		t.Error("reset accepted an unsettled predecessor")
	}
	close(release)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
}
