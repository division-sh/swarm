package transactiontest

import (
	"context"
	"testing"
	"time"
)

func TestMutationPhaseReceiptsFollowTransactionOwner(t *testing.T) {
	var slot Slot
	collector, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	attempt := slot.Begin(false, false)
	attempt.Begun()
	ctx := WithAttempt(context.Background(), attempt)
	for _, phase := range []MutationPhase{MutationFence, MutationDomain, MutationFinalize} {
		span := BeginMutationPhase(ctx, phase)
		time.Sleep(time.Millisecond)
		span.End()
	}
	Mark(ctx, DeliveryClaim)
	attempt.BeforeCommit()
	time.Sleep(time.Millisecond)
	attempt.Committed()
	attempt.Finish(nil)
	got := collector.Snapshot().ByOperation[DeliveryClaim]
	if got.Mutation.FenceCalls != 1 || got.Mutation.FenceDuration < time.Millisecond ||
		got.Mutation.DomainCalls != 1 || got.Mutation.DomainDuration < time.Millisecond ||
		got.Mutation.FinalizeCalls != 1 || got.Mutation.FinalizeDuration < time.Millisecond ||
		got.CommitDuration < time.Millisecond {
		t.Fatalf("mutation phase receipt = %+v", got)
	}
	BeginMutationPhase(context.Background(), MutationFence).End()
}
