package sessions

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func requireRotationRefusal(t *testing.T, err error, want RotationRefusalReason) {
	t.Helper()
	var refusal *RotationRefusal
	if !errors.As(err, &refusal) || refusal.Reason != want {
		t.Fatalf("rotation refusal=%v want=%s", err, want)
	}
}

func TestRotationRequestNormalization(t *testing.T) {
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	base, err := NormalizeRotationRequest(identity, " owner ", RotationMetadata{
		OperationID: " op ", CheckpointSummary: " summary ", RetryReason: " session in use ",
		TerminationReason: " NORMAL ",
	})
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := NormalizeRotationRequest(identity, "owner", RotationMetadata{
		OperationID: "op", CheckpointSummary: "summary", RetryReason: "session in use", TerminationReason: TerminationReasonNormal,
	})
	if err != nil || base.Digest != equivalent.Digest || base.Metadata != equivalent.Metadata || base.LockOwner != equivalent.LockOwner {
		t.Fatalf("canonical request mismatch: first=%+v second=%+v err=%v", base, equivalent, err)
	}
	for _, reason := range []TerminationReason{"unknown", TerminationReasonLegacy} {
		if _, err := NormalizeRotationRequest(identity, "owner", RotationMetadata{TerminationReason: reason}); err == nil {
			t.Fatalf("accepted invalid reason %q", reason)
		}
	}
	if _, err := NormalizeRotationRequest(identity, " ", RotationMetadata{}); err == nil {
		t.Fatal("accepted blank owner")
	}
	defaulted, err := NormalizeRotationRequest(identity, "owner", RotationMetadata{RetryReason: "session not found"})
	if err != nil || defaulted.Metadata.TerminationReason != TerminationReasonNormal {
		t.Fatalf("retry diagnostic classified termination: request=%+v err=%v", defaulted, err)
	}
}

func TestInMemoryRotationReceipts(t *testing.T) {
	ctx := context.Background()
	registry := NewInMemoryRegistry(time.Minute)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	other := testIdentity(t, "agent-b", "run-a", "chat-a")
	if _, err := registry.Acquire(ctx, identity, "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(ctx, other, "owner"); err != nil {
		t.Fatal(err)
	}
	metadata := RotationMetadata{OperationID: "rotation-a", CheckpointSummary: "summary", RetryReason: "session in use"}
	first, err := registry.Rotate(ctx, identity, "owner", metadata)
	if err != nil || first == nil {
		t.Fatalf("first rotation=%+v err=%v", first, err)
	}
	replayed, err := registry.Rotate(ctx, identity, " owner ", RotationMetadata{
		OperationID: " rotation-a ", CheckpointSummary: " summary ", RetryReason: " session in use ", TerminationReason: " NORMAL ",
	})
	if err != nil || replayed == nil || *replayed != *first || len(registry.History(identity)) != 1 {
		t.Fatalf("replay=%+v first=%+v history=%d err=%v", replayed, first, len(registry.History(identity)), err)
	}
	replayed.RetryReason = "caller-mutated"
	unchanged, err := registry.Rotate(ctx, identity, "owner", metadata)
	if err != nil || unchanged == nil || unchanged.RetryReason != first.RetryReason {
		t.Fatalf("caller mutated immutable receipt: lease=%+v err=%v", unchanged, err)
	}
	for _, changed := range []RotationMetadata{
		{OperationID: "rotation-a", CheckpointSummary: "different", RetryReason: metadata.RetryReason},
		{OperationID: "rotation-a", CheckpointSummary: metadata.CheckpointSummary, RetryReason: "different"},
		{OperationID: "rotation-a", CheckpointSummary: metadata.CheckpointSummary, RetryReason: metadata.RetryReason, TerminationReason: TerminationReasonFailed},
	} {
		_, err := registry.Rotate(ctx, identity, "owner", changed)
		requireRotationRefusal(t, err, RotationRequestConflict)
	}
	_, err = registry.Rotate(ctx, other, "owner", metadata)
	requireRotationRefusal(t, err, RotationRequestConflict)
	_, err = registry.Rotate(ctx, identity, "different-owner", metadata)
	requireRotationRefusal(t, err, RotationRequestConflict)
	if len(registry.History(identity)) != 1 {
		t.Fatal("conflict mutated history")
	}
	if _, err := registry.ReleaseOutcome(ctx, first); err != nil {
		t.Fatal(err)
	}
	_, err = registry.Rotate(ctx, identity, "owner", metadata)
	requireRotationRefusal(t, err, RotationSuccessorNotCurrent)
	otherLease, err := registry.Acquire(ctx, identity, "other-owner")
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Rotate(ctx, identity, "owner", metadata)
	requireRotationRefusal(t, err, RotationSuccessorNotCurrent)
	if _, err := registry.ReleaseOutcome(ctx, otherLease); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(ctx, identity, "owner"); err != nil {
		t.Fatal(err)
	}
	_, err = registry.Rotate(ctx, identity, "owner", metadata)
	requireRotationRefusal(t, err, RotationSuccessorNotCurrent)
	second, err := registry.Rotate(ctx, identity, "owner", RotationMetadata{OperationID: "rotation-b"})
	if err != nil || second == nil {
		t.Fatalf("second rotation=%+v err=%v", second, err)
	}
	_, err = registry.Rotate(ctx, identity, "owner", metadata)
	requireRotationRefusal(t, err, RotationSuccessorNotCurrent)
	if len(registry.History(identity)) != 2 {
		t.Fatalf("stale replay mutated history: %d", len(registry.History(identity)))
	}
	if registry.History(identity)[0].TerminationReason != TerminationReasonNormal.String() {
		t.Fatal("diagnostic retry string changed termination reason")
	}
}

func TestInMemoryRotationSameKeyConcurrent(t *testing.T) {
	registry := NewInMemoryRegistry(time.Minute)
	identity := testIdentity(t, "agent-a", "run-a", "chat-a")
	ctx := context.Background()
	if _, err := registry.Acquire(ctx, identity, "owner"); err != nil {
		t.Fatal(err)
	}
	results := make([]*Lease, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = registry.Rotate(ctx, identity, "owner", RotationMetadata{OperationID: "same-key"})
		}(i)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0] == nil || results[1] == nil || results[0].SessionID != results[1].SessionID || len(registry.History(identity)) != 1 {
		t.Fatalf("concurrent rotation: results=%+v errors=%v history=%d", results, errs, len(registry.History(identity)))
	}
}
