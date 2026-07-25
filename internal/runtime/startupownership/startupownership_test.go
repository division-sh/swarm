package startupownership

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	startupBundleHashA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	startupBundleHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	startupBundleHashC = "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestStartupAuthorityRejectsNormalizedBundleHashInput(t *testing.T) {
	if _, err := NewColdAuthority(AcquireRequest{
		OwnerID: "owner-a", BootID: uuid.NewString(), BundleHash: " " + startupBundleHashA,
	}, "postgres"); err == nil {
		t.Fatal("NewColdAuthority accepted bundle_hash after normalization")
	}

	initial, err := NewColdAuthority(AcquireRequest{
		OwnerID: "owner-a", BootID: uuid.NewString(), BundleHash: startupBundleHashA,
	}, "postgres")
	if err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
	lease, err := NewLease(initial, nil, nil)
	if err != nil {
		t.Fatalf("NewLease: %v", err)
	}
	if _, err := lease.PrepareHandoff(context.Background(), HandoffRequest{
		CandidateOwnerID: "owner-b", CandidateBootID: uuid.NewString(), CandidateBundleHash: startupBundleHashB + " ",
	}); err == nil {
		t.Fatal("PrepareHandoff accepted bundle_hash after normalization")
	}
}

func TestColdStartupProbeSettledFailureReleasesLeaseForSuccessor(t *testing.T) {
	ctx := context.Background()
	initial, err := NewColdAuthority(AcquireRequest{
		OwnerID: "failed-owner", BootID: uuid.NewString(), BundleHash: startupBundleHashA,
	}, "postgres")
	if err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
	released := make(chan struct{}, 1)
	lease, err := NewLease(initial, nil, func(context.Context) error {
		released <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("NewLease: %v", err)
	}
	if _, err := lease.MarkProbesSettled(ctx, []string{uuid.NewString()}); err != nil {
		t.Fatalf("MarkProbesSettled: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("backend lease release callback was not invoked")
	}
	authority, err := lease.Authority()
	if err != nil {
		t.Fatalf("Authority: %v", err)
	}
	if authority.State != StateReleased {
		t.Fatalf("authority state = %s, want released", authority.State)
	}

	successor, err := NewColdAuthority(AcquireRequest{
		OwnerID: "successor", BootID: uuid.NewString(), BundleHash: startupBundleHashA,
	}, "postgres")
	if err != nil {
		t.Fatalf("successor NewColdAuthority: %v", err)
	}
	if _, err := NewLease(successor, nil, nil); err != nil {
		t.Fatalf("successor NewLease: %v", err)
	}
}

func TestFinalizedHandoffBecomesNextReplacementAuthority(t *testing.T) {
	ctx := context.Background()
	initial, err := NewColdAuthority(AcquireRequest{
		OwnerID: "owner-a", BootID: uuid.NewString(), BundleHash: startupBundleHashA,
	}, "memory")
	if err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
	lease, err := NewLease(initial, nil, nil)
	if err != nil {
		t.Fatalf("NewLease: %v", err)
	}
	if _, err := lease.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("MarkProbesSettled: %v", err)
	}
	if _, err := lease.AdmitExecution(ctx); err != nil {
		t.Fatalf("AdmitExecution: %v", err)
	}

	first, err := lease.PrepareHandoff(ctx, HandoffRequest{
		CandidateOwnerID: "owner-b", CandidateBootID: uuid.NewString(), CandidateBundleHash: startupBundleHashB,
	})
	if err != nil {
		t.Fatalf("PrepareHandoff first: %v", err)
	}
	if _, err := first.MarkProbesSettled(ctx, []string{uuid.NewString()}); err != nil {
		t.Fatalf("first MarkProbesSettled: %v", err)
	}
	committed, err := first.Commit(ctx)
	if err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	finalized, err := first.Finalize(ctx)
	if err != nil {
		t.Fatalf("first Finalize: %v", err)
	}
	if finalized.State != StateFinalized || finalized.AuthorityID != committed.AuthorityID {
		t.Fatalf("finalized authority = %#v, want finalized committed authority", finalized)
	}

	second, err := lease.PrepareHandoff(ctx, HandoffRequest{
		CandidateOwnerID: "owner-c", CandidateBootID: uuid.NewString(), CandidateBundleHash: startupBundleHashC,
	})
	if err != nil {
		t.Fatalf("PrepareHandoff second: %v", err)
	}
	secondAuthority, err := second.Authority()
	if err != nil {
		t.Fatalf("second Authority: %v", err)
	}
	if secondAuthority.PredecessorOwnerID != "owner-b" || secondAuthority.PredecessorBootID != finalized.BootID {
		t.Fatalf("second predecessor = %#v, want finalized first candidate", secondAuthority)
	}
}

func TestRolledBackHandoffCanPrepareRestorationCandidate(t *testing.T) {
	ctx := context.Background()
	initial, err := NewColdAuthority(AcquireRequest{
		OwnerID: "owner-a", BootID: uuid.NewString(), BundleHash: startupBundleHashA,
	}, "memory")
	if err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
	lease, err := NewLease(initial, nil, nil)
	if err != nil {
		t.Fatalf("NewLease: %v", err)
	}
	if _, err := lease.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("MarkProbesSettled: %v", err)
	}
	if _, err := lease.AdmitExecution(ctx); err != nil {
		t.Fatalf("AdmitExecution: %v", err)
	}

	failedCandidate, err := lease.PrepareHandoff(ctx, HandoffRequest{
		CandidateOwnerID: "owner-b", CandidateBootID: uuid.NewString(), CandidateBundleHash: startupBundleHashB,
	})
	if err != nil {
		t.Fatalf("PrepareHandoff failed candidate: %v", err)
	}
	restored, err := failedCandidate.Rollback(ctx)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if restored.State != StateActive || restored.OwnerID != "owner-a" {
		t.Fatalf("restored authority = %#v, want active owner-a", restored)
	}

	replacement, err := lease.PrepareHandoff(ctx, HandoffRequest{
		CandidateOwnerID: "owner-a-restored", CandidateBootID: uuid.NewString(), CandidateBundleHash: startupBundleHashA,
	})
	if err != nil {
		t.Fatalf("PrepareHandoff restoration candidate: %v", err)
	}
	replacementAuthority, err := replacement.Authority()
	if err != nil {
		t.Fatalf("restoration Authority: %v", err)
	}
	if replacementAuthority.PredecessorOwnerID != restored.OwnerID || replacementAuthority.PredecessorBootID != restored.BootID {
		t.Fatalf("restoration predecessor = %#v, want exact rolled-back head %#v", replacementAuthority, restored)
	}
}
