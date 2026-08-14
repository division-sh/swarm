package bundledelete

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
)

func TestCoordinatorExecutesForceDeleteOwnerChain(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	runID := "00000000-0000-0000-0000-000000000101"
	owner := newFakeOwners(runID)
	coordinator := owner.coordinator(now)

	result, err := coordinator.Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
		RequestedAt:  now,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.OK || result.Status != "completed" || !result.Deleted || result.ActiveRunsStopped != 1 || result.DeliveriesCancelled != 1 || result.ContainersStopped != 1 {
		t.Fatalf("result = %#v", result)
	}
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "quiesce", "cleanup", "containers", "final", "commit"}; !stringSlicesEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	if owner.cleanupRequest.Targets[0].ReasonCode != preservationcleanup.BundleForceDeletedReason {
		t.Fatalf("cleanup reason = %q", owner.cleanupRequest.Targets[0].ReasonCode)
	}
	if owner.finalRequest.OperationID != testOperationID {
		t.Fatalf("final operation id = %q, want %q", owner.finalRequest.OperationID, testOperationID)
	}
}

func TestCoordinatorDryRunDoesNotMutateCleanupOrFinalizer(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
		DryRun:       true,
		RequestedAt:  now,
	})
	if err != nil {
		t.Fatalf("Execute dry-run: %v", err)
	}
	if !result.OK || result.Status != "dry_run" || result.Deleted {
		t.Fatalf("dry-run result = %#v", result)
	}
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "containers"}; !stringSlicesEqual(got, want) {
		t.Fatalf("dry-run calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorPhaseFailureStopsBeforeFinalMutation(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.containerFailed = true
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
		RequestedAt:  now,
	})
	if err != nil {
		t.Fatalf("Execute with container failure: %v", err)
	}
	if result.OK || result.Status != "partial_failure" || !result.PartialFailure || result.Deleted {
		t.Fatalf("partial result = %#v", result)
	}
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "quiesce", "cleanup", "containers", "restore"}; !stringSlicesEqual(got, want) {
		t.Fatalf("partial calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorRestoresQuiescedRuntimeForEveryPreCommitForceFailure(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		configure func(*fakeOwners)
	}{
		{name: "preservation cleanup", configure: func(o *fakeOwners) { o.cleanupErr = errors.New("cleanup failed") }},
		{name: "container cleanup", configure: func(o *fakeOwners) { o.containerErr = errors.New("containers failed") }},
		{name: "container result", configure: func(o *fakeOwners) { o.containerFailed = true }},
		{name: "final mutation", configure: func(o *fakeOwners) { o.finalErr = errors.New("final mutation failed") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
			tt.configure(owner)
			result, err := owner.coordinator(now).Execute(context.Background(), Request{
				OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
				BundleHash: testBundleHash, Force: true, RequestedAt: now,
			})
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if !result.PartialFailure || result.Deleted {
				t.Fatalf("result = %#v, want uncommitted partial failure", result)
			}
			if len(owner.calls) == 0 || owner.calls[len(owner.calls)-1] != "restore" {
				t.Fatalf("calls = %#v, want terminal runtime restoration", owner.calls)
			}
		})
	}
}

func TestCoordinatorCommitsQuiescenceWhenFinalMutationCommittedBeforePostCommitFailure(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.finalErr = errors.New("survivor refresh failed")
	owner.finalCommitted = true
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
		BundleHash: testBundleHash, Force: true, RequestedAt: now,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.PartialFailure || !result.Deleted || !result.FinalMutation.Deleted {
		t.Fatalf("result = %#v, want committed partial failure", result)
	}
	if got := owner.calls[len(owner.calls)-1]; got != "commit" {
		t.Fatalf("terminal quiescence call = %q, want commit; calls=%#v", got, owner.calls)
	}
}

func TestCoordinatorNonForceRestoresBeforeCommitAndCommitsAfterDurableDelete(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		finalCommitted bool
		wantTerminal   string
		wantDeleted    bool
	}{
		{name: "pre-commit failure restores", wantTerminal: "restore"},
		{name: "post-commit failure remains terminal", finalCommitted: true, wantTerminal: "commit", wantDeleted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := newFakeOwners("")
			owner.activeRuns = false
			owner.finalErr = errors.New("final mutation failed")
			owner.finalCommitted = tt.finalCommitted
			result, err := owner.coordinator(now).Execute(context.Background(), Request{
				OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
				BundleHash: testBundleHash, RequestedAt: now,
			})
			if err == nil || !strings.Contains(err.Error(), "final mutation failed") {
				t.Fatalf("Execute error = %v, want final mutation failure", err)
			}
			if result.Deleted != tt.wantDeleted {
				t.Fatalf("result.Deleted = %v, want %v", result.Deleted, tt.wantDeleted)
			}
			if got := owner.calls[len(owner.calls)-1]; got != tt.wantTerminal {
				t.Fatalf("terminal quiescence call = %q, want %q; calls=%#v", got, tt.wantTerminal, owner.calls)
			}
		})
	}
}

func TestCoordinatorReturnsRuntimeRestorationFailure(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("")
	owner.activeRuns = false
	owner.finalErr = errors.New("final mutation failed")
	owner.restoreErr = errors.New("runtime restoration failed")
	_, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
		BundleHash: testBundleHash, RequestedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "final mutation failed") || !strings.Contains(err.Error(), "runtime restoration failed") {
		t.Fatalf("Execute error = %v, want joined final mutation and restoration failures", err)
	}
}

func TestCoordinatorCancellationIsNotRecordedAsPartialFailure(t *testing.T) {
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.inventoryErr = context.Canceled
	result, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute cancellation error = %v, want context canceled", err)
	}
	if result.PartialFailure || result.Status == "partial_failure" {
		t.Fatalf("cancellation result = %#v, must not become a partial business result", result)
	}
}

func TestCoordinatorShutdownFailureBlocksCleanupAndFinalMutation(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.quiesceErr = errors.New("shutdown failed")
	_, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash", BundleHash: testBundleHash, Force: true, RequestedAt: now,
	})
	if !errors.Is(err, owner.quiesceErr) {
		t.Fatalf("Execute error = %v, want shutdown failure", err)
	}
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "quiesce"}; !stringSlicesEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorNonForceQuiescesBeforeFinalMutation(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("")
	owner.activeRuns = false
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash", BundleHash: testBundleHash, RequestedAt: now,
	})
	if err != nil {
		t.Fatalf("Execute non-force: %v", err)
	}
	if !result.OK || !result.Deleted {
		t.Fatalf("non-force result = %#v, want completed deletion", result)
	}
	if got, want := owner.calls, []string{"lock", "plan", "quiesce", "final", "commit"}; !stringSlicesEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	if owner.finalRequest.OperationID != testOperationID {
		t.Fatalf("final operation id = %q, want %q", owner.finalRequest.OperationID, testOperationID)
	}
}

func TestCoordinatorRejectsMissingOperationIdentityBeforeOwnerCalls(t *testing.T) {
	owner := newFakeOwners("")
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		ActorTokenID: "token", RequestHash: "hash", BundleHash: testBundleHash,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Execute error = %v, want ErrInvalidRequest", err)
	}
	if len(owner.calls) != 0 {
		t.Fatalf("owner calls = %#v, want none", owner.calls)
	}
}

func TestCoordinatorReplaysCommittedFinalMutationWhenBundleIsAbsentFromPlanning(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("")
	owner.planErr = ErrBundleNotFound
	owner.replayResult = Result{
		OK: true, Status: "completed", OperationName: DefaultOperationName,
		BundleHash: testBundleHash, Deleted: true,
		Plan: Plan{BundleHash: testBundleHash, PlannedAt: now.Add(-2 * time.Minute)},
		FinalMutation: FinalMutationResult{
			OperationName: DefaultOperationName, BundleHash: testBundleHash,
			AppliedAt: now.Add(-time.Minute), BundleRowsDeleted: 1, Deleted: true,
		},
	}
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
		BundleHash: testBundleHash, RequestedAt: now,
	})
	if err != nil {
		t.Fatalf("Execute replay: %v", err)
	}
	if !result.OK || !result.Deleted || result.FinalMutation.OperationName != owner.replayResult.FinalMutation.OperationName ||
		result.FinalMutation.BundleHash != owner.replayResult.FinalMutation.BundleHash ||
		!result.FinalMutation.AppliedAt.Equal(owner.replayResult.FinalMutation.AppliedAt) ||
		result.FinalMutation.BundleRowsDeleted != owner.replayResult.FinalMutation.BundleRowsDeleted {
		t.Fatalf("replay result = %#v, want stored final mutation %#v", result, owner.replayResult)
	}
	if got, want := owner.calls, []string{"lock", "plan", "replay"}; !stringSlicesEqual(got, want) {
		t.Fatalf("replay calls = %#v, want %#v", got, want)
	}
	if owner.finalRequest.OperationID != testOperationID || owner.finalRequest.BundleHash != testBundleHash {
		t.Fatalf("replay request = %#v", owner.finalRequest)
	}
}

func TestCoordinatorDoesNotReplayMissingBundleForDryRun(t *testing.T) {
	owner := newFakeOwners("")
	owner.planErr = ErrBundleNotFound
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash",
		BundleHash: testBundleHash, DryRun: true,
	})
	if !errors.Is(err, ErrBundleNotFound) {
		t.Fatalf("Execute dry-run missing bundle error = %v, want ErrBundleNotFound", err)
	}
	if got, want := owner.calls, []string{"lock", "plan"}; !stringSlicesEqual(got, want) {
		t.Fatalf("dry-run missing bundle calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorNonForceShutdownFailureBlocksFinalMutation(t *testing.T) {
	owner := newFakeOwners("")
	owner.activeRuns = false
	owner.quiesceErr = errors.New("shutdown failed")
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		OperationID: testOperationID, ActorTokenID: "token", RequestHash: "hash", BundleHash: testBundleHash,
	})
	if !errors.Is(err, owner.quiesceErr) {
		t.Fatalf("Execute error = %v, want shutdown failure", err)
	}
	if got, want := owner.calls, []string{"lock", "plan", "quiesce"}; !stringSlicesEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorBusyFailsClosed(t *testing.T) {
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.lockAcquired = false
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
	})
	if !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("busy error = %v, want ErrOperationInProgress", err)
	}
}

func TestCoordinatorPropagatesLockReleaseFailure(t *testing.T) {
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.lockReleaseErr = errors.New("release bundle delete lock")
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		OperationID:  testOperationID,
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
		DryRun:       true,
	})
	if !errors.Is(err, owner.lockReleaseErr) {
		t.Fatalf("lock release error = %v, want %v", err, owner.lockReleaseErr)
	}
}

const (
	testBundleHash  = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testOperationID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
)

type fakeOwners struct {
	calls           []string
	lockAcquired    bool
	runID           string
	cleanupRequest  preservationcleanup.Request
	finalRequest    FinalMutationRequest
	containerFailed bool
	containerErr    error
	cleanupErr      error
	finalErr        error
	finalCommitted  bool
	restoreErr      error
	inventoryErr    error
	quiesceErr      error
	lockReleaseErr  error
	activeRuns      bool
	planErr         error
	replayResult    Result
	replayErr       error
}

func newFakeOwners(runID string) *fakeOwners {
	return &fakeOwners{lockAcquired: true, runID: runID, activeRuns: true}
}

func (o *fakeOwners) coordinator(now time.Time) *Coordinator {
	return &Coordinator{
		Planner:            o,
		Cleaner:            o,
		Finalizer:          o,
		Locks:              o,
		ContainerInventory: o,
		Containers:         o,
		RuntimeQuiescer:    o,
		Now:                func() time.Time { return now },
	}
}

func (o *fakeOwners) QuiesceBundleRuntime(_ context.Context, _ string) (RuntimeQuiescence, error) {
	o.calls = append(o.calls, "quiesce")
	if o.quiesceErr != nil {
		return nil, o.quiesceErr
	}
	return fakeRuntimeQuiescence{owner: o}, nil
}

func (o *fakeOwners) AcquireBundleDelete(context.Context) (LockLease, bool, error) {
	o.calls = append(o.calls, "lock")
	if !o.lockAcquired {
		return nil, false, nil
	}
	return fakeLease{err: o.lockReleaseErr}, true, nil
}

func (o *fakeOwners) PlanBundleDelete(_ context.Context, req Request) (Plan, error) {
	o.calls = append(o.calls, "plan")
	if o.planErr != nil {
		return Plan{}, o.planErr
	}
	plan := Plan{
		BundleHash: req.BundleHash,
		AffectedRuns: []RunRef{{
			RunID:        o.runID,
			Status:       "running",
			BundleHash:   req.BundleHash,
			BundleSource: "persisted",
		}},
		ActiveDeliveries: []DeliveryRef{{DeliveryID: "delivery-1", RunID: o.runID, Status: "pending"}},
	}
	if o.activeRuns {
		plan.ActiveRuns = append([]RunRef(nil), plan.AffectedRuns...)
	}
	return plan, nil
}

func (o *fakeOwners) ManagedResetContainerInventory(_ context.Context) ([]destructivereset.ContainerRef, error) {
	o.calls = append(o.calls, "inventory")
	if o.inventoryErr != nil {
		return nil, o.inventoryErr
	}
	return []destructivereset.ContainerRef{{
		Name:          "swarm-agent-1",
		Kind:          "agent",
		Action:        destructivereset.ContainerActionStop,
		ResetEligible: true,
		RunID:         o.runID,
	}, {
		Name:          "swarm-agent-other",
		Kind:          "agent",
		Action:        destructivereset.ContainerActionStop,
		ResetEligible: true,
		RunID:         "00000000-0000-0000-0000-000000000202",
	}}, nil
}

func (o *fakeOwners) ApplyBundleForceDeletePreservationCleanup(_ context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	o.calls = append(o.calls, "cleanup")
	o.cleanupRequest = req
	result := preservationcleanup.Result{
		OperationName: req.OperationName,
		AppliedAt:     req.RequestedAt,
		ControlledBy:  req.ControlledBy,
		Runs:          []preservationcleanup.RunResult{{RunID: o.runID, Status: preservationcleanup.RunStatusCancelled}},
		Deliveries:    []preservationcleanup.DeliveryResult{{DeliveryID: "delivery-1", RunID: o.runID, Status: preservationcleanup.DeliveryOutcomeDeadLetter}},
	}
	return result, o.cleanupErr
}

func (o *fakeOwners) Apply(_ context.Context, req destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error) {
	o.calls = append(o.calls, "containers")
	result := destructivereset.ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
		Selected:      req.Result.Plan.EntityContainers,
	}
	if req.Result.DryRun {
		return result, nil
	}
	if o.containerFailed {
		result.Failed = []destructivereset.ContainerStopFailure{{Container: req.Result.Plan.EntityContainers[0], Error: "stop failed"}}
		return result, nil
	}
	result.Stopped = req.Result.Plan.EntityContainers
	return result, o.containerErr
}

func (o *fakeOwners) ApplyBundleDeleteFinalMutation(_ context.Context, req FinalMutationRequest) (FinalMutationResult, error) {
	o.calls = append(o.calls, "final")
	o.finalRequest = req
	committed := o.finalErr == nil || o.finalCommitted
	result := FinalMutationResult{
		OperationName: req.OperationName,
		BundleHash:    req.BundleHash,
		AppliedAt:     req.RequestedAt,
		Deleted:       committed,
	}
	if committed {
		result.RunsMarkedDeleted = 1
		result.BundleRowsDeleted = 1
	}
	return result, o.finalErr
}

func (o *fakeOwners) ReplayBundleDeleteFinalMutation(_ context.Context, req FinalMutationRequest) (Result, error) {
	o.calls = append(o.calls, "replay")
	o.finalRequest = req
	return o.replayResult, o.replayErr
}

type fakeRuntimeQuiescence struct{ owner *fakeOwners }

func (q fakeRuntimeQuiescence) Restore(context.Context) error {
	q.owner.calls = append(q.owner.calls, "restore")
	return q.owner.restoreErr
}

func (q fakeRuntimeQuiescence) Commit() {
	q.owner.calls = append(q.owner.calls, "commit")
}

type fakeLease struct{ err error }

func (l fakeLease) Release(context.Context) error { return l.err }

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
