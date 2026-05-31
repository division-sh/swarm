package bundledelete

import (
	"context"
	"errors"
	"testing"
	"time"

	"swarm/internal/runtime/destructivereset"
	"swarm/internal/runtime/preservationcleanup"
)

func TestCoordinatorExecutesForceDeleteOwnerChain(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	runID := "00000000-0000-0000-0000-000000000101"
	owner := newFakeOwners(runID)
	coordinator := owner.coordinator(now)

	result, err := coordinator.Execute(context.Background(), Request{
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
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "cleanup", "containers", "final"}; !stringSlicesEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	if owner.cleanupRequest.Targets[0].ReasonCode != preservationcleanup.BundleForceDeletedReason {
		t.Fatalf("cleanup reason = %q", owner.cleanupRequest.Targets[0].ReasonCode)
	}
	if owner.lockKey != destructivereset.DefaultLockKey {
		t.Fatalf("lock key = %q, want shared destructive key %q", owner.lockKey, destructivereset.DefaultLockKey)
	}
}

func TestCoordinatorDryRunDoesNotMutateCleanupOrFinalizer(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	result, err := owner.coordinator(now).Execute(context.Background(), Request{
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
	if got, want := owner.calls, []string{"lock", "plan", "inventory", "cleanup", "containers"}; !stringSlicesEqual(got, want) {
		t.Fatalf("partial calls = %#v, want %#v", got, want)
	}
}

func TestCoordinatorBusyFailsClosed(t *testing.T) {
	owner := newFakeOwners("00000000-0000-0000-0000-000000000101")
	owner.lockAcquired = false
	_, err := owner.coordinator(time.Now()).Execute(context.Background(), Request{
		ActorTokenID: "token",
		RequestHash:  "hash",
		BundleHash:   testBundleHash,
		Force:        true,
	})
	if !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("busy error = %v, want ErrOperationInProgress", err)
	}
}

const testBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeOwners struct {
	calls           []string
	lockKey         string
	lockAcquired    bool
	runID           string
	cleanupRequest  preservationcleanup.Request
	containerFailed bool
}

func newFakeOwners(runID string) *fakeOwners {
	return &fakeOwners{lockAcquired: true, runID: runID}
}

func (o *fakeOwners) coordinator(now time.Time) *Coordinator {
	return &Coordinator{
		Planner:            o,
		Cleaner:            o,
		Finalizer:          o,
		Locks:              o,
		ContainerInventory: o,
		Containers:         o,
		Now:                func() time.Time { return now },
	}
}

func (o *fakeOwners) TryAcquire(_ context.Context, lockKey string) (destructivereset.LockLease, bool, error) {
	o.calls = append(o.calls, "lock")
	o.lockKey = lockKey
	if !o.lockAcquired {
		return nil, false, nil
	}
	return fakeLease{}, true, nil
}

func (o *fakeOwners) PlanBundleDelete(_ context.Context, req Request) (Plan, error) {
	o.calls = append(o.calls, "plan")
	return Plan{
		BundleHash: req.BundleHash,
		ActiveRuns: []RunRef{{
			RunID:        o.runID,
			Status:       "running",
			BundleHash:   req.BundleHash,
			BundleSource: "persisted",
		}},
		AffectedRuns: []RunRef{{
			RunID:        o.runID,
			Status:       "running",
			BundleHash:   req.BundleHash,
			BundleSource: "persisted",
		}},
		ActiveDeliveries: []DeliveryRef{{DeliveryID: "delivery-1", RunID: o.runID, Status: "pending"}},
	}, nil
}

func (o *fakeOwners) ManagedResetContainerInventory(_ context.Context) ([]destructivereset.ContainerRef, error) {
	o.calls = append(o.calls, "inventory")
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
	return preservationcleanup.Result{
		OperationName: req.OperationName,
		AppliedAt:     req.RequestedAt,
		ControlledBy:  req.ControlledBy,
		Runs:          []preservationcleanup.RunResult{{RunID: o.runID, Status: preservationcleanup.RunStatusCancelled}},
		Deliveries:    []preservationcleanup.DeliveryResult{{DeliveryID: "delivery-1", RunID: o.runID, Status: preservationcleanup.DeliveryOutcomeDeadLetter}},
	}, nil
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
	return result, nil
}

func (o *fakeOwners) ApplyBundleDeleteFinalMutation(_ context.Context, req FinalMutationRequest) (FinalMutationResult, error) {
	o.calls = append(o.calls, "final")
	return FinalMutationResult{
		OperationName:     req.OperationName,
		BundleHash:        req.BundleHash,
		AppliedAt:         req.RequestedAt,
		RunsMarkedDeleted: 1,
		BundleRowsDeleted: 1,
		Deleted:           true,
	}, nil
}

type fakeLease struct{}

func (fakeLease) Release(context.Context) error { return nil }

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
