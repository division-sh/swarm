package startuprecovery

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func TestRecoverFailsPersistedMissingBeforeCleanupOrContainers(t *testing.T) {
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:        "11111111-1111-1111-1111-111111111111",
			Status:       "running",
			BundleSource: storerunlifecycle.BundleSourcePersisted,
			BundleHash:   "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222",
			ErrorCode:    runbundle.CodeBundleDataIntegrityError,
			Cause:        "persisted_missing_bundle_row",
		},
		{
			RunID:        "22222222-2222-2222-2222-222222222222",
			Status:       "running",
			BundleSource: storerunlifecycle.BundleSourceLegacy,
			ErrorCode:    runbundle.CodeBundleUnavailable,
			Cause:        storerunlifecycle.BundleSourceLegacy,
		},
	}}
	cleanup := &fakeCleanupStore{}
	containers := &fakeContainerOwner{items: []ManagedContainer{{Name: "swarm-agent", RunID: "22222222-2222-2222-2222-222222222222", Kind: "agent"}}}

	result, err := Recover(context.Background(), Request{
		AvailabilityReader: reader,
		CleanupStore:       cleanup,
		Containers:         containers,
		RequestedAt:        time.Date(2026, 5, 27, 10, 0, 0, 0, time.UTC),
	})
	if err == nil || !IsDataIntegrityError(err) || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("Recover err = %v, want data integrity error", err)
	}
	if len(result.DataIntegrityErrors) != 1 || len(result.OrphanTargets) != 1 {
		t.Fatalf("result = %#v, want classified data-integrity plus orphanable target", result)
	}
	if cleanup.called {
		t.Fatal("cleanup store was called despite data-integrity failure")
	}
	if len(containers.stopped) != 0 {
		t.Fatalf("stopped containers = %#v, want none before cleanup", containers.stopped)
	}
}

func TestRecoverStopsRunScopedContainersThenAppliesPreservationCleanup(t *testing.T) {
	runID := "22222222-2222-2222-2222-222222222222"
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:        runID,
			Status:       "running",
			BundleSource: storerunlifecycle.BundleSourceLegacy,
			ErrorCode:    runbundle.CodeBundleUnavailable,
			Cause:        storerunlifecycle.BundleSourceLegacy,
		},
		{
			RunID:            "33333333-3333-3333-3333-333333333333",
			Status:           "running",
			BundleSource:     storerunlifecycle.BundleSourcePersisted,
			BundleHash:       "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
			BundleRowPresent: true,
		},
	}}
	cleanup := &fakeCleanupStore{}
	containers := &fakeContainerOwner{items: []ManagedContainer{
		{Name: "swarm-agent-selected", RunID: runID, Kind: "agent"},
		{Name: "swarm-agent-other", RunID: "44444444-4444-4444-4444-444444444444", Kind: "agent"},
	}}

	result, err := Recover(context.Background(), Request{
		AvailabilityReader: reader,
		CleanupStore:       cleanup,
		Containers:         containers,
		RequestedAt:        time.Date(2026, 5, 27, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.OrphanTargets) != 1 || result.OrphanTargets[0].RunID != runID || result.OrphanTargets[0].ReasonCode != preservationcleanup.BundleLegacyOrphanedReason {
		t.Fatalf("orphan targets = %#v, want legacy run target", result.OrphanTargets)
	}
	if len(containers.stopped) != 1 || containers.stopped[0] != "swarm-agent-selected" {
		t.Fatalf("stopped containers = %#v, want selected run-scoped container only", containers.stopped)
	}
	if !cleanup.called || len(cleanup.request.Targets) != 1 || cleanup.request.Targets[0].RunID != runID {
		t.Fatalf("cleanup request = called:%v %#v, want selected run", cleanup.called, cleanup.request)
	}
}

type fakeAvailabilityReader struct {
	items []runbundle.Availability
	err   error
}

func (r fakeAvailabilityReader) ActiveRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]runbundle.Availability(nil), r.items...), nil
}

type fakeCleanupStore struct {
	called  bool
	request preservationcleanup.Request
}

func (s *fakeCleanupStore) ApplyUnavailableBundleStartupPreservationCleanup(_ context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	s.called = true
	s.request = req
	return preservationcleanup.Result{
		OperationName: req.OperationName,
		AppliedAt:     req.RequestedAt,
		ControlledBy:  req.ControlledBy,
	}, nil
}

type fakeContainerOwner struct {
	items   []ManagedContainer
	stopped []string
}

func (o *fakeContainerOwner) ManagedContainers(context.Context) ([]ManagedContainer, error) {
	return append([]ManagedContainer(nil), o.items...), nil
}

func (o *fakeContainerOwner) StopManagedContainer(_ context.Context, name string) error {
	o.stopped = append(o.stopped, name)
	return nil
}
