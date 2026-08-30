package serveapp

import (
	"context"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

const (
	topologyOperationBundleA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	topologyOperationBundleB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestAdministrativeTopologyMutationIDsSurviveRepeatedSourceCycles(t *testing.T) {
	capability, session := newTopologyOperationTestCapability(t)
	initial := topologyOperationPlan(t, topologyOperationBundleA)
	if err := installServeSourceSet(context.Background(), capability, initial); err != nil {
		t.Fatalf("install initial source set: %v", err)
	}

	finalizer := processOwnedBundleDeleteFinalizer{capability: capability}
	firstDeleteID := uuid.NewString()
	if _, err := finalizer.ApplyBundleDeleteFinalMutation(context.Background(), runtimebundledelete.FinalMutationRequest{
		OperationID: firstDeleteID, OperationName: runtimebundledelete.DefaultOperationName,
		BundleHash: topologyOperationBundleA, RequestedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("first bundle delete: %v", err)
	}
	if err := installServeSourceSet(context.Background(), capability, initial); err != nil {
		t.Fatalf("restore source after first deletion: %v", err)
	}
	secondDeleteID := uuid.NewString()
	if _, err := finalizer.ApplyBundleDeleteFinalMutation(context.Background(), runtimebundledelete.FinalMutationRequest{
		OperationID: secondDeleteID, OperationName: runtimebundledelete.DefaultOperationName,
		BundleHash: topologyOperationBundleA, RequestedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("second bundle delete: %v", err)
	}
	if len(session.bundleDeleteRequests) != 2 || session.bundleDeleteRequests[0].OperationID != firstDeleteID || session.bundleDeleteRequests[1].OperationID != secondDeleteID {
		t.Fatalf("bundle delete operation requests = %#v", session.bundleDeleteRequests)
	}

	if err := installServeSourceSet(context.Background(), capability, initial); err != nil {
		t.Fatalf("restore source before destructive reset: %v", err)
	}
	resetter := processOwnedDestructiveResetStore{capability: capability}
	firstResetID := uuid.NewString()
	if _, err := resetter.ApplyDestructiveResetCleanup(context.Background(), topologyResetRequest(firstResetID)); err != nil {
		t.Fatalf("first destructive reset: %v", err)
	}
	if err := installServeSourceSet(context.Background(), capability, initial); err != nil {
		t.Fatalf("restore source after first destructive reset: %v", err)
	}
	secondResetID := uuid.NewString()
	if _, err := resetter.ApplyDestructiveResetCleanup(context.Background(), topologyResetRequest(secondResetID)); err != nil {
		t.Fatalf("second destructive reset: %v", err)
	}
	if len(session.destructiveResetRequests) != 2 || session.destructiveResetRequests[0].OperationID != firstResetID || session.destructiveResetRequests[1].OperationID != secondResetID {
		t.Fatalf("destructive reset operation requests = %#v", session.destructiveResetRequests)
	}
	assertDistinctTopologyOperationIDs(t, session.sourceSetRequests)
}

func newTopologyOperationTestCapability(t *testing.T) (runtimestartupownership.ProcessCapability, *supervisorTestRetainedSession) {
	t.Helper()
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "topology-operation-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "topology_operation_test")
	if err != nil {
		t.Fatalf("construct process authority: %v", err)
	}
	session := &supervisorTestRetainedSession{authority: authority}
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		t.Fatalf("construct process capability: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	return capability, session
}

func topologyOperationPlan(t *testing.T, bundleHash string) runtimeagenttopology.SourceSetPlan {
	t.Helper()
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{BundleHash: bundleHash, BundleSource: "ephemeral"}}, nil)
	if err != nil {
		t.Fatalf("construct source set plan: %v", err)
	}
	return plan
}

func topologyResetRequest(operationID string) runtimedestructivereset.CleanupRequest {
	now := time.Now().UTC()
	return runtimedestructivereset.CleanupRequest{
		OperationID: operationID, ActorTokenID: "operator", RequestedAt: now,
		Result:     runtimedestructivereset.Result{OperationName: runtimedestructivereset.DefaultOperationName, IncludeBundles: true, PlannedAt: now},
		Quiescence: runtimedestructivereset.QuiescenceResult{OperationName: runtimedestructivereset.DefaultOperationName, AppliedAt: now},
	}
}

func assertDistinctTopologyOperationIDs(t *testing.T, requests []runtimeagenttopology.SourceSetCommitRequest) {
	t.Helper()
	seen := make(map[string]struct{}, len(requests))
	for i, req := range requests {
		if _, err := uuid.Parse(req.OperationID); err != nil {
			t.Fatalf("source-set request %d operation id = %q: %v", i, req.OperationID, err)
		}
		if _, exists := seen[req.OperationID]; exists {
			t.Fatalf("source-set request %d reused operation id %q", i, req.OperationID)
		}
		seen[req.OperationID] = struct{}{}
	}
}
