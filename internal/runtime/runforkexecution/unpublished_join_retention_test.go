package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

// Exercise the actual cleanup owner, with both ancestors selected and C kept.
// Quiescence is a separate admitted phase; the cleanup itself must write nothing.
func requireUnpublishedJoinAncestorPurgeRefused(t *testing.T, ctx context.Context, selected startupownership.Store, owner SelectedContractExecutionOwner, capability startupownership.ProcessCapability, ancestor, parent string) {
	t.Helper()
	now := time.Now().UTC()
	operation, err := capability.AdmitResetOperation(ctx, destructivereset.Request{
		OperationID: uuid.NewString(), ActorTokenID: "pending-join-retention", RequestHash: "pending-join-ancestor-purge",
		RequestedAt: now, IncludeSourceArtifactsSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	planned := operation
	planned.Phase, planned.Revision = destructivereset.PhasePlanned, operation.Revision+1
	planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now,
		Plan: destructivereset.Plan{CleanupRunSetKnown: true, CleanupRuns: []destructivereset.RunRef{
			{RunID: ancestor, Status: "forked"}, {RunID: parent, Status: "forked"},
		}}}
	if err := capability.AdvanceResetOperation(ctx, operation, planned); err != nil {
		t.Fatal(err)
	}
	quiesced, err := selected.(destructivereset.QuiescenceStore).ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatal(err)
	}
	history, err := owner.PrepareResetHistory(ctx, []string{ancestor, parent}, SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore), PlatformPackBases: bases,
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	_, err = capability.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now,
		Result: *planned.Plan, Quiescence: quiesced, SelectedHistory: history,
	}, nil)
	if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "while dependent fork") {
		t.Fatalf("cleanup admitted deleting the committed cause beneath its preserved descendant: %v", err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused ancestor cleanup changed retained native facts: %v", err)
	}
}
