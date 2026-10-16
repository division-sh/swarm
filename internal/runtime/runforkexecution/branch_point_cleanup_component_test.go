package runforkexecution

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Old root: runtimepersistence.TestSelectedBranchPointCleanupIsolationBothStores.
// Both exact children retire their component lifetimes before the baseline.
func TestSelectedBranchPointCleanupIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
			for _, operation := range []string{"discard", "reset"} {
				t.Run(backend+"/"+string(kind)+"/"+operation, func(t *testing.T) {
					proveQ1BranchPointCleanup(t, backend, kind, operation)
				})
			}
		}
	}
}

func proveQ1BranchPointCleanup(t *testing.T, backend string, kind runfork.RunForkPointKind, operation string) {
	t.Helper()
	fixture := newActivityForkFixture(t, backend)
	owner, selected := fixture.owner, fixture.owner.ports.fork
	// Source constructors and the sole process exist before either pending fork.
	ctx, eventRequest, advanceEvent := q1GateComponentSource(t, owner, false)
	deploymentCtx, deploymentRequest, advanceDeployment := q1DeploymentComponentSource(t, owner)
	if operation == "discard" {
		eventRequest.ForkOperation = nil
	}
	first := q1StageBranchPointComponent(t, ctx, eventRequest)
	second := q1StageBranchPointComponent(t, deploymentCtx, deploymentRequest)
	advanceEvent()
	advanceDeployment()
	for _, attachment := range []*q1BranchPointAttachment{first, second} {
		result, err := attachment.activate()
		if err != nil || !result.Activated || result.BranchDivergence == nil || result.BranchDivergence.ForkPoint.Kind != result.ForkPoint.Kind || result.BranchDivergence.ForkPoint.Revision != result.ForkPoint.Revision || result.BranchDivergence.ForkPoint.EventID != result.ForkPoint.EventID {
			t.Fatalf("sibling branch fixture did not activate: %+v err=%v", result, err)
		}
		attachment.close(t)
	}
	before := q1ComponentSnapshot(t, selected)
	victim, survivor := first.materialized.ForkRunID, second.materialized.ForkRunID
	if kind == runfork.RunForkPointDeploymentRevision {
		victim, survivor = survivor, victim
	}
	wantVictimEvidence := 0
	if operation == "discard" {
		q1DiscardBranchPointComponent(t, ctx, owner, victim, kind)
		if kind == runfork.RunForkPointDeploymentRevision {
			wantVictimEvidence = 1
		}
	} else {
		q1ResetBranchPointComponent(t, ctx, owner, victim)
	}
	after := q1ComponentSnapshot(t, selected)
	if count := len(q1ComponentBranchRows(t, after, victim)); count != wantVictimEvidence {
		t.Fatalf("victim branch evidence=%d, want %d", count, wantVictimEvidence)
	}
	if count := len(q1ComponentBranchRows(t, after, survivor)); count != 1 {
		t.Fatalf("survivor branch evidence=%d, want 1", count)
	}
	for _, preserved := range []string{eventRequest.SourceRunID, deploymentRequest.SourceRunID, survivor} {
		if !reflect.DeepEqual(q1ComponentRowsForRun(t, before, preserved), q1ComponentRowsForRun(t, after, preserved)) {
			t.Fatalf("%s mutated preserved source/sibling %s", operation, preserved)
		}
	}
}

func q1DiscardBranchPointComponent(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, victim string, kind runfork.RunForkPointKind) {
	t.Helper()
	selected := owner.ports.fork
	if _, err := storetest.TransitionRun(ctx, selected.(storetest.RunFixtureStore), runlifecycle.ActiveTransitionRequest{RunID: victim, State: runlifecycle.StatePaused}); err != nil {
		t.Fatal(err)
	}
	if kind != runfork.RunForkPointDeploymentRevision {
		if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, victim); err != nil {
			t.Fatal(err)
		}
		return
	}
	before := q1ComponentSnapshot(t, selected)
	err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, victim)
	if err == nil || !strings.Contains(err.Error(), "delete selected-contract fork binding") || !strings.Contains(strings.ToUpper(err.Error()), "FOREIGN KEY") {
		t.Fatalf("permanent deployment invocation was not protected: %v", err)
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, selected)) {
		t.Fatal("refused operation-free discard partially erased permanent branch evidence")
	}
}

func q1ResetBranchPointComponent(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, victim string) {
	t.Helper()
	capability, now := owner.ports.contexts.capability, time.Now().UTC().Add(time.Minute)
	operation, err := capability.AdmitResetOperation(ctx, destructivereset.Request{
		OperationID: uuid.NewString(), ActorTokenID: "operator-token", RequestHash: "cleanup-guard",
		RequestedAt: now, IncludeSourceArtifactsSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	planned := operation
	planned.Phase, planned.Revision = destructivereset.PhasePlanned, operation.Revision+1
	planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now,
		Plan: destructivereset.Plan{CleanupRunSetKnown: true, CleanupRuns: []destructivereset.RunRef{{RunID: victim}}, ActiveRuns: []destructivereset.RunRef{{RunID: victim, Status: "running"}}}}
	if err := capability.AdvanceResetOperation(ctx, operation, planned); err != nil {
		t.Fatal(err)
	}
	quiesced, err := owner.ports.fork.(destructivereset.QuiescenceStore).ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan, Quiescence: quiesced,
	}, nil); err != nil {
		t.Fatal(err)
	}
}
