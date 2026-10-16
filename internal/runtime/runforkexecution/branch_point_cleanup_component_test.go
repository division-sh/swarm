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
	victimAttachment, survivorAttachment := first, second
	if kind == runfork.RunForkPointDeploymentRevision {
		victimAttachment, survivorAttachment = second, first
	}
	victim, survivor := victimAttachment.materialized.ForkRunID, survivorAttachment.materialized.ForkRunID
	if operation == "discard" {
		q1DiscardBranchPointComponent(t, ctx, owner, victimAttachment)
	} else {
		q1ResetBranchPointComponent(t, ctx, owner, victim)
	}
	after := q1ComponentSnapshot(t, selected)
	if count := len(q1ComponentBranchRows(t, after, victim)); count != 0 {
		t.Fatalf("victim branch evidence=%d, want 0", count)
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

func q1DiscardBranchPointComponent(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, attachment *q1BranchPointAttachment) {
	t.Helper()
	selected, victim := owner.ports.fork, attachment.materialized.ForkRunID
	if _, err := storetest.TransitionRun(ctx, selected.(storetest.RunFixtureStore), runlifecycle.ActiveTransitionRequest{RunID: victim, State: runlifecycle.StatePaused}); err != nil {
		t.Fatal(err)
	}
	before := q1ComponentSnapshot(t, selected)
	if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, victim); err != nil {
		t.Fatal(err)
	}
	after := q1ComponentSnapshot(t, selected)
	for _, table := range []string{"run_fork_selected_contract_runtime_executions", "run_fork_selected_contract_bindings", "run_fork_operations"} {
		old := q1ComponentRowsWithIdentity(t, before, table, "fork_run_id", victim)
		if table != "run_fork_operations" && len(old) != 1 {
			t.Fatalf("retained %s baseline rows=%d, want 1", table, len(old))
		}
		if !reflect.DeepEqual(old, q1ComponentRowsWithIdentity(t, after, table, "fork_run_id", victim)) {
			t.Fatalf("retained cleanup changed exact %s identity/completion evidence", table)
		}
	}
	run, err := storetest.ReadRuntimeLogRunSnapshot(ctx, selected, victim)
	if err != nil || run.State != runlifecycle.StateCancelled {
		t.Fatalf("retained discard lost cancelled run tombstone: %+v err=%v", run, err)
	}
	q1RequireDiscardedMutableStateAbsent(t, after, victim)
	if _, err := attachment.activate(); err == nil {
		t.Fatal("retained discarded execution remained executable")
	}
	if request := attachment.prepared.forkOperation; request != nil {
		record, found, err := selected.LoadForkOperation(ctx, request.Actor, request.IdempotencyKey, request.TransportHash)
		if err != nil || !found || record.Status != runfork.ForkOperationActivated || record.ForkRunID != victim || record.Result == nil || record.Result.ForkRunID != victim {
			t.Fatalf("retained permanent operation ACK unreadable: %+v found=%t err=%v", record, found, err)
		}
	}
	if !reflect.DeepEqual(after, q1ComponentSnapshot(t, selected)) {
		t.Fatal("retained completion readback or refused activation replay mutated evidence")
	}
}

func q1RequireDiscardedMutableStateAbsent(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, victim string) {
	t.Helper()
	for _, table := range []string{"flow_instances", "entity_state", "event_deliveries", "events", "agent_sessions", "timers", "reply_contexts", "activity_attempts", "fan_out_intents", "fan_out_obligation_barriers", "fan_out_outcomes", "run_fork_selected_contract_executions", "run_fork_selected_contract_route_recoveries", "run_fork_selected_contract_branch_divergences"} {
		column := "run_id"
		if strings.HasPrefix(table, "run_fork_selected_contract_") {
			column = "fork_run_id"
		}
		if rows := q1ComponentRowsWithIdentity(t, snapshot, table, column, victim); len(rows) != 0 {
			t.Fatalf("retained discard left %d mutable %s rows", len(rows), table)
		}
	}
}

func TestSelectedBranchPointPermanentUnattachedOperationRefusesDiscardBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			owner, selected := fixture.owner, fixture.owner.ports.fork
			ctx, request, _ := q1DeploymentComponentSource(t, owner)
			prepared, err := owner.Prepare(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			materialized, acknowledged, err := owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
			if err != nil || !acknowledged {
				t.Fatalf("permanent original unattached child: %+v err=%v", materialized, err)
			}
			before := q1ComponentSnapshot(t, selected)
			permanent := prepared.forkOperation
			record, found, err := selected.LoadForkOperation(ctx, permanent.Actor, permanent.IdempotencyKey, permanent.TransportHash)
			if err != nil || !found || record.Status != runfork.ForkOperationMaterialized || record.ForkRunID != materialized.ForkRunID {
				t.Fatalf("unattached control lacks its permanent materialized operation: %+v found=%t err=%v", record, found, err)
			}
			if rows := q1ComponentRowsWithIdentity(t, before, "run_fork_selected_contract_runtime_executions", "fork_run_id", materialized.ForkRunID); len(rows) != 0 {
				t.Fatal("unattached protection control unexpectedly owns an execution")
			}
			err = selected.DiscardMaterializedSelectedContractExecutionFork(ctx, materialized.ForkRunID)
			if err == nil || !strings.Contains(err.Error(), "delete selected-contract fork binding") || !strings.Contains(strings.ToUpper(err.Error()), "FOREIGN KEY") {
				t.Fatalf("permanent operation without retained execution was not protected: %v", err)
			}
			if !reflect.DeepEqual(before, q1ComponentSnapshot(t, selected)) {
				t.Fatal("refused unattached discard partially erased permanent operation evidence")
			}
		})
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
