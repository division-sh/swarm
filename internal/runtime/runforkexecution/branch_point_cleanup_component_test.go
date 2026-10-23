package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
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
			t.Run(backend+"/"+string(kind)+"/reset_source_artifacts", func(t *testing.T) {
				proveQ1DetachedAcknowledgmentWithoutSource(t, backend, kind)
			})
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
		if attachment == first && kind == runfork.RunForkPointEvent && operation == "reset" {
			inputs, err := attachment.container.commitSelectedInputs()
			if err != nil || len(inputs) != 1 {
				t.Fatalf("reset requires a real linked selected publication: inputs=%d err=%v", len(inputs), err)
			}
			for _, input := range inputs {
				if err := attachment.container.dispatchCommittedInput(input); err != nil {
					t.Fatal(err)
				}
			}
		}
		attachment.close(t)
	}
	before := q1ComponentSnapshot(t, selected)
	victimAttachment, survivorAttachment := first, second
	if kind == runfork.RunForkPointDeploymentRevision {
		victimAttachment, survivorAttachment = second, first
	}
	victim, survivor := victimAttachment.materialized.ForkRunID, survivorAttachment.materialized.ForkRunID
	if kind == runfork.RunForkPointEvent && operation == "reset" {
		for _, table := range []string{"events", "event_deliveries"} {
			if rows := q1ComponentRowsWithIdentity(t, before, table, "run_id", victim); len(rows) == 0 {
				t.Fatalf("reset lacked its real selected %s precondition", table)
			}
		}
	}
	if operation == "discard" {
		q1DiscardBranchPointComponent(t, ctx, owner, victimAttachment)
	} else {
		q1ResetBranchPointComponent(t, ctx, owner, []string{victim}, false)
		q1RequireDiscardedMutableStateAbsent(t, q1ComponentSnapshot(t, selected), victim)
		permanent := victimAttachment.prepared.forkOperation
		if permanent == nil {
			t.Fatal("reset fixture lost its actual permanent operation")
		}
		beforeReplay := q1ComponentSnapshot(t, selected)
		recorded, found, err := selected.LoadForkOperation(ctx, permanent.Actor, permanent.IdempotencyKey, permanent.TransportHash)
		if err != nil || !found || recorded.Status != runfork.ForkOperationActivated || recorded.ForkRunID != victim ||
			recorded.Result == nil || recorded.Result.ForkRunID != victim || victimAttachment.materialized.SelectedContractBinding == nil ||
			recorded.BindingID != victimAttachment.materialized.SelectedContractBinding.BindingID {
			t.Fatalf("reset lost exact immutable activated acknowledgment: %+v found=%t err=%v", recorded, found, err)
		}
		if _, err := victimAttachment.activate(); err == nil {
			t.Fatal("detached historical acknowledgment restored fresh activation authority")
		}
		if !reflect.DeepEqual(beforeReplay, q1ComponentSnapshot(t, selected)) {
			t.Fatal("detached acknowledgment read or refused fresh activation changed durable state")
		}
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

// Reach: the real execution entrypoint returns its immutable operation ACK
// after source, child, event, binding and admitted source bytes are gone.
func proveQ1DetachedAcknowledgmentWithoutSource(t *testing.T, backend string, kind runfork.RunForkPointKind) {
	t.Helper()
	fixture := newActivityForkFixture(t, backend)
	owner, selected := fixture.owner, fixture.owner.ports.fork
	build := q1DeploymentComponentSource
	if kind == runfork.RunForkPointEvent {
		build = func(t *testing.T, owner SelectedContractExecutionOwner) (context.Context, SelectedContractExecutionRequest, func()) {
			return q1GateComponentSource(t, owner, false)
		}
	}
	ctx, request, advance := build(t, owner)
	attachment := q1StageBranchPointComponent(t, ctx, request)
	advance()
	activation, err := attachment.activate()
	if err != nil || !activation.Activated {
		t.Fatalf("source-removal control did not activate: %+v err=%v", activation, err)
	}
	attachment.close(t)
	runRows, err := storetest.DecodeSelectedForkSnapshotRows(q1ComponentSnapshot(t, selected)["runs"])
	if err != nil {
		t.Fatal(err)
	}
	var runs []string
	for _, row := range runRows {
		id, err := row["run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, id)
	}
	q1ResetBranchPointComponent(t, ctx, owner, runs, true)
	if _, err := selected.(SourceArtifactSelectedContractSourceStore).GetSourceArtifact(ctx, request.ForkOperation.TargetBundleHash); !errors.Is(err, sourceartifact.ErrNotFound) {
		t.Fatalf("reset did not remove the original source artifact: %v", err)
	}
	before := q1ComponentSnapshot(t, selected)
	replayed, err := ExecuteSelectedContractRunFork(ctx, request)
	if err != nil || replayed.Materialization.ForkRunID != attachment.materialized.ForkRunID || !replayed.Activation.Activated || replayed.ForkLocalRuntimeContainer != nil {
		t.Fatalf("detached operation needed a deleted source to replay its recorded ACK: %+v err=%v", replayed, err)
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, selected)) {
		t.Fatal("source-less operation replay recreated or mutated durable state")
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
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			}()
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

func q1ResetBranchPointComponent(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, runs []string, includeSource bool) {
	t.Helper()
	capability, now := owner.ports.contexts.capability, time.Now().UTC().Add(time.Minute)
	operation, err := capability.AdmitResetOperation(ctx, destructivereset.Request{
		OperationID: uuid.NewString(), ActorTokenID: "operator-token", RequestHash: "cleanup-guard",
		RequestedAt: now, IncludeSourceArtifacts: includeSource, IncludeSourceArtifactsSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	planned := operation
	planned.Phase, planned.Revision = destructivereset.PhasePlanned, operation.Revision+1
	planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now, IncludeSourceArtifacts: includeSource,
		Plan: destructivereset.Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: includeSource}}
	for _, run := range runs {
		planned.Plan.Plan.CleanupRuns = append(planned.Plan.Plan.CleanupRuns, destructivereset.RunRef{RunID: run})
		state, err := storetest.ReadRuntimeLogRunSnapshot(ctx, owner.ports.fork, run)
		if err != nil {
			t.Fatal(err)
		}
		if state.State == runlifecycle.StateRunning {
			planned.Plan.Plan.ActiveRuns = append(planned.Plan.Plan.ActiveRuns, destructivereset.RunRef{RunID: run, Status: "running"})
		}
	}
	if err := capability.AdvanceResetOperation(ctx, operation, planned); err != nil {
		t.Fatal(err)
	}
	quiesced, err := owner.ports.fork.(destructivereset.QuiescenceStore).ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatal(err)
	}
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t), Store: owner.ports.fork.(SourceArtifactSelectedContractSourceStore), PlatformPackBases: bases,
	}
	history, err := owner.PrepareResetHistory(ctx, runs, loader)
	if err != nil || len(history) != 1 {
		t.Fatalf("prepare original selected reset history: cohorts=%d err=%v", len(history), err)
	}
	missingResolver := loader
	missingResolver.PlatformPackBases = nil
	if _, err := owner.PrepareResetHistory(ctx, runs, missingResolver); err == nil {
		t.Fatal("selected reset silently used a default pack resolver")
	}
	request := destructivereset.CleanupRequest{
		OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan, Quiescence: quiesced,
		SelectedHistory: history,
	}
	var topology *agenttopology.SourceSetCommitRequest
	if includeSource && operation.SourceSet != nil {
		empty, err := agenttopology.EmptySourceSetPlan()
		if err != nil {
			t.Fatal(err)
		}
		topology = &agenttopology.SourceSetCommitRequest{OperationID: request.OperationID, ExpectedRevision: operation.SourceSet.Revision, Plan: empty}
	}
	before := q1ComponentSnapshot(t, owner.ports.fork)
	dry := request
	dry.Result.DryRun = true
	projected, err := capability.ApplyDestructiveResetCleanup(ctx, dry, nil)
	if err != nil {
		t.Fatalf("validate original reset source/cut without mutation: %v", err)
	}
	if !projected.SelectedHistory.Projected || len(projected.SelectedHistory.Receipts) != 1 {
		t.Fatalf("dry-run lost its explicit projected retained disposition: %+v", projected.SelectedHistory)
	}
	for _, corruption := range []string{"source", "cut", "origin", "duplicate", "omitted"} {
		invalid := dry
		invalid.SelectedHistory = append([]runfork.SelectedResetPreparation(nil), history...)
		switch corruption {
		case "source":
			invalid.SelectedHistory[0].Binding.SourceRunID = uuid.NewString()
		case "cut":
			invalid.SelectedHistory[0].Plan.ForkPoint.Revision++
		case "origin":
			invalid.SelectedHistory[0].Original = semanticview.OriginalLoopCarriage{}
		case "duplicate":
			invalid.SelectedHistory = append(invalid.SelectedHistory, history[0])
		case "omitted":
			invalid.SelectedHistory = nil
		}
		if _, err := capability.ApplyDestructiveResetCleanup(ctx, invalid, nil); err == nil {
			t.Fatalf("selected reset admitted %s preparation", corruption)
		}
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, owner.ports.fork)) {
		t.Fatal("reset preparation/current-cut controls mutated the application")
	}
	applied, err := capability.ApplyDestructiveResetCleanup(ctx, request, topology)
	if err != nil {
		t.Fatal(err)
	}
	projected.SelectedHistory.Projected = false
	if !reflect.DeepEqual(projected.SelectedHistory, applied.SelectedHistory) {
		t.Fatalf("projected and acknowledged selected reset dispositions differ: planned=%+v applied=%+v", projected.SelectedHistory, applied.SelectedHistory)
	}
	committed := q1ComponentSnapshot(t, owner.ports.fork)
	replayed, err := capability.ApplyDestructiveResetCleanup(ctx, request, topology)
	if err != nil {
		t.Fatalf("cleanup acknowledgment replay failed: %v", err)
	}
	// The durable acknowledgment excludes this process-local notification flag;
	// replay must not issue the original channel-change notification again.
	expected := applied
	expected.ChannelSourcesChanged = false
	if !reflect.DeepEqual(expected, replayed) {
		t.Fatal("cleanup acknowledgment replay changed its durable disposition or repeated notification")
	}
	if after := q1ComponentSnapshot(t, owner.ports.fork); !reflect.DeepEqual(committed, after) {
		for name, table := range committed {
			if !reflect.DeepEqual(table, after[name]) {
				t.Errorf("cleanup acknowledgment replay changed table %s", name)
			}
		}
		t.FailNow()
	}
}
