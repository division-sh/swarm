package runforkexecution

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestIssue642PublishedChildRemainsHistoricallyPlannableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected SelectedContractForkLifecycle
			var dsn string
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, dsn = s, s.Path()
			} else {
				dsn = testutil.StartPostgresDSN(t)
				s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
				selected = s
			}
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, "retained_join_event_committed")
			source, err := selected.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.SourceRun})
			if err != nil || len(source.JoinSchedules) != 1 {
				t.Fatalf("source control: joins=%d err=%v", len(source.JoinSchedules), err)
			}
			id := activityidentity.ForkLineageEventID(checkpoint.ForkRun, source.JoinSchedules[0].CurrentEventID)
			if _, err := selected.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.ForkRun, At: id}); err != nil {
				t.Fatalf("acknowledged child cannot reconstruct its own committed publication cut: %v", err)
			}
		})
	}
}

func TestIssue642PublishedChildHistoricalClosureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_join_after_activation", "retained_join_event_committed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				var selected startupownership.Store
				var construct func() SelectedContractExecutionOwner
				var dsn string
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, dsn = s, s.Path()
					construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					dsn = testutil.StartPostgresDSN(t)
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
					selected = s
					construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				lifecycle := selected.(SelectedContractForkLifecycle)
				source, err := lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.SourceRun})
				if err != nil || len(source.JoinSchedules) != 1 {
					t.Fatalf("original source control: joins=%d err=%v", len(source.JoinSchedules), err)
				}
				id := activityidentity.ForkLineageEventID(checkpoint.ForkRun, source.JoinSchedules[0].CurrentEventID)
				start, err := lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.ForkRun, AtStart: true})
				if err != nil || len(start.JoinSchedules) != 0 || len(start.TransferredJoins) != 1 {
					t.Fatalf("acknowledged/prepublication cut: joins=%d transfers=%d err=%v", len(start.JoinSchedules), len(start.TransferredJoins), err)
				}
				if _, found := start.HistoricalArrivalPublication(id); found {
					t.Fatal("prepublication history invented a committed event")
				}
				var published runfork.RunForkPlan
				if cut == "retained_join_event_committed" {
					published, err = lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.ForkRun, At: id})
					if err != nil || len(published.JoinSchedules) != 0 || len(published.TransferredJoins) != 1 {
						t.Fatalf("committed publication cut: %+v err=%v", published, err)
					}
					if _, found := published.HistoricalArrivalPublication(id); !found {
						t.Fatal("publication cut omitted exact transferred event evidence")
					}
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				process, _ := worklifetime.ProcessFromContext(ctx)
				owner := construct()
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
				if err != nil || len(recovered) != 1 || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("recover exact transferred work: %+v err=%v", recovered, err)
				}
				waitPublishedChildCompletion(t, selected, process, baseline, checkpoint.ForkRun)
				again, err := lifecycle.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: checkpoint.ForkRun, AtStart: true})
				if err != nil || !reflect.DeepEqual(start.Entities, again.Entities) || !reflect.DeepEqual(start.TransferredJoins, again.TransferredJoins) {
					t.Fatalf("restart/settlement changed the old prepublication cut: %v", err)
				}
				if cut != "retained_join_event_committed" {
					return
				}
				again, err = lifecycle.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: checkpoint.ForkRun, At: id})
				if err != nil || !reflect.DeepEqual(published.Entities, again.Entities) || !reflect.DeepEqual(published.PendingWork, again.PendingWork) ||
					!reflect.DeepEqual(published.HistoricalArrivalCoordinates(), again.HistoricalArrivalCoordinates()) {
					t.Fatalf("restart/settlement changed the old publication cut: %v", err)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				availability, err := owner.ports.fork.LoadRunBundleAvailability(ctx, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
				operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "published-descendant", IdempotencyKey: "child-cut", TransportHash: "descendant-transport",
					SourceRunID: checkpoint.ForkRun, ForkEventID: id, TargetBundleHash: availability.BundleHash, AllowSourceFreeze: true, ContractSelection: selection}
				result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: checkpoint.ForkRun, At: id, AllowSourceFreeze: true,
					Owner: owner, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection,
					AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
				if err != nil || !result.Activation.Activated {
					t.Fatalf("child-as-source exact owed delivery: %+v err=%v", result, err)
				}
				grandchild := result.Materialization.ForkRunID
				waitPublishedChildCompletion(t, selected, process, baseline, grandchild)
				grandEvent := storetest.LoadCanonicalEventRecord(t, ctx, selected, activityidentity.ForkLineageEventID(grandchild, id))
				lineage, found := grandEvent.SelectedForkLineage()
				if !found || lineage.SourceRunID() != checkpoint.ForkRun || lineage.SourceEventID() != id ||
					!grandEvent.CreatedAt().Equal(source.JoinSchedules[0].CurrentDueAt) {
					t.Fatal("descendant lost exact immediate lineage or original due coordinate")
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(ctx, grandchild)
				if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
					t.Fatalf("descendant repeated or omitted owed delivery: %+v err=%v", settlement, err)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, grandchild, "platform.join_complete"); err != nil || count != 1 {
					t.Fatalf("descendant publication count=%d err=%v", count, err)
				}
				if storage := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, selected, grandchild, grandchild); storage.Timers != 0 {
					t.Fatalf("descendant manufactured a timer: %+v", storage)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, checkpoint.ForkRun)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("descendant changed its completed parent's business facts: %v", err)
				}
			})
		}
	}
}

func waitPublishedChildCompletion(t *testing.T, selected any, process *worklifetime.Process, baseline uint64, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	reader := selected.(interface {
		LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	})
	for {
		header, err := reader.LoadRunHeader(ctx, runID)
		if err != nil || header.Failure != nil {
			t.Fatalf("continued child failed: %+v err=%v", header, err)
		}
		if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("continued child did not complete: %+v leases=%d baseline=%d", header, process.ActiveCount(), baseline)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
