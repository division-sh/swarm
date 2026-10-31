package runforkexecution

import (
	"context"
	"database/sql"
	"errors"

	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642UnpublishedTransferredArrivalReforkBothStores(t *testing.T) {
	verifyUnpublishedTransferredArrivalRefork(t, false)
}

func TestIssue642UnpublishedArrivalIgnoresLateSourcePublicationBothStores(t *testing.T) {
	verifyUnpublishedTransferredArrivalRefork(t, true)
}

type unpublishedJoinLateSourceActivation struct {
	SelectedContractForkLifecycle
	before func()
	once   sync.Once
}

func (s *unpublishedJoinLateSourceActivation) RequireRunForkSelectedContractBinding(ctx context.Context, runID string) (runfork.RunForkSelectedContractBinding, error) {
	s.once.Do(s.before)
	return s.SelectedContractForkLifecycle.RequireRunForkSelectedContractBinding(ctx, runID)
}

func verifyUnpublishedTransferredArrivalRefork(t *testing.T, latePublication bool) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			baseline := process.ActiveCount()
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: publishedJoinFixture(t)}
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
			if err != nil {
				t.Fatal(err)
			}
			ancestor := uuid.NewString()
			cut, cause := seedPublishedJoinSource(t, ctx, selected, owner, loaded, ancestor)
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			lifecycle := selected.(SelectedContractForkLifecycle)
			parentRequest := SelectedContractExecutionRequest{SourceRunID: ancestor, At: cut,
				Owner: owner, SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}}
			if latePublication {
				parentRequest.ForkOperation = &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "unpublished-parent", IdempotencyKey: "late-publication",
					TransportHash: "late-parent-transport", SourceRunID: ancestor, ForkEventID: cut,
					TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), TargetRootReference: loaded.RootedSource.Admission().Reference(), ContractSelection: selection}
			}
			preparedParent, err := owner.Prepare(ctx, parentRequest)
			if err != nil {
				t.Fatal(err)
			}
			defer preparedParent.Close()
			materialized, acknowledged, err := owner.materializePrepared(preparedParent.operation.PreparationContext(), preparedParent)
			if err != nil || !acknowledged {
				t.Fatalf("original unpublished parent materialization: %+v acknowledged=%t err=%v", materialized, acknowledged, err)
			}
			parentRun := materialized.ForkRunID
			if err := preparedParent.Close(); err != nil {
				t.Fatal(err)
			}
			ordinary, contexts := liveForkRuntimeFixture(t, ctx, selected, owner, loaded, ancestor, executionposture.MockOnly)
			parentRequest.LiveRuntime, parentRequest.LiveContexts, parentRequest.LiveRuns = ordinary, contexts, selected.(runlifecycle.LiveRunReader)
			parent, err := lifecycle.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: parentRun, AtStart: true})
			if err != nil || len(parent.TransferredJoins) != 1 || len(parent.JoinSchedules) != 0 {
				t.Fatalf("unpublished immediate-parent obligation: %+v error=%v", parent, err)
			}
			pending := parent.TransferredJoins[0]
			if pending.Publication.SourceRunID != ancestor || pending.Publication.SourceEventID != cause.CurrentEventID ||
				pending.Publication.EventID != activityidentity.ForkLineageEventID(parentRun, cause.CurrentEventID) {
				t.Fatal("immediate parent's pending correspondence lost the committed cause")
			}
			if _, exists := parent.HistoricalJoinPublication(pending.Publication.EventID); exists {
				t.Fatal("exclusive parent start fabricated its pending publication")
			}
			if _, exists, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, pending.Publication.EventID); err != nil || exists {
				t.Fatalf("parent occurrence must remain physically unpublished: exists=%t error=%v", exists, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, parentRun)
			if err != nil {
				t.Fatal(err)
			}
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "unpublished-descendant", IdempotencyKey: "parent-start",
				TransportHash: "unpublished-descendant-transport", SourceRunID: parentRun, AtStart: true,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), ContractSelection: selection, TargetRootReference: loaded.RootedSource.Admission().Reference(),
			}
			childOwner := owner
			if latePublication {
				childPorts := *owner.ports
				childOwner.ports = &childPorts
				childOwner.ports.fork = &unpublishedJoinLateSourceActivation{SelectedContractForkLifecycle: owner.ports.fork, before: func() {
					// The child already owns its immutable cut/materialization. Let B
					// physically publish EB before C acquires execution authority.
					parentResult, err := ExecuteSelectedContractRunFork(ctx, parentRequest)
					if err != nil || !parentResult.Activation.Activated || parentResult.Materialization.ForkRunID != parentRun {
						t.Fatalf("late parent publication lost its original materialized child: %+v err=%v", parentResult, err)
					}
					waitUnpublishedReforkRunCompletion(t, ctx, selected, parentRun)
					original := storetest.LoadCanonicalEventRecord(t, ctx, selected, pending.Publication.EventID)
					if original.RunID() != parentRun || !original.CreatedAt().Equal(cause.CurrentDueAt) {
						t.Fatal("late immediate-source publication changed its owner or original due")
					}
					before, err = storetest.ReadSelectedForkSourceDomain(ctx, selected, parentRun)
					if err != nil {
						t.Fatal(err)
					}
				}}
			}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: parentRun, AtStart: true,
				Owner: childOwner, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection,
				LiveRuntime: ordinary, LiveContexts: contexts, LiveRuns: selected.(runlifecycle.LiveRunReader),
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("continue the immediate parent's admitted unpublished obligation: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			binding, err := lifecycle.RequireRunForkSelectedContractBinding(ctx, child)
			if err != nil || binding.SourceRunID != parentRun || binding.ForkPoint.Kind != runfork.RunForkPointRunStart {
				t.Fatalf("unpublished continuation flattened the parent boundary: %+v error=%v", binding, err)
			}
			id := activityidentity.ForkLineageEventID(child, pending.Publication.EventID)
			waitUnpublishedReforkRunCompletion(t, ctx, selected, child)
			event := storetest.LoadCanonicalEventRecord(t, ctx, selected, id)
			lineage, exists := event.SelectedForkLineage()
			if !exists || lineage.SourceRunID() != parentRun || lineage.SourceEventID() != pending.Publication.EventID ||
				!event.CreatedAt().Equal(cause.CurrentDueAt) {
				t.Fatal("continued arrival lost immediate-parent identity or original due")
			}
			if _, exists, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, pending.Publication.EventID); err != nil || exists != latePublication {
				t.Fatalf("child publication changed the parent's physical occurrence: exists=%t error=%v", exists, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, child, "platform.join_complete"); err != nil || count != 1 {
				t.Fatalf("continued arrival publication count=%d error=%v", count, err)
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(ctx, child)
			if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
				t.Fatalf("continued arrival did not settle exactly once: %+v error=%v", settlement, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, parentRun)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("unpublished descendant changed its immediate parent's business facts: %v", err)
			}
			childBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			parentState, err := storetest.ReadSelectedExecutionStorage(ctx, selected, parentRun)
			if latePublication {
				if err != nil || parentState.State != "closed" || parentState.RunStatus != "paused" || parentState.Occurrences != 1 {
					t.Fatalf("late source publication lost its actual selected lifetime: %+v err=%v", parentState, err)
				}
			} else if !errors.Is(err, sql.ErrNoRows) || parentState.Occurrences != 0 {
				t.Fatalf("unpublished source acquired execution authority: %+v err=%v", parentState, err)
			}
			availability, err := lifecycle.LoadRunBundleAvailability(ctx, parentRun)
			if err != nil || !availability.Available() || availability.Status != "paused" || result.Activation.SourceFrozen {
				t.Fatalf("source did not retain its paused nonterminal disposition: %+v activation=%+v err=%v", availability, result.Activation, err)
			}
			if _, exists, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, pending.Publication.EventID); err != nil || exists != latePublication {
				t.Fatalf("retired parent changed its actual publication evidence: exists=%t error=%v", exists, err)
			}
			childAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, child)
			if err != nil || !reflect.DeepEqual(childBefore, childAfter) {
				t.Fatalf("late parent publication changed its settled child: %v", err)
			}
			if err := owner.RetireSelectedContexts(ctx); err != nil {
				t.Fatal(err)
			}
			if err := ordinary.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if process.ActiveCount() != baseline {
				t.Fatalf("selected retirement leaked process work: got%d want%d", process.ActiveCount(), baseline)
			}
		})
	}
}

func waitUnpublishedReforkRunCompletion(t *testing.T, ctx context.Context, selected startupownership.Store, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last storetest.SelectedExecutionStorage
	for time.Now().Before(deadline) {
		state, err := storetest.ReadSelectedExecutionStorage(ctx, selected, runID)
		if err != nil {
			t.Fatal(err)
		}
		quiescence, err := selected.(interface {
			LoadRunTestQuiescence(context.Context, string, time.Time) (operatorread.RunTestQuiescence, error)
		}).LoadRunTestQuiescence(ctx, runID, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if state.RunStatus == "live" && state.State == "closed" && quiescence.Ready {
			return
		}
		last = state
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	t.Fatalf("continued unpublished obligation did not complete within the existing bound: state=%+v executions=%+v runs=%+v snapshot_error=%v", last,
		snapshot["run_fork_selected_contract_runtime_executions"], snapshot["runs"], err)
}
