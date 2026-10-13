package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
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
}

func (s unpublishedJoinLateSourceActivation) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	s.before()
	return s.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
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
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: publishedJoinFixture(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
			if err != nil {
				t.Fatal(err)
			}
			ancestor := uuid.NewString()
			cut, cause := seedPublishedJoinSource(t, ctx, selected, owner, loaded, ancestor)
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			lifecycle := selected.(SelectedContractForkLifecycle)
			activated, release := make(chan string, 1), make(chan struct{})
			var releaseOnce sync.Once
			releaseParent := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseParent()
			parentOwner := owner
			parentPorts := *owner.ports
			parentOwner.ports = &parentPorts
			parentOwner.ports.fork = selectedCrashActivated{SelectedContractForkLifecycle: owner.ports.fork, checkpoint: func(runID string) {
				activated <- runID
				select {
				case <-release:
				case <-ctx.Done():
				}
			}}
			type completed struct {
				result SelectedContractExecutionResult
				err    error
			}
			parentDone := make(chan completed, 1)
			parentJoined := false
			defer func() {
				releaseParent()
				if !parentJoined {
					select {
					case result := <-parentDone:
						if result.err != nil {
							t.Errorf("join unpublished parent preparation: %v", result.err)
						}
					case <-time.After(15 * time.Second):
						t.Error("unpublished parent preparation did not join")
					}
				}
			}()
			go func() {
				result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: ancestor, At: cut,
					AllowSourceFreeze: true, Owner: parentOwner, SourceLoader: loader, ContractSelection: selection,
					AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
				parentDone <- completed{result: result, err: err}
			}()
			var parentRun string
			select {
			case parentRun = <-activated:
			case result := <-parentDone:
				parentJoined = true
				t.Fatalf("parent did not reach its acknowledged unpublished cut: %+v error=%v", result.result, result.err)
			case <-time.After(15 * time.Second):
				t.Fatal("parent activation did not reach the existing bound")
			}
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
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			childOwner := owner
			if latePublication {
				childPorts := *owner.ports
				childOwner.ports = &childPorts
				childOwner.ports.fork = unpublishedJoinLateSourceActivation{SelectedContractForkLifecycle: owner.ports.fork, before: func() {
					// The child already owns its immutable cut/materialization. Let B
					// physically publish EB before C acquires execution authority.
					releaseParent()
					select {
					case parentResult := <-parentDone:
						parentJoined = true
						if parentResult.err != nil || !parentResult.result.Activation.Activated {
							t.Fatalf("late parent publication activation: %+v error=%v", parentResult.result, parentResult.err)
						}
					case <-time.After(15 * time.Second):
						t.Fatal("late parent publication activation did not join")
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
				AllowSourceFreeze: true, Owner: childOwner, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection,
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
			releaseParent()
			if !parentJoined {
				select {
				case parentResult := <-parentDone:
					parentJoined = true
					if parentResult.err != nil || !parentResult.result.Activation.Activated {
						t.Fatalf("late parent activation: %+v error=%v", parentResult.result, parentResult.err)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("late parent activation did not finish")
				}
			}
			parentDeadline := time.Now().Add(15 * time.Second)
			var parentState storetest.SelectedExecutionStorage
			for time.Now().Before(parentDeadline) {
				parentState, err = storetest.ReadSelectedExecutionStorage(ctx, selected, parentRun)
				if err != nil {
					t.Fatal(err)
				}
				if parentState.State == "closed" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			wantParentStatus := "forked"
			if latePublication {
				wantParentStatus = "completed"
			}
			if result.Activation.SourceFrozen == latePublication || parentState.State != "closed" || parentState.RunStatus != wantParentStatus {
				t.Fatalf("late execution must retain the exact frozen/diverged source outcome: activation=%+v parent=%+v", result.Activation, parentState)
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
		if state.RunStatus == "completed" && state.State == "closed" {
			return
		}
		last = state
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	t.Fatalf("continued unpublished obligation did not complete within the existing bound: state=%+v executions=%+v runs=%+v snapshot_error=%v", last,
		snapshot["run_fork_selected_contract_runtime_executions"], snapshot["runs"], err)
}
