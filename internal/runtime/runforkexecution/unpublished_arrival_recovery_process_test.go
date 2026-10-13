package runforkexecution

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func runUnpublishedArrivalCrashHelper(t *testing.T, ctx context.Context, selected startupownership.Store, owner SelectedContractExecutionOwner, capability startupownership.ProcessCapability) {
	t.Helper()
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: publishedJoinFixture(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
	if err != nil {
		t.Fatal(err)
	}
	ancestor := uuid.NewString()
	marker, original := seedPublishedJoinSource(t, ctx, selected, owner, loaded, ancestor)
	selection := runforkadmission.SelectedContractSelection(loaded.Source)
	activated, parentDone := make(chan string, 1), make(chan error, 1)
	parent := owner
	parentPorts := *owner.ports
	parent.ports = &parentPorts
	parent.ports.fork = selectedCrashActivated{SelectedContractForkLifecycle: owner.ports.fork, checkpoint: func(runID string) {
		activated <- runID
		<-ctx.Done()
	}}
	go func() {
		_, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: ancestor, At: marker,
			AllowSourceFreeze: true, Owner: parent, SourceLoader: loader, ContractSelection: selection,
			AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
		parentDone <- err
	}()
	var parentRun string
	select {
	case parentRun = <-activated:
	case err := <-parentDone:
		t.Fatalf("pending parent missed its actual activation acknowledgment: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("pending parent activation exceeded the existing bound")
	}
	plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: parentRun, AtStart: true})
	if err != nil || len(plan.TransferredJoins) != 1 || len(plan.JoinSchedules) != 0 {
		t.Fatalf("pending parent cut: %+v error=%v", plan, err)
	}
	pending := plan.TransferredJoins[0]
	if pending.Publication.CommittedCause.RunID != ancestor || pending.Publication.CommittedCause.EventID != original.CurrentEventID ||
		pending.Publication.EventID != activityidentity.ForkLineageEventID(parentRun, original.CurrentEventID) {
		t.Fatal("pending parent lost its exact committed cause")
	}
	if _, exists, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, pending.Publication.EventID); err != nil || exists {
		t.Fatalf("pending parent physically published before its cut: exists=%t error=%v", exists, err)
	}
	checkpoint := func(child string) {
		pipe := os.NewFile(3, "pending-arrival-checkpoint")
		if err := json.NewEncoder(pipe).Encode(selectedForkCrashCheckpoint{SourceRun: parentRun, ForkRun: child}); err != nil {
			t.Fatal(err)
		}
		_ = pipe.Close()
		var input [1]byte
		_, err := os.Stdin.Read(input[:])
		t.Fatalf("pending arrival checkpoint returned without SIGKILL: %v", err)
	}
	cut := os.Getenv("SWARM_SELECTED_CRASH_CUT")
	childOwner := owner
	childPorts := *owner.ports
	childOwner.ports = &childPorts
	switch cut {
	case "retained_pending_join_after_activation":
		childOwner.ports.fork = selectedCrashActivated{SelectedContractForkLifecycle: owner.ports.fork, checkpoint: checkpoint}
	case "retained_pending_join_event_committed":
		childOwner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(ctx context.Context) {
			occurrence, found := worklifetime.OccurrenceFromContext(ctx)
			selected, exact := occurrence.(*worklifetime.SelectedForkOccurrence)
			if !found || !exact {
				t.Fatal("pending publication lost its selected occurrence")
			}
			checkpoint(selected.Identity().RunID)
		}}
	default:
		t.Fatalf("unknown pending arrival crash cut %q", cut)
	}
	operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "pending-join-crash", IdempotencyKey: "parent-start",
		TransportHash: "pending-join-crash-transport", SourceRunID: parentRun, AtStart: true,
		TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: parentRun, AtStart: true,
		AllowSourceFreeze: true, Owner: childOwner, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
	if err != nil || !result.Activation.Activated {
		t.Fatalf("activate pending arrival before SIGKILL: %+v error=%v", result, err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(20 * time.Second):
	}
	t.Fatal("pending arrival never reached its acknowledged publication checkpoint")
}

func TestIssue642UnpublishedArrivalCrashSuccessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_pending_join_after_activation", "retained_pending_join_event_committed"} {
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
				crashed := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				lifecycle := selected.(SelectedContractForkLifecycle)
				parent, err := lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: crashed.SourceRun, AtStart: true})
				if err != nil || len(parent.TransferredJoins) != 1 || len(parent.JoinSchedules) != 0 {
					t.Fatalf("crash lost its immediate-source pending obligation: %+v error=%v", parent, err)
				}
				pending := parent.TransferredJoins[0]
				id := activityidentity.ForkLineageEventID(crashed.ForkRun, pending.Publication.EventID)
				binding, err := lifecycle.RequireRunForkSelectedContractBinding(t.Context(), crashed.ForkRun)
				if err != nil || binding.SourceRunID != crashed.SourceRun || binding.ForkPoint.Kind != runfork.RunForkPointRunStart {
					t.Fatalf("crash flattened the immediate-source boundary: %+v error=%v", binding, err)
				}
				start, err := lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: crashed.ForkRun, AtStart: true})
				if err != nil || len(start.TransferredJoins) != 1 || len(start.JoinSchedules) != 0 ||
					start.TransferredJoins[0].Publication.SourceRunID != crashed.SourceRun || start.TransferredJoins[0].Publication.SourceEventID != pending.Publication.EventID ||
					start.TransferredJoins[0].Publication.CommittedCause != pending.Publication.CommittedCause {
					t.Fatalf("child start changed its pending obligation or cause: %+v error=%v", start, err)
				}
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(t.Context(), selected)
				if err != nil {
					t.Fatal(err)
				}
				ctx := runForkTestContext(t)
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				inspected, err := InspectSelectedForkRecoveries(ctx, selected.(SelectedForkRecoveryInspectionReader), loader)
				if err != nil || len(inspected) != 2 {
					t.Fatalf("read-only two-boundary recovery inventory: %+v error=%v", inspected, err)
				}
				for _, entry := range inspected {
					switch entry.Entry.Binding.ForkRunID {
					case crashed.SourceRun:
						if entry.Evidence.RunState != "forked" || entry.Evidence.ExecutionState != "running" ||
							entry.Evidence.Plan.Disposition != runfork.SelectedForkRecoveryFailed || entry.Evidence.Plan.Continuation != nil || entry.Evidence.Plan.Operation != nil {
							t.Fatalf("frozen interrupted parent gained continuation authority: %+v", entry)
						}
					case crashed.ForkRun:
						if entry.Evidence.RunState != "running" || entry.Evidence.ExecutionState != "running" ||
							entry.Evidence.Plan.Disposition != runfork.SelectedForkRecoveryResume || entry.Evidence.Plan.Continuation == nil {
							t.Fatalf("acknowledged child lost its exact continuation: %+v", entry)
						}
					default:
						t.Fatalf("recovery inventory contains an unrelated boundary: %+v", entry)
					}
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(t.Context(), selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("read-only pending inspection mutated retained evidence: %v", err)
				}
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				process, _ := worklifetime.ProcessFromContext(ctx)
				owner := construct()
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				operations := selected.(interface {
					LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
				})
				ack, found, err := operations.LoadForkOperation(ctx, "pending-join-crash", "parent-start", "pending-join-crash-transport")
				if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.ForkRunID != crashed.ForkRun || ack.Result == nil ||
					ack.Request.ResolvedPoint == nil || *ack.Request.ResolvedPoint != binding.ForkPoint {
					t.Fatalf("pending crash lost permanent cut/activation acknowledgment: %+v error=%v", ack, err)
				}
				parentBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, crashed.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				ancestorBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, pending.Publication.CommittedCause.RunID)
				if err != nil {
					t.Fatal(err)
				}
				physical, found, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, id)
				if err != nil || found != (cut == "retained_pending_join_event_committed") {
					t.Fatalf("wrong physical crash boundary: found=%t error=%v", found, err)
				}
				if found && (!physical.Event.Event().CreatedAt().Equal(pending.Command.Due.Absolute) || physical.Event.Event().RunID() != crashed.ForkRun) {
					t.Fatal("physical crash checkpoint changed child identity or original due")
				}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability}})
				if err != nil || len(recovered) != 2 {
					t.Fatalf("pending arrival successor recovery: %+v error=%v", recovered, err)
				}
				for _, entry := range recovered {
					want := runfork.SelectedForkRecoveryFailed
					if entry.RunID == crashed.ForkRun {
						want = runfork.SelectedForkRecoveryResume
					}
					if entry.Disposition != want {
						t.Fatalf("successor confused frozen parent and owed child: %+v want=%s", entry, want)
					}
				}
				waitPublishedChildCompletion(t, selected, process, baseline, crashed.ForkRun)
				state, err := storetest.ReadSelectedExecutionStorage(ctx, selected, crashed.ForkRun)
				if err != nil || state.State != "closed" || state.RunStatus != "completed" || state.Occurrences != 2 {
					t.Fatalf("successor did not complete through one fresh execution: %+v error=%v", state, err)
				}
				parentState, err := storetest.ReadSelectedExecutionStorage(ctx, selected, crashed.SourceRun)
				if err != nil || parentState.State != "closed" || parentState.RunStatus != "forked" || parentState.Occurrences != 1 {
					t.Fatalf("successor reexecuted or changed the frozen parent: %+v error=%v", parentState, err)
				}
				event := storetest.LoadCanonicalEventRecord(t, ctx, selected, id)
				lineage, found := event.SelectedForkLineage()
				if !found || lineage.SourceRunID() != crashed.SourceRun || lineage.SourceEventID() != pending.Publication.EventID ||
					!event.CreatedAt().Equal(pending.Command.Due.Absolute) {
					t.Fatal("successor replaced immediate-source lineage or original due")
				}
				if physical.Event.ID() != "" {
					retained, found, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, id)
					if err != nil || !found || !reflect.DeepEqual(physical, retained) {
						t.Fatalf("successor replaced the already committed publication: %v", err)
					}
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(ctx, crashed.ForkRun)
				if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
					t.Fatalf("successor repeated or omitted pending input settlement: %+v error=%v", settlement, err)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, crashed.ForkRun, "platform.join_complete"); err != nil || count != 1 {
					t.Fatalf("successor output count=%d error=%v", count, err)
				}
				if count, err := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, crashed.ForkRun); err != nil || count != 0 {
					t.Fatalf("successor left input settlement outstanding: count=%d error=%v", count, err)
				}
				if _, exists, err := owner.ports.busDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, pending.Publication.EventID); err != nil || exists {
					t.Fatalf("successor invented the parent's event: exists=%t error=%v", exists, err)
				}
				again, err := lifecycle.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: crashed.ForkRun, AtStart: true})
				if err != nil || !reflect.DeepEqual(start.Entities, again.Entities) || !reflect.DeepEqual(start.TransferredJoins, again.TransferredJoins) {
					t.Fatalf("successor changed immutable start-cut evidence: %v", err)
				}
				for _, source := range []struct {
					runID  string
					before map[string]storetest.SelectedForkStorageTableSnapshot
				}{{crashed.SourceRun, parentBefore}, {pending.Publication.CommittedCause.RunID, ancestorBefore}} {
					after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, source.runID)
					if err != nil || !reflect.DeepEqual(source.before, after) {
						t.Fatalf("successor changed source/cause business facts: run=%s error=%v", source.runID, err)
					}
				}
				stable, found, err := operations.LoadForkOperation(ctx, ack.Request.Actor, ack.Request.IdempotencyKey, ack.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(ack, stable) {
					t.Fatalf("successor changed the permanent acknowledgment: %+v error=%v", stable, err)
				}
				requireUnpublishedJoinAncestorPurgeRefused(t, ctx, selected, capability, pending.Publication.CommittedCause.RunID, crashed.SourceRun)
				if err := owner.RetireSelectedContexts(ctx); err != nil || process.ActiveCount() != baseline {
					t.Fatalf("pending successor retained process work: count=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
				}
			})
		}
	}
}
