package pipeline_test

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642UnpublishedTransferredBarrierReforkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			producer, err := identity.AdmitExecutableNodeDeclaration("producer", "producer")
			if err != nil {
				t.Fatal(err)
			}
			final, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
			if err != nil {
				t.Fatal(err)
			}
			probe := &publishedBarrierClaimProbe{Probe: lifecycleprobe.New(), owner: producer.Key(), claim: make(chan forkEngineObservation, 1), resume: make(chan struct{}), completed: make(chan string, 1)}
			root := canonicalrouting.CopyForkFanOutBarrierReceiver(t)
			rt, selected, repo, capability, _, _ := startForkEngineRuntimeOwner(t, backend, root, probe)
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(probe.resume) }) })
			ctx := correlation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
			ctx = correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
			phase, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			ancestor := uuid.NewString()
			seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "pending-barrier-proof", "", []byte(`{"token":"seed"}`), 0, ancestor, "", events.EventEnvelope{}, time.Now().UTC())
			if err := rt.Bus.PublishAndWait(phase, seed); err != nil {
				t.Fatal(err)
			}
			trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "pending-barrier-proof", "", []byte(`{"token":"7","tokens":["7"]}`), 0, ancestor, events.EventEnvelope{}, time.Now().UTC())
			if err := rt.Bus.Publish(phase, trigger); err != nil {
				t.Fatal(err)
			}
			var original forkEngineObservation
			select {
			case original = <-probe.claim:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			family, found := selected.RunFork()
			if !found {
				t.Fatal("pending barrier requires the selected fork owner")
			}
			cut, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: ancestor, At: original.id})
			if err != nil || len(cut.FanOutObligations) != 1 || cut.FanOutObligations[0].Barrier == nil {
				t.Fatalf("committed barrier cut: %+v error=%v", cut, err)
			}
			release.Do(func() { close(probe.resume) })
			var completed string
			select {
			case completed = <-probe.completed:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			if _, err := probe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: completed, SubscriberID: final.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			waitPublishedBarrierPipelineReceipts(t, phase, selected.RuntimeDeps().DeliveryStore, original.id, completed)
			if err := rt.WorkOccurrence().WaitForQuiescence(phase); err != nil {
				t.Fatalf("join ordinary source work before measuring selected ownership: %v", err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, filepath.Join(repo, "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			request := runforkexecution.SelectedContractExecutionRequest{SourceRunID: ancestor, At: original.id,
				AllowSourceFreeze: true, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(),
				SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: selected.SourceArtifactStore()},
				ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
				AgentRuntime:      runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture}}
			baseline := rt.Options.ProcessWorkOwner.ActiveCount()
			prepared, err := family.Prepare(phase, request)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			}()
			materialization, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			writer := selected.RuntimeDeps().DeliveryStore.(interface {
				MaterializeRunForkForSelectedContractExecution(context.Context, runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			parent, err := writer.MaterializeRunForkForSelectedContractExecution(phase, materialization)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: parent.ForkRunID, AtStart: true})
			if err != nil || len(pending.FanOutObligations) != 1 || pending.FanOutObligations[0].Barrier == nil {
				t.Fatalf("unpublished parent's exact barrier: %+v error=%v", pending, err)
			}
			barrier := pending.FanOutObligations[0].Barrier
			if barrier.Status != fanoutbarrier.StatusClosedPending || barrier.TransferredOccurrence == nil {
				t.Fatal("parent does not own the admitted unpublished barrier")
			}
			occurrence := barrier.TransferredOccurrence
			if occurrence.Publication.SourceRunID != ancestor || occurrence.Publication.SourceEventID != original.id ||
				occurrence.Publication.CommittedCause.RunID != ancestor || occurrence.Publication.CommittedCause.EventID != original.id {
				t.Fatal("parent barrier lost its exact committed cause")
			}
			if _, found := pending.HistoricalJoinPublication(occurrence.Publication.EventID); found {
				t.Fatal("pending parent barrier was fabricated as a physical event")
			}
			before, err := storetest.ReadSelectedForkSourceDomain(phase, selected.RuntimeDeps().DeliveryStore, parent.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			childProbe := &publishedBarrierClaimProbe{Probe: lifecycleprobe.New(), owner: producer.Key(), claim: make(chan forkEngineObservation, 1), completed: make(chan string, 1)}
			request.SourceRunID, request.At, request.AtStart = parent.ForkRunID, "", true
			request.ForkOperation = &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "pending-barrier-refork", IdempotencyKey: "parent-start",
				TransportHash: "pending-barrier-refork-transport", SourceRunID: parent.ForkRunID, AtStart: true,
				TargetBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: request.ContractSelection}
			request.AgentRuntime.AgentManagerOptions = manager.AgentManagerOptions{TestLifecycleProbe: childProbe}
			child, err := family.Execute(phase, request)
			if err != nil || !child.Activation.Activated {
				t.Fatalf("continue pending barrier: %+v error=%v", child, err)
			}
			id := activityidentity.ForkLineageEventID(child.Materialization.ForkRunID, occurrence.Publication.EventID)
			if _, err := childProbe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: id, SubscriberID: producer.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			select {
			case completed = <-childProbe.completed:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			if _, err := childProbe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: completed, SubscriberID: final.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			waitPublishedBarrierPipelineReceipts(t, phase, selected.RuntimeDeps().DeliveryStore, id, completed)
			event := storetest.LoadCanonicalEventRecord(t, phase, selected.RuntimeDeps().DeliveryStore, id)
			lineage, found := event.SelectedForkLineage()
			if !found || lineage.SourceRunID() != parent.ForkRunID || lineage.SourceEventID() != occurrence.Publication.EventID || !event.CreatedAt().Equal(occurrence.DueAt) {
				t.Fatal("pending barrier skipped its parent boundary or changed its original due")
			}
			for _, count := range []struct {
				runID string
				want  int
			}{{parent.ForkRunID, 0}, {child.Materialization.ForkRunID, 1}} {
				got, err := storetest.ReadLifecycleEventCardinality(phase, selected.RuntimeDeps().DeliveryStore, count.runID, "platform.join_complete")
				if err != nil || got != count.want {
					t.Fatalf("exact barrier publication count=%d want%d error=%v", got, count.want, err)
				}
			}
			key := barrier.Registration.IntentKey
			key.RunID = child.Materialization.ForkRunID
			settled, err := storetest.ReadExactFanOutBarrier(phase, selected.RuntimeDeps().DeliveryStore, key)
			if err != nil || settled.Status != fanoutbarrier.StatusFired || settled.TransferredOccurrence == nil ||
				settled.TransferredOccurrence.Publication.SourceRunID != parent.ForkRunID ||
				settled.TransferredOccurrence.Publication.SourceEventID != occurrence.Publication.EventID ||
				settled.TransferredOccurrence.Publication.CommittedCause != occurrence.Publication.CommittedCause ||
				!settled.TransferredOccurrence.DueAt.Equal(occurrence.DueAt) {
				t.Fatalf("pending barrier lost exact terminal settlement/cause/due: %+v error=%v", settled, err)
			}
			// This fixture's on_complete moves to active and its root consumer
			// only writes data. Settled work does not terminalize those stages.
			state, err := storetest.ReadSelectedExecutionStorage(phase, selected.RuntimeDeps().DeliveryStore, child.Materialization.ForkRunID)
			if err != nil || state.State != "running" || state.RunStatus != "running" {
				t.Fatalf("settled barrier changed the intentionally open run: %+v error=%v", state, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(phase, selected.RuntimeDeps().DeliveryStore, parent.ForkRunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("pending child changed its parent's business facts: %v", err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			if err := family.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
			if rt.Options.ProcessWorkOwner.ActiveCount() != baseline {
				t.Fatalf("pending barrier retained process work: got%d want%d", rt.Options.ProcessWorkOwner.ActiveCount(), baseline)
			}
		})
	}
}
