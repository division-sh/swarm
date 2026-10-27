package pipeline_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func proveMixedFanOutChildRefork(t *testing.T, ctx context.Context, selected *storeselected.Owner, request runforkexecution.SelectedContractExecutionRequest, parent operatorread.OperatorEntityFull, retained fanoutobligation.InheritedTerminalDisposition) {
	t.Helper()
	family, ok := selected.RunFork()
	if !ok {
		t.Fatal("second-generation proof requires the canonical fork family")
	}
	probe := &forkEngineProbe{claimed: make(chan forkEngineObservation, 1), settled: make(chan lifecycleprobe.Signal, 1),
		resume: make(chan struct{}), resumeSettled: make(chan struct{})}
	var release, releaseSettlement sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(probe.resume) })
		releaseSettlement.Do(func() { close(probe.resumeSettled) })
	})
	request.AgentRuntime.AgentManagerOptions.TestLifecycleProbe = probe
	request.ForkOperation = nil
	request = forkEngineLiveRequest(t, request.LiveRuntime, selected, request)
	forkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	acknowledged := make(chan forkEngineAcknowledgment, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, err := family.Execute(forkCtx, request)
		acknowledged <- forkEngineAcknowledgment{result: result, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		release.Do(func() { close(probe.resume) })
		releaseSettlement.Do(func() { close(probe.resumeSettled) })
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("second-generation execution did not join")
		}
	})
	var observed forkEngineObservation
	select {
	case observed = <-probe.claimed:
	case ack := <-acknowledged:
		t.Fatalf("second-generation fork returned before its controlled claim: %+v err=%v", ack.result, ack.err)
	case <-forkCtx.Done():
		t.Fatalf("second-generation receiver claim did not start: %v", forkCtx.Err())
	}
	claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
	if !claimed || claim.RunID() == request.SourceRunID || observed.id == request.At {
		t.Fatal("second-generation fork reused predecessor authority or event identity")
	}
	snapshot, err := selected.RuntimeDeps().DeliveryStore.Snapshot(forkCtx, claim.DeliveryID())
	if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.Route.Target.Route().EntityID != parent.Entity.EntityID {
		t.Fatalf("second-generation receiver lost exact ownership: %+v err=%v", snapshot, err)
	}
	observationRequest := forkEngineClaimProofRequest(t, forkCtx, selected, request.SourceRunID, request.At, observed)
	physical := readForkEngineExecutionProof(t, forkCtx, selected, observationRequest)
	if physical.SourceEventID != request.At || physical.BindingID != observationRequest.BindingID || physical.ExecutionCount != 1 || physical.PublicationCount != 1 {
		t.Fatalf("second-generation pending claim lost native lineage: %+v", physical)
	}
	cut, err := family.Plan(forkCtx, runfork.RunForkPlanRequest{SourceRunID: claim.RunID(), At: observed.id})
	if err != nil {
		t.Fatal(err)
	}
	assertMixedFanOutPendingPlan(t, cut, observed.id, retained)
	before, err := selected.Entities().LoadOperatorEntity(forkCtx, parent.Entity.EntityID, claim.RunID())
	if err != nil || before.Entity.RunID != claim.RunID() || before.Fields["processed_token"] != "seeded" {
		t.Fatalf("second-generation state came from live parent rather than the cut: %+v err=%v", before, err)
	}
	release.Do(func() { close(probe.resume) })
	select {
	case signal := <-probe.settled:
		if signal.EventID != observed.id || signal.Status != "delivered" {
			t.Fatalf("second-generation settlement changed the owed recipient: %+v", signal)
		}
	case <-forkCtx.Done():
		t.Fatal(forkCtx.Err())
	}
	settlement, err := selected.RuntimeDeps().DeliveryStore.Snapshot(forkCtx, claim.DeliveryID())
	if err != nil || settlement.Status != deliverylifecycle.StatusDelivered || !settlement.MatchesSettlementClaim(claim) {
		t.Fatalf("second-generation child lost its own settlement claim: %+v err=%v", settlement, err)
	}
	releaseSettlement.Do(func() { close(probe.resumeSettled) })
	waitForkEnginePipelineSettlement(t, forkCtx, selected, observationRequest, settlement)
	select {
	case ack := <-acknowledged:
		if ack.err != nil || !ack.result.Activation.Activated || observationRequest != forkEngineExecutionProofRequest(t, ack.result, request.At, observed.id, claim) {
			t.Fatalf("second-generation fork lost its actual post-settlement handoff: %+v err=%v", ack.result, ack.err)
		}
		assertForkEngineCommittedEvidence(t, physical, ack.result, request.At)
	case <-forkCtx.Done():
		t.Fatal(forkCtx.Err())
	}
	after, err := selected.Entities().LoadOperatorEntity(forkCtx, parent.Entity.EntityID, claim.RunID())
	if err != nil || after.Fields["processed_token"] != parent.Fields["processed_token"] || forkFixtureEntityRevision(t, after.Entity.Revision) != forkFixtureEntityRevision(t, before.Entity.Revision)+1 {
		t.Fatalf("second-generation child did not execute exactly once: before=%+v after=%+v err=%v", before, after, err)
	}
	unchanged, err := selected.Entities().LoadOperatorEntity(forkCtx, parent.Entity.EntityID, request.SourceRunID)
	if err != nil || forkFixtureEntityRevision(t, unchanged.Entity.Revision) != forkFixtureEntityRevision(t, parent.Entity.Revision) || !reflect.DeepEqual(unchanged.Fields, parent.Fields) {
		t.Fatalf("second-generation execution changed its parent: before=%+v after=%+v err=%v", parent, unchanged, err)
	}
	outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(forkCtx, selected.RuntimeDeps().DeliveryStore, claim.RunID())
	if err != nil || len(outcomes) != 1 || outcomes[0].Ordinal != 0 || outcomes[0].EventID != observed.id || outcomes[0].Kind != string(fanoutobligation.OutcomeContinued) {
		t.Fatalf("second-generation child lost the immediate-parent continued ordinal: %+v err=%v", outcomes, err)
	}
	report, err := selected.Runs().LoadRunDebugReport(forkCtx, claim.RunID(), operatorread.RunDebugQueryOptions{})
	if err != nil || report.FanOut.Settled != 1 || report.FanOut.Unsettled != 0 || report.FanOut.Committed != 1 ||
		len(report.Deliveries) != 1 || report.Deliveries[0].SubscriberID != snapshot.Route.Recipient.ID() || report.Deliveries[0].Status != "delivered" || report.Deliveries[0].Count != 1 {
		t.Fatalf("second-generation fork reissued retained work or lost terminal reduction: %+v err=%v", report, err)
	}
}
