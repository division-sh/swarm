package pipeline_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
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
	forkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := family.Execute(forkCtx, request)
	if err != nil || !result.Activation.Activated || result.ForkLocalRuntimeContainer == nil {
		t.Fatalf("continuing child cannot itself execute a fixed-cut fork: %+v err=%v", result, err)
	}
	var observed forkEngineObservation
	select {
	case observed = <-probe.claimed:
	case <-forkCtx.Done():
		t.Fatalf("second-generation receiver claim did not start: %v", forkCtx.Err())
	}
	claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
	admission, admitted := managedexecution.FromContext(observed.ctx)
	proof := result.ForkLocalRuntimeContainer
	if !claimed || !admitted || !admission.AuthorizesSelected(proof.RuntimeExecutionID, proof.ForkRunID, proof.RuntimeGeneration) ||
		claim.RunID() != proof.ForkRunID || claim.RunID() == request.SourceRunID || observed.id == request.At {
		t.Fatal("second-generation fork reused predecessor authority or event identity")
	}
	snapshot, err := selected.RuntimeDeps().DeliveryStore.Snapshot(forkCtx, claim.DeliveryID())
	if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.Route.Target.Route().EntityID != parent.Entity.EntityID {
		t.Fatalf("second-generation receiver lost exact ownership: %+v err=%v", snapshot, err)
	}
	observationRequest := forkEngineExecutionProofRequest(t, result, request.At, observed.id, claim)
	assertForkEngineCommittedEvidence(t, readForkEngineExecutionProof(t, forkCtx, selected, observationRequest), result, request.At)
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
	after, err := selected.Entities().LoadOperatorEntity(forkCtx, parent.Entity.EntityID, claim.RunID())
	if err != nil || after.Fields["processed_token"] != parent.Fields["processed_token"] || after.Entity.Revision != before.Entity.Revision+1 {
		t.Fatalf("second-generation child did not execute exactly once: before=%+v after=%+v err=%v", before, after, err)
	}
	unchanged, err := selected.Entities().LoadOperatorEntity(forkCtx, parent.Entity.EntityID, request.SourceRunID)
	if err != nil || unchanged.Entity.Revision != parent.Entity.Revision || !reflect.DeepEqual(unchanged.Fields, parent.Fields) {
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
