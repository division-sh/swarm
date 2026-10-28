package pipeline_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type mixedFanOutSourceProbe struct {
	runID, receiver string
	history         *lifecycleprobe.Probe
	claimed         chan forkEngineObservation
	completed       chan forkEngineObservation
	cut             chan string
	resume          chan struct{}
	once, cutOnce   sync.Once
}

func (p *mixedFanOutSourceProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.history.NotifyLifecycle(ctx, signal)
	if p.completed != nil && signal.Kind == lifecycleprobe.DeliveryStatusChanged && signal.EventType == "producer/batch.completed" && signal.Status == "delivered" {
		if _, ok := deliverylifecycle.ClaimFromContext(ctx); ok {
			p.completed <- forkEngineObservation{ctx: ctx, id: signal.EventID}
		}
	}
	if signal.Kind == lifecycleprobe.EventPersisted && signal.EventType == "work.cut" {
		p.cutOnce.Do(func() { p.cut <- signal.EventID })
	}
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.EventType != "producer/work.ready" ||
		signal.SubscriberID != p.receiver || signal.Status != "in_progress" {
		return
	}
	claim, ok := deliverylifecycle.ClaimFromContext(ctx)
	if !ok || claim.RunID() != p.runID {
		return
	}
	p.once.Do(func() {
		p.claimed <- forkEngineObservation{ctx: ctx, id: signal.EventID}
		select {
		case <-p.resume:
		case <-ctx.Done():
		}
	})
}

func TestIssue642FanOutIssuedPendingRecipientMatrix(t *testing.T) {
	runMixedFanOutPendingMatrix(t, false)
}

func TestIssue642FanOutContinuedChildReforkBothStores(t *testing.T) {
	runMixedFanOutPendingMatrix(t, true)
}

func runMixedFanOutPendingMatrix(t *testing.T, refork bool) {
	runMixedFanOutPendingMatrixWithCrash(t, refork, nil)
}

func runMixedFanOutPendingMatrixWithCrash(t *testing.T, refork bool, crash *mixedFanOutCrashFixture) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, retained := range []fanoutobligation.InheritedTerminalDisposition{fanoutobligation.InheritedSucceeded, fanoutobligation.InheritedDeadLettered} {
			if crash != nil && (backend != crash.backend || retained != crash.retained) {
				continue
			}
			t.Run(backend+"/"+string(retained), func(t *testing.T) {
				runID := uuid.NewString()
				consumer, err := identity.AdmitExecutableNodeDeclaration("consumer", "collector")
				if err != nil {
					t.Fatal(err)
				}
				settled, err := identity.AdmitExecutableNodeDeclaration("settled", "collector")
				if err != nil {
					t.Fatal(err)
				}
				sourceProbe := &mixedFanOutSourceProbe{runID: runID, receiver: consumer.Key(), history: lifecycleprobe.New(),
					claimed: make(chan forkEngineObservation, 1), cut: make(chan string, 1), resume: make(chan struct{})}
				root := canonicalrouting.CopyForkFanOutReceiverOwnership(t)
				var address string
				if crash != nil {
					address = crash.address
				}
				rt, selected, repo, capability, _, _ := startForkEngineRuntimeOwnerAt(t, backend, root, sourceProbe, address)
				var sourceRelease sync.Once
				t.Cleanup(func() { sourceRelease.Do(func() { close(sourceProbe.resume) }) })
				ctx := correlation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
				ctx = correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
				ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
				ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("mixed proof requires the canonical fork owner")
				}
				seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "mixed-fanout-proof", "", []byte(`{"token":"seed"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.Publish(ctx, seed); err != nil {
					t.Fatal(err)
				}
				phaseCtx, phaseCancel := context.WithTimeout(ctx, 30*time.Second)
				defer phaseCancel()
				waitMixedFanOutSeed(t, phaseCtx, selected.Entities(), runID)
				token, status := "7", "delivered"
				if retained == fanoutobligation.InheritedDeadLettered {
					token, status = "not-an-integer", "dead_letter"
				}
				payload, err := json.Marshal(map[string]any{"token": token, "tokens": []string{token}})
				if err != nil {
					t.Fatal(err)
				}
				trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "mixed-fanout-proof", "", payload, 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.Publish(ctx, trigger); err != nil {
					t.Fatal(err)
				}
				var sourceClaim forkEngineObservation
				select {
				case sourceClaim = <-sourceProbe.claimed:
				case <-phaseCtx.Done():
					t.Fatalf("source did not reach its real pending claim: %v", phaseCtx.Err())
				}
				if _, err := sourceProbe.history.Wait(phaseCtx, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged,
					EventID: sourceClaim.id, SubscriberType: "node", SubscriberID: settled.Key(), Status: status}); err != nil {
					t.Fatalf("source sibling did not reach exact %s settlement: %v", status, err)
				}
				checkpoint := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.checkpointed", "mixed-fanout-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.Publish(ctx, checkpoint); err != nil {
					t.Fatal(err)
				}
				var cutID string
				select {
				case cutID = <-sourceProbe.cut:
				case <-phaseCtx.Done():
					t.Fatalf("authored handler did not commit the cut: %v", phaseCtx.Err())
				}
				sourcePlan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: cutID})
				if err != nil {
					t.Fatal(err)
				}
				assertMixedFanOutPendingPlan(t, sourcePlan, sourceClaim.id, retained)
				sourceConsumer := mixedFanOutEntity(t, sourcePlan, "consumer")
				beforeSource, err := selected.Entities().LoadOperatorEntity(ctx, sourceConsumer.EntityID, runID)
				if err != nil || beforeSource.Fields["processed_token"] != "seeded" {
					t.Fatalf("source pending receiver changed before fork: %+v err=%v", beforeSource, err)
				}
				sourceRelease.Do(func() { close(sourceProbe.resume) })
				if _, err := sourceProbe.history.Wait(phaseCtx, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged,
					EventID: sourceClaim.id, SubscriberType: "node", SubscriberID: consumer.Key(), Status: "delivered"}); err != nil {
					t.Fatalf("source pending claim did not join after capturing its historical cut: %v", err)
				}
				beforeSource, err = selected.Entities().LoadOperatorEntity(ctx, sourceConsumer.EntityID, runID)
				if err != nil || beforeSource.Fields["processed_token"] != token {
					t.Fatalf("source continuation did not commit before the fork conservation baseline: %+v err=%v", beforeSource, err)
				}
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, filepath.Join(repo, "platform-spec.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				probe := &forkEngineProbe{claimed: make(chan forkEngineObservation, 1), settled: make(chan lifecycleprobe.Signal, 1),
					resume: make(chan struct{}), resumeSettled: make(chan struct{})}
				var release, releaseSettlement sync.Once
				t.Cleanup(func() {
					release.Do(func() { close(probe.resume) })
					releaseSettlement.Do(func() { close(probe.resumeSettled) })
				})
				forkCtx, forkCancel := context.WithTimeout(ctx, 30*time.Second)
				defer forkCancel()
				executionRequest := runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: runID, At: cutID, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
					SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.SourceArtifactStore()},
					ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
					AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture,
						AgentManagerOptions: manager.AgentManagerOptions{TestLifecycleProbe: probe}},
				}
				rooted, err := bundle.RootedSource()
				if err != nil {
					t.Fatal(err)
				}
				executionRequest.ForkOperation = &runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "mixed-fanout-crash", IdempotencyKey: "pending-cut", TransportHash: "mixed-fanout-crash-transport",
					SourceRunID: runID, ForkEventID: cutID, TargetBundleHash: executionRequest.ExpectedBundleHash,
					TargetRootReference: rooted.Admission().Reference(),
					AllowSourceFreeze:   true, ContractSelection: executionRequest.ContractSelection,
				}
				executionRequest = forkEngineLiveRequest(t, rt, selected, executionRequest)
				acknowledged := make(chan forkEngineAcknowledgment, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					result, err := family.Execute(forkCtx, executionRequest)
					acknowledged <- forkEngineAcknowledgment{result: result, err: err}
				}()
				t.Cleanup(func() {
					forkCancel()
					release.Do(func() { close(probe.resume) })
					releaseSettlement.Do(func() { close(probe.resumeSettled) })
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("mixed fan-out execution did not join")
					}
				})
				var observed forkEngineObservation
				select {
				case observed = <-probe.claimed:
				case ack := <-acknowledged:
					t.Fatalf("mixed fan-out returned before its controlled claim: %+v err=%v", ack.result, ack.err)
				case <-forkCtx.Done():
					t.Fatalf("child did not acquire the exact owed receiver: %v", forkCtx.Err())
				}
				claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
				if !claimed || claim.RunID() == runID || observed.id == sourceClaim.id {
					t.Fatal("mixed continuation reused historical execution authority")
				}
				snapshot, err := selected.RuntimeDeps().DeliveryStore.Snapshot(forkCtx, claim.DeliveryID())
				if err != nil || snapshot.Route.Recipient.ID() != consumer.Key() || snapshot.Status != deliverylifecycle.StatusInProgress {
					t.Fatalf("child executed settled or wrong recipient: %+v err=%v", snapshot, err)
				}
				request := forkEngineClaimProofRequest(t, forkCtx, selected, runID, sourceClaim.id, observed)
				claimedProof := readForkEngineExecutionProof(t, forkCtx, selected, request)
				if claimedProof.SourceEventID != cutID || claimedProof.BindingID != request.BindingID || claimedProof.ExecutionCount != 1 || claimedProof.PublicationCount != 1 {
					t.Fatalf("pending continuation lacks its exact committed cut and native executor: %+v", claimedProof)
				}
				childPlan, err := family.Plan(forkCtx, runfork.RunForkPlanRequest{SourceRunID: claim.RunID(), At: observed.id})
				if err != nil {
					t.Fatalf("continuing child publication is not itself a valid cut: %v", err)
				}
				assertMixedFanOutPendingPlan(t, childPlan, observed.id, retained)
				childEntity := snapshot.Route.Target.Route().EntityID
				beforeChild, err := selected.Entities().LoadOperatorEntity(forkCtx, childEntity, claim.RunID())
				if err != nil || beforeChild.Fields["processed_token"] != "seeded" || childEntity != sourceConsumer.EntityID ||
					beforeChild.Entity.RunID != claim.RunID() || beforeChild.Entity.FlowInstance != "consumer" {
					t.Fatalf("child did not retain run-scoped fixed-cut receiver ownership: %+v err=%v", beforeChild, err)
				}
				if crash != nil {
					crash.publishCheckpoint(t, mixedFanOutCrashCheckpoint{
						Proof: request, BundleHash: rt.Options.SourceArtifactFact.BundleHash(),
						SourceEntityID: sourceConsumer.EntityID, ChildEntityID: childEntity,
						SourceRevision: forkFixtureEntityRevision(t, beforeSource.Entity.Revision), ChildRevision: forkFixtureEntityRevision(t, beforeChild.Entity.Revision), Token: token,
					})
				}
				release.Do(func() { close(probe.resume) })
				select {
				case signal := <-probe.settled:
					if signal.EventID != observed.id || signal.Status != "delivered" {
						t.Fatalf("child settlement changed the owed recipient outcome: %+v", signal)
					}
				case <-forkCtx.Done():
					t.Fatal(forkCtx.Err())
				}
				settlement, err := selected.RuntimeDeps().DeliveryStore.Snapshot(forkCtx, claim.DeliveryID())
				if err != nil || settlement.Status != deliverylifecycle.StatusDelivered || !settlement.MatchesSettlementClaim(claim) {
					t.Fatalf("child lost its fresh settlement: %+v err=%v", settlement, err)
				}
				releaseSettlement.Do(func() { close(probe.resumeSettled) })
				waitForkEnginePipelineSettlement(t, forkCtx, selected, request, settlement)
				select {
				case ack := <-acknowledged:
					if ack.err != nil || !ack.result.Activation.Activated || request != forkEngineExecutionProofRequest(t, ack.result, sourceClaim.id, observed.id, claim) {
						t.Fatalf("mixed fan-out lost its exact post-settlement handoff: %+v err=%v", ack.result, ack.err)
					}
					assertForkEngineCommittedEvidence(t, claimedProof, ack.result, cutID)
				case <-forkCtx.Done():
					t.Fatal(forkCtx.Err())
				}
				afterChild, err := selected.Entities().LoadOperatorEntity(forkCtx, childEntity, claim.RunID())
				if err != nil || afterChild.Fields["processed_token"] != token || forkFixtureEntityRevision(t, afterChild.Entity.Revision) != forkFixtureEntityRevision(t, beforeChild.Entity.Revision)+1 {
					t.Fatalf("child did not write exactly once: before=%+v after=%+v err=%v", beforeChild, afterChild, err)
				}
				afterSource, err := selected.Entities().LoadOperatorEntity(forkCtx, sourceConsumer.EntityID, runID)
				if err != nil || !reflect.DeepEqual(beforeSource.Fields, afterSource.Fields) || forkFixtureEntityRevision(t, beforeSource.Entity.Revision) != forkFixtureEntityRevision(t, afterSource.Entity.Revision) {
					t.Fatalf("child changed source business state: before=%+v after=%+v err=%v", beforeSource, afterSource, err)
				}
				report, err := selected.Runs().LoadRunDebugReport(forkCtx, claim.RunID(), operatorread.RunDebugQueryOptions{})
				if err != nil || report.FanOut.Intents != 1 || report.FanOut.Cardinality != 1 || report.FanOut.Cursor != 1 ||
					report.FanOut.Committed != 1 || report.FanOut.Settled != 1 || report.FanOut.Unsettled != 0 || report.FanOut.Owed != 0 {
					t.Fatalf("live child summary did not settle exactly one continued ordinal: %+v err=%v", report.FanOut, err)
				}
				if len(report.Deliveries) != 1 || report.Deliveries[0].SubscriberID != consumer.Key() ||
					report.Deliveries[0].Status != "delivered" || report.Deliveries[0].Count != 1 {
					t.Fatalf("child reissued a settled recipient or duplicated pending work: %+v", report.Deliveries)
				}
				outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(forkCtx, selected.RuntimeDeps().DeliveryStore, claim.RunID())
				if err != nil || len(outcomes) != 1 || outcomes[0].Ordinal != 0 || outcomes[0].EventID != observed.id || outcomes[0].Kind != string(fanoutobligation.OutcomeContinued) {
					t.Fatalf("child duplicated or replaced the continued outcome: %+v err=%v", outcomes, err)
				}
				// A historical event cut stays pending even after its live continuation settles.
				finalCut, err := family.Plan(forkCtx, runfork.RunForkPlanRequest{SourceRunID: claim.RunID(), At: observed.id})
				if err != nil {
					t.Fatal(err)
				}
				assertMixedFanOutPendingPlan(t, finalCut, observed.id, retained)
				if refork {
					executionRequest.SourceRunID, executionRequest.At = claim.RunID(), observed.id
					executionRequest.ForkOperation = &runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "mixed-fanout-refork", IdempotencyKey: "continued-cut", TransportHash: "mixed-fanout-refork-transport",
						SourceRunID: executionRequest.SourceRunID, ForkEventID: executionRequest.At, TargetBundleHash: executionRequest.ExpectedBundleHash,
						TargetRootReference: rooted.Admission().Reference(),
						AllowSourceFreeze:   executionRequest.AllowSourceFreeze, ContractSelection: executionRequest.ContractSelection,
					}
					proveMixedFanOutChildRefork(t, ctx, selected, executionRequest, afterChild, retained)
				}
				cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(forkCtx), 10*time.Second)
				defer cleanupCancel()
				if err := family.RetireSelectedContexts(cleanupCtx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func waitMixedFanOutSeed(t *testing.T, ctx context.Context, entities operatorread.EntityReader, runID string) {
	t.Helper()
	for {
		page, err := entities.ListOperatorEntities(ctx, operatorread.OperatorEntityListOptions{RunID: runID, Limit: 16})
		if err != nil {
			t.Fatal(err)
		}
		ready := 0
		for _, entity := range page.Entities {
			if (entity.FlowInstance == "consumer" || entity.FlowInstance == "settled") && entity.CurrentState != nil && *entity.CurrentState == "active" {
				ready++
			}
		}
		if ready == 2 && page.NextCursor == "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ordinary seed did not initialize both independent receivers: %+v cause=%v", page.Entities, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertMixedFanOutPendingPlan(t *testing.T, plan runfork.RunForkPlan, eventID string, retained fanoutobligation.InheritedTerminalDisposition) {
	t.Helper()
	if len(plan.FanOutObligations) != 1 {
		t.Fatalf("fixed cut must contain one real source intent: %+v", plan.FanOutObligations)
	}
	obligation := plan.FanOutObligations[0]
	if obligation.Intent.Cursor != 1 || obligation.Intent.Request.Cardinality != 1 || len(obligation.Outcomes) != 0 || len(obligation.PendingReplays) != 1 {
		t.Fatalf("fixed cut lost the exact issued-but-unsettled ordinal: %+v", obligation)
	}
	pending := obligation.PendingReplays[0]
	if pending.SourceEventID != eventID || pending.Ordinal != 0 || len(pending.SourceDeliveryIDs) != 1 || pending.InheritedDisposition != retained {
		t.Fatalf("fixed cut lost unfinished/retained partition: %+v", pending)
	}
}

func mixedFanOutEntity(t *testing.T, plan runfork.RunForkPlan, path string) runfork.RunForkEntityState {
	t.Helper()
	for _, entity := range plan.Entities {
		if entity.MaterializationMetadata != nil && entity.MaterializationMetadata.FlowInstance == path {
			return entity
		}
	}
	t.Fatalf("cut lacks exact constructed receiver %s", path)
	return runfork.RunForkEntityState{}
}
