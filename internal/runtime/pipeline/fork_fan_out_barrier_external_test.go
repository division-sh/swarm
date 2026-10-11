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
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type forkBarrierCompletionProbe struct {
	*lifecycleprobe.Probe
	completed chan string
	once      sync.Once
	claimed   chan forkEngineObservation
	claimOnce sync.Once
	mu        sync.Mutex
	history   []lifecycleprobe.Signal
}

func (p *forkBarrierCompletionProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.Probe.NotifyLifecycle(ctx, signal)
	p.mu.Lock()
	p.history = append(p.history, signal)
	p.mu.Unlock()
	if signal.Kind == lifecycleprobe.DeliveryStatusChanged && signal.EventType == "producer/work.ready" && signal.Status == "in_progress" {
		p.claimOnce.Do(func() { p.claimed <- forkEngineObservation{ctx: ctx, id: signal.EventID} })
	}
	if signal.Kind == lifecycleprobe.DeliveryStatusChanged && signal.EventType == "producer/batch.completed" && signal.Status == "delivered" {
		p.once.Do(func() { p.completed <- signal.EventID })
	}
}

func TestIssue642FanOutBarrierFrontierBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, retained := range []fanoutobligation.InheritedTerminalDisposition{fanoutobligation.InheritedSucceeded, fanoutobligation.InheritedDeadLettered} {
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
				finalConsumer, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
				if err != nil {
					t.Fatal(err)
				}
				producer, err := identity.AdmitExecutableNodeDeclaration("producer", "producer")
				if err != nil {
					t.Fatal(err)
				}
				sourceProbe := &mixedFanOutSourceProbe{runID: runID, receiver: consumer.Key(), history: lifecycleprobe.New(),
					claimed: make(chan forkEngineObservation, 1), cut: make(chan string, 1), resume: make(chan struct{})}
				root := canonicalrouting.CopyForkFanOutBarrierReceiver(t)
				rt, selected, repo, capability, _, _ := startForkEngineRuntimeOwner(t, backend, root, sourceProbe)
				var sourceRelease sync.Once
				t.Cleanup(func() { sourceRelease.Do(func() { close(sourceProbe.resume) }) })
				ctx := correlation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
				ctx = correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
				ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
				ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("barrier journey requires the canonical fork owner")
				}
				seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "barrier-proof", "", []byte(`{"token":"seed"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.PublishAndWait(ctx, seed); err != nil {
					t.Fatal(err)
				}
				baseline := rt.Options.ProcessWorkOwner.ActiveCount()
				phase, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				payload, token, settledStatus := []byte(`{"token":"7","tokens":["7"]}`), "7", "delivered"
				if retained == fanoutobligation.InheritedDeadLettered {
					payload, token, settledStatus = []byte(`{"token":"not-an-integer","tokens":["not-an-integer"]}`), "not-an-integer", "dead_letter"
				}
				trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "barrier-proof", "", payload, 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.Publish(ctx, trigger); err != nil {
					t.Fatal(err)
				}
				var sourceClaim forkEngineObservation
				select {
				case sourceClaim = <-sourceProbe.claimed:
				case <-phase.Done():
					t.Fatal(phase.Err())
				}
				if _, err := sourceProbe.history.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged,
					EventID: sourceClaim.id, SubscriberType: "node", SubscriberID: settled.Key(), Status: settledStatus}); err != nil {
					t.Fatal(err)
				}
				checkpoint := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.checkpointed", "barrier-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.PublishAndWait(ctx, checkpoint); err != nil {
					t.Fatal(err)
				}
				var cutID string
				select {
				case cutID = <-sourceProbe.cut:
				case <-phase.Done():
					t.Fatal(phase.Err())
				}
				plan, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: runID, At: cutID})
				if err != nil {
					t.Fatal(err)
				}
				assertMixedFanOutPendingPlan(t, plan, sourceClaim.id, retained)
				if plan.FanOutObligations[0].Barrier == nil || plan.FanOutObligations[0].Barrier.Status != fanoutbarrier.StatusArmed {
					t.Fatalf("cut lacks its real open barrier: %+v", plan.FanOutObligations[0].Barrier)
				}
				sourceRoot := mixedFanOutEntity(t, plan, runID)
				beforeSource, err := selected.Entities().LoadOperatorEntity(phase, sourceRoot.EntityID, runID)
				if err != nil {
					t.Fatal(err)
				}
				sourceConsumer := mixedFanOutEntity(t, plan, "consumer")
				beforeConsumer, err := selected.Entities().LoadOperatorEntity(phase, sourceConsumer.EntityID, runID)
				if err != nil {
					t.Fatal(err)
				}
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, filepath.Join(repo, "platform-spec.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				probe := &forkBarrierCompletionProbe{Probe: lifecycleprobe.New(), completed: make(chan string, 1), claimed: make(chan forkEngineObservation, 1)}
				result, err := family.Execute(phase, runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: runID, At: cutID, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
					SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: selected.SourceArtifactStore()},
					ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
					AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture,
						AgentManagerOptions: manager.AgentManagerOptions{TestLifecycleProbe: probe}},
				})
				if err != nil || !result.Activation.Activated || result.ForkLocalRuntimeContainer == nil {
					t.Fatalf("barrier child did not acknowledge activation: %+v err=%v", result, err)
				}
				proof := result.ForkLocalRuntimeContainer
				defer func() {
					cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					defer stop()
					if err := family.RetireSelectedContexts(cleanup); err != nil {
						t.Error(err)
					}
				}()
				var observed forkEngineObservation
				select {
				case observed = <-probe.claimed:
				case <-phase.Done():
					t.Fatal(phase.Err())
				}
				claim, claimed := deliverylifecycle.ClaimFromContext(observed.ctx)
				admission, admitted := managedexecution.FromContext(observed.ctx)
				if !claimed || !admitted || !admission.AuthorizesSelected(proof.RuntimeExecutionID, proof.ForkRunID, proof.RuntimeGeneration) ||
					claim.RunID() != proof.ForkRunID || observed.id == sourceClaim.id {
					t.Fatal("barrier continuation reused source execution authority")
				}
				var completedID string
				select {
				case completedID = <-probe.completed:
				case <-phase.Done():
					probe.mu.Lock()
					t.Logf("child lifecycle evidence: %+v", probe.history)
					probe.mu.Unlock()
					diagnostic, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
					defer stop()
					report, readErr := selected.Runs().LoadRunDebugReport(diagnostic, proof.ForkRunID, operatorread.RunDebugQueryOptions{})
					t.Logf("child durable evidence: run=%s fanout=%+v deliveries=%+v quiescence=%+v read_error=%v",
						report.RunTableStatus, report.FanOut, report.Deliveries, report.TestQuiescence, readErr)
					t.Fatalf("selected barrier did not publish its outcome: %v", phase.Err())
				}
				final, err := probe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged,
					EventID: completedID, SubscriberType: "node", SubscriberID: finalConsumer.Key(), Status: "delivered"})
				if err != nil {
					t.Fatalf("selected barrier did not reach its real final consumer: %v", err)
				}
				child, err := selected.Entities().LoadOperatorEntity(phase, proof.ForkRunID, proof.ForkRunID)
				if err != nil || child.Fields["completed_total"] != float64(1) || child.Entity.Revision != 2 {
					t.Fatalf("final consumer did not retain exactly one ordinal: %+v err=%v", child, err)
				}
				childPlan, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: proof.ForkRunID, At: final.EventID})
				if err != nil || len(childPlan.FanOutObligations) != 1 {
					t.Fatalf("final-consumer cut lost the barrier: %+v err=%v", childPlan, err)
				}
				barrier := childPlan.FanOutObligations[0].Barrier
				wantSummary := fanoutbarrier.Summary{Total: 1, Succeeded: 1}
				if retained == fanoutobligation.InheritedDeadLettered {
					wantSummary.Succeeded, wantSummary.DeadLettered = 0, 1
				}
				if barrier == nil || barrier.Status != fanoutbarrier.StatusFired || barrier.Summary == nil || *barrier.Summary != wantSummary {
					t.Fatalf("child barrier did not retain its exact all-settled result: %+v", barrier)
				}
				request := forkEngineExecutionProofRequest(t, result, sourceClaim.id, observed.id, claim)
				native := readForkEngineExecutionProof(t, phase, selected, request)
				assertForkEngineCommittedEvidence(t, native, result, cutID)
				if !native.DeliveryHandoff || native.PipelineOutcome != "success" {
					t.Fatalf("barrier progressed without exact settled receiver evidence: %+v", native)
				}
				childConsumer, err := selected.Entities().LoadOperatorEntity(phase, sourceConsumer.EntityID, proof.ForkRunID)
				if err != nil || childConsumer.Fields["processed_token"] != token || childConsumer.Entity.Revision != 2 {
					t.Fatalf("continued receiver did not mutate exactly once: %+v err=%v", childConsumer, err)
				}
				report, err := selected.Runs().LoadRunDebugReport(phase, proof.ForkRunID, operatorread.RunDebugQueryOptions{})
				if err != nil || report.FanOut.Settled != 1 || report.FanOut.Unsettled != 0 || report.FanOut.Owed != 0 ||
					report.FanOut.BarrierTerminal != 1 || len(report.Deliveries) != 3 {
					t.Fatalf("barrier duplicated work or failed to settle: %+v err=%v", report, err)
				}
				wantRecipients := map[string]bool{consumer.Key(): true, finalConsumer.Key(): true, producer.Key(): true}
				for _, delivery := range report.Deliveries {
					if !wantRecipients[delivery.SubscriberID] || delivery.Status != "delivered" || delivery.Count != 1 {
						t.Fatalf("barrier reissued a settled recipient or duplicated a consumer: %+v", report.Deliveries)
					}
					delete(wantRecipients, delivery.SubscriberID)
				}
				if len(wantRecipients) != 0 {
					t.Fatalf("barrier omitted consumers: %+v", wantRecipients)
				}
				afterSource, err := selected.Entities().LoadOperatorEntity(phase, sourceRoot.EntityID, runID)
				if err != nil || !reflect.DeepEqual(beforeSource.Fields, afterSource.Fields) || beforeSource.Entity.Revision != afterSource.Entity.Revision {
					t.Fatalf("child barrier modified source: before=%+v after=%+v err=%v", beforeSource, afterSource, err)
				}
				afterConsumer, err := selected.Entities().LoadOperatorEntity(phase, sourceConsumer.EntityID, runID)
				if err != nil || !reflect.DeepEqual(beforeConsumer.Fields, afterConsumer.Fields) || beforeConsumer.Entity.Revision != afterConsumer.Entity.Revision {
					t.Fatalf("child receiver modified source: before=%+v after=%+v err=%v", beforeConsumer, afterConsumer, err)
				}
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer stop()
				if err := family.RetireSelectedContexts(cleanup); err != nil {
					t.Fatal(err)
				}
				if active := rt.Options.ProcessWorkOwner.ActiveCount(); active != baseline {
					t.Fatalf("barrier retirement leaked work: got=%d baseline=%d", active, baseline)
				}
			})
		}
	}
}
