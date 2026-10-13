package pipeline_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
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
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
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

type publishedBarrierClaimProbe struct {
	*lifecycleprobe.Probe
	owner          string
	claim          chan forkEngineObservation
	resume         chan struct{}
	once           sync.Once
	completed      chan string
	completionOnce sync.Once
}

func (p *publishedBarrierClaimProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.Probe.NotifyLifecycle(ctx, signal)
	if p.completed != nil && signal.Kind == lifecycleprobe.DeliveryStatusChanged && signal.EventType == "producer/batch.completed" && signal.Status == "delivered" {
		p.completionOnce.Do(func() { p.completed <- signal.EventID })
	}
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.EventType != "platform.join_complete" || signal.SubscriberID != p.owner || signal.Status != "in_progress" {
		return
	}
	if _, valid := deliverylifecycle.ClaimFromContext(ctx); !valid {
		return
	}
	p.once.Do(func() {
		p.claim <- forkEngineObservation{ctx: ctx, id: signal.EventID}
		if p.resume != nil {
			select {
			case <-p.resume:
			case <-ctx.Done():
			}
		}
	})
}

func TestIssue642PublishedFanOutBarrierContinuesFinalConsumerBothStores(t *testing.T) {
	verifyPublishedFanOutBarrierContinuation(t, publishedBarrierCompletionSuccess)
}

func TestIssue642PublishedBarrierCompletionEvaluationFailureBothStores(t *testing.T) {
	verifyPublishedFanOutBarrierContinuation(t, publishedBarrierCompletionEvaluationFailure)
}

func TestIssue642PublishedBarrierCompletionSQLFailureRollsBackBothStores(t *testing.T) {
	verifyPublishedFanOutBarrierContinuation(t, publishedBarrierCompletionSQLFailure)
}

type publishedBarrierCompletionProof uint8

const (
	publishedBarrierCompletionSuccess publishedBarrierCompletionProof = iota
	publishedBarrierCompletionEvaluationFailure
	publishedBarrierCompletionSQLFailure
)

func verifyPublishedFanOutBarrierContinuation(t *testing.T, proof publishedBarrierCompletionProof) {
	failingCompletion := proof != publishedBarrierCompletionSuccess
	for _, backend := range []string{"sqlite", "postgres"} {
		if crashBackend := os.Getenv("SWARM_PUBLISHED_BARRIER_CRASH_BACKEND"); crashBackend != "" && backend != crashBackend {
			continue
		}
		t.Run(backend, func(t *testing.T) {
			producer, err := identity.AdmitExecutableNodeDeclaration("producer", "producer")
			if err != nil {
				t.Fatal(err)
			}
			finalConsumer, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
			if err != nil {
				t.Fatal(err)
			}
			sourceProbe := &publishedBarrierClaimProbe{Probe: lifecycleprobe.New(), owner: producer.Key(), claim: make(chan forkEngineObservation, 1), resume: make(chan struct{}), completed: make(chan string, 1)}
			root := canonicalrouting.CopyForkFanOutBarrierReceiver(t)
			rt, selected, repo, capability, _, _ := startForkEngineRuntimeOwnerAt(t, backend, root, sourceProbe, os.Getenv("SWARM_PUBLISHED_BARRIER_CRASH_ADDRESS"))
			var sourceRelease sync.Once
			t.Cleanup(func() { sourceRelease.Do(func() { close(sourceProbe.resume) }) })
			ctx := correlation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
			ctx = correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
			phase, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			runID := uuid.NewString()
			seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "published-barrier-proof", "", []byte(`{"token":"seed"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := rt.Bus.PublishAndWait(phase, seed); err != nil {
				t.Fatal(err)
			}
			trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "published-barrier-proof", "", []byte(`{"token":"7","tokens":["7"]}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := rt.Bus.Publish(phase, trigger); err != nil {
				t.Fatal(err)
			}
			var original forkEngineObservation
			select {
			case original = <-sourceProbe.claim:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			family, found := selected.RunFork()
			if !found {
				t.Fatal("missing canonical fork owner")
			}
			plan, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: runID, At: original.id})
			if err != nil {
				t.Fatalf("real committed join-event cut: %v", err)
			}
			if len(plan.FanOutObligations) != 1 || plan.FanOutObligations[0].Barrier == nil || plan.FanOutObligations[0].Barrier.Status != fanoutbarrier.StatusClosedPending {
				t.Fatal("published cut lost exact unfinished barrier")
			}
			// Preserve the earlier published cut, but join actual source pipeline
			// completion before the supported historical transfer. This is not a
			// request to resume an active post-cut source claim or provider turn.
			sourceRelease.Do(func() { close(sourceProbe.resume) })
			var sourceCompletedID string
			select {
			case sourceCompletedID = <-sourceProbe.completed:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			if _, err := sourceProbe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: sourceCompletedID, SubscriberID: finalConsumer.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				t.Fatal(err)
			}
			waitPublishedBarrierPipelineReceipts(t, phase, selected.RuntimeDeps().DeliveryStore, original.id, sourceCompletedID)
			before, err := selected.Entities().LoadOperatorEntity(phase, runID, runID)
			if err != nil {
				t.Fatal(err)
			}
			targetRoot := root
			if proof == publishedBarrierCompletionEvaluationFailure {
				targetRoot = canonicalrouting.CopyForkFanOutBarrierCompletionFailure(t)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, targetRoot, filepath.Join(repo, "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if proof == publishedBarrierCompletionEvaluationFailure {
				joins := semanticview.Wrap(bundle).WorkflowJoins()
				if len(joins) != 1 || len(joins[0].Spec.OnComplete.DataAccumulation.Writes) != 1 || !joins[0].Spec.OnComplete.DataAccumulation.Writes[0].Value.HasCELValue() {
					t.Fatal("completion failure must compile a real evaluation, not a literal")
				}
				storetest.RequireBundleDataCatalog(t, phase, selected.SourceArtifactWriter(), bundle)
			}
			childProbe := &publishedBarrierClaimProbe{Probe: lifecycleprobe.New(), owner: producer.Key(), claim: make(chan forkEngineObservation, 1), completed: make(chan string, 1)}
			crashing := os.Getenv("SWARM_PUBLISHED_BARRIER_CRASH_BACKEND") != ""
			if crashing || failingCompletion {
				childProbe.resume = make(chan struct{})
				t.Cleanup(func() {
					select {
					case <-childProbe.resume:
					default:
						close(childProbe.resume)
					}
				})
			}
			request := runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: runID, At: original.id, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
				SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: selected.SourceArtifactStore()},
				ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
				AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture,
					AgentManagerOptions: manager.AgentManagerOptions{TestLifecycleProbe: childProbe}},
			}
			if proof == publishedBarrierCompletionEvaluationFailure {
				request.ExpectedBundleHash = bundle.SourceArtifact.BundleHash()
				request.ContractSelection = runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: request.ExpectedBundleHash}
				if request.ExpectedBundleHash == rt.Options.SourceArtifactFact.BundleHash() {
					t.Fatal("completion failure must select a genuinely different admitted artifact")
				}
			}
			if crashing {
				request.ForkOperation = &runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "published-barrier-crash", IdempotencyKey: "pending-cut", TransportHash: "published-barrier-crash-transport",
					SourceRunID: runID, ForkEventID: original.id, TargetBundleHash: rt.Options.SourceArtifactFact.BundleHash(),
					AllowSourceFreeze: true, ContractSelection: request.ContractSelection,
				}
			}
			baselineWork := rt.Options.ProcessWorkOwner.ActiveCount()
			var sourceDomain map[string]storetest.SelectedForkStorageTableSnapshot
			if failingCompletion {
				sourceDomain, err = storetest.ReadSelectedForkSourceDomain(phase, selected.RuntimeDeps().DeliveryStore, runID)
				if err != nil {
					t.Fatal(err)
				}
			}
			result, err := family.Execute(phase, request)
			defer func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer stop()
				if err := family.RetireSelectedContexts(cleanup); err != nil {
					t.Error(err)
				}
			}()
			if err != nil || !result.Activation.Activated || result.ForkLocalRuntimeContainer == nil {
				t.Fatalf("published barrier selected activation: %+v err=%v", result, err)
			}
			var childClaim forkEngineObservation
			select {
			case childClaim = <-childProbe.claim:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			claim, valid := deliverylifecycle.ClaimFromContext(childClaim.ctx)
			if !valid || claim.RunID() != result.Materialization.ForkRunID || childClaim.id == original.id {
				t.Fatal("child reused original outcome claim")
			}
			if crashing {
				publishBarrierCrashCheckpoint(t, phase, selected, result, original.id, childClaim, before.Entity.Revision)
			}
			if failingCompletion {
				verifyPublishedBarrierCompletionFailure(t, phase, selected, family, result, plan, original, childClaim, childProbe, runID, before, proof)
				if active := rt.Options.ProcessWorkOwner.ActiveCount(); active != baselineWork {
					t.Fatalf("failed barrier completion retained owned process work: active=%d baseline=%d", active, baselineWork)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(phase, selected.RuntimeDeps().DeliveryStore, runID)
				if err != nil || !reflect.DeepEqual(sourceDomain, after) {
					t.Fatalf("failed barrier transfer changed source business evidence: %v", err)
				}
				return
			}
			if _, err := childProbe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: childClaim.id, SubscriberID: producer.Key(), SubscriberType: "node", Status: "delivered"}); err != nil {
				diagnostic, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				defer stop()
				request := forkEngineExecutionProofRequest(t, result, original.id, childClaim.id, claim)
				physical, readErr := storetest.ReadSelectedForkReceiverExecutionProof(diagnostic, selected.RuntimeDeps().DeliveryStore, request)
				t.Logf("exact child barrier receiver: %+v read_error=%v", physical, readErr)
				report, readErr := selected.Runs().LoadRunDebugReport(diagnostic, claim.RunID(), operatorread.RunDebugQueryOptions{})
				t.Logf("child durable state: run=%s fanout=%+v deliveries=%+v read_error=%v", report.RunTableStatus, report.FanOut, report.Deliveries, readErr)
				t.Fatal(err)
			}
			var completedID string
			select {
			case completedID = <-childProbe.completed:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			final, err := childProbe.Wait(phase, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: completedID, SubscriberID: finalConsumer.Key(), SubscriberType: "node", Status: "delivered"})
			if err != nil {
				t.Fatal(err)
			}
			childRun := result.Materialization.ForkRunID
			poll := time.NewTicker(10 * time.Millisecond)
			defer poll.Stop()
			for {
				child, err := selected.Entities().LoadOperatorEntity(phase, childRun, childRun)
				if err != nil {
					t.Fatal(err)
				}
				if child.Fields["completed_total"] == float64(1) {
					if child.Entity.Revision != 2 {
						t.Fatalf("final consumer did not commit once: %+v", child)
					}
					break
				}
				select {
				case <-poll.C:
				case <-phase.Done():
					t.Fatal(phase.Err())
				}
			}
			finished, err := family.Plan(phase, runfork.RunForkPlanRequest{SourceRunID: childRun, At: final.EventID})
			if err != nil || len(finished.FanOutObligations) != 1 {
				t.Fatalf("child barrier history: %+v err=%v", finished, err)
			}
			barrier := finished.FanOutObligations[0].Barrier
			if barrier == nil || barrier.Status != fanoutbarrier.StatusFired || barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 1, Succeeded: 1}) || barrier.TransferredOccurrence == nil || barrier.ScheduleActivationID != "" || barrier.TransferredOccurrence.Publication.EventID != childClaim.id || barrier.TransferredOccurrence.Publication.SourceEventID != original.id {
				t.Fatalf("barrier history changed summary/lineage or invented a timer: %+v", barrier)
			}
			after, err := selected.Entities().LoadOperatorEntity(phase, runID, runID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("barrier transfer changed source root: %v", err)
			}
		})
	}
}

func waitPublishedBarrierPipelineReceipts(t *testing.T, ctx context.Context, selected any, ids ...string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for _, id := range ids {
		for {
			receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, selected, id)
			if err == nil {
				if receipt.Outcome != "success" {
					t.Fatalf("source pipeline %s settled unsuccessfully: %+v", id, receipt)
				}
				break
			}
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatal(err)
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
}
