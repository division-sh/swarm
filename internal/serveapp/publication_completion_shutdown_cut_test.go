package serveapp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPublicCompletionAcknowledgedShutdownBeforeFinalizationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			t.Run(string(backend)+"/"+method, func(t *testing.T) {
				provePublicCompletionAcknowledgedShutdownCut(t, backend, method, false, false)
			})
		}
	}
}

func TestPublicCompletionAcknowledgedDefaultAbandonmentBothStores(t *testing.T) {
	runPublicCompletionShutdownVariants(t, true, false)
}

func TestPublicCompletionAcknowledgedFenceRecoveryBothStores(t *testing.T) {
	runPublicCompletionShutdownVariants(t, false, true)
}

func runPublicCompletionShutdownVariants(t *testing.T, abandon, fence bool) {
	t.Helper()
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			name := method
			if method == "run.start" {
				name += "/event-backed"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				provePublicCompletionAcknowledgedShutdownCut(t, backend, method, abandon, fence)
			})
		}
	}
}

func provePublicCompletionAcknowledgedShutdownCut(t *testing.T, backend servedparity.Backend, method string, abandon, fence bool) {
	t.Helper()
	probe := installPublicationCompletionAudit(t, true)
	defer probe.releaseOnce.Do(func() { close(probe.release) })
	h := newChannelOnboardingE2EHarness(t, backend, false)
	if !abandon {
		h.opts.AbandonActiveRuns = false
	} else if !h.opts.AbandonActiveRuns {
		t.Fatal("default onboarding must exercise serve abandonment")
	}
	h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
	runtimes := make(chan *runtimepkg.Runtime, 2)
	h.opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { runtimes <- rt }
	h.start(t)
	t.Cleanup(func() { h.stop(t) })
	rt := <-runtimes
	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
	runID := requireServedLiveLandingRunID(t, h.rpcEndpoint(), identity.SourceArtifacts[0].BundleHash, ".")
	params := map[string]any{
		"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": runID, "idempotency_key": uuid.NewString(),
		"event_name": "scan.requested", "payload": map[string]any{"topic": "shutdown-cut"},
	}
	first := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params)
	if first.err != nil || first.rpcError != nil {
		t.Fatalf("initial native acknowledgment: %+v", first)
	}
	select {
	case <-probe.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("accepted publication did not reach its exact claim settlement cut")
	}
	probe.mu.Lock()
	native, request, process := probe.native, probe.request, probe.process
	acknowledged := probe.committed.Acknowledged
	probe.mu.Unlock()
	if !acknowledged || process.PublicCompletionCount() != 1 {
		t.Fatal("held finalization lacks its native ACK and exact reservation")
	}
	plan := requirePublicationCompletionReadiness(t, probe).Plan
	flow := flowidentity.RunScopedFlowInstance{RunID: plan.RunID, Route: plan.Identity.Route()}
	before, err := storetest.ReadReceiverConstructionPublication(t.Context(), native, flow, plan.Identity.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
	if err != nil || !found || !bytes.Equal(first.result, stored.Response) {
		t.Fatalf("native completion at held ACK: found=%t err=%v first=%s stored=%s", found, err, first.result, stored.Response)
	}
	if fence {
		// Use the production withdrawal boundary while its coordinator is still
		// live. The refusal must preserve durable work, not masquerade as success.
		rt.CloseAdmission()
		probe.releaseOnce.Do(func() { close(probe.release) })
		select {
		case <-probe.claimDisposed:
		case <-time.After(10 * time.Second):
			t.Fatal("fenced publication did not settle or release its exact native claim")
		}
		probe.mu.Lock()
		count, disposeErr := probe.claimDisposeCount, probe.claimDisposeError
		probe.mu.Unlock()
		if count != 1 || disposeErr != nil {
			t.Fatalf("fenced publication claim disposition: count=%d err=%v", count, disposeErr)
		}
	}
	// Serving retirement must join the exact acknowledged publication claim.
	h.stop(t)
	if _, err := rt.WorkOccurrence().RetireAndWait(t.Context()); err != nil || process.PublicCompletionCount() != 0 {
		t.Fatalf("serving retirement left accepted ownership: count=%d err=%v", process.PublicCompletionCount(), err)
	}
	probe.mu.Lock()
	probe.hold = false
	probe.mu.Unlock()
	h.start(t)
	restarted := <-runtimes
	probe.mu.Lock()
	replayOwner := probe.native
	probe.mu.Unlock()
	beforeReplay, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), replayOwner)
	if err != nil {
		t.Fatal(err)
	}
	replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params)
	if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, stored.Response) {
		t.Fatalf("recovery changed the exact ACK: %+v stored=%s", replay, stored.Response)
	}
	probe.mu.Lock()
	native, workflow, obligations := probe.native, probe.workflow, probe.obligations
	calls := probe.commits + probe.deployments
	probe.mu.Unlock()
	if calls != 1 {
		t.Fatalf("restart/replay repeated native creation/publication: %d", calls)
	}
	afterReplay, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), native)
	if err != nil || beforeReplay != afterReplay {
		t.Fatalf("recorded replay repeated a consequence: before=%+v after=%+v err=%v", beforeReplay, afterReplay, err)
	}
	inspection, err := restarted.WorkOccurrence().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restartedProcess, ok := worklifetime.ProcessFromContext(inspection.Context())
	if err := inspection.Done(); err != nil || !ok || restartedProcess.PublicCompletionCount() != 0 {
		t.Fatalf("restarted process retained public capacity: found=%t err=%v", ok, err)
	}
	after, err := storetest.ReadReceiverConstructionPublication(t.Context(), native, flow, plan.Identity.EntityID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("restart/replay changed immutable construction: before=%+v after=%+v err=%v", before, after, err)
	}
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), plan.RunID, plan.Identity.Route())
	if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady {
		t.Fatalf("authorized startup did not recover exact attachment: found=%t readiness=%+v err=%v", found, readiness, err)
	}
	var result struct {
		Run operatorread.RunHeader `json:"run"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.get", map[string]any{"run_id": plan.RunID}, &result)
	if result.Run.RunID != plan.RunID || result.Run.Status != "live" || result.Run.ControlReason != "" || result.Run.EndedAt != nil {
		t.Fatalf("restart ended the retained live flow: %+v", result.Run)
	}
	pending, err := obligations.SummarizeRun(t.Context(), plan.RunID)
	if err != nil || pending.HasOpenWork() {
		t.Fatalf("recovery left publication unsettled: summary=%+v err=%v", pending, err)
	}
	deliveries, err := storetest.ReadServedRunDeliverySummary(t.Context(), native, plan.RunID)
	if err != nil || deliveries.Pending != 0 || deliveries.InProgress != 0 || deliveries.RetryScheduled != 0 || len(deliveries.ActiveDeliveryIDs) != 0 {
		t.Fatalf("recovery retains live deliveries: summary=%+v err=%v", deliveries, err)
	}
	{
		probe.mu.Lock()
		eventID := probe.eventID
		probe.mu.Unlock()
		if method == "event.publish" {
			var response servedEventPublishRPCResult
			if err := json.Unmarshal(stored.Response, &response); err != nil || response.EventID != eventID {
				t.Fatalf("native ACK lost exact event identity: %+v err=%v", response, err)
			}
		}
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, plan.RunID, eventID)
		if !evidence.RecordFound || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != "success" || evidence.PipelineReceiptFailure != nil {
			t.Fatalf("recovery lost or duplicated exact event settlement: %+v", evidence)
		}
	}
	h.stop(t)
	if _, err := restarted.WorkOccurrence().RetireAndWait(t.Context()); err != nil {
		t.Fatalf("restarted serving owner did not join: %v", err)
	}
}
