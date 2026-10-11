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
	"github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPublicCompletionAcknowledgedShutdownBeforeFinalizationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, method := range []string{"event.publish", "run.start"} {
			t.Run(string(backend)+"/"+method, func(t *testing.T) {
				provePublicCompletionAcknowledgedShutdownCut(t, backend, method, false, false, false)
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
		for _, variant := range []struct {
			name, method string
			eventBacked  bool
		}{
			{"event.publish", "event.publish", true},
			{"run.start/event-backed", "run.start", true},
			{"run.start/feed-only", "run.start", false},
		} {
			t.Run(string(backend)+"/"+variant.name, func(t *testing.T) {
				provePublicCompletionAcknowledgedShutdownCut(t, backend, variant.method, variant.eventBacked, abandon, fence)
			})
		}
	}
}

func provePublicCompletionAcknowledgedShutdownCut(t *testing.T, backend servedparity.Backend, method string, eventBacked, abandon, fence bool) {
	t.Helper()
	probe := installPublicationCompletionAudit(t, false)
	probe.holdBeforeReadiness = true
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
	params := map[string]any{
		"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
		"data": map[string]any{"imports": []any{map[string]any{
			"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
			"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
		}}, "pins": []any{}},
	}
	if method == "event.publish" || eventBacked {
		params["event_name"], params["payload"] = "scan.requested", map[string]any{"topic": "shutdown-cut"}
	}
	first := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params)
	if first.err != nil || first.rpcError != nil {
		t.Fatalf("initial native acknowledgment: %+v", first)
	}
	select {
	case <-probe.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("accepted publication did not reach pre-Manager readiness cut")
	}
	probe.mu.Lock()
	native, request, process := probe.native, probe.request, probe.process
	var activation pipeline.CommittedFlowInstanceActivation
	if method == "event.publish" || eventBacked {
		activation = probe.committed.Publication.Activations[0]
	} else {
		activation = probe.deployment.Activations[0]
	}
	probe.mu.Unlock()
	if !activation.Acknowledged || process.PublicCompletionCount() != 1 {
		t.Fatal("held finalization lacks its native ACK and exact reservation")
	}
	plan := activation.Plan
	flow := flowidentity.RunScopedFlowInstance{RunID: plan.Readiness.RunID, Route: plan.Identity.Route()}
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
		if method == "event.publish" || eventBacked {
			select {
			case <-probe.claimReleased:
			case <-time.After(10 * time.Second):
				t.Fatal("fenced publication did not release its exact native claim")
			}
			probe.mu.Lock()
			count, releaseErr := probe.claimReleaseCount, probe.claimReleaseError
			probe.mu.Unlock()
			if count != 1 || releaseErr != nil {
				t.Fatalf("fenced publication claim release: count=%d err=%v", count, releaseErr)
			}
		}
	}
	// Real serving shutdown fences and retires its dependency set. This cut is
	// before readiness acquires a Manager lease, not inside an admitted attempt.
	h.stop(t)
	if _, err := rt.WorkOccurrence().RetireAndWait(t.Context()); err != nil || process.PublicCompletionCount() != 0 {
		t.Fatalf("serving retirement left accepted ownership: count=%d err=%v", process.PublicCompletionCount(), err)
	}
	probe.mu.Lock()
	probe.holdBeforeReadiness = false
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
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), plan.Readiness.RunID, plan.Identity.Route())
	if abandon {
		if err != nil || !found || !readiness.Terminal() || readiness.Eligible() || readiness.Pending() {
			t.Fatalf("abandoned attachment remains actionable: found=%t readiness=%+v err=%v", found, readiness, err)
		}
		var result struct {
			Run operatorread.RunHeader `json:"run"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.get", map[string]any{"run_id": plan.Readiness.RunID}, &result)
		if result.Run.RunID != plan.Readiness.RunID || result.Run.Status != "cancelled" || result.Run.ControlReason != runquiescence.ServeAbandonReasonCode || result.Run.EndedAt == nil {
			t.Fatalf("publisher lacks exact durable abandonment: %+v", result.Run)
		}
		pending, err := obligations.SummarizeRun(t.Context(), plan.Readiness.RunID)
		if err != nil || pending.HasOpenWork() {
			t.Fatalf("abandoned run retains pipeline work: summary=%+v err=%v", pending, err)
		}
		deliveries, err := storetest.ReadServedRunDeliverySummary(t.Context(), native, plan.Readiness.RunID)
		if err != nil || deliveries.Pending != 0 || deliveries.InProgress != 0 || deliveries.RetryScheduled != 0 || len(deliveries.ActiveDeliveryIDs) != 0 {
			t.Fatalf("abandoned run retains live deliveries: summary=%+v err=%v", deliveries, err)
		}
		if process.PublicCompletionCount() != 0 {
			t.Fatal("default restart/replay retained a public reservation")
		}
	} else if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady {
		t.Fatalf("authorized startup did not recover exact attachment: found=%t readiness=%+v err=%v", found, readiness, err)
	}
	if method == "event.publish" || eventBacked {
		probe.mu.Lock()
		eventID := probe.eventID
		probe.mu.Unlock()
		if method == "event.publish" {
			var response servedEventPublishRPCResult
			if err := json.Unmarshal(stored.Response, &response); err != nil || response.EventID != eventID {
				t.Fatalf("native ACK lost exact event identity: %+v err=%v", response, err)
			}
		}
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, plan.Readiness.RunID, eventID)
		outcome := "success"
		if abandon {
			outcome = "dead_letter"
		}
		if !evidence.RecordFound || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != outcome || evidence.PipelineReceiptFailure != nil {
			t.Fatalf("recovery lost or duplicated exact event settlement: %+v", evidence)
		}
		if abandon {
			if evidence.PipelineReceiptReason != runquiescence.ServeAbandonReasonCode {
				t.Fatalf("abandonment lost its exact pipeline reason: %+v", evidence)
			}
			rows, err := storetest.ReadEventDeliveryDiagnosticRows(t.Context(), native, eventID)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if row.Status != "dead_letter" || row.Reason != runquiescence.ServeAbandonReasonCode {
					t.Fatalf("abandoned exact delivery remains unsettled: %+v", row)
				}
			}
		}
	}
	h.stop(t)
	if _, err := restarted.WorkOccurrence().RetireAndWait(t.Context()); err != nil {
		t.Fatalf("restarted serving owner did not join: %v", err)
	}
}
