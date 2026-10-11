package serveapp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
				provePublicCompletionAcknowledgedShutdownCut(t, backend, method)
			})
		}
	}
}

func provePublicCompletionAcknowledgedShutdownCut(t *testing.T, backend servedparity.Backend, method string) {
	t.Helper()
	probe := installPublicationCompletionAudit(t, false)
	probe.holdBeforeReadiness = true
	defer probe.releaseOnce.Do(func() { close(probe.release) })
	h := newChannelOnboardingE2EHarness(t, backend, false)
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
	if method == "event.publish" {
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
	if method == "event.publish" {
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
	<-runtimes
	replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), method, params)
	if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, stored.Response) {
		t.Fatalf("recovery changed the exact ACK: %+v stored=%s", replay, stored.Response)
	}
	probe.mu.Lock()
	native, workflow := probe.native, probe.workflow
	calls := probe.commits + probe.deployments
	probe.mu.Unlock()
	if calls != 1 {
		t.Fatalf("restart/replay repeated native creation/publication: %d", calls)
	}
	after, err := storetest.ReadReceiverConstructionPublication(t.Context(), native, flow, plan.Identity.EntityID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("restart/replay changed immutable construction: before=%+v after=%+v err=%v", before, after, err)
	}
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), plan.Readiness.RunID, plan.Identity.Route())
	if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady {
		t.Fatalf("authorized startup did not recover exact attachment: found=%t readiness=%+v err=%v", found, readiness, err)
	}
	if method == "event.publish" {
		var response servedEventPublishRPCResult
		if err := json.Unmarshal(stored.Response, &response); err != nil {
			t.Fatal(err)
		}
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, response.RunID, response.EventID)
		if !evidence.RecordFound || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != "success" || evidence.PipelineReceiptFailure != nil {
			t.Fatalf("recovery lost or duplicated exact event settlement: %+v", evidence)
		}
	}
	h.stop(t)
	if probe.process.PublicCompletionCount() != 0 {
		t.Fatal("restarted serving owner leaked completion capacity")
	}
}
