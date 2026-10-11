package serveapp

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type publicationCommittedHandoffObservation struct {
	runtimebus.DeliveryContinuationOwner
	result chan error
}

func (o *publicationCommittedHandoffObservation) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	err := o.DeliveryContinuationOwner.AcceptCommitted(proofs)
	if len(proofs) != 0 {
		o.result <- err
	}
	return err
}

func TestPublicCompletionManagedCommitCutBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, boundary := range []string{"bundle", "supervisor"} {
			for _, method := range []string{"event.publish", "run.start/event-backed", "run.start/feed-only"} {
				t.Run(string(backend)+"/"+boundary+"/"+method, func(t *testing.T) {
					provePublicCompletionManagedCommitCut(t, backend, boundary, method)
				})
			}
		}
	}
}

func provePublicCompletionManagedCommitCut(t *testing.T, backend servedparity.Backend, boundary, method string) {
	t.Helper()
	probe := installPublicationCompletionAudit(t, false)
	entered := make(chan struct{}, 1)
	probe.afterCommit = func(_ context.Context, acknowledged bool) {
		if acknowledged {
			entered <- struct{}{}
			<-probe.release
		}
	}
	defer probe.releaseOnce.Do(func() { close(probe.release) })
	h := newChannelOnboardingE2EHarness(t, backend, false)
	h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
	managers := make(chan *runtimepkg.RuntimeContextManager, 2)
	runtimes := make(chan *runtimepkg.Runtime, 2)
	h.opts.TestRuntimeContextsReadyHook = func(m *runtimepkg.RuntimeContextManager) { managers <- m }
	h.opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { runtimes <- rt }
	h.start(t)
	t.Cleanup(func() { h.stop(t) })
	manager, rt := <-managers, <-runtimes
	observer := &publicationCommittedHandoffObservation{DeliveryContinuationOwner: rt.Bus.DeliveryContinuationOwner(), result: make(chan error, 4)}
	if err := rt.Bus.SetDeliveryContinuationOwner(observer); err != nil {
		t.Fatal(err)
	}
	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
	hash := identity.SourceArtifacts[0].BundleHash
	params := map[string]any{
		"bundle_hash": hash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
		"data": map[string]any{"imports": []any{map[string]any{
			"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
			"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
		}}, "pins": []any{}},
	}
	eventBacked := method != "run.start/feed-only"
	verb := "run.start"
	if method == "event.publish" {
		verb = method
	}
	if eventBacked {
		params["event_name"], params["payload"] = "scan.requested", map[string]any{"topic": "managed-commit-cut"}
	}
	response := make(chan publicationAuditHTTPResult, 1)
	go func() { response <- publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), verb, params) }()
	select {
	case <-entered:
	case result := <-response:
		t.Fatalf("native COMMIT cut was not reached: %+v", result)
	case <-time.After(10 * time.Second):
		t.Fatal("native COMMIT cut was not reached")
	}
	probe.mu.Lock()
	native, request, process, runID := probe.native, probe.request, probe.process, probe.run
	var activation pipeline.CommittedFlowInstanceActivation
	completion := probe.committed.Completion.Response
	if eventBacked {
		activation = probe.committed.Publication.Activations[0]
	} else {
		activation = probe.deployment.Activations[0]
		completion = probe.deployment.Completion.Response
	}
	probe.mu.Unlock()
	stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
	if err != nil || !found || process.PublicCompletionCount() != 1 || !activation.Acknowledged || !bytes.Equal(completion, stored.Response) {
		t.Fatalf("held native ACK lacks durable outcome/ownership: found=%t err=%v", found, err)
	}
	plan := activation.Plan
	flow := flowidentity.RunScopedFlowInstance{RunID: plan.Readiness.RunID, Route: plan.Identity.Route()}
	construction, err := storetest.ReadReceiverConstructionPublication(t.Context(), native, flow, plan.Identity.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	shutdown := make(chan error, 1)
	go func() {
		if boundary == "bundle" {
			result := manager.DeactivateBundleHash(hash, runtimepkg.RuntimeContextCauseUnloaded)
			if !result.Found || !result.Changed {
				shutdown <- fmt.Errorf("bundle was not withdrawn: %+v", result)
				return
			}
			shutdown <- result.ShutdownErr
			return
		}
		supervisor := newProcessLifecycleSupervisor(nil, rt)
		supervisor.SetRuntimeContextManager(manager, rt.Options.SourceArtifactFact)
		shutdown <- supervisor.ShutdownProcessWithOptions(t.Context(), runtimepkg.DefaultShutdownOptions())
	}()
	deadline := time.Now().Add(5 * time.Second)
	for manager.LookupBundleHashStatus(hash).Loaded() {
		if time.Now().After(deadline) {
			t.Fatal("managed retirement did not withdraw the admitted runtime")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-shutdown:
		t.Fatalf("managed retirement abandoned the in-flight native commit: %v", err)
	default:
	}
	// An empty batch probes only the existing admission boundary. It cannot
	// fabricate a delivery or stand in for the actual post-release handoff below.
	if err := observer.DeliveryContinuationOwner.AcceptCommitted(nil); err != nil {
		t.Errorf("coordinator retired before admitted native COMMIT returned: %v", err)
	}
	probe.releaseOnce.Do(func() { close(probe.release) })
	first := <-response
	if first.err != nil || first.rpcError != nil || !bytes.Equal(first.result, stored.Response) {
		t.Fatalf("retirement changed the winning native ACK: %+v stored=%s", first, stored.Response)
	}
	if err := <-shutdown; err != nil {
		t.Errorf("managed shutdown failed: %v", err)
	}
	handoffObserved, handoffAccepted := false, false
	select {
	case err := <-observer.result:
		handoffObserved, handoffAccepted = true, err == nil
		t.Logf("native committed handoff observed=%t accepted=%t error=%v", handoffObserved, handoffAccepted, err)
	default:
		t.Log("native committed handoff UNOBSERVED; require durable terminal disposition instead")
	}
	if _, err := rt.WorkOccurrence().RetireAndWait(t.Context()); err != nil || process.PublicCompletionCount() != 0 {
		t.Fatalf("managed shutdown retained exact ownership: count=%d err=%v", process.PublicCompletionCount(), err)
	}
	h.stop(t)
	probe.afterCommit = nil
	h.start(t)
	restarted := <-runtimes
	probe.mu.Lock()
	replayOwner := probe.native
	probe.mu.Unlock()
	beforeReplay, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), replayOwner)
	if err != nil {
		t.Fatal(err)
	}
	replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), verb, params)
	if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, stored.Response) {
		t.Fatalf("default restart changed recorded native ACK: %+v", replay)
	}
	probe.mu.Lock()
	native, calls, obligations, workflow, eventID := probe.native, probe.commits+probe.deployments, probe.obligations, probe.workflow, probe.eventID
	disposals, disposeErr := probe.claimDisposeCount, probe.claimDisposeError
	probe.mu.Unlock()
	if calls != 1 {
		t.Fatalf("managed restart repeated native COMMIT: %d", calls)
	}
	afterReplay, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), native)
	if err != nil || beforeReplay != afterReplay {
		t.Fatalf("recorded ACK replay repeated a consequence: before=%+v after=%+v err=%v", beforeReplay, afterReplay, err)
	}
	afterConstruction, err := storetest.ReadReceiverConstructionPublication(t.Context(), native, flow, plan.Identity.EntityID)
	if err != nil || !reflect.DeepEqual(construction, afterConstruction) {
		t.Fatalf("managed restart changed immutable construction: before=%+v after=%+v err=%v", construction, afterConstruction, err)
	}
	inspection, err := restarted.WorkOccurrence().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restartedProcess, ok := worklifetime.ProcessFromContext(inspection.Context())
	if err := inspection.Done(); err != nil || !ok || restartedProcess.PublicCompletionCount() != 0 {
		t.Fatalf("restarted process retained public capacity: found=%t err=%v", ok, err)
	}
	var result struct {
		Run operatorread.RunHeader `json:"run"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.get", map[string]any{"run_id": runID}, &result)
	abandoned := result.Run.Status == "cancelled" && result.Run.ControlReason == runquiescence.ServeAbandonReasonCode
	completed := result.Run.Status == "completed" && eventBacked && handoffAccepted
	if result.Run.RunID != runID || (!abandoned && !completed) || result.Run.EndedAt == nil {
		t.Fatalf("publisher lacks durable managed disposition: %+v", result.Run)
	}
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), runID, plan.Identity.Route())
	if err != nil || !found || !readiness.Terminal() || readiness.Eligible() || readiness.Pending() {
		t.Fatalf("terminal managed run retains actionable readiness: found=%t readiness=%+v err=%v", found, readiness, err)
	}
	open, err := obligations.SummarizeRun(t.Context(), runID)
	if err != nil || open.HasOpenWork() {
		t.Fatalf("managed abandonment retains pipeline work: %+v err=%v", open, err)
	}
	deliveries, err := storetest.ReadServedRunDeliverySummary(t.Context(), native, runID)
	if err != nil || deliveries.Pending != 0 || deliveries.InProgress != 0 || deliveries.RetryScheduled != 0 || len(deliveries.ActiveDeliveryIDs) != 0 {
		t.Fatalf("managed abandonment retains live deliveries: %+v err=%v", deliveries, err)
	}
	if !eventBacked && handoffObserved {
		t.Fatal("feed-only start fabricated a delivery handoff")
	}
	if eventBacked {
		if disposals != 1 || disposeErr != nil {
			t.Fatalf("managed commit lost exact native claim disposition: count=%d err=%v", disposals, disposeErr)
		}
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, runID, eventID)
		outcome := "dead_letter"
		if completed {
			outcome = "success"
		}
		if !evidence.RecordFound || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != outcome || evidence.PipelineReceiptFailure != nil || (abandoned && evidence.PipelineReceiptReason != runquiescence.ServeAbandonReasonCode) {
			t.Fatalf("managed restart lost or duplicated native event settlement: %+v", evidence)
		}
		rows, err := storetest.ReadEventDeliveryDiagnosticRows(t.Context(), native, eventID)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if abandoned && (row.Status != "dead_letter" || row.Reason != runquiescence.ServeAbandonReasonCode) {
				t.Fatalf("managed abandoned delivery remains unsettled: %+v", row)
			}
		}
	}
	h.stop(t)
	if _, err := restarted.WorkOccurrence().RetireAndWait(t.Context()); err != nil {
		t.Fatalf("restarted serving owner did not join: %v", err)
	}
}
