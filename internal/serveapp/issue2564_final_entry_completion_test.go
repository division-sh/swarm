package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564H2CompletionObserver func(context.Context, lifecycleprobe.Signal)

func (observe issue2564H2CompletionObserver) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	observe(ctx, signal)
}

// Normal boot must finish real attachment readiness. Native timer fixtures
// deliberately retain planned readiness and cannot establish this overlap.
func TestIssue2564H2FinalEntryCompletionOrderingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, path := range []string{"handler", "accepted_timer"} {
			t.Run(backend+"/"+path, func(t *testing.T) {
				issue2564H2FinalEntryCompletion(t, backend, path == "accepted_timer")
			})
		}
	}
}

func issue2564H2FinalEntryCompletion(t *testing.T, backend string, acceptedTimer bool) {
	t.Helper()
	opts, start := issue2564ServeHarness(t, backend, canonicalrouting.CopyIssue2564FinalEntryCompletion(t), false)
	probe := lifecycleprobe.New()
	held := make(chan string, 1)
	release := make(chan struct{})
	returned := make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var acknowledgments, retirements atomic.Int32
	var completionEntity atomic.Value
	opts.TestLifecycleProbe = issue2564H2CompletionObserver(func(ctx context.Context, signal lifecycleprobe.Signal) {
		probe.NotifyLifecycle(ctx, signal)
		if signal.Kind == lifecycleprobe.WorkflowTerminalCommitted {
			retirements.Add(1)
		}
		if signal.Kind == lifecycleprobe.HandlerCompleted {
			t.Logf("H2_FINAL_ENTRY_HANDLER_SIGNAL %+v", signal)
		}
		if acceptedTimer || signal.Kind != lifecycleprobe.HandlerCompleted || signal.Status != "completed" ||
			signal.EventType != "overlap.finish" {
			return
		}
		if acknowledgments.Add(1) != 1 {
			return
		}
		held <- signal.EventID
		select {
		case <-release:
			returned <- nil
		case <-ctx.Done():
			returned <- ctx.Err()
		}
	})
	if acceptedTimer {
		opts.TestEntityStateHook = func(entityID, state string) {
			if state != "done" || completionEntity.Load() != entityID || acknowledgments.Add(1) != 1 {
				return
			}
			held <- entityID
			<-release
			returned <- nil
		}
	}
	process, rt := start()
	// Register after the serve helper so failure cleanup releases the hook before
	// joining the process, and checks its exit code rather than losing shutdown errors.
	t.Cleanup(func() {
		unblock()
		if code := process.stop(); code != 0 {
			t.Errorf("final-entry serve shutdown exit=%d\n%s", code, process.outputString())
		}
	})
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "overlap.start", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{"case_id": "ordering"}, "idempotency_key": "ordering-start",
	})
	entityID := rt.waitEntityStage(t, seed.RunID, "", "waiting")
	completionEntity.Store(entityID)
	rt.waitDeliveries(t, seed.RunID)
	entity, err := rt.selected.LoadOperatorEntity(t.Context(), entityID, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(seed.RunID, flowidentity.RouteForInstancePath(entity.Entity.FlowInstance))
	if err != nil {
		t.Fatal(err)
	}
	before := issue2564H2CompletionWorkflow(t, rt, owner)
	readiness, found, err := rt.selected.LoadDynamicFlowRuntimeReadiness(t.Context(), seed.RunID, owner.Route)
	if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady {
		t.Fatalf("normal boot did not establish exact ready attachment: found=%v readiness=%+v err=%v", found, readiness, err)
	}
	initial, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || initial.Status != "running" || initial.EndedAt != nil || initial.Failure != nil {
		t.Fatalf("final-entry run was not active before its final ACK: header=%+v err=%v", initial, err)
	}
	name := "overlap.finish"
	if acceptedTimer {
		name = "overlap.arm"
	}
	input := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": name, "run_id": seed.RunID,
		"payload": map[string]any{"case_id": "ordering"}, "idempotency_key": "ordering-final",
	})
	var heldIdentity string
	select {
	case heldIdentity = <-held:
	case <-time.After(servedProofPollDeadline):
		t.Fatalf("final-entry ACK hook never entered\n%s\n%s", issue2564H2CompletionReceipt(t, rt, seed.RunID), process.outputString())
	}
	// Close the routing root after business input admission; the child's actual
	// acknowledgment or pending timer receipt still determines completion.
	requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "overlap.root_closed", "run_id": seed.RunID,
		"payload": map[string]any{}, "idempotency_key": "ordering-root-close",
	})
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "done")
	ack := issue2564H2CompletionWorkflow(t, rt, owner)
	transitions := 1
	if acceptedTimer {
		transitions = 2 // The authored arm transition, then one accepted timer entry.
	}
	counter, counterOK := ack.Fields["acknowledgments"].(int64)
	if ack.CurrentState != "done" || ack.Status != "active" || !ack.TerminatedAt.IsZero() ||
		ack.Revision != before.Revision+int64(transitions) || len(ack.TransitionHistory) != 1 ||
		ack.Fields["case_id"] != "ordering" || !counterOK || counter != 1 {
		t.Fatalf("final ACK lost fields/history or acquired retirement authority: before=%+v ack=%+v", before, ack)
	}
	last := ack.TransitionHistory[len(ack.TransitionHistory)-1]
	entry, found, err := workflowlifecycle.LoadStageEntry(ack.Bookkeeping)
	if err != nil || !found || entry.RunID != seed.RunID || entry.InstancePath != owner.Route.InstancePath ||
		entry.EntityID != entityID || entry.Stage != "done" || entry.EventID != last.TriggerEventID || entry.TransitionID != last.TransitionID {
		t.Fatalf("final ACK lost exact stage-entry cause: entry=%+v history=%+v found=%v err=%v", entry, last, found, err)
	}
	compiled, found := last.Evidence.Compiled()
	if err := last.Evidence.Validate(); err != nil || !found || last.TransitionID != last.Evidence.ID() || compiled.FlowID() != "work" ||
		last.To != "done" || last.FiredAt.IsZero() || !ack.EnteredStageAt.Equal(last.FiredAt) {
		t.Fatalf("final ACK lost compiled transition/history identity: history=%+v found=%v err=%v", last, found, err)
	}
	if acceptedTimer {
		if heldIdentity != entityID || entry.Cause != "timer" || last.From != "timed" || compiled.Edge().Source != "timer" {
			t.Fatalf("accepted timer ACK was not the exact final-entry writer: held=%s entry=%+v history=%+v", heldIdentity, entry, last)
		}
		occurrences, err := storetest.ObserveH2Occurrences(t.Context(), rt.selected, seed.RunID)
		if err != nil || len(occurrences) != 1 || occurrences[0].ID != last.TriggerEventID || occurrences[0].Outcome != "" {
			t.Fatalf("held timer lost its exact unsettled control occurrence: occurrences=%+v err=%v", occurrences, err)
		}
		occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(occurrences[0].Task)
		if !valid || occurrence.Activation.ActivationID != entry.OccurrenceID || occurrences[0].Instance != owner.Route.InstancePath {
			t.Fatalf("final timer entry borrowed an occurrence: entry=%+v occurrences=%+v", entry, occurrences)
		}
		pending, err := rt.persistence.deps.PipelineObligations.SummarizeRun(t.Context(), seed.RunID)
		if err != nil || !pending.BlocksCompletion() || pending.Replayable != 1 || pending.TerminalNonSuccess != 0 || pending.Deferred != 0 {
			t.Fatalf("held accepted timer did not retain exactly its real pipeline obligation: summary=%+v err=%v", pending, err)
		}
		receipt := storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, last.TriggerEventID)
		blocked, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
		if err != nil || blocked.Status != "running" || blocked.EndedAt != nil || blocked.Failure != nil || receipt.Count != 0 {
			t.Fatalf("completion crossed unsettled accepted timer: run=%+v receipt=%+v err=%v\n%s", blocked, receipt, err, issue2564H2CompletionReceipt(t, rt, seed.RunID))
		}
		t.Logf("H2_FINAL_ENTRY decision=blocked_at_timer_ACK event=%s revision=%d pipeline=%+v receipt=%+v\n%s", last.TriggerEventID, ack.Revision, pending, receipt, issue2564H2CompletionReceipt(t, rt, seed.RunID))
		unblock()
		issue2564H2CompletionJoinHook(t, returned)
		// Committed timer replay does not emit PostCommitDispatchCompleted. Its
		// pipeline receipt is written only after the held interceptor returns.
		issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, last.TriggerEventID)
	} else {
		if heldIdentity != input.EventID || last.TriggerEventID != input.EventID || entry.Cause != "delivery" || last.From != "waiting" {
			t.Fatalf("handler ACK lost its exact accepted delivery: held=%s input=%+v entry=%+v history=%+v", heldIdentity, input, entry, last)
		}
		issue2564H2CompletionDelivery(t, rt, seed.RunID, input.EventID, false)
	}
	completed := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	if completed.BundleHash != rt.BundleHash || completed.EndedAt == nil || !completed.EndedAt.After(last.FiredAt) || completed.Failure != nil {
		t.Fatalf("completion lost exact final-entry ordering: history=%+v run=%+v", last, completed)
	}
	if !acceptedTimer {
		select {
		case err := <-returned:
			t.Fatalf("handler caller returned before completion was observed under its ACK barrier: %v", err)
		default:
		}
		t.Logf("H2_FINAL_ENTRY decision=completed_while_handler_ACK_held event=%s revision=%d ended_at=%s\n%s", input.EventID, ack.Revision, completed.EndedAt.Format(time.RFC3339Nano), issue2564H2CompletionReceipt(t, rt, seed.RunID))
		unblock()
		issue2564H2CompletionJoinHook(t, returned)
		ctx, cancel := context.WithTimeout(t.Context(), servedProofPollDeadline)
		defer cancel()
		if _, err := probe.WaitForDeliveryStatus(ctx, input.EventID, "node", "", "delivered"); err != nil {
			t.Fatalf("acknowledged handler cleanup did not join: %v\n%s", err, issue2564H2CompletionReceipt(t, rt, seed.RunID))
		}
	}
	rt.waitDeliveries(t, seed.RunID)
	after := issue2564H2CompletionWorkflow(t, rt, owner)
	if !reflect.DeepEqual(ack, after) || acknowledgments.Load() != 1 || retirements.Load() != 0 {
		t.Fatalf("post-completion cleanup rewrote canonical state or attempted implicit retirement: ack=%+v after=%+v ACKs=%d retirements=%d", ack, after, acknowledgments.Load(), retirements.Load())
	}
	for _, eventID := range []string{seed.EventID, input.EventID} {
		issue2564H2CompletionDelivery(t, rt, seed.RunID, eventID, true)
	}
	if receipt := storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, last.TriggerEventID); receipt.Count != 1 || receipt.Outcome != "success" {
		t.Fatalf("final-entry pipeline settlement was not once-only: %+v", receipt)
	}
	if acceptedTimer {
		occurrences, err := storetest.ObserveH2Occurrences(t.Context(), rt.selected, seed.RunID)
		if err != nil || len(occurrences) != 1 || occurrences[0].ID != last.TriggerEventID || occurrences[0].Outcome != "success" {
			t.Fatalf("accepted timer repeated/lost its once-only settlement: occurrences=%+v err=%v", occurrences, err)
		}
		t.Logf("H2_FINAL_ENTRY decision=completed_after_timer_receipt_settled event=%s revision=%d ended_at=%s\n%s", last.TriggerEventID, after.Revision, completed.EndedAt.Format(time.RFC3339Nano), issue2564H2CompletionReceipt(t, rt, seed.RunID))
	}
	logs, err := rt.selected.ListOperatorRuntimeLogs(t.Context(), operatorread.OperatorRuntimeLogListOptions{RunID: seed.RunID, Level: "error", Limit: 100})
	if err != nil || len(logs.Logs) != 0 || logs.NextCursor != "" {
		t.Fatalf("final-entry cleanup produced an error, including refused implicit retirement: logs=%+v err=%v", logs, err)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("final-entry clean process/service join exit=%d\n%s", code, process.outputString())
	}
	t.Logf("H2_FINAL_ENTRY_JOIN backend=%s accepted_timer=%v exit=0 ACKs=%d implicit_retirements=%d", backend, acceptedTimer, acknowledgments.Load(), retirements.Load())
}

func issue2564H2CompletionWorkflow(t *testing.T, rt issue2564ServedFixture, owner flowidentity.RunScopedFlowInstance) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := rt.selected.LoadWorkflowInstance(t.Context(), owner)
	if err != nil || !found {
		t.Fatalf("read exact final-entry workflow: found=%v err=%v", found, err)
	}
	return instance
}

func issue2564H2CompletionJoinHook(t *testing.T, returned <-chan error) {
	t.Helper()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("final-entry ACK hook was canceled instead of released: %v", err)
		}
	case <-time.After(servedProofPollDeadline):
		t.Fatal("released final-entry ACK hook did not join")
	}
}

func issue2564H2CompletionWaitRun(t *testing.T, rt issue2564ServedFixture, runID string) operatorread.RunHeader {
	t.Helper()
	var last operatorread.RunHeader
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		var err error
		last, err = rt.selected.LoadRunHeader(t.Context(), runID)
		if err != nil {
			t.Fatalf("read exact run completion: %v\n%s", err, issue2564H2CompletionReceipt(t, rt, runID))
		}
		if last.Status == "completed" && last.EndedAt != nil && last.Failure == nil {
			return last
		}
		if last.Status != "running" || last.Failure != nil {
			t.Fatalf("completion reached an unexpected state: %+v\n%s", last, issue2564H2CompletionReceipt(t, rt, runID))
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("actual run never completed; blocked/quiescent is not PASS: %+v\n%s", last, issue2564H2CompletionReceipt(t, rt, runID))
	return last
}

func issue2564H2CompletionWaitReceipt(t *testing.T, rt issue2564ServedFixture, runID, eventID string) {
	t.Helper()
	var last storetest.PipelineReceiptEvidence
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		last = storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, eventID)
		if last.Count == 1 && last.Outcome == "success" {
			t.Logf("H2_FINAL_ENTRY_TIMER_CALLER_RECEIPT event=%s receipt=%+v", eventID, last)
			return
		}
		if last.Count != 0 {
			t.Fatalf("accepted timer caller returned a failed or repeated receipt: event=%s receipt=%+v\n%s", eventID, last, issue2564H2CompletionReceipt(t, rt, runID))
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("released timer caller never settled its exact pipeline receipt: event=%s receipt=%+v\n%s", eventID, last, issue2564H2CompletionReceipt(t, rt, runID))
}

func issue2564H2CompletionDelivery(t *testing.T, rt issue2564ServedFixture, runID, eventID string, joined bool) {
	t.Helper()
	evidence := storetest.ObserveDeliveryEventEvidence(t, t.Context(), rt.selected, eventID)
	if evidence.DeadLetters != 0 || len(evidence.Deliveries) != 1 {
		t.Fatalf("final-entry business event lost its one exact delivery: event=%s evidence=%+v", eventID, evidence)
	}
	row := evidence.Deliveries[0]
	if row.RunID != runID || row.EventID != eventID || row.SubscriberType != "node" || row.Status != "delivered" || row.RetryCount != 0 ||
		row.ClaimVersion != 1 || row.HandlerSelections != 1 || len(row.Attempts) != 1 || joined && !row.HandoffPresent {
		t.Fatalf("business delivery was not acknowledged/settled exactly once: event=%s evidence=%+v", eventID, evidence)
	}
	attempt := row.Attempts[0]
	if attempt.ClaimVersion != 1 || attempt.ClosureKind != "settled" || attempt.Outcome != "delivered" {
		t.Fatalf("exact claim was not closed with successful settlement: event=%s attempt=%+v", eventID, attempt)
	}
}

func issue2564H2CompletionReceipt(t *testing.T, rt issue2564ServedFixture, runID string) string {
	t.Helper()
	debug, debugErr := rt.selected.LoadRunDebugReport(t.Context(), runID, operatorread.RunDebugQueryOptions{})
	candidates, candidateErr := rt.persistence.deps.RunLifecycleCandidates.ListCompletionCandidates(t.Context(), runlifecycle.CandidateScope{BundleHash: rt.BundleHash}, runlifecycle.CandidateCursor{}, 100)
	raw, err := json.Marshal(struct {
		Debug          operatorread.RunDebugReport `json:"debug"`
		DebugError     string                      `json:"debug_error"`
		Candidates     runlifecycle.CandidatePage  `json:"candidates"`
		CandidateError string                      `json:"candidate_error"`
	}{debug, fmt.Sprint(debugErr), candidates, fmt.Sprint(candidateErr)})
	if err != nil {
		t.Fatalf("serialize full completion receipt: %v; debug_error=%v candidate_error=%v", err, debugErr, candidateErr)
	}
	if debugErr != nil || candidateErr != nil {
		t.Fatalf("completion observation owner failed: %s", raw)
	}
	return string(raw)
}
