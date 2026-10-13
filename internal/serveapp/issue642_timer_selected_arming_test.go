package serveapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue642ArmingForkDiagnostic struct {
	apiv1.RunForkExecutor
	t *testing.T
}

func (p issue642ArmingForkDiagnostic) ExecuteRunFork(ctx context.Context, req apiv1.RunForkExecutionRequest) (apiv1.RunForkExecutionResult, error) {
	started := time.Now()
	sampled := make(chan struct{})
	probe := time.AfterFunc(4*time.Second, func() {
		defer close(sampled)
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		p.t.Logf("ISSUE642_NEW_ARM_SLOW_FORK_STACK\n%s", stacks.String())
	})
	result, err := p.RunForkExecutor.ExecuteRunFork(ctx, req)
	if !probe.Stop() {
		<-sampled
	}
	p.t.Logf("ISSUE642_NEW_ARM_FORK_DURATION duration=%s err=%v", time.Since(started), err)
	return result, err
}

// Supplemental served new-arm proof, not registry-v5 qualification.
func TestIssue642NewlyArmedTimerUsesSelectedDelayAndEffectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, delay := range []string{"25ms", "1500ms"} {
			t.Run(backend+"/"+delay, func(t *testing.T) {
				issue642NewlyArmedSelectedTimer(t, backend, delay)
			})
		}
	}
}

func issue642NewArmTarget(t *testing.T, delay string) string {
	t.Helper()
	root := issue642TimerContinuationSource(t)
	for name, count := range map[string]int{"nodes.yaml": 3, "events.yaml": 1, "schema.yaml": 2} {
		path := filepath.Join(root, name)
		body, err := os.ReadFile(path)
		if err != nil || strings.Count(string(body), "timer.check") != count {
			t.Fatalf("new-arm fixture lost its exact timer effect in %s: %v", name, err)
		}
		selected := strings.ReplaceAll(string(body), "timer.check", "timer.selected_check")
		if name == "nodes.yaml" {
			if strings.Count(selected, "delay: 1s") != 1 {
				t.Fatal("new-arm fixture lost its exact original delay")
			}
			selected = strings.Replace(selected, "delay: 1s", "delay: "+delay, 1)
		}
		if err := os.WriteFile(path, []byte(selected), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func issue642NewlyArmedSelectedTimer(t *testing.T, backend, delay string) {
	t.Helper()
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, req)
		if err == nil {
			caps.RunFork = issue642ArmingForkDiagnostic{RunForkExecutor: caps.RunFork, t: t}
		}
		return caps, err
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = previous })
	selectedDelay, err := time.ParseDuration(delay)
	if err != nil || selectedDelay <= 0 || selectedDelay == time.Second {
		t.Fatalf("new-arm proof requires a changed positive selected delay: %v", err)
	}
	sourceRoot, targetRoot := issue642TimerContinuationSource(t), issue642NewArmTarget(t, delay)
	opts, start := issue2564ServeHarness(t, backend, sourceRoot, false)
	opts.SourceRoot = targetRoot
	targetProcess, target := start()
	if code := targetProcess.stop(); code != 0 {
		t.Fatalf("new-arm target artifact shutdown=%d\n%s", code, targetProcess.outputString())
	}
	opts.SourceRoot = sourceRoot
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("new-arm serve shutdown=%d\n%s", code, process.outputString())
		}
	})
	if target.BundleHash == rt.BundleHash {
		t.Fatal("changed delay/effect did not create a distinct selected artifact")
	}
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("served selected store lacks canonical new-arm readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-new-arm-source",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	armedPlan, sourceArm := issue642TimerContinuationPlan(t, reader, seed.RunID, seed.EventID, marker.EventID)
	if sourceArm.FireAt.Sub(sourceArm.CreatedAt) != time.Second {
		t.Fatalf("original source no longer provides the distinct one-second arm: %+v", sourceArm)
	}
	plan, err := reader.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: seed.EventID})
	if err != nil || plan.ForkPoint.Kind != runfork.RunForkPointEvent || plan.ForkPoint.EventID != seed.EventID ||
		plan.ForkPoint.Revision >= armedPlan.ForkPoint.Revision || len(plan.WorkflowTimers) != 0 {
		t.Fatalf("new-arm source cut is not the exact timer-free creating publication: point=%+v timers=%+v err=%v", plan.ForkPoint, plan.WorkflowTimers, err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: semanticview.Wrap(loadWorkflowValidationBundleAt(t, targetRoot)),
	})
	if err != nil || frontier.Owner != runfork.RunForkContractFrontierAdmissionOwner || !frontier.NonMutating || len(frontier.FrontierEvents) != 1 ||
		frontier.FrontierEvents[0].SourceEventID != seed.EventID || frontier.FrontierEvents[0].EventName != "timer.scheduled" {
		t.Fatalf("new-arm cut lost its real owed creating input: frontier=%+v err=%v", frontier, err)
	}
	settledSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || settledSource.Status != "fired" || settledSource.FiredAt.Before(sourceArm.FireAt) {
		t.Fatalf("source did not independently consume its original timer: timer=%+v found=%v err=%v", settledSource, found, err)
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var fork apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
		"source_run_id": seed.RunID, "fork_event_id": seed.EventID, "bundle_hash": target.BundleHash,
		"allow_source_freeze": true, "idempotency_key": "issue642-new-selected-arm-fork",
	}, &fork)
	if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
		fork.ForkEventID != seed.EventID || fork.ForkRevision != plan.ForkPoint.Revision || fork.SourceFrozen || fork.SourceRunStatus != "completed" {
		t.Fatalf("new-arm fork lost its timer-free cut or independent completed source: %+v", fork)
	}
	child := rt
	child.BundleHash = target.BundleHash
	childMarker := issue642TimerContinuationMarker(t, child, fork.ForkRunID)
	replayed := issue642NewArmEvent(t, child, fork.ForkRunID, "timer.scheduled")
	lineage, valid := replayed.SelectedForkLineage()
	if !valid || replayed.ID() == seed.EventID || lineage.SourceRunID() != seed.RunID ||
		lineage.SourceEventID() != seed.EventID || lineage.DestinationRunID() != fork.ForkRunID || childMarker.SourceEventID != replayed.ID() {
		t.Fatalf("new arm did not come from the real selected owed-input turn: event=%+v marker=%+v", replayed, childMarker)
	}
	child.waitEntityStage(t, fork.ForkRunID, fork.ForkRunID, "checked")
	completed := issue2564H2CompletionWaitRun(t, child, fork.ForkRunID)
	child.waitDeliveries(t, fork.ForkRunID)
	issue2564H2CompletionWaitReceipt(t, child, fork.ForkRunID, replayed.ID())
	actual := issue642RequireNewSelectedArm(t, child, reader, sourceArm, replayed, completed, selectedDelay)
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("new selected arm changed source business/publication/delivery facts: %v", err)
	}
	unchanged, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSource.Canonical(), unchanged.Canonical()) {
		t.Fatalf("new selected arm changed the original source timer: found=%v err=%v", found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("new selected arm changed original source completion: %v", err)
	}
	t.Logf("ISSUE642_NEW_SELECTED_TIMER_ARM backend=%s selected_delay=%s source=%s child=%s cut=%d timer=%s arm=%s due=%s effect=%s completed=%s",
		backend, delay, seed.RunID, fork.ForkRunID, plan.ForkPoint.Revision, actual.Ref.ActivationID,
		actual.CreatedAt.Format(time.RFC3339Nano), actual.FireAt.Format(time.RFC3339Nano), actual.EventType, completed.EndedAt.Format(time.RFC3339Nano))
}

func issue642NewArmEvent(t *testing.T, rt issue2564ServedFixture, runID, name string) events.Event {
	t.Helper()
	page, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: name}, Limit: 2,
	})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("new-arm event %s is not exactly one publication: page=%+v err=%v", name, page, err)
	}
	return storetest.LoadCanonicalEventRecord(t, t.Context(), rt.selected, page.Events[0].EventID)
}

func issue642RequireNewSelectedArm(t *testing.T, rt issue2564ServedFixture, reader issue642TimerContinuationReaders, source pipeline.WorkflowTimerActivation, creating events.Event, completed operatorread.RunHeader, delay time.Duration) pipeline.WorkflowTimerActivation {
	t.Helper()
	fired := issue642NewArmEvent(t, rt, completed.RunID, "timer.selected_check")
	occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(fired.TaskID())
	if !valid {
		t.Fatal("selected publication lacks its exact timer occurrence")
	}
	timer, found, err := reader.LoadWorkflowTimerActivation(t.Context(), occurrence.Activation.ActivationID)
	if err != nil || !found {
		t.Fatalf("completed new-arm child lacks its published timer: timer=%+v found=%v err=%v", timer, found, err)
	}
	actual := timer.Canonical()
	if err := actual.Validate(); err != nil {
		t.Fatal(err)
	}
	if actual.RunID != completed.RunID || actual.EntityID != completed.RunID || actual.Route != issue642TimerContinuationRoot(t, completed.RunID).Route ||
		actual.Ref.Cause != timeridentity.WorkflowTimerActivationCauseEvent || actual.Ref.DeclarationKey != source.Ref.DeclarationKey ||
		actual.Ref.DeclarationRevision == source.Ref.DeclarationRevision || actual.Ref.Generation != source.Ref.Generation ||
		actual.OwnerAgent != source.OwnerAgent || actual.EventType != "timer.selected_check" || actual.Recurring || actual.RecurrenceInterval != 0 {
		t.Fatalf("new arm lost its exact selected declaration/effect/root owner: source=%+v child=%+v", source, actual)
	}
	if actual.SourceTimerID != "" || actual.ForkedFromRunID != "" || actual.ForkedFromPointKind != "" || actual.ForkedFromPointRevision != 0 ||
		actual.ForkedFromEventID != "" || !actual.SourceArmedAt.IsZero() || actual.ReconstructionOwner != "" {
		t.Fatalf("new arm borrowed inherited fixed-cut timer lineage: %+v", actual)
	}
	if !actual.CreatedAt.Equal(runlifecycle.CanonicalTimestamp(creating.CreatedAt())) || actual.CreatedAt.Before(completed.StartedAt) ||
		actual.FireAt.Sub(actual.CreatedAt) != delay || actual.FireAt.Equal(source.FireAt) || actual.Status != "fired" || actual.FiredAt.Before(actual.FireAt) ||
		actual.CancelCause != "" || !actual.CancelledAt.IsZero() {
		t.Fatalf("new arm reused source due or ignored selected delay instead of its fresh child cause: delay=%s creating=%+v timer=%+v", delay, creating, actual)
	}
	expectedID := timeridentity.WorkflowTimerActivationID(completed.RunID, completed.RunID, completed.RunID,
		actual.Ref.DeclarationKey, actual.Ref.DeclarationRevision, string(actual.Ref.Cause), actual.Ref.Generation.KeySuffix(),
		creating.ID(), string(creating.Type()), "", "", "")
	if actual.Ref.ActivationID != expectedID {
		t.Fatalf("new arm identity does not bind its fresh selected creating input: expected=%s actual=%+v", expectedID, actual)
	}
	if !valid || occurrence != actual.Occurrence() || fired.ID() != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) ||
		fired.RunID() != completed.RunID || fired.SourceAgent() != "runtime.workflow_timer" || fired.ProducerType() != events.EventProducerPlatform ||
		!bytes.Equal(fired.Payload(), actual.Payload) || fired.RoutingSource() != actual.RoutingSource || fired.ExecutionMode() != actual.ExecutionMode ||
		!runlifecycle.CanonicalTimestamp(fired.CreatedAt()).Equal(actual.FiredAt) {
		t.Fatalf("selected new arm did not publish its exact canonical occurrence: event=%+v timer=%+v", fired, actual)
	}
	issue2564H2CompletionWaitReceipt(t, rt, completed.RunID, fired.ID())
	if receipt := storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, fired.ID()); receipt.Count != 1 || receipt.Outcome != "success" {
		t.Fatalf("new-arm occurrence did not settle exactly once: %+v", receipt)
	}
	oldEffect, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: completed.RunID, EventName: "timer.check"}, Limit: 2,
	})
	if err != nil || len(oldEffect.Events) != 0 || oldEffect.NextCursor != "" {
		t.Fatalf("new selected arm also executed the obsolete source effect: page=%+v err=%v", oldEffect, err)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, completed.RunID, completed.RunID); observed.Timers != 1 || observed.ActiveTimers != 0 {
		t.Fatalf("new selected arm left extra or unconsumed timer work: %+v", observed)
	}
	if completed.BundleHash != rt.BundleHash || completed.Status != "completed" || completed.EndedAt == nil || completed.Failure != nil {
		t.Fatalf("new-arm checked receiver is not actual selected run completion: %+v", completed)
	}
	return actual
}
