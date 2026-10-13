package serveapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue642TimerContinuationReaders interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	LoadWorkflowTimerActivation(context.Context, string) (pipeline.WorkflowTimerActivation, bool, error)
}

// Supplemental served continuation proof, not registry-v5 qualification.
func TestIssue642OrdinaryTimerForkContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			issue642RetainedOrdinaryTimerFork(t, backend)
		})
	}
}

func TestIssue642ArmedTimerKeepsDueAcrossSelectedDelayChangeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, delay := range []string{"1ms", "1h"} {
			t.Run(backend+"/"+delay, func(t *testing.T) {
				issue642RetainedOrdinaryTimerFork(t, backend, delay)
			})
		}
	}
}

func issue642RetainedOrdinaryTimerFork(t *testing.T, backend string, selectedDelay ...string) {
	t.Helper()
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, req)
		if err == nil {
			caps.RunFork = sourceForkErrorProbe{RunForkExecutor: caps.RunFork, t: t}
		}
		return caps, err
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = previous })
	sourceRoot := issue642TimerContinuationSource(t)
	opts, start := issue2564ServeHarness(t, backend, sourceRoot, false)
	targetRoot, targetHash := sourceRoot, ""
	if len(selectedDelay) != 0 {
		targetRoot = issue642TimerContinuationSource(t)
		path := filepath.Join(targetRoot, "nodes.yaml")
		nodes, err := os.ReadFile(path)
		if err != nil || len(selectedDelay) != 1 || strings.Count(string(nodes), "delay: 1s") != 1 {
			t.Fatalf("changed-delay fixture lost its exact declaration: %v", err)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(nodes), "delay: 1s", "delay: "+selectedDelay[0], 1)), 0o600); err != nil {
			t.Fatal(err)
		}
		// Admit the target through normal boot, retaining its immutable artifact
		// in the same selected store before deploying the original source.
		opts.SourceRoot = targetRoot
		targetProcess, target := start()
		targetHash = target.BundleHash
		if code := targetProcess.stop(); code != 0 {
			t.Fatalf("target artifact boot shutdown=%d\n%s", code, targetProcess.outputString())
		}
		opts.SourceRoot = sourceRoot
	}
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("ordinary timer serve shutdown exit=%d\n%s", code, process.outputString())
		}
	})
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("selected serve store lacks canonical timer/fork readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-timer-arm",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	if marker.SourceEventID != seed.EventID {
		t.Fatalf("post-arm marker lost the real creating-handler cause: marker=%+v ingress=%s", marker, seed.EventID)
	}
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, sourceTimer := issue642TimerContinuationPlan(t, reader, seed.RunID, seed.EventID, marker.EventID)
	selectedTimer := sourceTimer
	if targetHash != "" {
		if targetHash == rt.BundleHash {
			t.Fatal("changed delay did not create a distinct selected source artifact")
		}
		selectedSource := semanticview.Wrap(loadWorkflowValidationBundleAt(t, targetRoot))
		construction, _, found, err := runforkadmission.FixedConstructionForRoute(selectedSource, plan, sourceTimer.Route)
		if err != nil || !found {
			t.Fatalf("changed source lost exact fixed construction: found=%v err=%v", found, err)
		}
		selected, err := pipeline.SelectInheritedWorkflowTimer(selectedSource, sourceTimer, pipeline.WorkflowInstance{
			InstanceID: construction.InstanceID, StorageRef: construction.InstancePath, EntityID: construction.EntityID,
			WorkflowName: construction.TemplateID, ParentFlowID: construction.ParentRoute.FlowID,
			ParentFlowInstance: construction.ParentRoute.FlowInstance, ParentEntityID: construction.ParentEntityID,
		})
		if err != nil || selected == nil || selected.Ref.DeclarationRevision == sourceTimer.Ref.DeclarationRevision {
			t.Fatalf("changed delay did not select the new declaration: timer=%+v err=%v", selected, err)
		}
		selectedTimer = *selected
	}
	settledSourceTimer, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceTimer.Ref.ActivationID)
	if err != nil || !found || settledSourceTimer.Status != "fired" || settledSourceTimer.FiredAt.Before(sourceTimer.FireAt) {
		t.Fatalf("source did not lawfully progress beyond its active fixed-cut timer: timer=%+v found=%v err=%v", settledSourceTimer, found, err)
	}
	expectedSettledSource := sourceTimer
	expectedSettledSource.Status, expectedSettledSource.FiredAt = "fired", settledSourceTimer.FiredAt
	if !reflect.DeepEqual(expectedSettledSource.Canonical(), settledSourceTimer.Canonical()) {
		t.Fatalf("source timer's lawful progress changed its immutable arm: fixed=%+v current=%+v", sourceTimer, settledSourceTimer)
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var fork apiv1.RunForkExecutionResult
	params := map[string]any{
		"source_run_id": seed.RunID, "fork_event_id": marker.EventID,
		"allow_source_freeze": true, "idempotency_key": "issue642-retained-timer-fork",
	}
	if targetHash != "" {
		params["bundle_hash"] = targetHash
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
	if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
		fork.ForkEventID != marker.EventID || fork.ForkRevision != plan.ForkPoint.Revision || fork.SourceFrozen || fork.SourceRunStatus != "completed" {
		t.Fatalf("timer fork did not acknowledge the exact historical arm and independently completed source: %+v", fork)
	}
	rt.waitEntityStage(t, fork.ForkRunID, fork.ForkRunID, "checked")
	completed := issue2564H2CompletionWaitRun(t, rt, fork.ForkRunID)
	rt.waitDeliveries(t, fork.ForkRunID)
	childReadback := rt
	if targetHash != "" {
		childReadback.BundleHash = targetHash
	}
	issue642TimerContinuationReadback(t, childReadback, reader, sourceTimer, plan.ForkPoint, completed, selectedTimer)
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("timer continuation changed source business/publication/delivery facts: %v", err)
	}
	unchanged, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceTimer.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSourceTimer.Canonical(), unchanged.Canonical()) {
		t.Fatalf("child continuation changed the original settled source timer: before=%+v after=%+v found=%v err=%v", settledSourceTimer, unchanged, found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("child continuation changed the original source completion: before=%+v after=%+v err=%v", sourceCompleted, sourceAfterCompletion, err)
	}
}

func issue642TimerContinuationSource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repoRootForTest(), "tests/tier5-flow-lifecycle/test-timer-fire"))); err != nil {
		t.Fatal(err)
	}
	// The marker is emitted by the same real handler commit that arms the timer.
	// Its first publication revision, unlike the creating ingress, contains the arm.
	for name, body := range map[string]string{
		"nodes.yaml": `test-node:
  execution_type: system_node
  subscribes_to: [timer.scheduled, timer.check]
  produces: [timer.armed]
  timers:
    - id: check_timer
      event: timer.check
      delay: 1s
      start_on: event:timer.scheduled
  event_handlers:
    timer.scheduled:
      advances_to: waiting
      emit: {event: timer.armed}
    timer.check:
      advances_to: checked
`,
		"events.yaml": "timer.scheduled:\ntimer.check:\ntimer.armed:\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(schema), "  outputs:\n    - timer.check\n") != 1 {
		t.Fatal("ordinary timer corpus lost its exact output declaration")
	}
	schema = []byte(strings.Replace(string(schema), "  outputs:\n    - timer.check\n", "  outputs:\n    - timer.check\n    - timer.armed\n", 1))
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), schema, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func issue642TimerContinuationMarker(t *testing.T, rt issue2564ServedFixture, runID string) operatorread.OperatorEventFull {
	t.Helper()
	armedNode, err := identity.AdmitExecutableNodeDeclaration(".", "test-node")
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		page, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "timer.armed"}, Limit: 2,
		})
		if err != nil || len(page.Events) > 1 || page.NextCursor != "" {
			t.Fatalf("post-arm marker publication is not exact: page=%+v err=%v", page, err)
		}
		if len(page.Events) == 1 {
			marker := page.Events[0]
			if marker.RunID != runID || marker.Source != armedNode.Key() || marker.ProducerType != events.EventProducerNode {
				t.Fatalf("post-arm selector is not the real source handler output: %+v", marker)
			}
			return marker
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("real timer arm handler did not publish its post-arm marker")
	return operatorread.OperatorEventFull{}
}

func issue642TimerContinuationRoot(t *testing.T, runID string) flowidentity.RunScopedFlowInstance {
	t.Helper()
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.Route{ScopeKey: ".", InstanceID: runID, InstancePath: runID})
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func issue642TimerContinuationPlan(t *testing.T, reader issue642TimerContinuationReaders, runID, ingressID, markerID string) (runfork.RunForkPlan, pipeline.WorkflowTimerActivation) {
	t.Helper()
	plan, err := reader.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: runID, At: markerID})
	if err != nil {
		t.Fatalf("plan exact post-arm publication: %v", err)
	}
	ingress, err := reader.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: runID, At: ingressID})
	if err != nil {
		t.Fatalf("plan creating ingress for the cut-order witness: %v", err)
	}
	if plan.ForkPoint.Kind != runfork.RunForkPointEvent || plan.ForkPoint.EventID != markerID ||
		plan.ForkPoint.Revision <= ingress.ForkPoint.Revision || len(plan.WorkflowTimers) != 1 {
		t.Fatalf("marker did not select a real later timer-bearing cut: ingress=%+v marker=%+v timers=%+v", ingress.ForkPoint, plan.ForkPoint, plan.WorkflowTimers)
	}
	captured, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(plan.WorkflowTimers[0])
	if err != nil || captured.Status != "active" || captured.RunID != runID || captured.EntityID != runID || captured.Recurring ||
		captured.Ref.Cause != timeridentity.WorkflowTimerActivationCauseEvent || captured.EventType != "timer.check" ||
		captured.Route != issue642TimerContinuationRoot(t, runID).Route {
		t.Fatalf("fixed-cut timer lost its exact active root/event arm: captured=%+v err=%v", captured, err)
	}
	if len(plan.Entities) != 1 || plan.Entities[0].EntityID != runID || plan.Entities[0].CurrentState != "waiting" {
		t.Fatalf("post-arm cut did not retain the actual historical waiting receiver: %+v", plan.Entities)
	}
	return plan, captured.Canonical()
}

func issue642TimerContinuationReadback(t *testing.T, rt issue2564ServedFixture, reader issue642TimerContinuationReaders, source pipeline.WorkflowTimerActivation, point runfork.RunForkPoint, completed operatorread.RunHeader, selection ...pipeline.WorkflowTimerActivation) {
	t.Helper()
	selected := source
	if len(selection) != 0 {
		if len(selection) != 1 {
			t.Fatal("timer readback requires exactly one selected effect")
		}
		selected = selection[0]
	}
	projected, err := runfork.ProjectWorkflowTimerRecord(selected.PersistenceRecord(), selected.Ref, completed.RunID, point, nil, runlifecycle.CanonicalTimestamp(completed.StartedAt))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projected)
	if err != nil {
		t.Fatal(err)
	}
	actual, found, err := reader.LoadWorkflowTimerActivation(t.Context(), projected.ActivationID)
	if err != nil || !found || actual.Status != "fired" || actual.FiredAt.IsZero() || actual.FiredAt.Before(source.FireAt) {
		t.Fatalf("child did not consume its exact retained due coordinate: timer=%+v found=%v err=%v", actual, found, err)
	}
	expected.Status, expected.FiredAt = "fired", actual.FiredAt
	if !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) || !actual.FireAt.Equal(source.FireAt) || !actual.SourceArmedAt.Equal(source.CreatedAt) {
		t.Fatalf("child timer changed due/arm/payload/lineage instead of continuing it: expected=%+v actual=%+v", expected, actual)
	}
	page, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: completed.RunID, EventName: "timer.check"}, Limit: 2,
	})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("child timer publication cardinality is not one: page=%+v err=%v", page, err)
	}
	event := storetest.LoadCanonicalEventRecord(t, t.Context(), rt.selected, page.Events[0].EventID)
	occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.TaskID())
	if !valid || occurrence != actual.Occurrence() || event.ID() != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) ||
		event.RunID() != completed.RunID || event.SourceAgent() != "runtime.workflow_timer" || event.ProducerType() != events.EventProducerPlatform ||
		!bytes.Equal(event.Payload(), source.Payload) || event.RoutingSource() != actual.RoutingSource {
		t.Fatalf("child timer event lost its exact native occurrence envelope: event=%+v timer=%+v", event, actual)
	}
	issue2564H2CompletionWaitReceipt(t, rt, completed.RunID, event.ID())
	if receipt := storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, event.ID()); receipt.Count != 1 || receipt.Outcome != "success" {
		t.Fatalf("child timer receipt was not settled exactly once: %+v", receipt)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, completed.RunID, completed.RunID); observed.Timers != 1 || observed.ActiveTimers != 0 {
		t.Fatalf("child rearmed or retained an extra workflow timer: %+v", observed)
	}
	if completed.BundleHash != rt.BundleHash || completed.Status != "completed" || completed.EndedAt == nil || completed.Failure != nil {
		t.Fatalf("child receiver quiescence is not actual run completion: %+v", completed)
	}
	t.Logf("ISSUE642_ORDINARY_TIMER_CONTINUED backend=%s source=%s child=%s cut=%d source_timer=%s child_timer=%s due=%s occurrence=%s completed=%s",
		rt.Backend, source.RunID, completed.RunID, point.Revision, source.Ref.ActivationID, actual.Ref.ActivationID,
		source.FireAt.Format(time.RFC3339Nano), event.ID(), completed.EndedAt.Format(time.RFC3339Nano))
}
