package serveapp

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Overdue recurrence may publish several distinct due coordinates before the
// accepted effect cancels its arm. This is not an exactly-one-tick contract.
func TestIssue642RecurringTimerForkPreservesDueArmAndCadenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { issue642RetainedRecurringTimerFork(t, backend) })
	}
}

func issue642RecurringTimerSource(t *testing.T) string {
	t.Helper()
	root := issue642TimerContinuationSource(t)
	path := filepath.Join(root, "nodes.yaml")
	nodes, err := os.ReadFile(path)
	const start = "      start_on: event:timer.scheduled\n"
	if err != nil || strings.Count(string(nodes), start) != 1 {
		t.Fatalf("recurring fixture lost its exact original event arm: %v", err)
	}
	selected := strings.Replace(string(nodes), start, start+"      cancel_on: event:timer.check\n      recurring: true\n", 1)
	if err := os.WriteFile(path, []byte(selected), 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func issue642RetainedRecurringTimerFork(t *testing.T, backend string) {
	t.Helper()
	_, start := issue2564ServeHarness(t, backend, issue642RecurringTimerSource(t), false)
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("recurring timer serve shutdown=%d\n%s", code, process.outputString())
		}
	})
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("served selected store lacks canonical recurring timer/fork readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-recurring-source-arm",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	if marker.SourceEventID != seed.EventID {
		t.Fatal("recurring cut lost its real creating-handler cause")
	}
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, sourceArm := issue642RecurringTimerPlan(t, reader, seed.RunID, marker.EventID)
	settledSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || settledSource.Status != "cancelled" || settledSource.CancelCause != "" || !settledSource.CancelledAt.IsZero() {
		t.Fatalf("explicit source cancel_on did not retire its ordinary recurring arm: timer=%+v found=%v err=%v", settledSource, found, err)
	}
	sourceOccurrences := issue642RecurringTimerOccurrences(t, rt, settledSource, sourceArm.FireAt)
	issue642RequireRecurringProgress(t, sourceArm, settledSource, len(sourceOccurrences))
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var fork apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
		"source_run_id": seed.RunID, "fork_event_id": marker.EventID,
		"allow_source_freeze": true, "idempotency_key": "issue642-retained-recurring-timer-fork",
	}, &fork)
	if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
		fork.ForkEventID != marker.EventID || fork.ForkRevision != plan.ForkPoint.Revision || fork.SourceFrozen || fork.SourceRunStatus != "completed" {
		t.Fatalf("recurring fork lost its exact retained active cut and independently completed source: %+v", fork)
	}
	rt.waitEntityStage(t, fork.ForkRunID, fork.ForkRunID, "checked")
	completed := issue2564H2CompletionWaitRun(t, rt, fork.ForkRunID)
	rt.waitDeliveries(t, fork.ForkRunID)
	if !sourceArm.FireAt.Before(completed.StartedAt) {
		t.Fatal("recurring proof requires the retained first due before child birth")
	}
	projected, err := runfork.ProjectWorkflowTimerRecord(sourceArm.PersistenceRecord(), sourceArm.Ref, fork.ForkRunID,
		plan.ForkPoint, nil, runlifecycle.CanonicalTimestamp(completed.StartedAt))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projected)
	if err != nil {
		t.Fatal(err)
	}
	actual, found, err := reader.LoadWorkflowTimerActivation(t.Context(), expected.Ref.ActivationID)
	if err != nil || !found || actual.Status != "cancelled" || actual.CancelCause != "" || !actual.CancelledAt.IsZero() {
		t.Fatalf("recurring child did not settle through its explicit ordinary cancel_on: timer=%+v found=%v err=%v", actual, found, err)
	}
	childOccurrences := issue642RecurringTimerOccurrences(t, rt, actual, sourceArm.FireAt)
	issue642RequireRecurringProgress(t, expected, actual, len(childOccurrences))
	if !actual.SourceArmedAt.Equal(sourceArm.CreatedAt) || actual.RecurrenceInterval != sourceArm.RecurrenceInterval {
		t.Fatalf("recurring child reset its original arm or cadence: source=%+v child=%+v", sourceArm, actual)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, fork.ForkRunID, fork.ForkRunID); observed.Timers != 1 || observed.ActiveTimers != 0 {
		t.Fatalf("recurring completion left an extra or live timer arm: %+v", observed)
	}
	if completed.BundleHash != rt.BundleHash || completed.Status != "completed" || completed.EndedAt == nil || completed.Failure != nil {
		t.Fatalf("recurring checked receiver is not actual run completion: %+v", completed)
	}
	instance := issue2564H2CompletionWorkflow(t, rt, issue642TimerContinuationRoot(t, fork.ForkRunID))
	if instance.CurrentState != "checked" {
		t.Fatalf("recurring effect did not close its real receiver: %+v", instance)
	}
	time.Sleep(100 * time.Millisecond)
	unchangedChild, found, err := reader.LoadWorkflowTimerActivation(t.Context(), actual.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), unchangedChild.Canonical()) ||
		!reflect.DeepEqual(childOccurrences, issue642RecurringTimerOccurrences(t, rt, unchangedChild, sourceArm.FireAt)) {
		t.Fatalf("completed recurring child rearmed or repeated an occurrence: found=%v err=%v", found, err)
	}
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("recurring child changed source business/publication/delivery facts: %v", err)
	}
	unchangedSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSource.Canonical(), unchangedSource.Canonical()) {
		t.Fatalf("recurring child changed the original settled source arm: found=%v err=%v", found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("recurring child changed source completion: %v", err)
	}
	t.Logf("ISSUE642_RECURRING_TIMER_CONTINUED backend=%s source=%s child=%s cut=%d timer=%s source_arm=%s first_due=%s cadence=%s occurrences=%d next_due=%s status=%s completed=%s",
		backend, seed.RunID, fork.ForkRunID, plan.ForkPoint.Revision, actual.Ref.ActivationID,
		actual.SourceArmedAt.Format(time.RFC3339Nano), sourceArm.FireAt.Format(time.RFC3339Nano), actual.RecurrenceInterval,
		len(childOccurrences), actual.FireAt.Format(time.RFC3339Nano), actual.Status, completed.EndedAt.Format(time.RFC3339Nano))
}

func issue642RecurringTimerPlan(t *testing.T, reader issue642TimerContinuationReaders, runID, markerID string) (runfork.RunForkPlan, pipeline.WorkflowTimerActivation) {
	t.Helper()
	plan, err := reader.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: runID, At: markerID})
	if err != nil || plan.ForkPoint.Kind != runfork.RunForkPointEvent || plan.ForkPoint.EventID != markerID || len(plan.WorkflowTimers) != 1 {
		t.Fatalf("recurring marker lost its exact retained timer-bearing cut: point=%+v timers=%+v err=%v", plan.ForkPoint, plan.WorkflowTimers, err)
	}
	arm, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(plan.WorkflowTimers[0])
	if err != nil || arm.Status != "active" || !arm.Recurring || arm.RecurrenceInterval != time.Second ||
		arm.FireAt.Sub(arm.CreatedAt) != arm.RecurrenceInterval || !arm.FiredAt.IsZero() ||
		arm.RunID != runID || arm.EntityID != runID || arm.EventType != "timer.check" ||
		arm.Ref.Cause != timeridentity.WorkflowTimerActivationCauseEvent || arm.Route != issue642TimerContinuationRoot(t, runID).Route {
		t.Fatalf("fixed cut did not keep its original active recurring arm: arm=%+v err=%v", arm, err)
	}
	if len(plan.Entities) != 1 || plan.Entities[0].EntityID != runID || plan.Entities[0].CurrentState != "waiting" {
		t.Fatalf("recurring cut lost its historical waiting receiver: %+v", plan.Entities)
	}
	return plan, arm.Canonical()
}

func issue642RequireRecurringProgress(t *testing.T, initial, actual pipeline.WorkflowTimerActivation, occurrences int) {
	t.Helper()
	expected := initial.Canonical()
	expected.Status, expected.FiredAt = "cancelled", actual.FiredAt
	expected.FireAt = expected.FireAt.Add(time.Duration(occurrences) * expected.RecurrenceInterval)
	if occurrences == 0 || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
		t.Fatalf("recurring lifecycle changed immutable arm/lineage or skipped its occurrence lattice: count=%d expected=%+v actual=%+v", occurrences, expected, actual)
	}
}

func issue642RecurringTimerOccurrences(t *testing.T, rt issue2564ServedFixture, activation pipeline.WorkflowTimerActivation, firstDue time.Time) []timeridentity.WorkflowTimerOccurrenceRef {
	t.Helper()
	seenEvents, seenCursors := map[string]bool{}, map[string]bool{}
	var occurrences []timeridentity.WorkflowTimerOccurrenceRef
	for cursor := ""; ; {
		page, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: activation.RunID, EventName: activation.EventType}, Limit: 100, Cursor: cursor,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, listed := range page.Events {
			if seenEvents[listed.EventID] {
				t.Fatalf("recurring readback repeated event %s", listed.EventID)
			}
			seenEvents[listed.EventID] = true
			event := storetest.LoadCanonicalEventRecord(t, t.Context(), rt.selected, listed.EventID)
			occurrence, err := activation.ValidatePublishedOccurrence(event)
			if err != nil || event.ProducerType() != events.EventProducerPlatform {
				t.Fatalf("recurring publication is not an admitted exact native occurrence: event=%+v err=%v", event, err)
			}
			if receipt := storetest.ObservePipelineReceipt(t, t.Context(), rt.selected, event.ID()); receipt.Count != 1 || receipt.Outcome == "" {
				t.Fatalf("recurring publication did not settle its one pipeline obligation: event=%s receipt=%+v", event.ID(), receipt)
			}
			occurrences = append(occurrences, occurrence)
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor || seenCursors[page.NextCursor] {
			t.Fatal("recurring event pagination repeated a cursor")
		}
		cursor, seenCursors[page.NextCursor] = page.NextCursor, true
	}
	sort.Slice(occurrences, func(i, j int) bool { return occurrences[i].DueAt.Before(occurrences[j].DueAt) })
	if len(occurrences) == 0 {
		t.Fatal("recurring arm completed without any actual timer effect")
	}
	for index, occurrence := range occurrences {
		if !occurrence.DueAt.Equal(firstDue.Add(time.Duration(index) * activation.RecurrenceInterval)) {
			t.Fatalf("recurring arm repeated, skipped or rebased its due coordinate: index=%d first=%s occurrence=%+v", index, firstDue, occurrence)
		}
	}
	return occurrences
}
