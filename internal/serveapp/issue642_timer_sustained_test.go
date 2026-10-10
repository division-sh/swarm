package serveapp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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

// Supplemental real serving proof. Two accepted tick effects, not publication
// cardinality or elapsed sleep, cause authored cancellation and completion.
func TestIssue642SustainedRecurringTimerForkExecutesTwoEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { issue642SustainedRecurringTimerFork(t, backend) })
	}
}

func issue642SustainedTimerSource(t *testing.T) string {
	t.Helper()
	root := issue642RecurringTimerSource(t)
	for name, body := range map[string]string{
		"nodes.yaml": `test-node:
  execution_type: system_node
  subscribes_to: [timer.scheduled, timer.check, timer.finished]
  produces: [timer.armed, timer.finished]
  timers:
    - id: check_timer
      event: timer.check
      delay: 1s
      start_on: event:timer.scheduled
      cancel_on: event:timer.finished
      recurring: true
  event_handlers:
    timer.scheduled:
      advances_to: waiting
      emit: {event: timer.armed}
    timer.check:
      rules:
        - id: second_tick
          when: entity.tick_count == 1
          advances_to: waiting
          emit: {event: timer.finished}
        - id: other_tick
          else: true
          advances_to: waiting
      data_accumulation:
        writes:
          - target_field: tick_count
            value: entity.tick_count + 1
    timer.finished:
      advances_to: checked
`,
		"entities.yaml": "test_entity:\n  tick_count: {type: integer, initial: 0}\n",
		"events.yaml":   "timer.scheduled:\ntimer.check:\ntimer.armed:\ntimer.finished:\n",
		"schema.yaml": `stages:
  waiting: {}
  checked: {final: true}
pins:
  inputs:
    - timer.scheduled
    - timer.check
  outputs:
    - timer.check
    - timer.armed
    - timer.finished
`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func issue642SustainedRecurringTimerFork(t *testing.T, backend string) {
	t.Helper()
	_, start := issue2564ServeHarness(t, backend, issue642SustainedTimerSource(t), false)
	process, rt := start()
	runtime := servedTestProcessRuntime(t, process)
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("sustained timer serve shutdown=%d\n%s", code, process.outputString())
		}
		if runtime.WorkOccurrence() == nil || runtime.Options.ProcessWorkOwner == nil {
			t.Error("sustained served proof lost its real runtime/process work owners")
			return
		}
		if leases := runtime.WorkOccurrence().ActiveCount(); leases != 0 {
			t.Errorf("sustained timer shutdown leaked runtime leases: %d", leases)
		}
		if leases := runtime.Options.ProcessWorkOwner.ActiveCount(); leases != 0 {
			t.Errorf("sustained timer shutdown leaked process leases: %d", leases)
		}
		t.Logf("ISSUE642_SUSTAINED_TIMER_SHUTDOWN backend=%s runtime_leases=%d process_leases=%d", backend,
			runtime.WorkOccurrence().ActiveCount(), runtime.Options.ProcessWorkOwner.ActiveCount())
	})
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("served selected store lacks canonical sustained timer/fork readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-sustained-source-arm",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	if marker.SourceEventID != seed.EventID {
		t.Fatal("sustained cut lost its real creating-handler cause")
	}
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, sourceArm := issue642RecurringTimerPlan(t, reader, seed.RunID, marker.EventID)
	if issue642SustainedInteger(t, plan.Entities[0].Fields["tick_count"]) != 0 {
		t.Fatal("fixed arm cut already executed ticks; child must owe two fresh effects")
	}
	settledSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || settledSource.Status != "cancelled" || settledSource.CancelCause != "" || !settledSource.CancelledAt.IsZero() {
		t.Fatalf("authored finish did not cancel the ordinary source arm: timer=%+v found=%v err=%v", settledSource, found, err)
	}
	sourceTicks := issue642RecurringTimerOccurrences(t, rt, settledSource, sourceArm.FireAt)
	issue642RequireRecurringProgress(t, sourceArm, settledSource, len(sourceTicks))
	sourceEffects := issue642RequireSustainedEffects(t, rt, sourceCompleted, sourceTicks, marker.Source)
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var fork apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
		"source_run_id": seed.RunID, "fork_event_id": marker.EventID,
		"allow_source_freeze": true, "idempotency_key": "issue642-sustained-recurring-fork",
	}, &fork)
	if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
		fork.ForkEventID != marker.EventID || fork.ForkRevision != plan.ForkPoint.Revision || fork.SourceFrozen || fork.SourceRunStatus != "completed" {
		t.Fatalf("sustained fork lost the exact active cut and independent completed source: %+v", fork)
	}
	rt.waitEntityStage(t, fork.ForkRunID, fork.ForkRunID, "checked")
	completed := issue2564H2CompletionWaitRun(t, rt, fork.ForkRunID)
	rt.waitDeliveries(t, fork.ForkRunID)
	if !sourceArm.FireAt.Before(completed.StartedAt) {
		t.Fatal("sustained child must retain the original overdue coordinate")
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
		t.Fatalf("authored finish did not cancel the ordinary child arm: timer=%+v found=%v err=%v", actual, found, err)
	}
	childTicks := issue642RecurringTimerOccurrences(t, rt, actual, sourceArm.FireAt)
	issue642RequireRecurringProgress(t, expected, actual, len(childTicks))
	if !actual.SourceArmedAt.Equal(sourceArm.CreatedAt) || actual.RecurrenceInterval != sourceArm.RecurrenceInterval {
		t.Fatalf("sustained child reset the original arm or cadence: source=%+v child=%+v", sourceArm, actual)
	}
	childEffects := issue642RequireSustainedEffects(t, rt, completed, childTicks, marker.Source)
	issue642WaitSustainedExecutionClosed(t, rt, fork.ForkRunID)
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, fork.ForkRunID, fork.ForkRunID); observed.Timers != 1 || observed.ActiveTimers != 0 {
		t.Fatalf("sustained child leaked an extra or active timer: %+v", observed)
	}
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("sustained child changed source business/publication/delivery facts: %v", err)
	}
	unchangedSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceArm.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSource.Canonical(), unchangedSource.Canonical()) {
		t.Fatalf("sustained child changed the original source timer: found=%v err=%v", found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("sustained child changed source completion/counts: %v", err)
	}
	t.Logf("ISSUE642_SUSTAINED_TIMER_CONTINUED backend=%s source=%s child=%s cut=%d timer=%s original_arm=%s first_due=%s cadence=%s source_effects=%d child_effects=%d child_occurrences=%d next_due=%s completed=%s",
		backend, seed.RunID, fork.ForkRunID, plan.ForkPoint.Revision, actual.Ref.ActivationID,
		actual.SourceArmedAt.Format(time.RFC3339Nano), sourceArm.FireAt.Format(time.RFC3339Nano), actual.RecurrenceInterval,
		sourceEffects, childEffects, len(childTicks), actual.FireAt.Format(time.RFC3339Nano), completed.EndedAt.Format(time.RFC3339Nano))
}

func issue642SustainedInteger(t *testing.T, value any) int {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := json.Unmarshal(raw, &count); err != nil || string(raw) == "null" {
		t.Fatalf("sustained tick count is not an exact declared integer: %s err=%v", raw, err)
	}
	return count
}

func issue642RequireSustainedEffects(t *testing.T, rt issue2564ServedFixture, completed operatorread.RunHeader, occurrences []timeridentity.WorkflowTimerOccurrenceRef, node string) int {
	t.Helper()
	entity, err := rt.selected.LoadOperatorEntity(t.Context(), completed.RunID, completed.RunID)
	if err != nil || entity.Entity.CurrentState != "checked" {
		t.Fatalf("sustained timer effects did not complete their actual receiver: entity=%+v err=%v", entity, err)
	}
	count := issue642SustainedInteger(t, entity.Fields["tick_count"])
	if count < 2 || count > len(occurrences) {
		t.Fatalf("sustained serving requires at least two executed effects, not just publications: effects=%d occurrences=%d", count, len(occurrences))
	}
	report, err := rt.selected.LoadRunDebugReport(t.Context(), completed.RunID, operatorread.RunDebugQueryOptions{})
	if err != nil || !report.TestQuiescence.Ready || report.TestQuiescence.ActiveSessionLeases != 0 {
		t.Fatalf("completed sustained run retains unsettled native work: quiescence=%+v err=%v", report.TestQuiescence, err)
	}
	tickIDs := make(map[string]bool, len(occurrences))
	for _, occurrence := range occurrences {
		tickIDs[timeridentity.WorkflowTimerOccurrenceEventID(occurrence)] = true
	}
	mutatedEvents, values := map[string]bool{}, map[int]string{}
	// SQLite's debug report omits mutations; use the existing both-store history owner.
	for _, mutation := range storetest.ObserveEntityMutationHistory(t, t.Context(), rt.selected, completed.RunID) {
		if mutation.Domain != "authored_field" || mutation.Path != "tick_count" || !tickIDs[mutation.CausedByEvent] {
			continue
		}
		var oldCount, newCount int
		if json.Unmarshal(mutation.OldValue, &oldCount) != nil || json.Unmarshal(mutation.NewValue, &newCount) != nil || newCount != oldCount+1 ||
			mutation.EntityID != completed.RunID || mutatedEvents[mutation.CausedByEvent] || values[newCount] != "" {
			t.Fatalf("sustained tick effect is missing, duplicated or not an exact increment: %+v", mutation)
		}
		mutatedEvents[mutation.CausedByEvent], values[newCount] = true, mutation.CausedByEvent
		issue2564H2CompletionWaitReceipt(t, rt, completed.RunID, mutation.CausedByEvent)
	}
	if len(mutatedEvents) != count {
		t.Fatalf("sustained count lacks its complete durable occurrence effects: count=%d mutations=%+v", count, values)
	}
	for expected := 1; expected <= count; expected++ {
		if values[expected] == "" {
			t.Fatalf("sustained count skipped an executed effect: expected=%d mutations=%+v", expected, values)
		}
	}
	finished := rt.events(t, completed.RunID, "timer.finished")
	if len(finished) != 1 {
		t.Fatalf("second tick did not emit exactly one authored finish: %+v", finished)
	}
	finish := storetest.LoadCanonicalEventRecord(t, t.Context(), rt.selected, finished[0].EventID)
	if finish.ParentEventID() != values[2] || finish.RunID() != completed.RunID || finish.SourceAgent() != node || finish.ProducerType() != events.EventProducerNode {
		t.Fatalf("authored cancellation is not caused by the second actual tick effect: finish=%+v second=%s", finish, values[2])
	}
	issue2564H2CompletionWaitReceipt(t, rt, completed.RunID, finish.ID())
	if all := rt.events(t, completed.RunID, ""); completed.EventCount != len(all) || report.EventCount != len(all) {
		t.Fatalf("sustained run count does not match exact persisted publications: header=%d report=%d publications=%d", completed.EventCount, report.EventCount, len(all))
	}
	if completed.BundleHash != rt.BundleHash || completed.Status != "completed" || completed.EndedAt == nil || completed.Failure != nil {
		t.Fatalf("sustained serving is not actual selected run completion: %+v", completed)
	}
	summary, err := rt.selected.SummarizeRun(t.Context(), completed.RunID)
	if err != nil || !summary.Settled() || summary.DeadLetter != 0 {
		t.Fatalf("sustained completion retained failed or live deliveries: summary=%+v err=%v", summary, err)
	}
	return count
}

func issue642WaitSustainedExecutionClosed(t *testing.T, rt issue2564ServedFixture, runID string) {
	t.Helper()
	var storage storetest.SelectedExecutionStorage
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		var err error
		storage, err = storetest.ReadSelectedExecutionStorage(t.Context(), rt.selected, runID)
		if err != nil {
			t.Fatal(err)
		}
		if storage.State == "closed" && storage.RunStatus == "completed" && storage.Occurrences == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("completed sustained child leaked its selected execution occurrence: %+v", storage)
}
