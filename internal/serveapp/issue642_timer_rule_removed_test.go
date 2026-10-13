package serveapp

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Removal does not make a non-final receiver complete. The second selected
// artifact explicitly declares that same retained stage final, without firing.
func TestIssue642RemovedArmedTimerCancelsWithoutPhantomFireBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, final := range []bool{false, true} {
			name := backend + map[bool]string{false: "/non_final", true: "/selected_final"}[final]
			t.Run(name, func(t *testing.T) { issue642RemovedArmedTimerFork(t, backend, final) })
		}
	}
}

func issue642RemovedTimerTarget(t *testing.T, final bool) string {
	t.Helper()
	root := issue642TimerContinuationSource(t)
	path := filepath.Join(root, "nodes.yaml")
	nodes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const declaration = "  timers:\n    - id: check_timer\n      event: timer.check\n      delay: 1s\n      start_on: event:timer.scheduled\n"
	if strings.Count(string(nodes), declaration) != 1 {
		t.Fatal("removed-rule fixture lost its exact original timer declaration")
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(nodes), declaration, "", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if final {
		const checkedHandler = "    timer.check:\n      advances_to: checked\n"
		terminalNodes := strings.Replace(string(nodes), declaration, "", 1)
		if strings.Count(terminalNodes, checkedHandler) != 1 {
			t.Fatal("selected final-stage fixture lost its original check handler")
		}
		if err := os.WriteFile(path, []byte(strings.Replace(terminalNodes, checkedHandler, "    timer.check: {}\n", 1)), 0o600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "schema.yaml")
		schema, err := os.ReadFile(path)
		if err != nil || strings.Count(string(schema), "  waiting: {}\n") != 1 || strings.Count(string(schema), "  checked: {final: true}\n") != 1 {
			t.Fatalf("selected final-stage fixture lost its original waiting declaration: %v", err)
		}
		terminalSchema := strings.Replace(string(schema), "  waiting: {}\n", "  waiting: {final: true}\n", 1)
		terminalSchema = strings.Replace(terminalSchema, "  checked: {final: true}\n", "", 1)
		if err := os.WriteFile(path, []byte(terminalSchema), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func issue642RemovedArmedTimerFork(t *testing.T, backend string, final bool) {
	t.Helper()
	sourceRoot, targetRoot := issue642TimerContinuationSource(t), issue642RemovedTimerTarget(t, final)
	opts, start := issue2564ServeHarness(t, backend, sourceRoot, false)
	opts.SourceRoot = targetRoot
	targetProcess, target := start()
	if code := targetProcess.stop(); code != 0 {
		t.Fatalf("removed-rule target artifact shutdown=%d\n%s", code, targetProcess.outputString())
	}
	opts.SourceRoot = sourceRoot
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("removed-rule serve shutdown=%d\n%s", code, process.outputString())
		}
	})
	if target.BundleHash == rt.BundleHash {
		t.Fatal("removing the rule did not create a distinct selected artifact")
	}
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("served selected store lacks canonical timer/fork readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-removed-rule-arm",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	if marker.SourceEventID != seed.EventID {
		t.Fatal("removed-rule cut lost its actual creating-handler cause")
	}
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, sourceTimer := issue642TimerContinuationPlan(t, reader, seed.RunID, seed.EventID, marker.EventID)
	issue642RequireRemovedTimerDeclaration(t, targetRoot, plan, sourceTimer)
	settledSource, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceTimer.Ref.ActivationID)
	if err != nil || !found || settledSource.Status != "fired" || settledSource.FiredAt.Before(sourceTimer.FireAt) {
		t.Fatalf("source did not settle beyond its active historical arm: timer=%+v found=%v err=%v", settledSource, found, err)
	}
	expectedSource := sourceTimer.Canonical()
	expectedSource.Status, expectedSource.FiredAt = "fired", settledSource.FiredAt
	if !reflect.DeepEqual(expectedSource.Canonical(), settledSource.Canonical()) {
		t.Fatal("source firing changed its immutable fixed-cut arm")
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var fork apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
		"source_run_id": seed.RunID, "fork_event_id": marker.EventID, "bundle_hash": target.BundleHash,
		"allow_source_freeze": true, "idempotency_key": "issue642-removed-armed-timer-fork",
	}, &fork)
	if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
		fork.ForkEventID != marker.EventID || fork.ForkRevision != plan.ForkPoint.Revision || fork.SourceFrozen || fork.SourceRunStatus != "completed" {
		t.Fatalf("removed-rule fork lost its exact cut and independent source completion: %+v", fork)
	}
	child := rt
	child.BundleHash = target.BundleHash
	header, err := rt.selected.LoadRunHeader(t.Context(), fork.ForkRunID)
	if err != nil || header.Failure != nil || header.BundleHash != target.BundleHash {
		t.Fatalf("removed rule lost its exact selected child: header=%+v err=%v", header, err)
	}
	if final {
		header = issue2564H2CompletionWaitRun(t, child, fork.ForkRunID)
	} else if header.Status != "running" || header.EndedAt != nil {
		t.Fatalf("removed rule incorrectly completed its non-final receiver: %+v", header)
	}
	if !sourceTimer.FireAt.Before(header.StartedAt) {
		t.Fatal("removed-rule proof needs the original due coordinate before child birth")
	}
	cancelled := issue642RequireRemovedTimerReadback(t, child, reader, sourceTimer, plan.ForkPoint, header)
	instance := issue2564H2CompletionWorkflow(t, child, issue642TimerContinuationRoot(t, fork.ForkRunID))
	if instance.CurrentState != "waiting" {
		t.Fatalf("removed rule ran a phantom timer handler: %+v", instance)
	}
	issue642RequireNoRemovedTimerFire(t, child, fork.ForkRunID)
	rt.waitDeliveries(t, fork.ForkRunID)
	settled, err := rt.selected.LoadRunHeader(t.Context(), fork.ForkRunID)
	if err != nil || !reflect.DeepEqual(header, settled) {
		t.Fatalf("cancelled arm changed child completion without work: before=%+v after=%+v err=%v", header, settled, err)
	}
	after := issue642RequireRemovedTimerReadback(t, child, reader, sourceTimer, plan.ForkPoint, settled)
	if !reflect.DeepEqual(cancelled.Canonical(), after.Canonical()) {
		t.Fatal("receiver completion changed or rearmed the rule-removed timer")
	}
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("removed-rule child changed source domain facts: %v", err)
	}
	unchanged, found, err := reader.LoadWorkflowTimerActivation(t.Context(), sourceTimer.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSource.Canonical(), unchanged.Canonical()) {
		t.Fatalf("removed-rule child changed the original settled timer: found=%v err=%v", found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(t.Context(), seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("removed-rule child changed source completion: %v", err)
	}
	t.Logf("ISSUE642_REMOVED_ARMED_TIMER_NO_PHANTOM backend=%s selected_final=%t source=%s child=%s cut=%d source_timer=%s child_timer=%s due=%s cancelled_at=%s child_status=%s",
		backend, final, seed.RunID, fork.ForkRunID, plan.ForkPoint.Revision, sourceTimer.Ref.ActivationID, after.Ref.ActivationID,
		sourceTimer.FireAt.Format(time.RFC3339Nano), after.CancelledAt.Format(time.RFC3339Nano), settled.Status)
}

func issue642RequireRemovedTimerDeclaration(t *testing.T, targetRoot string, plan runfork.RunForkPlan, source pipeline.WorkflowTimerActivation) {
	t.Helper()
	selected := semanticview.Wrap(loadWorkflowValidationBundleAt(t, targetRoot))
	construction, _, found, err := runforkadmission.FixedConstructionForRoute(selected, plan, source.Route)
	if err != nil || !found {
		t.Fatalf("removed rule lost the original constructed receiver: found=%v err=%v", found, err)
	}
	timer, err := pipeline.SelectInheritedWorkflowTimer(selected, source, pipeline.WorkflowInstance{
		InstanceID: construction.InstanceID, StorageRef: construction.InstancePath, EntityID: construction.EntityID,
		WorkflowName: construction.TemplateID, ParentFlowID: construction.ParentRoute.FlowID,
		ParentFlowInstance: construction.ParentRoute.FlowInstance, ParentEntityID: construction.ParentEntityID,
	})
	if err != nil || timer != nil {
		t.Fatalf("selected artifact did not actually remove the armed declaration: timer=%+v err=%v", timer, err)
	}
}

func issue642RequireRemovedTimerReadback(t *testing.T, rt issue2564ServedFixture, reader issue642TimerContinuationReaders, source pipeline.WorkflowTimerActivation, point runfork.RunForkPoint, header operatorread.RunHeader) pipeline.WorkflowTimerActivation {
	t.Helper()
	birth := runlifecycle.CanonicalTimestamp(header.StartedAt)
	projected, err := runfork.ProjectWorkflowTimerRecord(source.PersistenceRecord(), source.Ref, header.RunID, point, nil, birth)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projected)
	if err != nil {
		t.Fatal(err)
	}
	expected.Status, expected.CancelCause, expected.CancelledAt = "cancelled", pipeline.WorkflowTimerCancelCauseRuleRemoved, birth
	if err := expected.Validate(); err != nil {
		t.Fatal(err)
	}
	actual, found, err := reader.LoadWorkflowTimerActivation(t.Context(), projected.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) || !actual.FiredAt.IsZero() {
		t.Fatalf("removed arm lost exact cancellation/due/source lineage: expected=%+v actual=%+v found=%v err=%v", expected, actual, found, err)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, header.RunID, header.RunID); observed.Timers != 1 || observed.ActiveTimers != 0 {
		t.Fatalf("removed rule changed the exact child timer inventory: %+v", observed)
	}
	return actual
}

func issue642RequireNoRemovedTimerFire(t *testing.T, rt issue2564ServedFixture, runID string) {
	t.Helper()
	for deadline := time.Now().Add(100 * time.Millisecond); ; {
		page, err := rt.selected.ListOperatorEvents(t.Context(), operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "timer.check"}, Limit: 2,
		})
		if err != nil || len(page.Events) != 0 || page.NextCursor != "" {
			t.Fatalf("cancelled inherited arm published a phantom occurrence: page=%+v err=%v", page, err)
		}
		if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, runID, runID); observed.Events != 0 || observed.Timers != 1 || observed.ActiveTimers != 0 {
			t.Fatalf("rule removal rearmed or published child work before public input: %+v", observed)
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
