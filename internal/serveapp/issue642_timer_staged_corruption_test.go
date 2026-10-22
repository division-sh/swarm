package serveapp

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Native staging/activation proof, not the public registry-v5 acceptance run.
func TestIssue642StagedTimerCorruptionRefusesBeforeActivationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []storetest.StagedWorkflowTimerFault{
			storetest.StagedWorkflowTimerMissing, storetest.StagedWorkflowTimerProgressed, storetest.StagedWorkflowTimerForeignOrigin,
		} {
			t.Run(backend+"/"+string(fault), func(t *testing.T) {
				issue642StagedTimerCorruption(t, backend, fault)
			})
		}
	}
}

func issue642StagedTimerCorruption(t *testing.T, backend string, fault storetest.StagedWorkflowTimerFault) {
	t.Helper()
	captured := issue642CaptureTimerStagedOwners(t)
	_, start := issue2564ServeHarness(t, backend, issue642TimerContinuationSource(t), false)
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("staged timer corruption shutdown=%d\n%s", code, process.outputString())
		}
	})
	var owners issue642TimerStagedOwners
	select {
	case owners = <-captured:
	default:
		t.Fatal("ready serve did not expose native staging owners")
	}
	if !owners.available || owners.supervisor == nil {
		t.Fatal("missing deployed staging owners")
	}
	ctx := servedRuntimeProofAuthorActivityContext(t, owners.supervisor.CurrentRuntime(), rt.BundleHash)
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("missing canonical timer readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash, "payload": map[string]any{}, "idempotency_key": "issue642-staged-corruption",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, source := issue642TimerContinuationPlan(t, reader, seed.RunID, seed.EventID, marker.EventID)
	staged := issue642MaterializePausedTimer(t, ctx, owners, rt, reader, plan, source)
	_, timer := issue642RequirePausedStagedTimer(t, rt, reader, staged, source)
	if err := storetest.CorruptStagedWorkflowTimer(ctx, rt.selected, staged.ForkRunID, timer.Ref.ActivationID, fault); err != nil {
		t.Fatalf("install exact hostile staged timer: %v", err)
	}
	childBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, staged.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storetest.ReadSelectedExecutionStorage(ctx, rt.selected, staged.ForkRunID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unactivated staged child unexpectedly has an execution: %v", err)
	}
	activation, err := owners.family.Activate(ctx, runforkexecution.SelectedContractActivationGateRequest{
		ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, SourceLoader: owners.executor.SourceLoader, AgentRuntime: owners.executor.AgentRuntime,
	})
	if err == nil || activation.Activated || !strings.Contains(strings.ToLower(err.Error()), "timer") {
		t.Fatalf("hostile staged timer did not refuse at timer admission: activation=%+v err=%v", activation, err)
	}
	childAfter, readErr := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, staged.ForkRunID)
	if readErr != nil || !reflect.DeepEqual(childBefore, childAfter) {
		t.Fatalf("failed timer admission changed child: %v", readErr)
	}
	sourceAfter, readErr := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, seed.RunID)
	if readErr != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("failed timer admission changed source: %v", readErr)
	}
	_, readErr = storetest.ReadSelectedExecutionStorage(ctx, rt.selected, staged.ForkRunID)
	if !errors.Is(readErr, sql.ErrNoRows) {
		t.Fatalf("failed timer admission issued execution: %v", readErr)
	}
	header, err := rt.selected.LoadRunHeader(ctx, staged.ForkRunID)
	if err != nil || header.Status != "paused" || header.EndedAt != nil {
		t.Fatalf("failed timer admission activated child: header=%+v err=%v", header, err)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, ctx, rt.selected, staged.ForkRunID, staged.ForkRunID); observed.Events != 0 {
		t.Fatalf("hostile staging published: %+v", observed)
	}
	t.Logf("ISSUE642_STAGED_TIMER_REFUSED backend=%s fault=%s child=%s cut=%d unchanged=true", backend, fault, staged.ForkRunID, plan.ForkPoint.Revision)
}
