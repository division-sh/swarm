package serveapp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue642TimerStagedOwners struct {
	family     storeselected.RunFork
	executor   apiv1.SelectedContractRunForkExecutor
	supervisor *processLifecycleSupervisor
	available  bool
}

type issue642TimerStagedMaterializer interface {
	MaterializeRunForkForSelectedContractExecution(context.Context, runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
}

// Native staged-owner proof with real served source/receiver execution. This is
// not a public run.fork operation or registry-v5 acceptance substitute.
func TestIssue642OrdinaryTimerStagedActivationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			issue642StagedOrdinaryTimerFork(t, backend)
		})
	}
}

func issue642CaptureTimerStagedOwners(t *testing.T) <-chan issue642TimerStagedOwners {
	t.Helper()
	captured := make(chan issue642TimerStagedOwners, 1)
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, req)
		if err != nil {
			return caps, err
		}
		family, available := owner.RunFork()
		if req.SelectedFork != nil {
			family, available = *req.SelectedFork, true
		}
		executor, executorAvailable := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
		select {
		case captured <- issue642TimerStagedOwners{family, executor, req.RuntimeSupervisor, available && executorAvailable}:
		default:
		}
		return caps, nil
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = previous })
	return captured
}

func issue642StagedOrdinaryTimerFork(t *testing.T, backend string) {
	t.Helper()
	captured := issue642CaptureTimerStagedOwners(t)
	_, start := issue2564ServeHarness(t, backend, issue642TimerContinuationSource(t), false)
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("native staged timer serve shutdown exit=%d\n%s", code, process.outputString())
		}
	})
	var owners issue642TimerStagedOwners
	select {
	case owners = <-captured:
	default:
		t.Fatal("ready serve did not expose its deployed selected fork capabilities")
	}
	if !owners.available || owners.supervisor == nil || owners.executor.SourceLoader == nil || owners.executor.AgentRuntime.ProcessCapability == nil {
		t.Fatal("staged timer proof requires the actual deployed fork family, loader and process capability")
	}
	ctx := servedRuntimeProofAuthorActivityContext(t, owners.supervisor.CurrentRuntime(), rt.BundleHash)
	reader, ok := rt.selected.(issue642TimerContinuationReaders)
	if !ok {
		t.Fatalf("selected serve store lacks canonical timer/fork readers: %T", rt.selected)
	}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "timer.scheduled", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{}, "idempotency_key": "issue642-staged-timer-arm",
	})
	marker := issue642TimerContinuationMarker(t, rt, seed.RunID)
	if marker.SourceEventID != seed.EventID {
		t.Fatalf("staged timer marker lost its actual arm cause: %+v", marker)
	}
	rt.waitEntityStage(t, seed.RunID, seed.RunID, "checked")
	sourceCompleted := issue2564H2CompletionWaitRun(t, rt, seed.RunID)
	rt.waitDeliveries(t, seed.RunID)
	issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
	plan, sourceTimer := issue642TimerContinuationPlan(t, reader, seed.RunID, seed.EventID, marker.EventID)
	settledSourceTimer, found, err := reader.LoadWorkflowTimerActivation(ctx, sourceTimer.Ref.ActivationID)
	if err != nil || !found || settledSourceTimer.Status != "fired" || settledSourceTimer.FiredAt.Before(sourceTimer.FireAt) {
		t.Fatalf("source timer did not progress beyond the exact active historical cut: timer=%+v found=%v err=%v", settledSourceTimer, found, err)
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	staged := issue642MaterializePausedTimer(t, ctx, owners, rt, reader, plan, sourceTimer)
	activation, err := owners.family.Activate(ctx, runforkexecution.SelectedContractActivationGateRequest{
		ForkRunID: staged.ForkRunID, AllowSourceFreeze: true,
		SourceLoader: owners.executor.SourceLoader, AgentRuntime: owners.executor.AgentRuntime,
	})
	if err != nil || !activation.Activated || activation.ForkRunID != staged.ForkRunID || activation.ForkPoint != plan.ForkPoint ||
		activation.SourceRunID != seed.RunID || activation.SourceRunStatus != "completed" || activation.SourceFrozen {
		t.Fatalf("native staged activation did not acknowledge exact historical timer authority: activation=%+v err=%v", activation, err)
	}
	rt.waitEntityStage(t, staged.ForkRunID, staged.ForkRunID, "checked")
	completed := issue2564H2CompletionWaitRun(t, rt, staged.ForkRunID)
	rt.waitDeliveries(t, staged.ForkRunID)
	issue642TimerContinuationReadback(t, rt, reader, sourceTimer, plan.ForkPoint, completed)
	sourceAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, rt.selected, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
		t.Fatalf("native staged continuation changed settled source business/publication/delivery facts: %v", err)
	}
	unchanged, found, err := reader.LoadWorkflowTimerActivation(ctx, sourceTimer.Ref.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(settledSourceTimer.Canonical(), unchanged.Canonical()) {
		t.Fatalf("native staged continuation changed the settled source timer: before=%+v after=%+v found=%v err=%v", settledSourceTimer, unchanged, found, err)
	}
	sourceAfterCompletion, err := rt.selected.LoadRunHeader(ctx, seed.RunID)
	if err != nil || !reflect.DeepEqual(sourceCompleted, sourceAfterCompletion) {
		t.Fatalf("native staged continuation changed original source completion: before=%+v after=%+v err=%v", sourceCompleted, sourceAfterCompletion, err)
	}
	t.Logf("ISSUE642_NATIVE_STAGED_TIMER_COMPLETED backend=%s source=%s child=%s cut=%d due=%s",
		backend, seed.RunID, staged.ForkRunID, plan.ForkPoint.Revision, sourceTimer.FireAt.Format(time.RFC3339Nano))
}

func issue642MaterializePausedTimer(t *testing.T, ctx context.Context, owners issue642TimerStagedOwners, rt issue2564ServedFixture, reader issue642TimerContinuationReaders, plan runfork.RunForkPlan, source pipeline.WorkflowTimerActivation) runfork.RunForkMaterialization {
	t.Helper()
	prepared, err := owners.family.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: plan.SourceRunID, At: plan.ForkPoint.EventID, ExpectedBundleHash: rt.BundleHash,
		AllowSourceFreeze: true, ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
		SourceLoader: owners.executor.SourceLoader, AgentRuntime: owners.executor.AgentRuntime,
	})
	if err != nil {
		t.Fatalf("prepare native staged timer: %v", err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	if request.SourceRunID != plan.SourceRunID || request.At != plan.ForkPoint.EventID || request.SourceArtifactFact.BundleHash() != rt.BundleHash {
		t.Fatalf("native staged request lost its exact source/cut/artifact: %+v", request)
	}
	scope, err := authoractivity.BundleScopeForTarget(ctx, request.SourceArtifactFact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	materializationCtx := authoractivity.WithScope(correlation.WithSourceArtifactFact(ctx, request.SourceArtifactFact), scope)
	native, ok := owners.family.Availability().(issue642TimerStagedMaterializer)
	if !ok {
		t.Fatalf("deployed selected family lacks its native typed staging writer: %T", owners.family.Availability())
	}
	staged, err := native.MaterializeRunForkForSelectedContractExecution(materializationCtx, request)
	if err != nil || staged.ForkRunID == "" || staged.ForkRunID == plan.SourceRunID || staged.SourceRunID != plan.SourceRunID ||
		staged.ForkRunStatus != runfork.RunForkMaterializedStatus || staged.ForkPoint != plan.ForkPoint || staged.SelectedContractBinding == nil {
		t.Fatalf("native staging did not retain exact paused child/binding: staged=%+v err=%v", staged, err)
	}
	paused, timer := issue642RequirePausedStagedTimer(t, rt, reader, staged, source)
	if err := prepared.Close(); err != nil {
		t.Fatalf("close staging preparation before independent activation: %v", err)
	}
	afterClose, afterTimer := issue642RequirePausedStagedTimer(t, rt, reader, staged, source)
	if !reflect.DeepEqual(paused, afterClose) || !reflect.DeepEqual(timer.Canonical(), afterTimer.Canonical()) {
		t.Fatalf("closing preparation executed or changed the staged child: before=%+v/%+v after=%+v/%+v", paused, timer, afterClose, afterTimer)
	}
	t.Logf("ISSUE642_NATIVE_STAGED_TIMER_PAUSED backend=%s child=%s timer=%s due=%s preparation_closed=true publications=0",
		rt.Backend, staged.ForkRunID, timer.Ref.ActivationID, timer.FireAt.Format(time.RFC3339Nano))
	return staged
}

func issue642RequirePausedStagedTimer(t *testing.T, rt issue2564ServedFixture, reader issue642TimerContinuationReaders, staged runfork.RunForkMaterialization, source pipeline.WorkflowTimerActivation) (operatorread.RunHeader, pipeline.WorkflowTimerActivation) {
	t.Helper()
	header, err := rt.selected.LoadRunHeader(t.Context(), staged.ForkRunID)
	if err != nil || header.Status != "paused" || header.EndedAt != nil || header.Failure != nil || header.BundleHash != rt.BundleHash {
		t.Fatalf("staged timer acquired active or terminal run authority: header=%+v err=%v", header, err)
	}
	projected, err := runfork.ProjectWorkflowTimerRecord(source.PersistenceRecord(), source.Ref, staged.ForkRunID, staged.ForkPoint, nil, header.StartedAt.UTC().Truncate(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projected)
	if err != nil {
		t.Fatal(err)
	}
	actual, found, err := reader.LoadWorkflowTimerActivation(t.Context(), projected.ActivationID)
	if err != nil || !found || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) || actual.Status != "active" ||
		!actual.FiredAt.IsZero() || !actual.FireAt.Equal(source.FireAt) || time.Now().Before(actual.FireAt) {
		t.Fatalf("paused child lost, fired or rearmed its exact overdue inherited timer: expected=%+v actual=%+v found=%v err=%v", expected, actual, found, err)
	}
	if observed := storetest.ObserveWorkflowTimerReplayStorage(t, t.Context(), rt.selected, staged.ForkRunID, staged.ForkRunID); observed.Timers != 1 || observed.ActiveTimers != 1 || observed.Events != 0 {
		t.Fatalf("paused staging published an occurrence or changed the timer inventory: %+v", observed)
	}
	instance := issue2564H2CompletionWorkflow(t, rt, issue642TimerContinuationRoot(t, staged.ForkRunID))
	if instance.CurrentState != "waiting" {
		t.Fatalf("paused staged receiver executed before activation: %+v", instance)
	}
	return header, actual
}
