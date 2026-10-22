package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestIssue642RecordedActivityTerminalHistoryDoesNotReplayResultBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyTerminalHistory)
		})
	}
}

func proveCopiedActivityTerminalHistory(t *testing.T, h *runtimeHarness, selected copiedActivityReplyStore, sourceRun string, sourceAttempt pipeline.ActivityAttemptRecord,
	loader runforkexecution.SelectedContractSourceLoader, selection runfork.RunForkContractSelection, owner runforkexecution.SelectedContractExecutionOwner, options runforkexecution.SelectedContractAgentRuntimeOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	sourceRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || len(sourceRows) != 2 {
		t.Fatalf("terminal cut lacks its exact reply inventory: %+v error=%v", sourceRows, err)
	}
	cut := ""
	for _, event := range activityLineageEvents(t, ctx, h, sourceRun) {
		if event.EventName == "request.finished" {
			if cut != "" {
				t.Fatal("terminal source has multiple requester completions")
			}
			cut = event.EventID
		}
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
	if err != nil || cut == "" {
		t.Fatalf("admit the exact terminal publication cut: %v", err)
	}
	resultRecipients := 0
	for _, work := range plan.PendingWork {
		if work.EventID != sourceAttempt.ResultEventID || work.SubscriberType != "node" {
			continue
		}
		resultRecipients++
		if work.Classification != runfork.RunForkPendingClassificationDeliveredCompleted {
			t.Fatalf("terminal result cut did not settle its exact recipient: %+v", work)
		}
	}
	if resultRecipients != 1 {
		t.Fatalf("terminal cut omitted or duplicated the exact result recipient: %+v", plan.PendingWork)
	}
	operation := catalogStagedForkOperation(t, ctx, selected, selection, sourceRun, cut)
	staged := stageCatalogSelectedContractFork(t, ctx, selected, owner, loader, selection, options, sourceRun, cut, operation)
	executed, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: cut, ForkOperation: &operation, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("activate exact terminal-history execution: %+v error=%v", executed, err)
	}
	settlementCtx, cancelSettlement := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancelSettlement()
	for {
		run, err := storetest.ReadRuntimeLogRunSnapshot(settlementCtx, selected, staged.ForkRunID)
		if err != nil || run.Failure != nil {
			failure, _ := json.Marshal(run.Failure)
			cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(settlementCtx), 10*time.Second)
			diagnostic, readErr := storetest.ReadSelectedForkLifecycleDiagnosticStorage(cleanup, selected, staged.ForkRunID)
			for _, log := range diagnostic.Logs {
				t.Logf("terminal-history runtime log: %s", log.Payload)
			}
			t.Logf("terminal-history selected owner cause/join: %v", owner.RetireSelectedContexts(cleanup))
			cancelCleanup()
			t.Fatalf("terminal-history execution failed: %+v failure=%s error=%v diagnostics=%v", run, failure, err, readErr)
		}
		pending, pendingErr := storetest.ReadServedIncompletePipelineHandoffCount(settlementCtx, selected, staged.ForkRunID)
		cutPublished, found, cutErr := selected.LoadPreparedPublishEvent(settlementCtx, activityidentity.ForkLineageEventID(staged.ForkRunID, cut))
		if pendingErr != nil || cutErr != nil {
			t.Fatalf("read exact terminal-cut pipeline accounting: %v/%v", pendingErr, cutErr)
		}
		if found && pending == 0 {
			lineage, selected := cutPublished.Event.Event().SelectedForkLineage()
			if !selected || lineage.SourceRunID() != sourceRun || lineage.SourceEventID() != cut || string(run.State) != "running" {
				t.Fatalf("terminal-cut accounting changed source lineage or invented root completion: event=%+v run=%+v", cutPublished, run)
			}
			break
		}
		select {
		case <-settlementCtx.Done():
			t.Fatalf("terminal-history child failed to settle exact publication accounting within original bound: %+v pending=%d cut_found=%t cause=%v", run, pending, found, context.Cause(settlementCtx))
		case <-time.After(10 * time.Millisecond):
		}
	}
	// This fixture's root stays waiting until request.stop. The selected cut
	// publication still enters its pipeline; settled result recipients do not.
	for _, name := range []string{sourceAttempt.ResultEventType, "t/t.replied", "provider/provider.replied"} {
		count, err := storetest.ReadLifecycleEventCardinality(settlementCtx, selected, staged.ForkRunID, name)
		if err != nil || count != 0 {
			t.Fatalf("terminal history replayed %s: count=%d error=%v", name, count, err)
		}
	}
	finished, err := storetest.ReadLifecycleEventCardinality(settlementCtx, selected, staged.ForkRunID, "request.finished")
	if err != nil || finished != 1 {
		t.Fatalf("terminal child missed or duplicated its exact primary cut: count=%d error=%v", finished, err)
	}
	for _, work := range plan.PendingWork {
		if work.EventName != "platform.activity_requested" || work.Classification != runfork.RunForkPendingClassificationPending {
			continue
		}
		inputID := activityidentity.ForkLineageEventID(staged.ForkRunID, work.EventID)
		receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(settlementCtx, selected, inputID)
		if err != nil || !receipt.Found || receipt.Outcome != "success" || len(receipt.Failure) != 0 {
			t.Fatalf("terminal result dropped or failed its exact pending-input accounting: %+v error=%v", receipt, err)
		}
	}
	childRows, err := storetest.ReadReplyContextRecords(settlementCtx, selected, staged.ForkRunID)
	if err != nil || len(childRows) != 2 {
		t.Fatalf("terminal child lost retained reply evidence: %+v error=%v", childRows, err)
	}
	for _, row := range childRows {
		if row.RunID != staged.ForkRunID || row.State != replycontext.StateTerminal || row.AcceptedReplyEventID == "" {
			t.Fatalf("terminal-history execution reopened or discarded a settled reply: %+v", row)
		}
	}
	afterRows, rowsErr := storetest.ReadReplyContextRecords(settlementCtx, selected, sourceRun)
	afterAttempt, found, attemptErr := selected.LoadActivityAttempt(settlementCtx, sourceAttempt.RequestEventID)
	if rowsErr != nil || attemptErr != nil || !found || !reflect.DeepEqual(afterRows, sourceRows) || !reflect.DeepEqual(afterAttempt, sourceAttempt) {
		t.Fatalf("terminal-history execution changed its immediate source: errors=%v/%v", rowsErr, attemptErr)
	}
}
