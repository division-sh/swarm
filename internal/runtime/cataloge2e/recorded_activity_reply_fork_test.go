package cataloge2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642CopiedActivityPublishesAndReturnsToOwnRequesterBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, false, copiedReplyNoFollowup)
		})
	}
}

func TestIssue642TwoOpenReplyLevelsReturnThroughCopiedActivityBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyNoFollowup)
		})
	}
}

func TestIssue642TwoOpenReplyLevelsStagedRestartAndReforkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyStagedRestart)
		})
	}
}

func TestIssue642NestedContextOnlyOpenReforkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyContextOnlyOpen)
		})
	}
}

func TestIssue642NestedContextOnlyTerminalReforkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyContextOnlyTerminal)
		})
	}
}

func TestIssue642TwoOpenReplyLevelsReforkCopiedActivityAndReturnBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyReforkRequest)
		})
	}
}

func TestIssue642TwoOpenReplyLevelsReforkAtPublishedActivityResultBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyReforkResult)
		})
	}
}

func TestIssue642TwoOpenReplyLevelsRepeatedPhysicalResultReforkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplyRepeatedResult)
		})
	}
}

func TestIssue642TwoOpenReplyLevelSiblingForksReturnIndependentlyBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			runCopiedActivityReplyReturn(t, backend, true, copiedReplySibling)
		})
	}
}

type copiedActivityReplyStore interface {
	runforkexecution.SelectedContractForkLifecycle
	runforkexecution.SourceArtifactSelectedContractSourceStore
	storetest.DurableDataCatalogStore
	pipeline.ActivityAttemptJournal
	bus.PreparedPublishEventReader
	replycontext.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
}

type copiedReplyFollowup uint8

const (
	copiedReplyNoFollowup copiedReplyFollowup = iota
	copiedReplyReforkRequest
	copiedReplyReforkResult
	copiedReplySibling
	copiedReplyRepeatedResult
	copiedReplyTerminalHistory
	copiedReplyStagedRestart
	copiedReplyContextOnlyOpen
	copiedReplyContextOnlyTerminal
)

func runCopiedActivityReplyReturn(t *testing.T, backend catalogRuntimeBackend, nested bool, followup copiedReplyFollowup) {
	t.Helper()
	levels := 1
	if nested {
		levels = 2
	}
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var input map[string]string
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Errorf("read real provider input: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"account_id": input["account_id"], "request_id": input["request_id"], "result": "recorded-only"})
	}))
	t.Cleanup(provider.Close)
	var root string
	if nested {
		root = canonicalrouting.CopyNestedRecordedActivityReply(t, provider.URL)
	} else {
		root = canonicalrouting.CopyRecordedActivityReply(t, provider.URL)
	}
	h := newRuntimeHarnessForBackend(t, root, backend, true)
	step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "original-account", "request_id": "original-request"},
		eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
	h.publishAndWait(step, 20*time.Second)
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	var selected copiedActivityReplyStore = h.pg
	if h.sqlite != nil {
		selected = h.sqlite
	}
	cards, cursor, err := selected.ListDecisionCards(h.ctx, decisioncard.ListOptions{RunID: catalogRuntimeRunID, Limit: 10})
	if err != nil || cursor != "" || len(cards) != 1 || cards[0].Status != decisioncard.StatusPending || calls.Load() != 0 {
		t.Fatalf("real approval did not hold the original effect: cards=%+v cursor=%q calls=%d error=%v", cards, cursor, calls.Load(), err)
	}
	card, err := selected.GetDecisionCard(h.ctx, cards[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := selected.LoadProposedEffectContinuation(h.ctx, card.CardID)
	incomingRows, rowsErr := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
	input, inputFound, inputErr := selected.LoadPreparedPublishEvent(h.ctx, pending.SourceEventID)
	if err != nil || rowsErr != nil || inputErr != nil || !inputFound || len(incomingRows) != levels || len(input.DeliveryRoutes) != 1 || input.DeliveryRoutes[0].Context.ReplyContextID() != pending.ReplyContextID {
		t.Fatalf("pending effect lost its actual inbound reply before operator dispatch: continuation=%+v rows=%+v input=%+v errors=%v/%v/%v", pending, incomingRows, input, err, rowsErr, inputErr)
	}
	incoming, err := selected.LoadReplyContext(h.ctx, pending.ReplyContextID)
	if err != nil || incoming.State != replycontext.StateOpen || incoming.RunID != catalogRuntimeRunID {
		t.Fatalf("pending effect lacks its own open return row: %+v error=%v", incoming, err)
	}
	if nested {
		outer, err := selected.LoadReplyContext(h.ctx, incoming.EnclosingReplyContextID)
		if err != nil || outer.State != replycontext.StateOpen || outer.RunID != incoming.RunID || outer.EnclosingReplyContextID != "" ||
			incoming.RequesterFlowID != "provider" || incoming.ProviderFlowID != "t" || outer.ProviderFlowID != incoming.RequesterFlowID {
			t.Fatalf("two actual open levels do not share the exact enclosing continuation: inner=%+v outer=%+v error=%v", incoming, outer, err)
		}
	}
	decisionID, decidedAt := uuid.NewString(), time.Now().UTC()
	principal, err := selected.EnsureOperatorPrincipal(h.ctx, decidedAt)
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: card.CardID,
		RequestHash: "recorded-reply-" + decisionID, IdempotencyKey: uuid.NewString(), Now: decidedAt, TTL: time.Hour}
	decision := pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal.ID,
		ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: decisionID, Now: decidedAt})
	decisionCtx, cancelDecision := context.WithTimeout(correlation.WithRunID(h.ctx, catalogRuntimeRunID), 20*time.Second)
	defer cancelDecision()
	if _, replayed, err := h.rt.Pipeline.CommitDecisionCardMutation(decisionCtx, request, decision); err != nil || replayed {
		t.Fatalf("commit and dispatch the original approval: replay=%t error=%v", replayed, err)
	}
	h.waitForCatalogStoreQuiescence(20 * time.Second)
	cut, sourceFinished := "", 0
	for _, event := range catalogEventsSince(t, reader, h.startedAt) {
		if event.Name == "platform.activity_requested" {
			if cut != "" {
				t.Fatal("source performed more than one activity")
			}
			cut = event.ID
		}
		if event.Name == "request.finished" {
			sourceFinished++
		}
	}
	original, found, err := selected.LoadActivityAttempt(h.ctx, cut)
	if err != nil || !found || cut == "" || original.ReplyContextID != incoming.ID || original.Status != pipeline.ActivityAttemptStatusSucceeded || original.CompletedAt == nil || calls.Load() != 1 || sourceFinished != 1 {
		for _, event := range activityLineageEvents(t, h.ctx, h, catalogRuntimeRunID) {
			t.Logf("original publication phase: event=%s name=%s payload=%+v", event.EventID, event.EventName, event.Payload)
		}
		rows, _ := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
		t.Logf("original reply settlement phase: %+v", rows)
		t.Fatalf("real source did not record one complete reply-bearing result: journal=%+v found=%t calls=%d finished=%d error=%v", original, found, calls.Load(), sourceFinished, err)
	}
	sourceRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
	if err != nil || len(sourceRows) != levels {
		t.Fatalf("source's exact settled reply inventory: %+v error=%v", sourceRows, err)
	}
	sourceIDs := map[string]bool{}
	for _, row := range sourceRows {
		sourceIDs[row.ID] = true
		if row.State != replycontext.StateTerminal || row.AcceptedReplyEventID == "" {
			t.Fatalf("source did not settle each own level: %+v", row)
		}
	}
	if !sourceIDs[incoming.ID] {
		t.Fatal("original activity lost its exact source return row")
	}
	if nested {
		proveTwoLevelExecutedReplyRefusals(t, h, selected, sourceRows)
	}
	loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
	owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
	options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
	var operations []runfork.ForkOperationRequest
	if followup == copiedReplyStagedRestart {
		operations = append(operations, runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "nested-reply-restart",
			IdempotencyKey: uuid.NewString(), TransportHash: "nested-reply-restart", SourceRunID: catalogRuntimeRunID, ForkEventID: cut,
			TargetBundleHash: selection.BundleHash, AllowSourceFreeze: true, ContractSelection: selection})
	}
	staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut, operations...)
	t.Logf("fixed activity cut: point=%+v blockers=%+v", staged.ForkPoint, staged.ReplayResumeAdmission.UnsupportedBlockers)
	childRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, staged.ForkRunID)
	if err != nil || len(childRows) != levels {
		t.Fatalf("cut did not retain one open, independently owned child return: %+v error=%v", childRows, err)
	}
	childInner := replycontext.Record{}
	childByID := map[string]replycontext.Record{}
	for _, row := range childRows {
		if row.State != replycontext.StateOpen || row.RunID != staged.ForkRunID || sourceIDs[row.ID] {
			t.Fatalf("fork reused or settled an open level: %+v", row)
		}
		childByID[row.ID] = row
		if row.SourceRequestEventID == incoming.SourceRequestEventID {
			childInner = row
		}
	}
	if childInner.ID == "" || (nested && childByID[childInner.EnclosingReplyContextID].ProviderFlowID != childInner.RequesterFlowID) {
		t.Fatalf("child inner return lacks its own enclosing row: inner=%+v inventory=%+v", childInner, childRows)
	}
	if nested {
		outer := childByID[childInner.EnclosingReplyContextID]
		if sourceIDs[outer.ID] || outer.RunID != childInner.RunID || outer.EnclosingReplyContextID != "" || childInner.ProviderFlowID != "t" {
			t.Fatalf("inner projected return borrowed a source or foreign enclosing level: inner=%+v outer=%+v", childInner, outer)
		}
	}
	if followup == copiedReplyContextOnlyOpen {
		proveNativeNestedContextOnlyRefork(t, h, selected, staged.ForkRunID, childRows, "", owner, loader, selection, options)
		return
	}
	if followup == copiedReplyStagedRestart {
		h, owner = restartNestedReplyChild(t, h, staged, childRows, operations[0], step)
		selected = h.pg
		if h.sqlite != nil {
			selected = h.sqlite
		}
		reader, err = h.catalogOperatorEventLister()
		if err != nil {
			t.Fatal(err)
		}
		loader = runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootFromCatalogE2E(t), PlatformSpecPath: platformSpecPathFromCatalogE2E(t), Store: selected}
		options = selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
	} else {
		executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
			SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
		})
		if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
			t.Fatalf("activate real recorded-result child: %+v error=%v", executed, err)
		}
		t.Logf("acknowledged child: run=%s activated=%t", executed.Materialization.ForkRunID, executed.Activation.Activated)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	waitCopiedActivityRequesterReturn(t, ctx, selected, staged.ForkRunID)
	attempts, err := storetest.ReadActivityAttemptStorage(ctx, selected, staged.ForkRunID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("child copied-journal inventory: %+v error=%v", attempts, err)
	}
	copied, found, err := selected.LoadActivityAttempt(ctx, attempts[0].RequestEventID)
	if err != nil || !found || copied.ReplyContextID != childInner.ID || copied.RequestEventID == original.RequestEventID || copied.InputHash != original.InputHash || !reflect.DeepEqual(copied.ResultPayload, original.ResultPayload) || !copied.StartedAt.Equal(original.StartedAt) || copied.CompletedAt == nil || !copied.CompletedAt.Equal(*original.CompletedAt) {
		t.Fatalf("copy/publish hid an empty or changed recorded carrier: copied=%+v error=%v", copied, err)
	}
	result, found, err := selected.LoadPreparedPublishEvent(ctx, copied.ResultEventID)
	if err != nil || !found || len(result.DeliveryRoutes) != 1 || result.DeliveryRoutes[0].Context.ReplyContextID() != childInner.ID {
		t.Fatalf("actual recorded result lost its child continuation: %+v found=%t error=%v", result, found, err)
	}
	settled, err := selected.LoadReplyContext(ctx, childInner.ID)
	if err != nil || settled.State != replycontext.StateTerminal || settled.AcceptedReplyEventID == "" || settled.RunID != staged.ForkRunID {
		t.Fatalf("actual provider return failed to settle the own child row: %+v error=%v", settled, err)
	}
	returned, found, err := selected.LoadPreparedPublishEvent(ctx, settled.AcceptedReplyEventID)
	if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != settled.Origin || returned.DeliveryRoutes[0].Context.ReplyContextID() != settled.EnclosingReplyContextID {
		t.Fatalf("actual requester return borrowed another target/context: %+v found=%t error=%v", returned, found, err)
	}
	for _, row := range childRows {
		terminal, err := selected.LoadReplyContext(ctx, row.ID)
		if err != nil || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID == "" || terminal.EnclosingReplyContextID != row.EnclosingReplyContextID {
			t.Fatalf("an open child level did not settle its own original return: %+v error=%v", terminal, err)
		}
		event, found, err := selected.LoadPreparedPublishEvent(ctx, terminal.AcceptedReplyEventID)
		if err != nil || !found || len(event.DeliveryRoutes) != 1 || event.DeliveryRoutes[0].Target.Route() != terminal.Origin || event.DeliveryRoutes[0].Context.ReplyContextID() != terminal.EnclosingReplyContextID {
			t.Fatalf("a nested level did not restore its own enclosing binding: terminal=%+v event=%+v found=%t error=%v", terminal, event, found, err)
		}
	}
	finished := 0
	for _, event := range activityLineageEvents(t, ctx, h, staged.ForkRunID) {
		if event.EventName == "request.finished" {
			finished++
			if event.Payload["request_id"] != "original-request" || event.Payload["result"] != "recorded-only" {
				t.Fatalf("final requester received different business data: %+v", event)
			}
		}
	}
	afterJournal, found, err := selected.LoadActivityAttempt(ctx, original.RequestEventID)
	afterRows, rowsErr := storetest.ReadReplyContextRecords(ctx, reader, catalogRuntimeRunID)
	if err != nil || rowsErr != nil || !found || !reflect.DeepEqual(afterJournal, original) || !reflect.DeepEqual(afterRows, sourceRows) || calls.Load() != 1 || finished != 1 {
		t.Fatalf("child redispatched, missed final return or changed source: calls=%d child_finished=%d source_found=%t errors=%v/%v", calls.Load(), finished, found, err, rowsErr)
	}
	if followup == copiedReplyContextOnlyTerminal {
		terminalRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
		if err != nil || len(terminalRows) != 2 {
			t.Fatalf("terminal context-only source rows: %+v error=%v", terminalRows, err)
		}
		proveNativeNestedContextOnlyRefork(t, h, selected, staged.ForkRunID, terminalRows, copied.SourceEventID, owner, loader, selection, options)
		return
	}
	if followup != copiedReplyNoFollowup {
		if followup == copiedReplyTerminalHistory {
			proveCopiedActivityTerminalHistory(t, h, selected, staged.ForkRunID, copied, loader, selection, owner, options)
		} else if followup == copiedReplySibling {
			proveCopiedTwoLevelReplySibling(t, h, selected, original, sourceRows, staged.ForkRunID, loader, selection, owner, options)
		} else {
			atResult := followup == copiedReplyReforkResult || followup == copiedReplyRepeatedResult
			nextRun, nextJournal, nextResult := proveCopiedTwoLevelReplyRefork(t, h, selected, staged.ForkRunID, copied, copied.ResultEventID, loader, selection, owner, options, atResult)
			if followup == copiedReplyRepeatedResult {
				proveCopiedTwoLevelReplyRefork(t, h, selected, nextRun, nextJournal, nextResult, loader, selection, owner, options, true)
			}
		}
		finalCtx, cancelFinal := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
		defer cancelFinal()
		afterRows, rowsErr := storetest.ReadReplyContextRecords(finalCtx, selected, catalogRuntimeRunID)
		afterJournal, found, err := selected.LoadActivityAttempt(finalCtx, original.RequestEventID)
		if err != nil || rowsErr != nil || !found || !reflect.DeepEqual(afterRows, sourceRows) || !reflect.DeepEqual(afterJournal, original) || calls.Load() != 1 {
			t.Fatalf("refork redispatched or changed A: calls=%d errors=%v/%v", calls.Load(), err, rowsErr)
		}
	}
}

func proveCopiedTwoLevelReplySibling(t *testing.T, h *runtimeHarness, selected copiedActivityReplyStore, sourceAttempt pipeline.ActivityAttemptRecord,
	sourceRows []replycontext.Record, firstChild string, loader runforkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection, owner runforkexecution.SelectedContractExecutionOwner, options runforkexecution.SelectedContractAgentRuntimeOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	firstRows, err := storetest.ReadReplyContextRecords(ctx, selected, firstChild)
	if err != nil || len(firstRows) != 2 {
		t.Fatalf("sibling lacks its already-settled first child: %+v error=%v", firstRows, err)
	}
	operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "catalog-reply-sibling", IdempotencyKey: uuid.NewString(),
		TransportHash: "copied-reply-sibling", SourceRunID: catalogRuntimeRunID, ForkEventID: sourceAttempt.RequestEventID,
		TargetBundleHash: selection.BundleHash, AllowSourceFreeze: true, ContractSelection: selection}
	prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceRunID: catalogRuntimeRunID,
		At: sourceAttempt.RequestEventID, ForkOperation: &operation, AllowSourceFreeze: true, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
	if err != nil {
		t.Fatalf("prepare the sibling's exact durable operation: %v", err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Errorf("join sibling preparation: %v", err)
		}
	}()
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	request.ForkOperation = &operation
	staged, err := selected.MaterializeRunForkForSelectedContractExecution(correlation.WithSourceArtifactFact(ctx, request.SourceArtifactFact), request)
	if err != nil || staged.ForkRunID == "" || staged.ForkRunID == firstChild || staged.SourceRunID != catalogRuntimeRunID {
		t.Fatalf("distinct operation reused the first child: %+v error=%v", staged, err)
	}
	childRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || len(childRows) != 2 {
		t.Fatalf("sibling open reply inventory: %+v error=%v", childRows, err)
	}
	inner := requireReforkedOpenReplyRows(t, sourceRows, childRows, staged.ForkRunID)
	for _, row := range childRows {
		for _, first := range firstRows {
			if row.ID == first.ID || row.EnclosingReplyContextID == first.ID {
				t.Fatalf("sibling borrowed the first child's reply authority: %+v", row)
			}
		}
	}
	executed, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: catalogRuntimeRunID, At: sourceAttempt.RequestEventID, ForkOperation: &operation, AllowSourceFreeze: true,
		Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("activate the exact sibling operation: %+v error=%v", executed, err)
	}
	returnCtx, cancelReturn := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancelReturn()
	waitCopiedActivityRequesterReturn(t, returnCtx, selected, staged.ForkRunID)
	attempts, err := storetest.ReadActivityAttemptStorage(returnCtx, selected, staged.ForkRunID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("sibling lost or duplicated its own journal: %+v error=%v", attempts, err)
	}
	journal, found, err := selected.LoadActivityAttempt(returnCtx, attempts[0].RequestEventID)
	if err != nil || !found || journal.ReplyContextID != inner.ID || journal.InputHash != sourceAttempt.InputHash ||
		!reflect.DeepEqual(journal.ResultPayload, sourceAttempt.ResultPayload) || !journal.StartedAt.Equal(sourceAttempt.StartedAt) ||
		journal.CompletedAt == nil || !journal.CompletedAt.Equal(*sourceAttempt.CompletedAt) {
		t.Fatalf("sibling changed or borrowed its recorded result: %+v found=%t error=%v", journal, found, err)
	}
	requireReforkedReplyReturns(t, returnCtx, h, selected, childRows, catalogRuntimeRunID, sourceAttempt, journal, inner.ID, false, sourceAttempt.ResultEventID)
	afterFirst, err := storetest.ReadReplyContextRecords(returnCtx, selected, firstChild)
	firstFinished, countErr := storetest.ReadLifecycleEventCardinality(returnCtx, selected, firstChild, "request.finished")
	if err != nil || countErr != nil || !reflect.DeepEqual(afterFirst, firstRows) || firstFinished != 1 {
		t.Fatalf("sibling changed or repeated first-child settlement: rows=%+v finished=%d errors=%v/%v", afterFirst, firstFinished, err, countErr)
	}
}

func proveCopiedTwoLevelReplyRefork(t *testing.T, h *runtimeHarness, selected copiedActivityReplyStore, sourceRun string, sourceAttempt pipeline.ActivityAttemptRecord,
	sourceResultID string, loader runforkexecution.SelectedContractSourceLoader, selection runfork.RunForkContractSelection, owner runforkexecution.SelectedContractExecutionOwner, options runforkexecution.SelectedContractAgentRuntimeOptions, atResult bool) (string, pipeline.ActivityAttemptRecord, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	beforeRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || len(beforeRows) != 2 {
		t.Fatalf("refork lacks its immediate source's exact reply inventory: %+v error=%v", beforeRows, err)
	}
	for _, row := range beforeRows {
		if row.State != replycontext.StateTerminal {
			t.Fatalf("refork started before the source return completed: %+v", row)
		}
	}
	// The copied journal's request coordinate is not a physical publication in B.
	// Only B's actual inherited request or result is a lawful publication cut.
	_, requestPublished, requestErr := selected.LoadPreparedPublishEvent(ctx, sourceAttempt.RequestEventID)
	sourceResult, resultPublished, resultErr := selected.LoadPreparedPublishEvent(ctx, sourceResultID)
	if requestErr != nil || requestPublished || resultErr != nil || !resultPublished || len(sourceResult.DeliveryRoutes) != 1 ||
		sourceResult.Event.Event().RunID() != sourceRun || string(sourceResult.Event.Event().Type()) != sourceAttempt.ResultEventType || sourceResult.DeliveryRoutes[0].Context.ReplyContextID() != sourceAttempt.ReplyContextID {
		t.Fatalf("refork did not select an actual result-only cut: result=%+v request_published=%t errors=%v/%v", sourceResult, requestPublished, requestErr, resultErr)
	}
	cut := sourceResultID
	if !atResult {
		cut = sourceAttempt.SourceEventID
		request, found, err := selected.LoadPreparedPublishEvent(ctx, cut)
		if err != nil || !found || request.Event.Event().Type() != "platform.activity_requested" || request.Event.Event().RunID() != sourceRun {
			t.Fatalf("refork lacks B's actual inherited activity request: %+v found=%t error=%v", request, found, err)
		}
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
	if err != nil {
		t.Fatalf("read the exact refork pending-input cut: %v", err)
	}
	pendingInputs := 0
	for _, work := range plan.PendingWork {
		if work.EventName == "platform.activity_requested" && work.Classification == runfork.RunForkPendingClassificationPending {
			pendingInputs++
		}
	}
	if pendingInputs > 1 || (!atResult && pendingInputs != 1) {
		t.Fatalf("refork has an unexpected exact pending-input inventory: %+v", plan.PendingWork)
	}
	staged := stageCatalogSelectedContractFork(t, ctx, selected, owner, loader, selection, options, sourceRun, cut)
	t.Logf("physical result refork: source=%s cut=%s child=%s pending_inputs=%d", sourceRun, cut, staged.ForkRunID, pendingInputs)
	grandchildRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || len(grandchildRows) != 2 || staged.SourceRunID != sourceRun || staged.ForkRunID == sourceRun {
		t.Fatalf("refork did not use the immediate source's two open levels: %+v rows=%+v error=%v", staged, grandchildRows, err)
	}
	inner := requireReforkedOpenReplyRows(t, beforeRows, grandchildRows, staged.ForkRunID)
	executed, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("activate exact grandchild activity continuation: %+v error=%v", executed, err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 10*time.Second)
			defer cancel()
			diagnostic, err := storetest.ReadSelectedForkLifecycleDiagnosticStorage(cleanup, selected, staged.ForkRunID)
			t.Logf("exact refork terminal receipts: %+v error=%v", diagnostic.Receipts, err)
			for _, log := range diagnostic.Logs {
				t.Logf("exact refork runtime log: %s", log.Payload)
			}
			t.Logf("exact retained refork failure/cleanup join: %v", owner.RetireSelectedContexts(cleanup))
		}
	})
	returnCtx, cancelReturn := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancelReturn()
	waitCopiedActivityRequesterReturn(t, returnCtx, selected, staged.ForkRunID)
	attempts, err := storetest.ReadActivityAttemptStorage(returnCtx, selected, staged.ForkRunID)
	if err != nil || len(attempts) != pendingInputs {
		t.Fatalf("grandchild did not retain exactly the admitted pending activity journal: %+v error=%v", attempts, err)
	}
	var journal pipeline.ActivityAttemptRecord
	if pendingInputs != 0 {
		var found bool
		journal, found, err = selected.LoadActivityAttempt(returnCtx, attempts[0].RequestEventID)
		if err != nil || !found || journal.ReplyContextID != inner.ID || journal.RequestEventID == sourceAttempt.RequestEventID || journal.InputHash != sourceAttempt.InputHash ||
			!reflect.DeepEqual(journal.ResultPayload, sourceAttempt.ResultPayload) || !journal.StartedAt.Equal(sourceAttempt.StartedAt) ||
			journal.CompletedAt == nil || sourceAttempt.CompletedAt == nil || !journal.CompletedAt.Equal(*sourceAttempt.CompletedAt) {
			t.Fatalf("grandchild changed or borrowed the recorded result: %+v found=%t error=%v", journal, found, err)
		}
	}
	physicalResult := requireReforkedReplyReturns(t, returnCtx, h, selected, grandchildRows, sourceRun, sourceAttempt, journal, inner.ID, atResult, sourceResultID)
	afterRows, rowsErr := storetest.ReadReplyContextRecords(returnCtx, selected, sourceRun)
	afterAttempt, found, attemptErr := selected.LoadActivityAttempt(returnCtx, sourceAttempt.RequestEventID)
	sourceFinished, countErr := storetest.ReadLifecycleEventCardinality(returnCtx, selected, sourceRun, "request.finished")
	if rowsErr != nil || attemptErr != nil || countErr != nil || !found || !reflect.DeepEqual(afterRows, beforeRows) || !reflect.DeepEqual(afterAttempt, sourceAttempt) || sourceFinished != 1 {
		t.Fatalf("grandchild changed B or duplicated its final return: source_finished=%d errors=%v/%v/%v", sourceFinished, rowsErr, attemptErr, countErr)
	}
	return staged.ForkRunID, journal, physicalResult
}

func requireReforkedOpenReplyRows(t *testing.T, sourceRows, childRows []replycontext.Record, childRun string) replycontext.Record {
	t.Helper()
	byID := map[string]replycontext.Record{}
	inner := replycontext.Record{}
	for _, row := range childRows {
		if row.RunID != childRun || row.State != replycontext.StateOpen || row.AcceptedReplyEventID != "" || row.SourceRunID != catalogRuntimeRunID {
			t.Fatalf("refork borrowed executable or original evidence ownership: %+v", row)
		}
		matched := false
		for _, source := range sourceRows {
			if row.SourceRequestEventID == source.SourceRequestEventID {
				matched = true
				if row.ID == source.ID || row.RequestEventID == source.RequestEventID ||
					row.RequesterFlowID != source.RequesterFlowID || row.ProviderFlowID != source.ProviderFlowID || row.RequestCorrelationID != source.RequestCorrelationID {
					t.Fatalf("refork did not project the exact immediate-source row: source=%+v child=%+v", source, row)
				}
			}
		}
		if !matched {
			t.Fatalf("grandchild invented an original request: %+v", row)
		}
		byID[row.ID] = row
		if row.ProviderFlowID == "t" {
			inner = row
		}
	}
	outer := byID[inner.EnclosingReplyContextID]
	if inner.ID == "" || outer.ID == "" || outer.RunID != inner.RunID || outer.EnclosingReplyContextID != "" || outer.ProviderFlowID != inner.RequesterFlowID {
		t.Fatalf("refork failed to project the enclosing link to its own row: inner=%+v outer=%+v", inner, outer)
	}
	return inner
}

func requireReforkedReplyReturns(t *testing.T, ctx context.Context, h *runtimeHarness, selected copiedActivityReplyStore, childRows []replycontext.Record,
	sourceRun string, sourceAttempt, journal pipeline.ActivityAttemptRecord, innerID string, atResult bool, sourceResultID string) string {
	t.Helper()
	childRun := childRows[0].RunID
	for _, row := range childRows {
		terminal, err := selected.LoadReplyContext(ctx, row.ID)
		if err != nil || terminal.State != replycontext.StateTerminal || terminal.EnclosingReplyContextID != row.EnclosingReplyContextID {
			t.Fatalf("grandchild did not settle its own level: %+v error=%v", terminal, err)
		}
		returned, found, err := selected.LoadPreparedPublishEvent(ctx, terminal.AcceptedReplyEventID)
		if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != terminal.Origin || returned.DeliveryRoutes[0].Context.ReplyContextID() != terminal.EnclosingReplyContextID {
			t.Fatalf("grandchild return used a source/enclosing/sibling binding: %+v event=%+v found=%t error=%v", terminal, returned, found, err)
		}
	}
	expectedPayload, err := canonicaljson.MarshalPreservingNumberKinds(sourceAttempt.ResultPayload)
	if err != nil {
		t.Fatal(err)
	}
	finished, results, physicalResult := 0, 0, ""
	for _, event := range activityLineageEvents(t, ctx, h, childRun) {
		if event.EventName == sourceAttempt.ResultEventType {
			results++
			result, found, err := selected.LoadPreparedPublishEvent(ctx, event.EventID)
			lineage, hasLineage := result.Event.Event().SelectedForkLineage()
			if err != nil || !found || (atResult && (!hasLineage || lineage.SourceRunID() != sourceRun || lineage.SourceEventID() != sourceResultID)) ||
				(!atResult && result.Event.Event().ID() != journal.ResultEventID) || !bytes.Equal(result.Event.Event().Payload(), expectedPayload) ||
				len(result.DeliveryRoutes) != 1 || result.DeliveryRoutes[0].Context.ReplyContextID() != innerID {
				t.Fatalf("grandchild did not continue the exact result through its own inner row: %+v found=%t error=%v", result, found, err)
			}
			physicalResult = result.Event.Event().ID()
		}
		if event.EventName == "request.finished" {
			finished++
			if event.Payload["request_id"] != "original-request" || event.Payload["result"] != "recorded-only" {
				t.Fatalf("grandchild requester received different business data: %+v", event)
			}
		}
	}
	if finished != 1 || results != 1 {
		t.Fatalf("grandchild missed/duplicated the final return or recorded result: finished=%d results=%d", finished, results)
	}
	return physicalResult
}

func proveTwoLevelExecutedReplyRefusals(t *testing.T, h *runtimeHarness, owner interface {
	bus.PreparedPublishEventReader
	replycontext.Store
}, rows []replycontext.Record) {
	t.Helper()
	for _, row := range rows {
		accepted, found, err := owner.LoadPreparedPublishEvent(h.ctx, row.AcceptedReplyEventID)
		if err != nil || !found {
			t.Fatalf("refusal lacks its actual accepted publication: %+v found=%t error=%v", row, found, err)
		}
		for _, target := range rows {
			name, wantClass, wantCode := "second/"+row.ProviderFlowID, failures.ClassReplyAlreadyTerminal, "reply_already_terminal"
			if target.ID != row.ID {
				name, wantClass, wantCode = "wrong_level/"+row.ProviderFlowID+"_to_"+target.ProviderFlowID, failures.ClassStaleArrival, "stale_arrival"
			}
			t.Run(name, func(t *testing.T) {
				original := accepted.Event.Event()
				event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), original.Type(), original.Producer(), "", original.Payload(), original.ChainDepth(),
					events.LineageFromEvent(original), events.EnvelopeForSourceRoute(events.EventEnvelope{}, original.Envelope().Source), original.RoutingSource(), time.Now().UTC())
				ctx, cancel := context.WithTimeout(events.WithDeliveryContext(h.ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: target.ID}}), 20*time.Second)
				defer cancel()
				// Execute publication and its durable terminal refusal, not a preview.
				if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
					t.Fatalf("publish the second/wrong-level response: %v", err)
				}
				stored, found, err := owner.LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !found || len(stored.DeliveryRoutes) != 0 {
					t.Fatalf("refused response created executable recipients: %+v found=%t error=%v", stored, found, err)
				}
				receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, owner, event.ID())
				count, countErr := storetest.CountDeadLettersForOriginalEvent(ctx, owner, event.ID())
				var failure failures.Envelope
				decodeErr := json.Unmarshal(receipt.Failure, &failure)
				if err != nil || countErr != nil || decodeErr != nil || !receipt.Found || receipt.Outcome != "dead_letter" || failure.Class != wantClass || failure.Detail.Code != wantCode || count != 1 {
					t.Fatalf("publication did not settle the exact refusal: outcome=%+v failure=%+v dead_letters=%d errors=%v/%v/%v want=%s/%s", receipt, failure, count, err, countErr, decodeErr, wantClass, wantCode)
				}
				after, err := storetest.ReadReplyContextRecords(ctx, owner, catalogRuntimeRunID)
				finished, finishedErr := storetest.ReadLifecycleEventCardinality(ctx, owner, catalogRuntimeRunID, "request.finished")
				if err != nil || finishedErr != nil || !reflect.DeepEqual(rows, after) || finished != 1 {
					t.Fatalf("executed refusal changed a level or completed the requester again: finished=%d errors=%v/%v", finished, err, finishedErr)
				}
			})
		}
	}
}

func waitCopiedActivityRequesterReturn(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, runID, "request.finished")
		if err != nil {
			logCopiedActivityReturnPhase(t, ctx, selected, runID)
			t.Fatalf("read actual child requester completion: %v", err)
		}
		if count == 1 {
			return
		}
		if count > 1 {
			t.Fatalf("child produced %d requester completions", count)
		}
		run, err := storetest.ReadRuntimeLogRunSnapshot(ctx, selected, runID)
		if err != nil {
			t.Fatalf("read requester return lifecycle: %v", err)
		}
		if run.State.Terminal() {
			// Completion can commit between the two reads above. Confirm the
			// durable result again before classifying a terminal run as failure.
			final, readErr := storetest.ReadLifecycleEventCardinality(ctx, selected, runID, "request.finished")
			if readErr != nil {
				t.Fatalf("read terminal requester completion: %v", readErr)
			}
			if final == 1 {
				return
			}
			logCopiedActivityReturnPhase(t, ctx, selected, runID)
			t.Fatalf("child terminalized without its requester return: state=%s failure=%+v", run.State, run.Failure)
		}
		select {
		case <-ctx.Done():
			logCopiedActivityReturnPhase(t, ctx, selected, runID)
			t.Fatalf("child did not publish its own requester return within the original bound: %v", context.Cause(ctx))
		case <-tick.C:
		}
	}
}

func logCopiedActivityReturnPhase(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	storage, storageErr := storetest.ReadSelectedExecutionStorage(read, selected, runID)
	attempts, attemptsErr := storetest.ReadActivityAttemptStorage(read, selected, runID)
	t.Logf("child return phase: storage=%+v attempts=%+v read_errors=%v/%v", storage, attempts, storageErr, attemptsErr)
	run, runErr := storetest.ReadRuntimeLogRunSnapshot(read, selected, runID)
	if runErr == nil {
		t.Logf("child terminal phase: state=%s failure=%+v", run.State, run.Failure)
	} else {
		t.Logf("child terminal phase read failed: %v", runErr)
	}
}
