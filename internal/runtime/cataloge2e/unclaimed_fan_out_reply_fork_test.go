package cataloge2e

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type unclaimedFanOutReplyStore interface {
	runforkexecution.SelectedContractForkLifecycle
	runforkexecution.SourceArtifactSelectedContractSourceStore
	storetest.DurableDataCatalogStore
	replycontext.Store
	bus.PreparedPublishEventReader
	deliverylifecycle.Store
	ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
}

func TestIssue642TwoOpenRepliesUnclaimedFanOutInputBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyNestedFanOutReply(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			cut, sourceRows := prepareUnclaimedFanOutReplySource(t, h, selected)
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			childRows, inner := requireUnclaimedFanOutReplyStaging(t, h.ctx, selected, staged.ForkRunID, cut)
			t.Cleanup(func() { logUnclaimedFanOutReplyFailure(t, h.ctx, selected, owner, staged.ForkRunID) })
			executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate the unclaimed inner request without a shortcut: activated=%t child=%s expected=%s error=%v", executed.Activation.Activated, executed.Materialization.ForkRunID, staged.ForkRunID, err)
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			waitCopiedActivityRequesterReturn(t, ctx, selected, staged.ForkRunID)
			requireUnclaimedFanOutReplyReturn(t, ctx, selected, staged.ForkRunID, cut, inner, childRows, sourceRows)
		})
	}
}

// This isolates publication admission from the still-required full fan-out
// return above. A delivered request is not credit for its deferred ordinal.
func TestIssue642InheritedRequestPublicationReusesBothOpenRowsBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyNestedFanOutReply(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			cut, sourceRows := prepareUnclaimedFanOutReplySource(t, h, selected)
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			childRows, inner := requireUnclaimedFanOutReplyStaging(t, h.ctx, selected, staged.ForkRunID, cut)
			t.Cleanup(func() { logUnclaimedFanOutReplyFailure(t, h.ctx, selected, owner, staged.ForkRunID) })
			executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate actual inherited request: result=%+v error=%v", executed.Activation, err)
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			input := waitUnclaimedReplyRequestDelivery(t, ctx, selected, staged.ForkRunID, cut, inner.ID)
			deliveryID, err := deliverylifecycle.DeliveryID(input.Event.ID(), input.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := selected.Snapshot(ctx, deliveryID)
			if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.ClaimVersion != 1 {
				t.Fatalf("continued input did not settle once: delivery=%+v error=%v", delivery, err)
			}
			rows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
			if err != nil || len(rows) != len(childRows) {
				t.Fatalf("request publication recreated its inherited rows: rows=%+v error=%v", rows, err)
			}
			for i, row := range rows {
				if err := row.ValidateContinuationFrom(childRows[i], childRows[i].CreatedAt); err != nil {
					t.Fatalf("request publication changed its inherited binding or regressed settlement: %v", err)
				}
			}
			original, err := storetest.ReadReplyContextRecords(ctx, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(original, sourceRows) {
				t.Fatalf("request publication rewrote source replies: %v", err)
			}
			cleanup, stop := context.WithTimeout(context.WithoutCancel(h.ctx), 10*time.Second)
			defer stop()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitUnclaimedReplyRequestDelivery(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, child, cut, replyID string) bus.PreparedPublishEvent {
	t.Helper()
	inputID := activityidentity.ForkLineageEventID(child, cut)
	for {
		input, found, err := selected.LoadPreparedPublishEvent(ctx, inputID)
		if err != nil {
			t.Fatalf("read inherited request publication: %v", err)
		}
		if found {
			if len(input.DeliveryRoutes) != 1 || input.DeliveryRoutes[0].Context.ReplyContextID() != replyID {
				t.Fatalf("continued request changed complete own route: %+v", input.DeliveryRoutes)
			}
			id, err := deliverylifecycle.DeliveryID(inputID, input.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := selected.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if delivery.Status == deliverylifecycle.StatusDelivered {
				return input
			}
			if delivery.Terminal() {
				t.Fatalf("continued request terminally refused: %+v", delivery)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("original request delivery bound: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func prepareUnclaimedFanOutReplySource(t *testing.T, h *runtimeHarness, selected unclaimedFanOutReplyStore) (string, []replycontext.Record) {
	t.Helper()
	h.publishAndWait(catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "same-key", "items": []any{"same-key"}},
		eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, 20*time.Second)
	h.waitForCatalogStoreQuiescence(20 * time.Second)
	cut := ""
	for _, event := range activityLineageEvents(t, h.ctx, h, catalogRuntimeRunID) {
		if !strings.HasSuffix(event.EventName, "/t.requested") {
			continue
		}
		if cut != "" {
			t.Fatal("source produced two inner request cuts")
		}
		cut = event.EventID
	}
	rows, err := storetest.ReadReplyContextRecords(h.ctx, selected, catalogRuntimeRunID)
	if err != nil || cut == "" || len(rows) != 2 {
		t.Fatalf("actual source did not publish two own request levels: cut=%s rows=%+v error=%v", cut, rows, err)
	}
	for _, row := range rows {
		if row.State != replycontext.StateTerminal || row.AcceptedReplyEventID == "" {
			t.Fatalf("source failed to return before the historical proof: %+v", row)
		}
	}
	intents, err := selected.ListFanOutIntents(h.ctx, fanoutobligation.ListQuery{RunID: catalogRuntimeRunID, Limit: 10})
	if err != nil || len(intents.Intents) != 1 || intents.Intents[0].Status != fanoutobligation.StatusClosed || intents.Intents[0].Cursor != 1 {
		t.Fatalf("source did not drain exactly one actual ordinal: %+v error=%v", intents, err)
	}
	plan, err := selected.PlanRunFork(h.ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: cut})
	if err != nil {
		t.Fatal(err)
	}
	pending := 0
	for _, work := range plan.PendingWork {
		if work.EventID != cut {
			continue
		}
		pending++
		if work.SubscriberType != "node" || work.Classification != runfork.RunForkPendingClassificationPending {
			t.Fatalf("cut is not the actual unclaimed inner handler: %+v", work)
		}
	}
	if pending != 1 {
		t.Fatalf("cut has %d pending inner recipients, want one", pending)
	}
	return cut, rows
}

func requireUnclaimedFanOutReplyStaging(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, child, cut string) ([]replycontext.Record, replycontext.Record) {
	t.Helper()
	rows, err := storetest.ReadReplyContextRecords(ctx, selected, child)
	intents, intentErr := selected.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: child, Limit: 10})
	if err != nil || intentErr != nil || len(rows) != 2 || len(intents.Intents) != 0 {
		t.Fatalf("inner publication cut included later fan-out work or lost returns: rows=%+v intents=%+v errors=%v/%v", rows, intents, err, intentErr)
	}
	byID := map[string]replycontext.Record{}
	var inner replycontext.Record
	for _, row := range rows {
		if row.State != replycontext.StateOpen || row.RunID != child || row.AcceptedReplyEventID != "" {
			t.Fatalf("unclaimed request cut borrowed a later settlement: %+v", row)
		}
		byID[row.ID] = row
		if row.ProviderFlowID == "t" {
			inner = row
		}
	}
	outer, found := byID[inner.EnclosingReplyContextID]
	if !found || outer.ProviderFlowID != inner.RequesterFlowID || outer.EnclosingReplyContextID != "" {
		t.Fatalf("inner cut lost its exact child enclosing return: inner=%+v outer=%+v", inner, outer)
	}
	for _, id := range []string{inner.RequestEventID, activityidentity.ForkLineageEventID(child, cut)} {
		if _, found, err := selected.LoadPreparedPublishEvent(ctx, id); err != nil || found {
			t.Fatalf("reply staging fabricated an executable request publication %s: found=%t error=%v", id, found, err)
		}
	}
	return rows, inner
}

func requireUnclaimedFanOutReplyReturn(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, child, cut string, inner replycontext.Record, childRows, sourceRows []replycontext.Record) {
	t.Helper()
	inputID := activityidentity.ForkLineageEventID(child, cut)
	input, found, err := selected.LoadPreparedPublishEvent(ctx, inputID)
	if err != nil || !found || len(input.DeliveryRoutes) != 1 || input.DeliveryRoutes[0].Context.ReplyContextID() != inner.ID {
		t.Fatalf("independently owed child input was not published exactly once: routes=%+v found=%t error=%v", input.DeliveryRoutes, found, err)
	}
	deliveryID, err := deliverylifecycle.DeliveryID(inputID, input.DeliveryRoutes[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range childRows {
		terminal, err := selected.LoadReplyContext(ctx, row.ID)
		if err != nil || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID == "" || terminal.EnclosingReplyContextID != row.EnclosingReplyContextID {
			t.Fatalf("unclaimed inner execution missed an own return: %+v error=%v", terminal, err)
		}
		returned, found, err := selected.LoadPreparedPublishEvent(ctx, terminal.AcceptedReplyEventID)
		if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != terminal.Origin || returned.DeliveryRoutes[0].Context.ReplyContextID() != terminal.EnclosingReplyContextID {
			t.Fatalf("return borrowed another level's binding: %+v found=%t error=%v", returned, found, err)
		}
	}
	after, afterErr := storetest.ReadReplyContextRecords(ctx, selected, catalogRuntimeRunID)
	intent, intentErr := selected.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: child, Limit: 10})
	if afterErr != nil || intentErr != nil || !reflect.DeepEqual(after, sourceRows) || len(intent.Intents) != 1 || intent.Intents[0].Cursor != 1 || intent.Intents[0].Status != fanoutobligation.StatusClosed {
		t.Fatalf("child lost deferred progress or changed source: child=%+v errors=%v/%v", intent, afterErr, intentErr)
	}
	settled, err := selected.Snapshot(ctx, deliveryID)
	if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != 1 {
		t.Fatalf("inner request did not execute exactly once: %+v error=%v", settled, err)
	}
}

func logUnclaimedFanOutReplyFailure(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, owner runforkexecution.SelectedContractExecutionOwner, child string) {
	t.Helper()
	if !t.Failed() {
		return
	}
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	proof, err := storetest.ReadSelectedForkLifecycleDiagnosticStorage(read, selected, child)
	if err != nil {
		t.Logf("unclaimed-inner diagnostic read: %v", err)
	}
	for _, log := range proof.Logs {
		t.Logf("unclaimed-inner native diagnostic: %s", log.Payload)
	}
	inputID := ""
	rows, rowsErr := storetest.ReadReplyContextRecords(read, selected, child)
	t.Logf("unclaimed-inner child reply rows: %+v error=%v", rows, rowsErr)
	for _, row := range rows {
		if row.ProviderFlowID == "t" {
			inputID = activityidentity.ForkLineageEventID(child, row.SourceRequestEventID)
		}
	}
	intents, intentsErr := selected.ListFanOutIntents(read, fanoutobligation.ListQuery{RunID: child, Limit: 10})
	t.Logf("unclaimed-inner child intents: %+v error=%v", intents, intentsErr)
	if inputID != "" {
		deliveries, deliveryErr := storetest.ReadEventDeliveryDiagnosticRows(read, selected, inputID)
		receipt, receiptErr := storetest.ReadExactPipelineReceiptOutcomeReason(read, selected, inputID)
		t.Logf("unclaimed-inner input deliveries=%+v receipt=%+v errors=%v/%v", deliveries, receipt, deliveryErr, receiptErr)
	}
	outcomes, outcomesErr := storetest.ReadFanOutPublishedOutcomeStorage(read, selected, child)
	t.Logf("unclaimed-inner child outcomes: %+v error=%v", outcomes, outcomesErr)
	for _, outcome := range outcomes {
		deliveries, deliveryErr := storetest.ReadEventDeliveryDiagnosticRows(read, selected, outcome.EventID)
		receipt, receiptErr := storetest.ReadExactPipelineReceiptOutcomeReason(read, selected, outcome.EventID)
		t.Logf("unclaimed-inner outcome deliveries=%+v receipt=%+v errors=%v/%v", deliveries, receipt, deliveryErr, receiptErr)
	}
	t.Logf("unclaimed-inner selected owner cleanup/cause: %v", owner.RetireSelectedContexts(read))
}
