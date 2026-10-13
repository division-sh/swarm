package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642AcceptedReturnRepeatedPhysicalReforkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyMixedReplyResponse(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			h.publishAndWait(catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "refork-key"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, 20*time.Second)
			h.waitForCatalogStoreQuiescence(20 * time.Second)
			rows, err := storetest.ReadReplyContextRecords(h.ctx, selected, catalogRuntimeRunID)
			if err != nil || len(rows) != 1 || rows[0].State != replycontext.StateTerminal || rows[0].AcceptedReplyEventID == "" {
				t.Fatalf("ordinary source lacks its accepted return: rows=%+v error=%v", rows, err)
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			original, err := storetest.ReadSelectedForkSourceDomain(h.ctx, selected, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, cut := catalogRuntimeRunID, rows[0].AcceptedReplyEventID
			for hop := 0; hop < 3; hop++ {
				sourceRun, cut = executeAcceptedReturnRefork(t, h, selected, owner, loader, selection, options, sourceRun, cut)
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 10*time.Second)
			defer cancel()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(cleanup, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(after, original) {
				t.Fatalf("repeated return changed the original source: %v", err)
			}
			sourceReplyRows, err := storetest.ReadReplyContextRecords(cleanup, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(rows, sourceReplyRows) {
				t.Fatalf("reviewer: repeated return changed original source reply rows: before=%+v after=%+v error=%v", rows, sourceReplyRows, err)
			}
		})
	}
}

func executeAcceptedReturnRefork(t *testing.T, h *runtimeHarness, selected unclaimedFanOutReplyStore,
	owner runforkexecution.SelectedContractExecutionOwner, loader runforkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection, options runforkexecution.SelectedContractAgentRuntimeOptions, sourceRun, cut string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	beforeRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || len(beforeRows) != 1 || beforeRows[0].State != replycontext.StateTerminal {
		t.Fatalf("immediate source lacks its own terminal row: rows=%+v error=%v", beforeRows, err)
	}
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, cut)
	if err != nil || !found || publication.Event.Event().RunID() != sourceRun || len(publication.DeliveryRoutes) != 2 {
		t.Fatalf("refork cut is not the actual complete return: %+v found=%t error=%v", publication, found, err)
	}
	waitReplyReforkQuiescence(t, ctx, selected, sourceRun)
	sourceDomain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
	if err != nil {
		t.Fatal(err)
	}
	capturedReturn, err := plan.HistoricalReplyReturn(cut)
	if err != nil || capturedReturn == nil || !reflect.DeepEqual(*capturedReturn, beforeRows[0]) {
		t.Fatalf("native fixed cut lost physical return's exact own row: %+v error=%v", capturedReturn, err)
	}
	provider, err := identity.AdmitExecutableNodeDeclaration("provider", "provider-node")
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range plan.PendingWork {
		if work.SubscriberID == provider.Key() && work.Classification != runfork.RunForkPendingClassificationDeliveredCompleted {
			t.Fatalf("accepted-return cut rearmed the settled provider: %+v", work)
		}
	}
	staged := stageCatalogSelectedContractFork(t, ctx, selected, owner, loader, selection, options, sourceRun, cut)
	childRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || len(childRows) != 1 {
		t.Fatalf("refork did not materialize one owned context: rows=%+v error=%v", childRows, err)
	}
	sourceRow, row := beforeRows[0], childRows[0]
	projected, projectionErr := runfork.ProjectEntityOwnership(sourceRun, staged.ForkRunID, sourceRow.Origin.EntityID, sourceRow.Origin.FlowInstance)
	if projectionErr != nil || row.RunID != staged.ForkRunID || row.ID == sourceRow.ID || row.RequestEventID == sourceRow.RequestEventID ||
		row.AcceptedReplyEventID == sourceRow.AcceptedReplyEventID || row.AcceptedReplyEventID == activityidentity.ForkLineageEventID(staged.ForkRunID, cut) ||
		row.State != replycontext.StateTerminal || row.SourceRunID != sourceRow.SourceRunID || row.SourceRequestEventID != sourceRow.SourceRequestEventID ||
		row.Origin != (events.RouteIdentity{FlowID: sourceRow.Origin.FlowID, FlowInstance: projected.Fork.FlowInstance, EntityID: projected.Fork.EntityID}) ||
		row.RequestCorrelationID != sourceRow.RequestCorrelationID || row.RequesterFlowID != sourceRow.RequesterFlowID || row.ProviderFlowID != sourceRow.ProviderFlowID ||
		!row.CreatedAt.Equal(sourceRow.CreatedAt) || !row.UpdatedAt.Equal(sourceRow.UpdatedAt) || row.TerminalAt == nil || sourceRow.TerminalAt == nil || !row.TerminalAt.Equal(*sourceRow.TerminalAt) {
		t.Fatalf("refork changed terminal meaning or ownership: source=%+v child=%+v error=%v", sourceRow, row, projectionErr)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logCopiedActivityReturnPhase(t, h.ctx, selected, staged.ForkRunID)
			logUnclaimedFanOutReplyFailure(t, h.ctx, selected, owner, staged.ForkRunID)
		}
	})
	executed, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("activate exact terminal-return child: %+v error=%v", executed, err)
	}
	wait, stop := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer stop()
	waitCopiedActivityRequesterReturn(t, wait, selected, staged.ForkRunID)
	inputID := activityidentity.ForkLineageEventID(staged.ForkRunID, cut)
	for {
		returned, found, err := selected.LoadPreparedPublishEvent(wait, inputID)
		if err != nil || !found || len(returned.DeliveryRoutes) != 2 {
			t.Fatalf("refork omitted a reply or ordinary recipient: %+v found=%t error=%v", returned, found, err)
		}
		lineage, inherited := returned.Event.Event().SelectedForkLineage()
		if !inherited || lineage.SourceRunID() != sourceRun || lineage.SourceEventID() != cut || !reflect.DeepEqual(returned.Event.Event().Payload(), publication.Event.Event().Payload()) {
			t.Fatal("return publication skipped its immediate source or changed business data")
		}
		settled, replies, ordinary := 0, 0, 0
		for _, route := range returned.DeliveryRoutes {
			localEvent, present := route.ConnectClaim.ReceiverEvent()
			if !present || route.Context.ReplyContextID() != "" || route.Target.Route() != row.Origin {
				t.Fatalf("return recipient borrowed reply authority or another owner: %+v", route)
			}
			switch localEvent {
			case "provider.replied":
				replies++
			case "provider.observed":
				ordinary++
			default:
				t.Fatalf("return substituted a compiled receiver: %+v", route)
			}
			id, err := deliverylifecycle.DeliveryID(inputID, route)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := selected.Snapshot(wait, id)
			if err != nil || (delivery.Terminal() && delivery.Status != deliverylifecycle.StatusDelivered) {
				t.Fatalf("refork recipient refused: %+v error=%v", delivery, err)
			}
			if delivery.Status == deliverylifecycle.StatusDelivered {
				if delivery.ClaimVersion != 1 {
					t.Fatal("refork recipient executed more than once")
				}
				settled++
			}
		}
		observed, err := storetest.ReadLifecycleEventCardinality(wait, selected, staged.ForkRunID, "reply.observed")
		if err != nil || observed > 1 || replies != 1 || ordinary != 1 {
			t.Fatalf("refork changed the complete response partition: replies=%d ordinary=%d observed=%d error=%v", replies, ordinary, observed, err)
		}
		pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, staged.ForkRunID)
		if err != nil {
			t.Fatal(err)
		}
		if settled == 2 && observed == 1 && pending == 0 {
			break
		}
		select {
		case <-wait.Done():
			t.Fatalf("refork did not settle its exact return: settled=%d observed=%d pending=%d error=%v", settled, observed, pending, wait.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	waitReplyReforkQuiescence(t, wait, selected, staged.ForkRunID)
	afterRow, err := selected.LoadReplyContext(wait, row.ID)
	if err != nil || !reflect.DeepEqual(afterRow, row) {
		t.Fatalf("return rewrote or reopened terminal evidence: %+v error=%v", afterRow, err)
	}
	count, err := storetest.ReadLifecycleEventCardinality(wait, selected, staged.ForkRunID, string(publication.Event.Event().Type()))
	if err != nil || count != 1 {
		t.Fatalf("settled provider was re-executed or return duplicated: count=%d error=%v", count, err)
	}
	afterDomain, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
	if err != nil || !reflect.DeepEqual(afterDomain, sourceDomain) {
		for table, before := range sourceDomain {
			if after := afterDomain[table]; !reflect.DeepEqual(before, after) {
				t.Logf("immediate source %s at cut %s changed %s: before=%+v after=%+v", sourceRun, cut, table, before, after)
			}
		}
		t.Fatalf("refork changed the immediate source domain: %v", err)
	}
	sourceReplyRows, err := storetest.ReadReplyContextRecords(wait, selected, sourceRun)
	if err != nil || !reflect.DeepEqual(beforeRows, sourceReplyRows) {
		t.Fatalf("reviewer: physical-return child changed source reply rows: before=%+v after=%+v error=%v", beforeRows, sourceReplyRows, err)
	}
	return staged.ForkRunID, inputID
}
