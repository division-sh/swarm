package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642MixedRequestWholePublicationBothStores(t *testing.T) {
	runMixedRequestWholePublication(t, false, false)
}

func TestIssue642SettledReplyKeepsPendingObserverWholePublicationBothStores(t *testing.T) {
	runMixedRequestWholePublication(t, true, false)
}

func TestIssue642CompletedReplyKeepsPendingObserverWholePublicationBothStores(t *testing.T) {
	runMixedRequestWholePublication(t, true, true)
}

func TestIssue642AcceptedReturnWholePublicationRollbackAndDuplicateBothStores(t *testing.T) {
	runMixedRequestWholePublicationWithProof(t, true, false, true)
}

func TestIssue642AcceptedReturnSameOriginOrdinaryConnectionBothStores(t *testing.T) {
	runMixedRequestWholePublicationWithResponseObserver(t, true, false, true, true)
}

func runMixedRequestWholePublication(t *testing.T, settledProvider, returnedRequester bool) {
	runMixedRequestWholePublicationWithProof(t, settledProvider, returnedRequester, false)
}

func runMixedRequestWholePublicationWithProof(t *testing.T, settledProvider, returnedRequester, publicationProof bool) {
	t.Helper()
	runMixedRequestWholePublicationWithResponseObserver(t, settledProvider, returnedRequester, publicationProof, false)
}

func runMixedRequestWholePublicationWithResponseObserver(t *testing.T, settledProvider, returnedRequester, publicationProof, responseObserver bool) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyMixedReplyRequest(t)
			if returnedRequester {
				root = canonicalrouting.CopyHandedOffMixedReplyRequest(t)
			}
			if responseObserver {
				root = canonicalrouting.CopyMixedReplyResponse(t)
			}
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "mixed-key"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
			if returnedRequester {
				step.Payload["items"] = []any{step.Payload["request_id"]}
			}
			cut := ""
			if settledProvider {
				cut = holdMixedRequestObserverUntilProviderSettles(t, h, selected, step, returnedRequester)
			} else {
				h.publishAndWait(step, 20*time.Second)
			}
			h.waitForCatalogStoreQuiescence(20 * time.Second)
			if returnedRequester {
				progress, err := selected.ListFanOutIntents(h.ctx, fanoutobligation.ListQuery{RunID: catalogRuntimeRunID, Limit: 10})
				if err != nil || len(progress.Intents) != 1 || progress.NextCursor != "" ||
					progress.Intents[0].Cardinality != 1 || progress.Intents[0].Cursor != 1 || progress.Intents[0].Status != fanoutobligation.StatusClosed {
					t.Fatalf("one-item source did not complete its actual pump issuance: progress=%+v error=%v", progress, err)
				}
				outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(h.ctx, selected, catalogRuntimeRunID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Ordinal != 0 || outcomes[0].Kind != "committed" {
					t.Fatalf("one-item source did not retain its once-only physical ordinal: outcomes=%+v error=%v", outcomes, err)
				}
			}
			sourceRows, err := storetest.ReadReplyContextRecords(h.ctx, selected, catalogRuntimeRunID)
			if err != nil || len(sourceRows) != 1 || sourceRows[0].State != replycontext.StateTerminal || sourceRows[0].AcceptedReplyEventID == "" {
				t.Fatalf("ordinary source did not settle its own request: rows=%+v error=%v", sourceRows, err)
			}
			requestID := sourceRows[0].RequestEventID
			if cut == "" {
				cut = requestID
			}
			source, found, err := selected.LoadPreparedPublishEvent(h.ctx, requestID)
			if err != nil || !found || len(source.DeliveryRoutes) != 2 {
				t.Fatalf("ordinary request did not publish the complete mixed recipient set: %+v found=%t error=%v", source.DeliveryRoutes, found, err)
			}
			observer, err := identity.AdmitExecutableNodeDeclaration(".", "request-observer")
			if err != nil {
				t.Fatal(err)
			}
			var originalObserver events.DeliveryRoute
			for _, route := range source.DeliveryRoutes {
				if route.Recipient == events.MustNodeDeliveryRecipient(observer) {
					originalObserver = route
				}
			}
			if originalObserver.Recipient.Empty() || originalObserver.Context.ReplyContextID() != "" {
				t.Fatalf("ordinary observer borrowed provider reply authority: %+v", originalObserver)
			}
			plan, err := selected.PlanRunFork(h.ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: cut})
			if err != nil {
				t.Fatal(err)
			}
			pending := 0
			for _, work := range plan.PendingWork {
				if work.EventID == requestID {
					if settledProvider && work.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
						continue
					}
					allowed := work.Classification == runfork.RunForkPendingClassificationPending || (settledProvider && work.Classification == runfork.RunForkPendingClassificationInProgress)
					if !allowed || work.SubscriberType != "node" || (settledProvider && work.SubscriberID != observer.Key()) {
						t.Fatalf("request cut borrowed a later recipient settlement: %+v", work)
					}
					pending++
				}
			}
			wantRecipients, wantProviders, wantReplyState := 2, 1, replycontext.StateOpen
			if settledProvider {
				wantRecipients, wantProviders, wantReplyState = 1, 0, replycontext.StateTerminal
			}
			if pending != wantRecipients {
				t.Fatalf("request cut omitted a mixed recipient: pending=%d", pending)
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(h.ctx, selected, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			var owner runforkexecution.SelectedContractExecutionOwner
			var probe *acceptedReturnPublicationProbe
			if publicationProof {
				var replay runforkexecution.SelectedContractReplayPersistence = h.pg
				if h.sqlite != nil {
					replay = h.sqlite
				}
				probe = &acceptedReturnPublicationProbe{SelectedContractReplayPersistence: replay, selected: selected, done: make(chan error, 1)}
				owner = selectedContractExecutionOwnerForCatalogHarnessWithReplay(t, h, probe)
			} else {
				owner = selectedContractExecutionOwnerForCatalogHarness(t, h)
			}
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			operation := catalogStagedForkOperation(t, h.ctx, selected, selection, catalogRuntimeRunID, cut)
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut, operation)
			childRows, err := storetest.ReadReplyContextRecords(h.ctx, selected, staged.ForkRunID)
			if err != nil || len(childRows) != 1 || childRows[0].State != wantReplyState || childRows[0].SourceRunID != catalogRuntimeRunID || childRows[0].SourceRequestEventID != requestID || childRows[0].ID == sourceRows[0].ID {
				t.Fatalf("mixed request staging did not retain its exact own context: %+v error=%v", childRows, err)
			}
			t.Cleanup(func() {
				if t.Failed() {
					logCopiedActivityReturnPhase(t, h.ctx, selected, staged.ForkRunID)
					logUnclaimedFanOutReplyFailure(t, h.ctx, selected, owner, staged.ForkRunID)
				}
			})
			executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, ForkOperation: &operation, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate complete mixed request publication: %+v error=%v", executed.Activation, err)
			}
			wait, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			if probe != nil {
				select {
				case err := <-probe.done:
					if err != nil {
						t.Fatalf("real accepted-return publication proof: %v", err)
					}
				case <-wait.Done():
					t.Fatalf("accepted-return publication owner proof did not complete: %v", wait.Err())
				}
			}
			waitCopiedActivityRequesterReturn(t, wait, selected, staged.ForkRunID)
			inputID := activityidentity.ForkLineageEventID(staged.ForkRunID, requestID)
			for {
				input, found, err := selected.LoadPreparedPublishEvent(wait, inputID)
				if err != nil || !found || len(input.DeliveryRoutes) != wantRecipients {
					t.Fatalf("child publication omitted/invented mixed recipients: %+v found=%t error=%v", input.DeliveryRoutes, found, err)
				}
				settled := 0
				var providerCount, observerCount int
				for _, route := range input.DeliveryRoutes {
					if route.Recipient == originalObserver.Recipient {
						observerCount++
						projection, err := runfork.ProjectEntityOwnership(catalogRuntimeRunID, staged.ForkRunID, originalObserver.Target.Route().EntityID, originalObserver.Target.Route().FlowInstance)
						if err != nil || route.Target.Route() != (events.RouteIdentity{FlowID: originalObserver.Target.Route().FlowID, FlowInstance: projection.Fork.FlowInstance, EntityID: projection.Fork.EntityID}) ||
							!reflect.DeepEqual(route.Context, originalObserver.Context) || !route.ConnectClaim.Equal(originalObserver.ConnectClaim) {
							t.Fatalf("publication reinterpreted ordinary observer context/claim/ownership: %+v error=%v", route, err)
						}
					} else {
						providerCount++
						if route.Context.ReplyContextID() != childRows[0].ID || route.Target.Route().FlowID != childRows[0].ProviderFlowID {
							t.Fatalf("provider recipient lost its exact child reply binding: %+v", route)
						}
					}
					id, err := deliverylifecycle.DeliveryID(inputID, route)
					if err != nil {
						t.Fatal(err)
					}
					delivery, err := selected.Snapshot(wait, id)
					if err != nil || (delivery.Terminal() && delivery.Status != deliverylifecycle.StatusDelivered) {
						t.Fatalf("mixed recipient delivery refused: %+v error=%v", delivery, err)
					}
					if delivery.Status == deliverylifecycle.StatusDelivered {
						if delivery.ClaimVersion != 1 {
							t.Fatalf("mixed recipient executed more than once: %+v", delivery)
						}
						settled++
					}
				}
				if providerCount != wantProviders || observerCount != 1 {
					t.Fatal("mixed publication substituted a recipient")
				}
				observed, err := storetest.ReadLifecycleEventCardinality(wait, selected, staged.ForkRunID, "request.observed")
				if err != nil || observed > 1 {
					t.Fatalf("ordinary observer output count=%d error=%v", observed, err)
				}
				pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, staged.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				if settled == wantRecipients && observed == 1 && pending == 0 {
					if returnedRequester {
						providerReplies, err := storetest.ReadLifecycleEventCardinality(wait, selected, staged.ForkRunID, "provider.replied")
						if err != nil || providerReplies != 0 {
							t.Fatalf("completed reply/provider was reissued: count=%d error=%v", providerReplies, err)
						}
						retainedReturn := storetest.LoadCanonicalEventRecord(t, wait, selected, activityidentity.ForkLineageEventID(staged.ForkRunID, cut))
						lineage, isSelected := retainedReturn.SelectedForkLineage()
						if !isSelected || lineage.SourceEventID() != cut || string(retainedReturn.Type()) != "request.finished" {
							t.Fatal("completed return was re-executed rather than retaining its exact cut publication")
						}
					}
					break
				}
				select {
				case <-wait.Done():
					logCopiedActivityReturnPhase(t, wait, selected, staged.ForkRunID)
					t.Fatalf("mixed publication did not settle both exact recipients: settled=%d observed=%d pending=%d error=%v", settled, observed, pending, wait.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			terminal, err := selected.LoadReplyContext(wait, childRows[0].ID)
			if err != nil || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID == "" || terminal.ValidateContinuationFrom(childRows[0], childRows[0].CreatedAt) != nil {
				t.Fatalf("mixed request did not return through its retained own row: %+v error=%v", terminal, err)
			}
			if settledProvider {
				if !reflect.DeepEqual(terminal, childRows[0]) {
					t.Fatal("ordinary observer reopened or changed settled reply history")
				}
				proveMixedRequestObserverOnlyReforkPartition(t, wait, selected, staged.ForkRunID, inputID)
				if !publicationProof && !returnedRequester {
					sourceRun, reforkCut := staged.ForkRunID, inputID
					for hop := 0; hop < 2; hop++ {
						sourceRun, reforkCut = executeObserverOnlyRequestRefork(t, h, selected, owner, loader, selection, options, sourceRun, reforkCut)
					}
				}
			} else {
				returned, found, err := selected.LoadPreparedPublishEvent(wait, terminal.AcceptedReplyEventID)
				if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != terminal.Origin {
					t.Fatalf("mixed request returned to another owner: %+v found=%t error=%v", returned, found, err)
				}
			}
			if responseObserver {
				proveMixedResponseOrdinaryRecipient(t, wait, h, selected, sourceRows[0].AcceptedReplyEventID, staged.ForkRunID)
			}
			cleanup, stop := context.WithTimeout(context.WithoutCancel(h.ctx), 10*time.Second)
			defer stop()
			if err := owner.RetireSelectedContexts(cleanup); err != nil {
				t.Fatal(err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(cleanup, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("mixed child publication rewrote source domain: %v", err)
			}
			sourceReplyRows, err := storetest.ReadReplyContextRecords(cleanup, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(sourceRows, sourceReplyRows) {
				t.Fatalf("reviewer: mixed child changed original source reply rows: before=%+v after=%+v error=%v", sourceRows, sourceReplyRows, err)
			}
		})
	}
}

func proveMixedResponseOrdinaryRecipient(t *testing.T, ctx context.Context, h *runtimeHarness, selected unclaimedFanOutReplyStore, sourceEventID, child string) {
	t.Helper()
	source, found, err := selected.LoadPreparedPublishEvent(ctx, sourceEventID)
	if err != nil || !found || len(source.DeliveryRoutes) != 2 {
		t.Fatalf("source mixed response: %+v found=%t error=%v", source, found, err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "response-observer")
	if err != nil {
		t.Fatal(err)
	}
	var original events.DeliveryRoute
	for _, route := range source.DeliveryRoutes {
		if route.Recipient == events.MustNodeDeliveryRecipient(node) {
			original = route
		}
	}
	if original.Recipient.Empty() || original.Context.ReplyContextID() != "" {
		t.Fatal("source ordinary response observer borrowed reply authority")
	}
	inputID := activityidentity.ForkLineageEventID(child, sourceEventID)
	var ordinaryPlan pinrouting.ConnectRoutePlan
	for _, plan := range pinrouting.CompileConnectGraph(h.rt.Pipeline.SemanticSource()).Plans() {
		if plan.ReceiverLocalEvent() == "provider.observed" {
			ordinaryPlan = plan
		}
	}
	for {
		publication, found, err := selected.LoadPreparedPublishEvent(ctx, inputID)
		if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
			t.Fatalf("child mixed response: %+v found=%t error=%v", publication, found, err)
		}
		var ordinary events.DeliveryRoute
		for _, route := range publication.DeliveryRoutes {
			if route.Recipient == original.Recipient {
				ordinary = route
			}
		}
		projected, err := runfork.ProjectEntityOwnership(catalogRuntimeRunID, child, original.Target.Route().EntityID, original.Target.Route().FlowInstance)
		blueprint := ordinary.Target.Route()
		blueprint.EntityID = ""
		claim, claimErr := pinrouting.ConnectExecutionClaim(ordinaryPlan, pinrouting.ConnectDeliveryRoute{Recipient: original.Recipient,
			Target: blueprint, Handler: pinrouting.MustConnectReceiverHandler(node), Context: original.Context, PayloadProjection: original.PayloadProjection})
		if err != nil || ordinary.Target.Route() != (events.RouteIdentity{FlowID: original.Target.Route().FlowID, FlowInstance: projected.Fork.FlowInstance, EntityID: projected.Fork.EntityID}) ||
			claimErr != nil || !ordinary.ConnectClaim.Equal(claim) || !reflect.DeepEqual(ordinary.Context, original.Context) {
			t.Fatalf("ordinary response observer lost its own target/claim/context: %+v error=%v", ordinary, err)
		}
		id, err := deliverylifecycle.DeliveryID(inputID, ordinary)
		if err != nil {
			t.Fatal(err)
		}
		delivery, err := selected.Snapshot(ctx, id)
		if err != nil || (delivery.Terminal() && delivery.Status != deliverylifecycle.StatusDelivered) {
			t.Fatalf("ordinary response observer refused: %+v error=%v", delivery, err)
		}
		count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, child, "reply.observed")
		if err != nil || count > 1 {
			t.Fatalf("ordinary response observer output count=%d error=%v", count, err)
		}
		if delivery.Status == deliverylifecycle.StatusDelivered && count == 1 {
			if delivery.ClaimVersion != 1 {
				t.Fatal("ordinary response observer executed more than once")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ordinary response observer did not finish: %+v count=%d error=%v", delivery, count, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func proveMixedRequestObserverOnlyReforkPartition(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, child, inputID string) {
	t.Helper()
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, inputID)
	if err != nil || !found || len(publication.DeliveryRoutes) != 1 || publication.DeliveryRoutes[0].Context.ReplyContextID() != "" {
		t.Fatalf("refork source invented a provider or reply ref: publication=%+v found=%t error=%v", publication, found, err)
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: child, At: inputID})
	if err != nil {
		t.Fatalf("native observer-only refork planning: %v", err)
	}
	partition, present, err := plan.RequestPendingRecipientPartition(inputID)
	id, idErr := deliverylifecycle.DeliveryID(inputID, publication.DeliveryRoutes[0])
	if err != nil || idErr != nil || !present || len(partition) != 1 || partition[0] != id {
		t.Fatalf("native physical refork lost exact observer: partition=%v present=%t error=%v/%v", partition, present, err, idErr)
	}
}

func holdMixedRequestObserverUntilProviderSettles(t *testing.T, h *runtimeHarness, selected unclaimedFanOutReplyStore, step catalogTriggerStep, returnedRequester bool) string {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(".", "request-observer")
	if err != nil {
		t.Fatal(err)
	}
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	held := &nestedReplyWorkerProbe{nodeID: node.Key(), started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
	defer held.resume()
	h.rt.Pipeline.SetTestLifecycleProbe(held)
	// Keep the probe immutable for the runtime lifetime, including failed waits.
	// Releasing it does not race with an in-flight handler's completion callback.
	published := make(chan error, 1)
	go func() { published <- h.publishRuntimeEventResultForStep(step, 20*time.Second, false) }()
	ctx, cancel := context.WithTimeout(h.ctx, 20*time.Second)
	defer cancel()
	var claim lifecycleprobe.Signal
	select {
	case claim = <-held.started:
	case <-ctx.Done():
		t.Fatalf("ordinary mixed observer never acquired its actual claim: %v", ctx.Err())
	}
	loggedRequesterWait := false
	for {
		rows, err := storetest.ReadReplyContextRecords(ctx, selected, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 && rows[0].State == replycontext.StateTerminal && rows[0].AcceptedReplyEventID != "" {
			row := rows[0]
			if claim.EventID != row.RequestEventID {
				t.Fatal("held observer belongs to another request")
			}
			request, found, err := selected.LoadPreparedPublishEvent(ctx, row.RequestEventID)
			if err != nil || !found || len(request.DeliveryRoutes) != 2 {
				t.Fatalf("temporal cut lost the actual mixed publication: %+v found=%t error=%v", request, found, err)
			}
			provider, observer := 0, 0
			for _, route := range request.DeliveryRoutes {
				id, err := deliverylifecycle.DeliveryID(row.RequestEventID, route)
				if err != nil {
					t.Fatal(err)
				}
				delivery, err := selected.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if route.Recipient == events.MustNodeDeliveryRecipient(node) {
					if delivery.Terminal() || delivery.ClaimVersion != 1 {
						t.Fatalf("temporal cut did not hold the real unfinished observer: %+v", delivery)
					}
					observer++
				} else if route.Context.ReplyContextID() == row.ID && delivery.Status == deliverylifecycle.StatusDelivered {
					provider++
				}
			}
			if provider != 1 || observer != 1 {
				t.Fatalf("temporal cut lacks one settled provider and one unfinished observer: provider=%d observer=%d", provider, observer)
			}
			cut := row.AcceptedReplyEventID
			if returnedRequester {
				count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, catalogRuntimeRunID, "request.finished")
				if err != nil || count > 1 {
					t.Fatalf("ordinary requester return count=%d error=%v", count, err)
				}
				if count == 0 {
					if !loggedRequesterWait {
						loggedRequesterWait = true
						logMixedRequesterWait(t, ctx, selected, row)
					}
					select {
					case <-ctx.Done():
						t.Fatal("ordinary requester did not return within the original business bound")
					case <-time.After(10 * time.Millisecond):
						continue
					}
				}
				cut, err = storetest.ReadLatestNamedEventIdentityStorage(ctx, selected, "request.finished", "")
				if err != nil || cut == "" {
					t.Fatalf("ordinary committed requester return cut: %v", err)
				}
				finished := storetest.LoadCanonicalEventRecord(t, ctx, selected, cut)
				if finished.RunID() != catalogRuntimeRunID || finished.ParentEventID() != row.AcceptedReplyEventID {
					t.Fatal("requester return cut belongs to another run or accepted reply")
				}
				observed, err := storetest.ReadLifecycleEventCardinality(ctx, selected, catalogRuntimeRunID, "request.observed")
				if err != nil || observed != 0 {
					t.Fatalf("completed-requester cut no longer has an unfinished observer: observed=%d error=%v", observed, err)
				}
				proveMixedRequestSourceHandoff(t, ctx, h, selected, row)
				t.Logf("ordinary source reached completed requester with its original observer still claimed: request=%s return=%s cut=%s claim=%s",
					row.RequestEventID, row.AcceptedReplyEventID, cut, claim.EventID)
			}
			held.resume()
			select {
			case err := <-published:
				if err != nil {
					t.Fatalf("ordinary temporal source publication: %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("ordinary observer did not join after release: %v", ctx.Err())
			}
			return cut
		}
		select {
		case <-ctx.Done():
			t.Fatalf("provider did not settle while the real ordinary observer remained held: rows=%+v error=%v", rows, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func proveMixedRequestSourceHandoff(t *testing.T, ctx context.Context, h *runtimeHarness, selected unclaimedFanOutReplyStore, row replycontext.Record) {
	t.Helper()
	owner, ok := h.rt.Bus.DeliveryContinuationOwner().(*deliverycontinuation.Coordinator)
	if !ok || !owner.OwnsPersistedRecovery() {
		t.Fatal("completed-requester source requires the actual managed generation coordinator")
	}
	authority, err := h.rt.Bus.DeliveryAuthority()
	if err != nil || authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
		t.Fatalf("source lacks its current normal delivery authority: authority=%+v error=%v", authority, err)
	}
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, row.RequestEventID)
	if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
		t.Fatalf("source handoff lost its complete mixed request: publication=%+v found=%t error=%v", publication, found, err)
	}
	receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, selected, row.RequestEventID)
	if err != nil || receipt.Outcome != "success" || receipt.Reason != "pipeline_persisted" {
		t.Fatalf("source request was not acknowledged by its actual publication group: receipt=%+v error=%v", receipt, err)
	}
	outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(ctx, selected, catalogRuntimeRunID)
	if err != nil || len(outcomes) != 1 || outcomes[0].Ordinal != 0 || outcomes[0].EventID != row.RequestEventID || outcomes[0].Kind != "committed" {
		t.Fatalf("source request did not come from its one real committed ordinal: outcomes=%+v error=%v", outcomes, err)
	}
	// Snapshot validates the claim but does not project the physical handoff stamp.
	phases, err := storetest.ReadWorkspaceInvocationPhases(ctx, selected)
	if err != nil {
		t.Fatalf("read actual native source handoffs: %v", err)
	}
	for _, route := range publication.DeliveryRoutes {
		id, err := deliverylifecycle.DeliveryID(row.RequestEventID, route)
		if err != nil || !route.Recipient.IsNode() {
			t.Fatalf("source handoff substituted its original node recipient: route=%+v error=%v", route, err)
		}
		delivery, err := selected.Snapshot(ctx, id)
		if err != nil || delivery.StartedAt.IsZero() || !delivery.Authority.Equal(authority) {
			t.Fatalf("source recipient did not consume an acknowledged current-generation handoff: delivery=%+v error=%v", delivery, err)
		}
		matched := 0
		for _, phase := range phases.Deliveries {
			if phase.DeliveryID != id {
				continue
			}
			matched++
			if phase.EventID != row.RequestEventID || phase.RunID != catalogRuntimeRunID || phase.HandoffAt == nil || *phase.HandoffAt == "" ||
				phase.Status != string(delivery.Status) || phase.ClaimVersion != delivery.ClaimVersion {
				t.Fatalf("source recipient lacks its physical acknowledged handoff: phase=%+v delivery=%+v", phase, delivery)
			}
			t.Logf("actual source handoff: delivery=%s stamp=%s status=%s claim=%d", id, *phase.HandoffAt, phase.Status, phase.ClaimVersion)
		}
		if matched != 1 {
			t.Fatalf("source recipient physical handoff count=%d for delivery=%s", matched, id)
		}
	}
	t.Logf("source one-item mixed publication acknowledged before independent claims: request=%s receipt=%+v", row.RequestEventID, receipt)
}

func logMixedRequesterWait(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, row replycontext.Record) {
	t.Helper()
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, row.AcceptedReplyEventID)
	receipt, receiptErr := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, selected, row.AcceptedReplyEventID)
	t.Logf("held observer source phase: terminal_context=%s accepted_event=%s physical_return=%t routes=%d receipt=%+v errors=%v/%v",
		row.ID, row.AcceptedReplyEventID, found, len(publication.DeliveryRoutes), receipt, err, receiptErr)
	if err != nil || !found {
		return
	}
	for _, route := range publication.DeliveryRoutes {
		id, idErr := deliverylifecycle.DeliveryID(row.AcceptedReplyEventID, route)
		if idErr != nil {
			t.Logf("held observer return identity error: %v", idErr)
			continue
		}
		delivery, deliveryErr := selected.Snapshot(ctx, id)
		t.Logf("held observer return phase: delivery=%+v error=%v", delivery, deliveryErr)
	}
}
