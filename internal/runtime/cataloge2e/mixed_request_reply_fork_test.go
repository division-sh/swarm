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
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642MixedRequestWholePublicationBothStores(t *testing.T) {
	runMixedRequestWholePublication(t, false)
}

func TestIssue642SettledReplyKeepsPendingObserverWholePublicationBothStores(t *testing.T) {
	runMixedRequestWholePublication(t, true)
}

func runMixedRequestWholePublication(t *testing.T, settledProvider bool) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyMixedReplyRequest(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "mixed-key"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
			cut := ""
			if settledProvider {
				cut = holdMixedRequestObserverUntilProviderSettles(t, h, selected, step)
			} else {
				h.publishAndWait(step, 20*time.Second)
			}
			h.waitForCatalogStoreQuiescence(20 * time.Second)
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
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
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
				SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate complete mixed request publication: %+v error=%v", executed.Activation, err)
			}
			wait, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
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
			} else {
				returned, found, err := selected.LoadPreparedPublishEvent(wait, terminal.AcceptedReplyEventID)
				if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != terminal.Origin {
					t.Fatalf("mixed request returned to another owner: %+v found=%t error=%v", returned, found, err)
				}
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
		})
	}
}

func holdMixedRequestObserverUntilProviderSettles(t *testing.T, h *runtimeHarness, selected unclaimedFanOutReplyStore, step catalogTriggerStep) string {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(".", "request-observer")
	if err != nil {
		t.Fatal(err)
	}
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	held := &nestedReplyWorkerProbe{nodeID: node.Key(), started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
	defer held.resume()
	h.rt.Pipeline.SetTestLifecycleProbe(held)
	defer h.rt.Pipeline.SetTestLifecycleProbe(nil)
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
