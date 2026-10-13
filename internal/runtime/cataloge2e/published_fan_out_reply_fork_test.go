package cataloge2e

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642PublishedFanOutReturnsOwnChildReplyBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyPublishedFanOutReply(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			h.publishAndWait(catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "same-key", "items": []any{"same-key"}},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, 20*time.Second)
			h.waitForCatalogStoreQuiescence(20 * time.Second)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			var selected interface {
				runforkexecution.SelectedContractForkLifecycle
				runforkexecution.SourceArtifactSelectedContractSourceStore
				storetest.DurableDataCatalogStore
				replycontext.Store
				bus.PreparedPublishEventReader
				ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
			} = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			cut, sourceFinished := "", 0
			for _, event := range catalogEventsSince(t, reader, h.startedAt) {
				if strings.HasSuffix(event.Name, "/item.ready") {
					if cut != "" {
						t.Fatal("source emitted two ordinals")
					}
					cut = event.ID
				}
				if event.Name == "request.finished" {
					sourceFinished++
				}
			}
			sourceRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
			if err != nil || cut == "" || sourceFinished != 1 || len(sourceRows) != 1 || sourceRows[0].State != replycontext.StateTerminal {
				t.Fatalf("real source did not return through one deferred ordinal: cut=%s finished=%d rows=%+v error=%v", cut, sourceFinished, sourceRows, err)
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			childRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, staged.ForkRunID)
			page, pageErr := selected.ListFanOutIntents(h.ctx, fanoutobligation.ListQuery{RunID: staged.ForkRunID, Limit: 10})
			if err != nil || pageErr != nil || len(childRows) != 1 || childRows[0].State != replycontext.StateOpen || childRows[0].ID == sourceRows[0].ID ||
				len(page.Intents) != 1 || page.Intents[0].Status != fanoutobligation.StatusClosed || page.Intents[0].Cursor != 1 || page.Intents[0].Cardinality != 1 || page.Intents[0].Owed != 0 {
				t.Fatalf("cut did not retain the own open row and exact published carrier: rows=%+v intents=%+v errors=%v/%v", childRows, page, err, pageErr)
			}
			executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate the exact deferred-reply child: %+v error=%v", executed, err)
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			waitCopiedActivityRequesterReturn(t, ctx, selected, staged.ForkRunID)
			itemCount, finished := 0, 0
			for _, event := range activityLineageEvents(t, ctx, h, staged.ForkRunID) {
				if strings.HasSuffix(event.EventName, "/item.ready") {
					itemCount++
					published, found, readErr := selected.LoadPreparedPublishEvent(ctx, event.EventID)
					if readErr != nil || !found || len(published.DeliveryRoutes) != 1 || published.DeliveryRoutes[0].Context.ReplyContextID() != childRows[0].ID {
						t.Fatalf("actual ordinal publication lost its own child reference: %+v found=%t error=%v", published, found, readErr)
					}
				}
				if event.EventName == "request.finished" {
					finished++
					if event.Payload["request_id"] != "same-key" || event.Payload["result"] != "from-own-ordinal" {
						t.Fatalf("wrong ordinal returned to requester: %+v", event)
					}
				}
			}
			settled, err := selected.LoadReplyContext(ctx, childRows[0].ID)
			after, afterErr := storetest.ReadReplyContextRecords(ctx, reader, catalogRuntimeRunID)
			if err != nil || afterErr != nil || itemCount != 1 || finished != 1 || settled.State != replycontext.StateTerminal || settled.AcceptedReplyEventID == "" || !reflect.DeepEqual(after, sourceRows) {
				t.Fatalf("deferred return missed exact settlement, duplicated work or changed source: item=%d finished=%d settled=%+v errors=%v/%v", itemCount, finished, settled, err, afterErr)
			}
		})
	}
}
