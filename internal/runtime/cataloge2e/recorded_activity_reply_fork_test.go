package cataloge2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642CopiedActivityPublishesAndReturnsToOwnRequesterBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
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
			root := canonicalrouting.CopyRecordedActivityReply(t, provider.URL)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "original-account", "request_id": "original-request"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
			h.publishAndWait(step, 20*time.Second)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			var selected interface {
				runforkexecution.SelectedContractForkLifecycle
				runforkexecution.SourceArtifactSelectedContractSourceStore
				storetest.DurableDataCatalogStore
				pipeline.ActivityAttemptJournal
				bus.PreparedPublishEventReader
				replycontext.Store
				decisioncard.Store
				decisioncard.ProposedEffectStore
				EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
			} = h.pg
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
			if err != nil || rowsErr != nil || inputErr != nil || !inputFound || len(incomingRows) != 1 || len(input.DeliveryRoutes) != 1 || pending.ReplyContextID != incomingRows[0].ID || input.DeliveryRoutes[0].Context.ReplyContextID() != incomingRows[0].ID {
				t.Fatalf("pending effect lost its actual inbound reply before operator dispatch: continuation=%+v rows=%+v input=%+v errors=%v/%v/%v", pending, incomingRows, input, err, rowsErr, inputErr)
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
			if err != nil || !found || cut == "" || original.ReplyContextID == "" || original.Status != pipeline.ActivityAttemptStatusSucceeded || original.CompletedAt == nil || calls.Load() != 1 || sourceFinished != 1 {
				for _, event := range activityLineageEvents(t, h.ctx, h, catalogRuntimeRunID) {
					t.Logf("original publication phase: event=%s name=%s payload=%+v", event.EventID, event.EventName, event.Payload)
				}
				rows, _ := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
				t.Logf("original reply settlement phase: %+v", rows)
				t.Fatalf("real source did not record one complete reply-bearing result: journal=%+v found=%t calls=%d finished=%d error=%v", original, found, calls.Load(), sourceFinished, err)
			}
			sourceRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
			if err != nil || len(sourceRows) != 1 || sourceRows[0].ID != original.ReplyContextID || sourceRows[0].State != replycontext.StateTerminal {
				t.Fatalf("source's exact settled reply inventory: %+v error=%v", sourceRows, err)
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			childRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, staged.ForkRunID)
			if err != nil || len(childRows) != 1 || childRows[0].State != replycontext.StateOpen || childRows[0].ID == sourceRows[0].ID || childRows[0].SourceRequestEventID != sourceRows[0].SourceRequestEventID {
				t.Fatalf("cut did not retain one open, independently owned child return: %+v error=%v", childRows, err)
			}
			activated, err := runforkexecution.ActivateSelectedContractRunFork(correlation.WithRunID(h.ctx, staged.ForkRunID), runforkexecution.SelectedContractActivationGateRequest{
				ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, Store: runScopedCatalogStore(t, h), ExecutionOwner: owner, SourceLoader: loader, AgentRuntime: options,
			})
			if err != nil || !activated.Activated {
				t.Fatalf("activate real recorded-result child: %+v error=%v", activated, err)
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			attempts, err := storetest.ReadActivityAttemptStorage(ctx, selected, staged.ForkRunID)
			if err != nil || len(attempts) != 1 {
				t.Fatalf("child copied-journal inventory: %+v error=%v", attempts, err)
			}
			copied, found, err := selected.LoadActivityAttempt(ctx, attempts[0].RequestEventID)
			if err != nil || !found || copied.ReplyContextID != childRows[0].ID || copied.RequestEventID == original.RequestEventID || copied.InputHash != original.InputHash || !reflect.DeepEqual(copied.ResultPayload, original.ResultPayload) || !copied.StartedAt.Equal(original.StartedAt) || copied.CompletedAt == nil || !copied.CompletedAt.Equal(*original.CompletedAt) {
				t.Fatalf("copy/publish hid an empty or changed recorded carrier: copied=%+v error=%v", copied, err)
			}
			result, found, err := selected.LoadPreparedPublishEvent(ctx, copied.ResultEventID)
			if err != nil || !found || len(result.DeliveryRoutes) != 1 || result.DeliveryRoutes[0].Context.ReplyContextID() != childRows[0].ID {
				t.Fatalf("actual recorded result lost its child continuation: %+v found=%t error=%v", result, found, err)
			}
			settled, err := selected.LoadReplyContext(ctx, childRows[0].ID)
			if err != nil || settled.State != replycontext.StateTerminal || settled.AcceptedReplyEventID == "" || settled.RunID != staged.ForkRunID {
				t.Fatalf("actual provider return failed to settle the own child row: %+v error=%v", settled, err)
			}
			returned, found, err := selected.LoadPreparedPublishEvent(ctx, settled.AcceptedReplyEventID)
			if err != nil || !found || len(returned.DeliveryRoutes) != 1 || returned.DeliveryRoutes[0].Target.Route() != settled.Origin || returned.DeliveryRoutes[0].Context.ReplyContextID() != settled.EnclosingReplyContextID {
				t.Fatalf("actual requester return borrowed another target/context: %+v found=%t error=%v", returned, found, err)
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
		})
	}
}
