package cataloge2e

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type unissuedReplyChunkHold struct {
	once    sync.Once
	release sync.Once
	claimed chan fanoutobligation.Claim
	resume  chan struct{}
}

func (h *unissuedReplyChunkHold) observe(ctx context.Context, command pipeline.FanOutChunkCommand) {
	if command.Claim.Key.RunID != catalogRuntimeRunID {
		return
	}
	h.once.Do(func() {
		h.claimed <- command.Claim
		select {
		case <-h.resume:
		case <-ctx.Done():
		}
	})
}

func (h *unissuedReplyChunkHold) close() {
	h.release.Do(func() { close(h.resume) })
}

func TestIssue642InheritedUnissuedFanOutReturnsOwnChildReplyBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyUnissuedFanOutReply(t)
			held := &unissuedReplyChunkHold{claimed: make(chan fanoutobligation.Claim, 1), resume: make(chan struct{})}
			h := newRuntimeHarnessWithTerminalProvider(t, root, backend, true, nil, nil, held.observe)
			t.Cleanup(held.close)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			h.publishAndWait(catalogTriggerStep{Event: "request.started", Payload: map[string]any{
				"account_id": "own-account", "request_id": "own-key", "items": []any{"own-key"}},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, 20*time.Second)
			phase, cancel := context.WithTimeout(h.ctx, 20*time.Second)
			defer cancel()
			var originalClaim fanoutobligation.Claim
			select {
			case originalClaim = <-held.claimed:
			case <-phase.Done():
				t.Fatalf("source pump did not reach its original uncommitted ordinal: %v", phase.Err())
			}
			sourceRows, err := storetest.ReadReplyContextRecords(phase, selected, catalogRuntimeRunID)
			if err != nil || len(sourceRows) != 1 || sourceRows[0].State != replycontext.StateOpen {
				t.Fatalf("source intent did not retain its actual open reply: rows=%+v error=%v", sourceRows, err)
			}
			h.publishAndWait(catalogTriggerStep{Event: "request.checkpoint", Payload: map[string]any{"note": "intent-committed-before-ordinal"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, 20*time.Second)
			cut := ""
			for _, event := range activityLineageEvents(t, phase, h, catalogRuntimeRunID) {
				if event.EventName == "request.checkpointed" {
					if cut != "" {
						t.Fatal("source created two checkpoint occurrences")
					}
					cut = event.EventID
				}
				if strings.HasSuffix(event.EventName, "/item.ready") || event.EventName == "request.finished" {
					t.Fatal("source published an ordinal or reply before the cut")
				}
			}
			plan, err := selected.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: cut})
			if err != nil || cut == "" || len(plan.FanOutObligations) != 1 {
				t.Fatalf("fixed cut lacks its committed unissued intent: cut=%s plan=%+v error=%v", cut, plan.FanOutObligations, err)
			}
			intent := plan.FanOutObligations[0].Intent
			if intent.Request.Key != originalClaim.Key || intent.Status != fanoutobligation.StatusOpen || intent.Cursor != 0 ||
				intent.Request.Cardinality != 1 || intent.Request.Capsule.Context.ReplyContextID() != sourceRows[0].ID ||
				len(plan.FanOutObligations[0].Outcomes) != 0 {
				t.Fatalf("cut confused fresh child work with the original unissued carrier: %+v", plan.FanOutObligations[0])
			}
			paused, err := h.rt.RunControl.Pause(phase, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID,
				Reason: "preserve-original-unissued-work", ControlledBy: "cataloge2e"})
			if err != nil || paused.Status != runcontrol.StatusPaused {
				t.Fatalf("pause original source before releasing its real claim: %+v error=%v", paused, err)
			}
			held.close()
			waitInheritedUnissuedSourceParked(t, phase, selected, originalClaim.Key)
			before, err := storetest.ReadSelectedForkSourceDomain(phase, selected, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			childRows, err := storetest.ReadReplyContextRecords(phase, selected, staged.ForkRunID)
			childPlan, planErr := selected.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: staged.ForkRunID, AtStart: true})
			if err != nil || planErr != nil || len(childRows) != 1 || childRows[0].State != replycontext.StateOpen ||
				childRows[0].ID == sourceRows[0].ID || len(childPlan.FanOutObligations) != 1 {
				t.Fatalf("staging lost the inherited own-row intent: rows=%+v intents=%+v errors=%v/%v", childRows, childPlan.FanOutObligations, err, planErr)
			}
			copied := childPlan.FanOutObligations[0].Intent
			if copied.Request.Key.RunID != staged.ForkRunID || copied.Request.Key == originalClaim.Key || copied.Cursor != 0 ||
				copied.Request.Cardinality != 1 || copied.Request.Capsule.Context.ReplyContextID() != childRows[0].ID ||
				!reflect.DeepEqual(copied.Request.Capsule.Entity, intent.Request.Capsule.Entity) || len(childPlan.FanOutObligations[0].Outcomes) != 0 {
				t.Fatalf("copied intent lost its frozen business data or own reply: %+v", copied)
			}
			executed, err := runforkexecution.ExecuteSelectedContractRunFork(h.ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
				ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate actual inherited unissued carrier: %+v error=%v", executed.Activation, err)
			}
			returned, stop := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer stop()
			waitCopiedActivityRequesterReturn(t, returned, selected, staged.ForkRunID)
			progress, err := storetest.ReadFanOutIntentCompletionStorage(returned, selected, copied.Request.Key)
			settled, replyErr := selected.LoadReplyContext(returned, childRows[0].ID)
			if err != nil || replyErr != nil || progress.Status != string(fanoutobligation.StatusClosed) ||
				progress.Cursor != 1 || progress.Cardinality != 1 || len(progress.Outcomes) != 1 ||
				settled.State != replycontext.StateTerminal || settled.AcceptedReplyEventID == "" {
				t.Fatalf("inherited ordinal or own-row return missed final settlement: progress=%+v reply=%+v errors=%v/%v", progress, settled, err, replyErr)
			}
			outcome := progress.Outcomes[0]
			publication, found, err := selected.LoadPreparedPublishEvent(returned, outcome.EventID)
			if err != nil || !found || outcome.Ordinal != 0 || outcome.Kind != string(fanoutobligation.OutcomeCommitted) ||
				len(publication.DeliveryRoutes) != 1 || publication.DeliveryRoutes[0].Context.ReplyContextID() != childRows[0].ID {
				t.Fatalf("ordinal publication borrowed or lost the return binding: outcome=%+v publication=%+v error=%v", outcome, publication, err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(outcome.EventID, publication.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := selected.Snapshot(returned, deliveryID)
			if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.ClaimVersion != 1 {
				t.Fatalf("ordinal final receiver did not settle once: %+v error=%v", delivery, err)
			}
			itemCount, finished := 0, 0
			for _, event := range activityLineageEvents(t, returned, h, staged.ForkRunID) {
				if strings.HasSuffix(event.EventName, "/item.ready") {
					itemCount++
				}
				if event.EventName == "request.finished" {
					finished++
					if event.Payload["request_id"] != "own-key" || event.Payload["result"] != "from-own-ordinal" {
						t.Fatalf("inherited ordinal returned different business data: %+v", event)
					}
				}
			}
			after, err := storetest.ReadSelectedForkSourceDomain(returned, selected, catalogRuntimeRunID)
			afterRows, rowsErr := storetest.ReadReplyContextRecords(returned, selected, catalogRuntimeRunID)
			if err != nil || rowsErr != nil || itemCount != 1 || finished != 1 || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sourceRows, afterRows) {
				t.Fatalf("child duplicated its unissued return or changed the source: item=%d finished=%d errors=%v/%v", itemCount, finished, err, rowsErr)
			}
		})
	}
}

func waitInheritedUnissuedSourceParked(t *testing.T, ctx context.Context, selected unclaimedFanOutReplyStore, key fanoutobligation.IntentKey) {
	t.Helper()
	for {
		page, err := selected.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: key.RunID, Limit: 10})
		if err != nil || len(page.Intents) != 1 || page.Intents[0].Key != key || page.Intents[0].Cursor != 0 {
			t.Fatalf("source claim published or lost the original intent after pause: %+v error=%v", page, err)
		}
		if page.Intents[0].ClaimOwner == "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("source did not release its original paused claim: intent=%+v error=%v", page.Intents[0], ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
