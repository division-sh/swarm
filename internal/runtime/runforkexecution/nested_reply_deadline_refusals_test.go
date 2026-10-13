package runforkexecution

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type selectedDeadlineReplyStore interface {
	bus.PreparedPublishEventReader
	replycontext.Store
	deliverylifecycle.Store
}

func proveSelectedDeadlineReplyRefusals(t *testing.T, servingCtx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, rows []replycontext.Record) {
	t.Helper()
	store := selected.(selectedDeadlineReplyStore)
	child := rows[0].RunID
	var childBus *bus.EventBus
	owner.ports.contexts.mu.Lock()
	for _, entry := range owner.ports.contexts.entries {
		if entry.binding.ForkRunID == child && entry.retained != nil && entry.retained.retainedRuntime != nil {
			childBus = entry.retained.retainedRuntime.bus
		}
	}
	owner.ports.contexts.mu.Unlock()
	if childBus == nil {
		t.Fatal("selected refusal proof lacks its actual retained child bus")
	}
	ctx, cancel := context.WithTimeout(servingCtx, 20*time.Second)
	defer cancel()
	var inner replycontext.Record
	for _, row := range rows {
		if row.ProviderFlowID == "u" {
			inner = row
		}
	}
	for {
		actual, err := store.LoadReplyContext(ctx, inner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if actual.State == replycontext.StateTerminal && actual.AcceptedReplyEventID != "" {
			inner = actual
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actual late inner child return: %v", context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
	}
	accepted, found, err := store.LoadPreparedPublishEvent(ctx, inner.AcceptedReplyEventID)
	if err != nil || !found {
		t.Fatalf("physical late child response: found=%t error=%v", found, err)
	}
	requireSelectedLateReplyReceipt(t, ctx, selected, store, inner, accepted)
	for _, row := range rows {
		if row.ProviderFlowID == "u" {
			continue
		}
		before, err := storetest.ReadReplyContextRecords(ctx, selected, child)
		if err != nil {
			t.Fatal(err)
		}
		var route events.RouteIdentity
		for _, origin := range rows {
			if origin.RequesterFlowID == row.ProviderFlowID {
				route = origin.Origin
			}
		}
		if route.FlowID != row.ProviderFlowID || route.EntityID == "" {
			t.Fatalf("provider lacks its exact projected constructed origin: %+v", row)
		}
		source, err := events.NewStaticFlowRoutingSource(route)
		if err != nil {
			t.Fatal(err)
		}
		node, err := identity.AdmitExecutableNodeDeclaration(row.ProviderFlowID, row.ProviderFlowID+"-node")
		if err != nil {
			t.Fatal(err)
		}
		parent := accepted.Event.Event()
		response := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), events.EventType(semanticview.ResolveFlowEventProof(loaded.Source, row.ProviderFlowID, row.ProviderOutputPin).EventKey()),
			eventtest.Producer(events.EventProducerNode, node.Key()), "", parent.Payload(), parent.ChainDepth()+1, events.LineageFromEvent(parent),
			events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), source, time.Now().UTC())
		if err := childBus.PublishAndWait(events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: row.ID}}), response); err != nil {
			t.Fatalf("execute selected late %s response: %v", row.ProviderFlowID, err)
		}
		publication, found, err := store.LoadPreparedPublishEvent(ctx, response.ID())
		if err != nil || !found {
			t.Fatalf("late response publication: found=%t error=%v", found, err)
		}
		requireSelectedLateReplyReceipt(t, ctx, selected, store, row, publication)
		for _, old := range before {
			actual, err := store.LoadReplyContext(ctx, old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if old.ID == row.ID {
				if !actual.SameIdentity(old) || actual.State != replycontext.StateTerminal || actual.AcceptedReplyEventID != response.ID() {
					t.Fatalf("late response changed its own context: %+v", actual)
				}
			} else if !reflect.DeepEqual(old, actual) {
				t.Fatalf("late response settled another level: before=%+v after=%+v", old, actual)
			}
		}
	}
	terminal, err := storetest.ReadReplyContextRecords(ctx, selected, child)
	if err != nil || len(terminal) != 3 {
		t.Fatalf("selected terminal reply inventory: %+v error=%v", terminal, err)
	}
	for _, row := range terminal {
		if row.State != replycontext.StateTerminal {
			t.Fatalf("selected own-level response did not settle: %+v", row)
		}
		publication, found, err := store.LoadPreparedPublishEvent(ctx, row.AcceptedReplyEventID)
		if err != nil || !found {
			t.Fatalf("selected accepted response: found=%t error=%v", found, err)
		}
		for _, target := range terminal {
			class, code := failures.ClassStaleArrival, "stale_arrival"
			if row.ID == target.ID {
				class, code = failures.ClassReplyAlreadyTerminal, "reply_already_terminal"
			}
			original := publication.Event.Event()
			response := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), original.Type(), original.Producer(), "", original.Payload(), original.ChainDepth(),
				events.LineageFromEvent(original), events.EnvelopeForSourceRoute(events.EventEnvelope{}, original.Envelope().Source), original.RoutingSource(), time.Now().UTC())
			if err := childBus.PublishAndWait(events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: target.ID}}), response); err != nil {
				t.Fatalf("selected %s to %s refusal publication: %v", row.ProviderFlowID, target.ProviderFlowID, err)
			}
			rejected, found, err := store.LoadPreparedPublishEvent(ctx, response.ID())
			if err != nil || !found || len(rejected.DeliveryRoutes) != 0 {
				t.Fatalf("refused response created recipients: %+v found=%t error=%v", rejected, found, err)
			}
			receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, selected, response.ID())
			if err != nil {
				t.Fatal(err)
			}
			var failure failures.Envelope
			if err := json.Unmarshal(receipt.Failure, &failure); err != nil {
				t.Fatal(err)
			}
			count, err := storetest.CountDeadLettersForOriginalEvent(ctx, selected, response.ID())
			if err != nil || !receipt.Found || receipt.Outcome != "dead_letter" || count != 1 || failure.Class != class || failure.Detail.Code != code {
				t.Fatalf("exact selected refusal: receipt=%+v failure=%+v count=%d error=%v", receipt, failure, count, err)
			}
			after, err := storetest.ReadReplyContextRecords(ctx, selected, child)
			if err != nil || !reflect.DeepEqual(terminal, after) {
				t.Fatalf("selected refusal changed a reply level: %+v error=%v", after, err)
			}
		}
	}
	for name, want := range map[string]int{"platform.join_timeout": 3, "platform.join_complete": 0, "request.finished": 0} {
		count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, child, name)
		if err != nil || count != want {
			t.Fatalf("selected refusal changed outcome count: name=%s count=%d error=%v", name, count, err)
		}
	}
	for _, row := range rows {
		arm := nestedDeadlineArm(t, ctx, owner, row)
		if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonDeadline || !arm.OutcomeFired || arm.OutcomePending || arm.Completed() != 0 {
			t.Fatalf("selected response reopened or contributed to an expired arm: %+v", arm)
		}
	}
}

func requireSelectedLateReplyReceipt(t *testing.T, ctx context.Context, selected any, store selectedDeadlineReplyStore, row replycontext.Record, publication bus.PreparedPublishEvent) {
	t.Helper()
	if len(publication.DeliveryRoutes) != 1 || publication.DeliveryRoutes[0].Target.Route() != row.Origin ||
		publication.DeliveryRoutes[0].Context.ReplyContextID() != row.EnclosingReplyContextID || !reflect.DeepEqual(publication.DeliveryRoutes[0].Context.Joins, row.ReturnJoins) {
		t.Fatalf("late child response changed retained return binding: %+v", publication)
	}
	id, err := deliverylifecycle.DeliveryID(publication.Event.ID(), publication.DeliveryRoutes[0])
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, id)
	for err == nil && !snapshot.Terminal() {
		select {
		case <-ctx.Done():
			t.Fatalf("late selected response did not reach its terminal delivery: snapshot=%+v cause=%v", snapshot, context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
		snapshot, err = store.Snapshot(ctx, id)
	}
	count, countErr := storetest.CountDeadLettersForOriginalEvent(ctx, selected, publication.Event.ID())
	if err != nil || countErr != nil || count != 1 || snapshot.Status != deliverylifecycle.StatusDeadLetter || snapshot.ClaimVersion != 1 || snapshot.ReasonCode != "handler_terminal_failure" ||
		snapshot.Failure == nil || snapshot.Failure.Class != failures.ClassStaleArrival || snapshot.Failure.Detail.Code != "join_stage_closed" {
		t.Fatalf("late selected response missed exact expired-arm refusal: snapshot=%+v failure=%+v count=%d errors=%v/%v", snapshot, snapshot.Failure, count, err, countErr)
	}
}
