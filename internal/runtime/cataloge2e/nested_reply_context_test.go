package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// These are ordinary real node executions. Fork/restart/deadline and agent K3
// cells have separate proof obligations and receive no credit from this root.
func TestIssue642NodeNestedReplyChainsBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, scenario := range []struct {
			name    string
			fixture func(testing.TB) string
			chains  int
		}{
			{"NR01_depth3_chain", canonicalrouting.CopyNestedNodeReplyDepth3, 1},
			{"NR01_two_concurrent_requesters", canonicalrouting.CopyNestedNodeReplyTwoRequesters, 2},
		} {
			t.Run(string(backend)+"/"+scenario.name, func(t *testing.T) {
				var held *nestedReplyWorkerProbe
				if scenario.chains == 2 {
					node, err := identity.AdmitExecutableNodeDeclaration("u", "u-node")
					if err != nil {
						t.Fatal(err)
					}
					held = &nestedReplyWorkerProbe{nodeID: node.Key(), started: make(chan lifecycleprobe.Signal, 2), release: make(chan struct{})}
					t.Cleanup(held.resume)
				}
				h := newRuntimeHarnessForBackend(t, scenario.fixture(t), backend, true)
				if held != nil {
					seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
					h.rt.Pipeline.SetTestLifecycleProbe(held)
					// Release before harness shutdown even if an overlap assertion fails.
					t.Cleanup(held.resume)
				}
				step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "same-business-key"}}
				step.eventID, step.createdAt, step.sourceAgent, step.inputKind = uuid.NewString(), time.Now().UTC(), "cataloge2e", catalogReplayInputRootIngress
				if held != nil {
					published := make(chan error, 2)
					for _, input := range []string{"r1.started", "r2.started"} {
						trigger := step
						trigger.Event, trigger.eventID = input, uuid.NewString()
						// No harness preview: both inputs must reach the real runtime
						// independently while the first downstream claim is held.
						go func() { published <- h.publishRuntimeEventResultForStep(trigger, 20*time.Second, false) }()
					}
					proveNestedReplyOpenOverlap(t, h, held, published)
					held.resume()
					for range 2 {
						if err := <-published; err != nil {
							t.Fatalf("publish interleaved request start: %v", err)
						}
					}
				} else {
					h.publishAndWait(step, 20*time.Second)
				}
				var expected catalogExpectedDocument
				expected.Expected.EmittedEvents = []string{"request.finished"}
				h.waitForExpectedEmittedEvents(expected, 20*time.Second)
				h.waitForCatalogStoreQuiescence(20 * time.Second)
				proveNestedReplyRowAndReturnBindings(t, h, scenario.chains)
			})
		}
	}
}

// Use the existing claimed-handler probe, before preparation or business writes.
// No output, destination, row, correlation or settlement is supplied by the probe.
type nestedReplyWorkerProbe struct {
	nodeID  string
	started chan lifecycleprobe.Signal
	release chan struct{}
	once    sync.Once
}

func (p *nestedReplyWorkerProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerStarted || signal.SubscriberID != p.nodeID {
		return
	}
	select {
	case p.started <- signal:
	case <-ctx.Done():
		return
	}
	select {
	case <-p.release:
	case <-ctx.Done():
	}
}

func (p *nestedReplyWorkerProbe) resume() { p.once.Do(func() { close(p.release) }) }

func proveNestedReplyOpenOverlap(t *testing.T, h *runtimeHarness, held *nestedReplyWorkerProbe, publications <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 20*time.Second)
	defer cancel()
	var claim lifecycleprobe.Signal
	select {
	case claim = <-held.started:
		if claim.EventID == "" {
			t.Fatal("inner worker proof lacks an actual claim")
		}
	case <-ctx.Done():
		t.Fatalf("wait for actual inner worker claim: %v", ctx.Err())
	}
	selected, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-publications:
			t.Fatalf("input completed before overlapping requester admission, error=%v", err)
		default:
		}
		records, err := storetest.ReadReplyContextRecords(ctx, selected, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		outer := map[string]bool{}
		rows := map[string]replycontext.Record{}
		var inner replycontext.Record
		for _, record := range records {
			if record.State != replycontext.StateOpen || record.AcceptedReplyEventID != "" {
				t.Fatalf("overlapping request already settled: %+v", record)
			}
			rows[record.ID] = record
			if record.EnclosingReplyContextID == "" {
				outer[record.RequesterFlowID] = true
			}
			if record.RequestEventID == claim.EventID {
				inner = record
			}
		}
		if outer["r1"] && outer["r2"] && len(outer) == 2 {
			middle, found := rows[inner.EnclosingReplyContextID]
			parent, parentFound := rows[middle.EnclosingReplyContextID]
			if !found || !parentFound || inner.ProviderFlowID != "u" || middle.ProviderFlowID != "t" || parent.ProviderFlowID != "provider" || parent.EnclosingReplyContextID != "" {
				t.Fatalf("held depth-three request lacks its own open enclosing chain: inner=%+v middle=%+v outer=%+v", inner, middle, parent)
			}
			t.Logf("two original requesters overlap before the first inner return: open_rows=%d first_requester=%s", len(rows), parent.RequesterFlowID)
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wait for both original requesters open before inner return: %v, outer=%v rows=%v", ctx.Err(), outer, rows)
		}
	}
}

func proveNestedReplyRowAndReturnBindings(t *testing.T, h *runtimeHarness, chains int) {
	t.Helper()
	var owner interface {
		bus.PreparedPublishEventReader
		replycontext.Store
	} = h.pg
	if h.sqlite != nil {
		owner = h.sqlite
	}
	selected, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	ids, err := storetest.ReadReplyContextRequestIDs(h.ctx, selected, catalogRuntimeRunID)
	if err != nil || len(ids) != 3*chains {
		t.Fatalf("depth-three request inventory: %v %v, want %d", ids, err, 3*chains)
	}
	contexts := map[string]replycontext.Record{}
	for _, requestID := range ids {
		request, found, err := owner.LoadPreparedPublishEvent(h.ctx, requestID)
		if err != nil || !found || len(request.DeliveryRoutes) != 1 {
			t.Fatalf("exact request row/routes: %s %+v %v", requestID, request, err)
		}
		id := request.DeliveryRoutes[0].Context.ReplyContextID()
		record, err := owner.LoadReplyContext(h.ctx, id)
		if err != nil || record.ID != id || record.State != replycontext.StateTerminal || record.AcceptedReplyEventID == "" ||
			record.RequestEventID != requestID || record.SourceRunID != catalogRuntimeRunID || record.SourceRequestEventID != requestID {
			t.Fatalf("own request terminal evidence: %+v %v", record, err)
		}
		if _, duplicate := contexts[id]; duplicate {
			t.Fatal("independent requests shared a context row")
		}
		contexts[id] = record
	}
	levels := map[string]int{}
	roots := map[string]bool{}
	for _, record := range contexts {
		levels[record.ProviderFlowID]++
		if record.EnclosingReplyContextID == "" {
			roots[record.RequesterFlowID] = true
			if record.ProviderFlowID != "provider" {
				t.Fatal("inner request lost its enclosing obligation")
			}
		} else {
			parent, present := contexts[record.EnclosingReplyContextID]
			if !present || parent.ProviderFlowID != record.RequesterFlowID || parent.RunID != record.RunID || parent.ID == record.ID {
				t.Fatalf("nested request borrowed a different chain: %+v parent=%+v", record, parent)
			}
		}
		reply, found, err := owner.LoadPreparedPublishEvent(h.ctx, record.AcceptedReplyEventID)
		if err != nil || !found || len(reply.DeliveryRoutes) != 1 {
			t.Fatalf("exact accepted reply routes: %+v %v", reply, err)
		}
		returned := reply.DeliveryRoutes[0]
		if returned.Context.ReplyContextID() != record.EnclosingReplyContextID || returned.Target.Route() != record.Origin {
			t.Fatalf("inner return failed to restore only its own parent/origin: context=%+v route=%+v record=%+v", returned.Context, returned.Target.Route(), record)
		}
	}
	if levels["provider"] != chains || levels["t"] != chains || levels["u"] != chains || len(roots) != chains {
		t.Fatalf("three levels/distinct requester ownership not proven: levels=%v roots=%v", levels, roots)
	}
	rows := catalogEventsSince(t, selected, h.startedAt)
	finished := map[string]int{}
	for _, row := range rows {
		if row.Name != "request.finished" {
			continue
		}
		event, found, err := owner.LoadPreparedPublishEvent(h.ctx, row.ID)
		if err != nil || !found {
			t.Fatalf("read actual final publication: %v %v", found, err)
		}
		raw := event.Event.Event().Payload()
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["request_id"] != "same-business-key" || payload["result"] != "approved" {
			t.Fatalf("nested result changed frozen business data: %s", raw)
		}
		requester := "."
		if chains == 2 {
			var valid bool
			requester, valid = payload["requester"].(string)
			if !valid || (requester != "r1" && requester != "r2") {
				t.Fatalf("wrong original requester result: %s", raw)
			}
		}
		finished[requester]++
	}
	if len(finished) != chains {
		t.Fatalf("final requester inventory: %v", finished)
	}
	for requester, count := range finished {
		if count != 1 {
			t.Fatalf("requester %s completed %d times, want one", requester, count)
		}
	}
	allEvents := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		allEvents[row.ID] = struct{}{}
	}
	assertPublishedEventDeadLetter(t, selected, allEvents, false)
	if chains == 1 {
		proveNestedReplyPerLevelPreflightRefusals(t, h, owner, selected, contexts)
	}
}

func proveNestedReplyPerLevelPreflightRefusals(t *testing.T, h *runtimeHarness, owner interface {
	bus.PreparedPublishEventReader
	replycontext.Store
}, selected any, contexts map[string]replycontext.Record) {
	t.Helper()
	levels := map[string]replycontext.Record{}
	for _, record := range contexts {
		level := map[string]string{"provider": "Q1", "t": "Q2", "u": "Q3"}[record.ProviderFlowID]
		if level == "" || levels[level].ID != "" {
			t.Fatal("depth-three refusal proof lacks one exact context per level")
		}
		levels[level] = record
	}
	for _, level := range []string{"Q1", "Q2", "Q3"} {
		record := levels[level]
		accepted, found, err := owner.LoadPreparedPublishEvent(h.ctx, record.AcceptedReplyEventID)
		if err != nil || !found {
			t.Fatalf("actual accepted response for %s: %v %v", level, found, err)
		}
		for _, target := range []string{"Q1", "Q2", "Q3"} {
			name, failure := "NR08_wrong_level/"+level+"_to_"+target, pinrouting.FailureStaleArrival.Code()
			if target == level {
				name, failure = "NR06_second_distinct_reply/"+level, pinrouting.FailureReplyAlreadyTerminal.Code()
			}
			t.Run(name, func(t *testing.T) {
				original := accepted.Event.Event()
				event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), original.Type(), original.Producer(), "", original.Payload(),
					original.ChainDepth(), events.LineageFromEvent(original), events.EnvelopeForSourceRoute(events.EventEnvelope{}, original.Envelope().Source), original.RoutingSource(), time.Now().UTC())
				ctx := events.WithDeliveryContext(h.ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: levels[target].ID}})
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(h.ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := h.rt.Bus.CheckPublishRecipientPlan(ctx, event)
				if err != nil || plan.TargetFailure != failure || len(plan.DeliveryRoutes) != 0 {
					t.Fatalf("exact per-level refusal: plan=%+v error=%v want=%s", plan, err, failure)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(h.ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("refusal changed application state/evidence: %v", err)
				}
				for _, expected := range levels {
					actual, err := owner.LoadReplyContext(h.ctx, expected.ID)
					if err != nil || !reflect.DeepEqual(actual, expected) {
						t.Fatalf("refusal changed a different level's reply binding/settlement: %+v %v", actual, err)
					}
				}
			})
		}
	}
}
