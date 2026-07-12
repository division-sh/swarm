package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	bus := eventReplayTestBus(t, pg)
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-b")
	chA := bus.Subscribe("agent-a")
	defer bus.Unsubscribe("agent-a")
	chB := bus.Subscribe("agent-b")
	defer bus.Unsubscribe("agent-b")
	original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a", "agent-b"}, eventReplayStatusDelivered)
	handler := eventReplayTestHandler(t, pg, bus)

	body := eventReplayBody(original.EventID, nil, "idem-replay")
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("event.replay error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	replayEventID := stringValue(t, result["replay_event_id"], "replay_event_id")
	auditEventID := stringValue(t, result["audit_event_id"], "audit_event_id")
	if replayEventID == original.EventID || auditEventID == original.EventID || auditEventID == replayEventID {
		t.Fatalf("event IDs not distinct: original=%s replay=%s audit=%s", original.EventID, replayEventID, auditEventID)
	}
	assertStringSet(t, stringSliceValue(t, result["subscribers_replayed"], "subscribers_replayed"), []string{"agent-a", "agent-b"})
	if got := len(asSlice(t, result["original_deliveries"])); got != 2 {
		t.Fatalf("original deliveries = %d, want 2", got)
	}
	newDeliveries := asSlice(t, result["new_deliveries"])
	if got := len(newDeliveries); got != 2 {
		t.Fatalf("new deliveries = %d, want 2", got)
	}
	for _, item := range newDeliveries {
		delivery := asMap(t, item)
		if strings.TrimSpace(fmt.Sprint(delivery["source_delivery_id"])) == "" {
			t.Fatalf("new delivery missing source_delivery_id: %#v", delivery)
		}
	}
	assertReplayEventDelivered(t, chA, replayEventID, original.EventID)
	assertReplayEventDelivered(t, chB, replayEventID, original.EventID)
	assertReplayPersistence(t, db, original.EventID, replayEventID, auditEventID, 2)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}

	replayed := rpcCall(t, handler, body)
	if replayed.Error != nil {
		t.Fatalf("event.replay idempotent retry error = %#v", replayed.Error)
	}
	replayedResult := asMap(t, replayed.Result)
	if replayedResult["replay_event_id"] != replayEventID || replayedResult["audit_event_id"] != auditEventID {
		t.Fatalf("idempotent result = %#v, want original replay/audit IDs", replayedResult)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after idempotent retry = %d, want original+replay", count)
	}

	conflict := rpcCall(t, handler, eventReplayBody(original.EventID, []string{"agent-a"}, "idem-replay"))
	if conflict.Error == nil {
		t.Fatal("event.replay idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("event.replay conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after conflict = %d, want no duplicate replay", count)
	}
}

func TestReplayEventFromOriginalUsesCanonicalEventEntityOnly(t *testing.T) {
	now := time.Unix(1700001200, 0).UTC()
	original := store.OperatorEventFull{
		EventID:   "evt-original",
		EventName: "scan.requested",
		RunID:     "run-1",
		Source:    "origin-agent",
		Payload:   map[string]any{"entity_id": "payload-entity", "topic": "medicine"},
	}

	replay, err := replayEventFromOriginal(original, "evt-replay", now)
	if err != nil {
		t.Fatalf("replayEventFromOriginal: %v", err)
	}
	if replay.EntityID() != "" {
		t.Fatalf("replay event entity_id = %q, want empty", replay.EntityID())
	}
	var replayPayload map[string]any
	if err := json.Unmarshal(replay.Payload(), &replayPayload); err != nil {
		t.Fatalf("unmarshal replay payload: %v", err)
	}
	if replayPayload["entity_id"] != "payload-entity" {
		t.Fatalf("replay payload = %#v, want payload entity_id preserved", replayPayload)
	}

	auditPayload, err := eventReplayAuditPayload(
		Request{ActorTokenID: "actor-1"},
		original,
		"evt-replay",
		"evt-audit",
		[]string{"agent-a"},
		nil,
	)
	if err != nil {
		t.Fatalf("eventReplayAuditPayload: %v", err)
	}
	var audit map[string]any
	if err := json.Unmarshal(auditPayload, &audit); err != nil {
		t.Fatalf("unmarshal audit payload: %v", err)
	}
	if audit["entity_id"] == "payload-entity" {
		t.Fatalf("audit entity_id = %#v, want canonical top-level identity only", audit["entity_id"])
	}
	if audit["entity_id"] != "" {
		t.Fatalf("audit entity_id = %#v, want empty canonical identity", audit["entity_id"])
	}
}

func TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	inner := eventReplayTestBus(t, pg)
	publisher := &failOnceAuditEventPublisher{inner: inner, err: errors.New("audit publish temporarily unavailable")}
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
	ch := inner.Subscribe("agent-a")
	defer inner.Unsubscribe("agent-a")
	original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
	handler := eventReplayTestHandler(t, pg, publisher)
	body := eventReplayBody(original.EventID, nil, "idem-audit-failure")

	first := rpcCall(t, handler, body)
	requireRPCFailure(t, first.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
	assertReplayEventDelivered(t, ch, latestEventIDByName(t, db, "scan.requested", original.EventID), original.EventID)
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after audit failure = %d, want original+replay", count)
	}
	if count := countEventsByName(t, db, "event.replayed"); count != 0 {
		t.Fatalf("event.replayed events after failed audit publish = %d, want 0", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after audit failure = %d, want 1", count)
	}

	retry := rpcCall(t, handler, body)
	if retry.Error != nil {
		t.Fatalf("retry event.replay after audit recovery error = %#v", retry.Error)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after retry = %d, want no duplicate replay", count)
	}
	if count := countEventsByName(t, db, "event.replayed"); count != 1 {
		t.Fatalf("event.replayed events after retry = %d, want 1", count)
	}
	result := asMap(t, retry.Result)
	if stringValue(t, result["event_id"], "event_id") != original.EventID {
		t.Fatalf("retry result = %#v, want original event id %s", result, original.EventID)
	}
}

func TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	bus := eventReplayTestBus(t, pg)
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
	ch := bus.Subscribe("agent-a")
	defer bus.Unsubscribe("agent-a")
	fillAgentChannel(t, ctx, bus, "agent-a", cap(ch))
	original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
	handler := eventReplayTestHandler(t, pg, bus)
	body := eventReplayBody(original.EventID, nil, "idem-direct-fanout-failure")

	first := rpcCall(t, handler, body)
	if first.Error == nil {
		t.Fatal("first event.replay unexpectedly succeeded")
	}
	failure := asMap(t, asMap(t, asMap(t, first.Error.Data)["details"])["failure"])
	detail := asMap(t, failure["detail"])
	if failure["class"] != string(runtimefailures.ClassTargetUnreachable) || detail["code"] != "authoritative_delivery_incomplete" {
		t.Fatalf("first event.replay failure = %#v, want canonical authoritative delivery failure", failure)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after fanout error = %d, want original+replay", count)
	}
	replayEventID := latestEventIDByName(t, db, "scan.requested", original.EventID)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after fanout error = %d, want 1", count)
	}
	if count := countEventsByName(t, db, "event.replayed"); count != 1 {
		t.Fatalf("event.replayed events after fanout error = %d, want 1", count)
	}

	drainAgentChannel(t, ch, cap(ch))
	retry := rpcCall(t, handler, body)
	if retry.Error != nil {
		t.Fatalf("retry event.replay after direct fanout recovery error = %#v", retry.Error)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after retry = %d, want no duplicate replay", count)
	}
	if count := countEventsByName(t, db, "event.replayed"); count != 1 {
		t.Fatalf("event.replayed events after retry = %d, want no duplicate audit", count)
	}
	result := asMap(t, retry.Result)
	if got := stringValue(t, result["replay_event_id"], "replay_event_id"); got != replayEventID {
		t.Fatalf("retry replay_event_id = %s, want persisted replay %s", got, replayEventID)
	}
}

func TestOperatorEventReplaySubsetAndFailClosedCases(t *testing.T) {
	t.Run("subset targets only requested original subscriber", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-b")
		chA := bus.Subscribe("agent-a")
		defer bus.Unsubscribe("agent-a")
		chB := bus.Subscribe("agent-b")
		defer bus.Unsubscribe("agent-b")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a", "agent-b"}, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, bus)

		resp := rpcCall(t, handler, eventReplayBody(original.EventID, []string{"agent-a"}, "idem-subset"))
		if resp.Error != nil {
			t.Fatalf("event.replay subset error = %#v", resp.Error)
		}
		result := asMap(t, resp.Result)
		assertStringSet(t, stringSliceValue(t, result["subscribers_replayed"], "subscribers_replayed"), []string{"agent-a"})
		assertReplayEventDelivered(t, chA, stringValue(t, result["replay_event_id"], "replay_event_id"), original.EventID)
		assertNoReplayEvent(t, chB)
	})

	t.Run("missing event", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		handler := eventReplayTestHandler(t, pg, eventReplayTestBus(t, pg))
		resp := rpcCall(t, handler, eventReplayBody(uuid.NewString(), nil, "idem-missing"))
		if resp.Error == nil {
			t.Fatal("missing event.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventNotFoundCode {
			t.Fatalf("missing event data = %#v, want %s", data, EventNotFoundCode)
		}
		if count := countEventsByName(t, db, "event.replayed"); count != 0 {
			t.Fatalf("audit event count = %d, want 0", count)
		}
	})

	t.Run("zero original agent delivery history", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", nil, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, eventReplayTestBus(t, pg))
		resp := rpcCall(t, handler, eventReplayBody(original.EventID, nil, "idem-empty"))
		if resp.Error == nil {
			t.Fatal("zero-history event.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplayNoDeliveryHistoryCode {
			t.Fatalf("zero-history data = %#v, want %s", data, EventReplayNoDeliveryHistoryCode)
		}
	})

	t.Run("pending node-only delivery is not replay eligible and does not mutate", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		eventID := uuid.NewString()
		runID := uuid.NewString()
		if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(
			eventID,
			events.EventType("scan.requested"),
			"workflow-runtime",
			"",
			[]byte(`{"topic":"medicine"}`),
			0,
			runID,
			"",
			events.EventEnvelope{EntityID: runID},
			time.Now().UTC())); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, retry_count, created_at
			)
			VALUES ($1::uuid, $2::uuid, 'node', 'workflow-runtime', 'pending', 0, now())
		`, runID, eventID); err != nil {
			t.Fatalf("seed node-only delivery: %v", err)
		}
		handler := eventReplayTestHandler(t, pg, eventReplayTestBus(t, pg))
		resp := rpcCall(t, handler, eventReplayBody(eventID, nil, "idem-node-only-pending"))
		if resp.Error == nil {
			t.Fatal("node-only event.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplayNoDeliveryHistoryCode {
			t.Fatalf("node-only data = %#v, want %s", data, EventReplayNoDeliveryHistoryCode)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 1 {
			t.Fatalf("scan.requested events after node-only replay = %d, want original only", count)
		}
		if count := countEventsByName(t, db, "event.replayed"); count != 0 {
			t.Fatalf("event.replayed events after node-only replay = %d, want 0", count)
		}
		if count := countAPIIdempotencyRows(t, db); count != 0 {
			t.Fatalf("api_idempotency rows after node-only replay = %d, want 0", count)
		}
		var status string
		if err := db.QueryRowContext(ctx, `
			SELECT status FROM event_deliveries
			WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
		`, eventID).Scan(&status); err != nil {
			t.Fatalf("load node-only delivery status: %v", err)
		}
		if status != eventReplayStatusPending {
			t.Fatalf("node-only delivery status = %q, want pending", status)
		}
	})

	t.Run("requested subscriber was not original", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		bus.Subscribe("agent-a")
		defer bus.Unsubscribe("agent-a")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, bus)
		resp := rpcCall(t, handler, eventReplayBody(original.EventID, []string{"agent-b"}, "idem-not-original"))
		if resp.Error == nil {
			t.Fatal("non-original event.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplaySubscriberNotOriginalCode {
			t.Fatalf("non-original data = %#v, want %s", data, EventReplaySubscriberNotOriginalCode)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 1 {
			t.Fatalf("scan.requested events = %d, want original only", count)
		}
	})

	t.Run("original subscriber no longer deliverable", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, bus)
		resp := rpcCall(t, handler, eventReplayBody(original.EventID, nil, "idem-unavailable"))
		if resp.Error == nil {
			t.Fatal("unavailable subscriber event.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplaySubscriberUnavailableCode {
			t.Fatalf("unavailable data = %#v, want %s", data, EventReplaySubscriberUnavailableCode)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 1 {
			t.Fatalf("scan.requested events = %d, want original only", count)
		}
	})

	for _, status := range []string{eventReplayStatusPending, eventReplayStatusInProgress} {
		t.Run("nonterminal original delivery is not eligible "+status, func(t *testing.T) {
			ctx := context.Background()
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			pg := &store.PostgresStore{DB: db}
			bus := eventReplayTestBus(t, pg)
			seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
			bus.Subscribe("agent-a")
			defer bus.Unsubscribe("agent-a")
			original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, status)
			handler := eventReplayTestHandler(t, pg, bus)
			resp := rpcCall(t, handler, eventReplayBody(original.EventID, nil, "idem-not-eligible-"+status))
			if resp.Error == nil {
				t.Fatal("not-eligible event.replay error = nil")
			}
			data := asMap(t, resp.Error.Data)
			details := asMap(t, data["details"])
			if data["code"] != EventReplayNotEligibleCode || details["status"] != status {
				t.Fatalf("not-eligible data = %#v, want %s status %s", data, EventReplayNotEligibleCode, status)
			}
			if count := countEventsByName(t, db, "event.replayed"); count != 0 {
				t.Fatalf("event.replayed events after nonterminal replay = %d, want 0", count)
			}
			if count := countAPIIdempotencyRows(t, db); count != 0 {
				t.Fatalf("api_idempotency rows after nonterminal replay = %d, want 0", count)
			}
			var persistedStatus string
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(status, '')
				FROM event_deliveries
				WHERE event_id = $1::uuid AND subscriber_type = 'agent' AND subscriber_id = 'agent-a'
			`, original.EventID).Scan(&persistedStatus); err != nil {
				t.Fatalf("load original delivery after nonterminal replay: %v", err)
			}
			if persistedStatus != status {
				t.Fatalf("original delivery status after replay = %q, want unchanged %q", persistedStatus, status)
			}
		})
	}
}

func TestOperatorAgentReplayProjectsSingletonEventReplayOwner(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	bus := eventReplayTestBus(t, pg)
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-b")
	chA := bus.Subscribe("agent-a")
	defer bus.Unsubscribe("agent-a")
	chB := bus.Subscribe("agent-b")
	defer bus.Unsubscribe("agent-b")
	original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a", "agent-b"}, eventReplayStatusDelivered)
	handler := eventReplayTestHandler(t, pg, bus)

	body := agentReplayBody(original.EventID, "agent-a", "idem-agent-replay")
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("agent.replay error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["event_id"] != original.EventID || result["agent_id"] != "agent-a" {
		t.Fatalf("agent.replay identity result = %#v, want event %s agent-a", result, original.EventID)
	}
	replayEventID := stringValue(t, result["replay_event_id"], "replay_event_id")
	auditEventID := stringValue(t, result["audit_event_id"], "audit_event_id")
	if replayEventID == original.EventID || auditEventID == original.EventID || auditEventID == replayEventID {
		t.Fatalf("event IDs not distinct: original=%s replay=%s audit=%s", original.EventID, replayEventID, auditEventID)
	}
	originalDelivery := asMap(t, result["original_delivery"])
	if originalDelivery["subscriber_id"] != "agent-a" || strings.TrimSpace(fmt.Sprint(originalDelivery["delivery_id"])) == "" {
		t.Fatalf("original_delivery = %#v, want agent-a delivery", originalDelivery)
	}
	newDelivery := asMap(t, result["new_delivery"])
	if newDelivery["subscriber_id"] != "agent-a" || newDelivery["source_delivery_id"] != originalDelivery["delivery_id"] {
		t.Fatalf("new_delivery = %#v, want agent-a source delivery %s", newDelivery, originalDelivery["delivery_id"])
	}
	assertReplayEventDelivered(t, chA, replayEventID, original.EventID)
	assertNoReplayEvent(t, chB)
	assertAgentReplayPersistence(t, db, original.EventID, replayEventID, auditEventID, "agent-a", 2)
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}

	replayed := rpcCall(t, handler, body)
	if replayed.Error != nil {
		t.Fatalf("agent.replay idempotent retry error = %#v", replayed.Error)
	}
	replayedResult := asMap(t, replayed.Result)
	if replayedResult["replay_event_id"] != replayEventID || replayedResult["audit_event_id"] != auditEventID {
		t.Fatalf("idempotent agent.replay result = %#v, want original replay/audit IDs", replayedResult)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after idempotent retry = %d, want original+replay", count)
	}

	conflict := rpcCall(t, handler, agentReplayBody(original.EventID, "agent-b", "idem-agent-replay"))
	if conflict.Error == nil {
		t.Fatal("agent.replay idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("agent.replay conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestOperatorAgentReplayFailClosedCases(t *testing.T) {
	t.Run("requested agent was not original subscriber", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		bus.Subscribe("agent-a")
		defer bus.Unsubscribe("agent-a")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, bus)

		resp := rpcCall(t, handler, agentReplayBody(original.EventID, "agent-b", "idem-agent-not-original"))
		if resp.Error == nil {
			t.Fatal("non-original agent.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplaySubscriberNotOriginalCode {
			t.Fatalf("non-original data = %#v, want %s", data, EventReplaySubscriberNotOriginalCode)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 1 {
			t.Fatalf("scan.requested events = %d, want original only", count)
		}
	})

	t.Run("original agent no longer deliverable", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
		handler := eventReplayTestHandler(t, pg, bus)

		resp := rpcCall(t, handler, agentReplayBody(original.EventID, "agent-a", "idem-agent-unavailable"))
		if resp.Error == nil {
			t.Fatal("unavailable agent.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplaySubscriberUnavailableCode {
			t.Fatalf("unavailable data = %#v, want %s", data, EventReplaySubscriberUnavailableCode)
		}
		if count := countEventsByName(t, db, "scan.requested"); count != 1 {
			t.Fatalf("scan.requested events = %d, want original only", count)
		}
	})

	t.Run("nonterminal original delivery is not eligible", func(t *testing.T) {
		ctx := context.Background()
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		bus := eventReplayTestBus(t, pg)
		seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
		bus.Subscribe("agent-a")
		defer bus.Unsubscribe("agent-a")
		original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusPending)
		handler := eventReplayTestHandler(t, pg, bus)

		resp := rpcCall(t, handler, agentReplayBody(original.EventID, "agent-a", "idem-agent-not-eligible"))
		if resp.Error == nil {
			t.Fatal("not-eligible agent.replay error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventReplayNotEligibleCode {
			t.Fatalf("not-eligible data = %#v, want %s", data, EventReplayNotEligibleCode)
		}
	})
}

func TestOperatorEventReplayQueuesWhenDispatchGated(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts runtimebus.EventBusOptions
	}{
		{name: "runtime paused", opts: runtimebus.EventBusOptions{RuntimeIngressDispatchGate: pausedRuntimeIngressGate{}}},
		{name: "run dispatch blocked", opts: runtimebus.EventBusOptions{RunDispatchGate: blockedRunDispatchGate{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			pg := &store.PostgresStore{DB: db}
			bus, err := runtimebus.NewEventBusWithOptions(pg, tc.opts)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "agent-a")
			ch := bus.Subscribe("agent-a")
			defer bus.Unsubscribe("agent-a")
			original := seedReplayableOperatorEvent(t, ctx, pg, "scan.requested", []string{"agent-a"}, eventReplayStatusDelivered)
			handler := eventReplayTestHandler(t, pg, bus)

			resp := rpcCall(t, handler, eventReplayBody(original.EventID, nil, "idem-"+strings.ReplaceAll(tc.name, " ", "-")))
			if resp.Error != nil {
				t.Fatalf("gated event.replay error = %#v", resp.Error)
			}
			result := asMap(t, resp.Result)
			assertNoReplayEvent(t, ch)
			replayEventID := stringValue(t, result["replay_event_id"], "replay_event_id")
			if got := countEventDeliveries(t, db, replayEventID); got != 1 {
				t.Fatalf("queued replay deliveries = %d, want 1", got)
			}
			if got := countPipelineReceiptsForEvent(t, ctx, db, replayEventID); got != 0 {
				t.Fatalf("queued replay pipeline receipts = %d, want 0 before release", got)
			}
		})
	}
}

type pausedRuntimeIngressGate struct{}

func (pausedRuntimeIngressGate) QueueableIngressPaused(context.Context) (bool, error) {
	return true, nil
}

type blockedRunDispatchGate struct{}

func (blockedRunDispatchGate) QueueableRunDispatchBlocked(context.Context, string) (bool, error) {
	return true, nil
}

func eventReplayTestBus(t *testing.T, pg *store.PostgresStore) *runtimebus.EventBus {
	t.Helper()
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(runStartTestBundle("scan.requested")),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return bus
}

type failOnceAuditEventPublisher struct {
	inner *runtimebus.EventBus
	err   error
}

func (p *failOnceAuditEventPublisher) Publish(ctx context.Context, evt events.Event) error {
	if strings.TrimSpace(string(evt.Type())) == eventReplaySyntheticEventName && p.err != nil {
		err := p.err
		p.err = nil
		return err
	}
	return p.inner.Publish(ctx, evt)
}

func (p *failOnceAuditEventPublisher) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	return p.inner.PublishDirect(ctx, evt, recipients)
}

func (p *failOnceAuditEventPublisher) CheckDirectRecipients(ctx context.Context, evt events.Event, recipients []string) (runtimebus.DirectRecipientStatus, error) {
	return p.inner.CheckDirectRecipients(ctx, evt, recipients)
}

func eventReplayTestHandler(t *testing.T, pg *store.PostgresStore, bus eventReplayPublisher) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:           func() time.Time { return time.Now().UTC() },
			Ready:         func() bool { return true },
			Database:      fakePinger{},
			Runs:          pg,
			Observability: pg,
			Idempotency:   pg,
			Events:        bus,
			Source:        semanticview.Wrap(runStartTestBundle("scan.requested")),
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				Fingerprint:     runStartTestFingerprint,
			},
		}),
	})
}

func seedReplayableOperatorEvent(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventName string, subscribers []string, status string) store.OperatorEventFull {
	t.Helper()
	eventID := uuid.NewString()
	runID := uuid.NewString()
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(
		eventID,
		events.EventType(eventName),
		"origin-agent",
		"",
		[]byte(`{"topic":"medicine"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, runID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, subscribers); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	for _, subscriber := range subscribers {
		sessionID := uuid.NewString()
		if _, err := pg.DB.ExecContext(ctx, `
			UPDATE event_deliveries
			SET status = $3, active_session_id = $4::uuid
			WHERE event_id = $1::uuid AND subscriber_id = $2
		`, eventID, subscriber, status, sessionID); err != nil {
			t.Fatalf("mark original delivery %s %s: %v", eventID, subscriber, err)
		}
	}
	event, err := pg.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("LoadOperatorEvent(%s): %v", eventID, err)
	}
	return event
}

func eventReplayBody(eventID string, subscribers []string, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"event_id":%q`, eventID),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if subscribers != nil {
		raw, _ := json.Marshal(subscribers)
		parts = append(parts, fmt.Sprintf(`"subscribers":%s`, raw))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"replay","method":"event.replay","params":{%s}}`, strings.Join(parts, ","))
}

func agentReplayBody(eventID, agentID, idempotencyKey string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"agent-replay","method":"agent.replay","params":{"event_id":%q,"agent_id":%q,"idempotency_key":%q}}`, eventID, agentID, idempotencyKey)
}

func assertReplayEventDelivered(t *testing.T, ch <-chan events.Event, replayEventID, originalEventID string) {
	t.Helper()
	got := requireAPIV1RuntimeBusEvent(t, ch, "replay event "+replayEventID)
	if got.ID() != replayEventID || got.ParentEventID() != originalEventID {
		t.Fatalf("delivered replay event id=%s parent=%s, want id=%s parent=%s", got.ID(), got.ParentEventID(), replayEventID, originalEventID)
	}
}

func assertNoReplayEvent(t *testing.T, ch <-chan events.Event) {
	t.Helper()
	requireNoAPIV1RuntimeBusEvent(t, ch, "replay assertion")
}

func fillAgentChannel(t *testing.T, ctx context.Context, bus *runtimebus.EventBus, agentID string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		err := bus.PublishDirect(ctx, eventtest.RootIngress(uuid.NewString(),
			events.EventType("filler.event"), "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()), []string{agentID})
		if err != nil {
			t.Fatalf("fill agent channel publish %d: %v", i, err)
		}
	}
}

func drainAgentChannel(t *testing.T, ch <-chan events.Event, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		requireAPIV1RuntimeBusEvent(t, ch, fmt.Sprintf("agent channel drain item %d", i))
	}
}

func assertReplayPersistence(t *testing.T, db *sql.DB, originalEventID, replayEventID, auditEventID string, wantDeliveries int) {
	t.Helper()
	if got := countEventDeliveries(t, db, replayEventID); got != wantDeliveries {
		t.Fatalf("replay event deliveries = %d, want %d", got, wantDeliveries)
	}
	if got := countEventDeliveries(t, db, originalEventID); got != wantDeliveries {
		t.Fatalf("original event deliveries = %d, want preserved %d", got, wantDeliveries)
	}
	if got := countEventsByName(t, db, "event.replayed"); got != 1 {
		t.Fatalf("event.replayed count = %d, want 1", got)
	}
	var sourceEventID, payloadRaw string
	if err := db.QueryRow(`
		SELECT COALESCE(source_event_id::text, ''), payload::text
		FROM events
		WHERE event_id = $1::uuid
	`, replayEventID).Scan(&sourceEventID, &payloadRaw); err != nil {
		t.Fatalf("load replay event lineage: %v", err)
	}
	if sourceEventID != originalEventID {
		t.Fatalf("replay source_event_id = %q, want original %q", sourceEventID, originalEventID)
	}
	if err := db.QueryRow(`
		SELECT COALESCE(source_event_id::text, ''), payload::text
		FROM events
		WHERE event_id = $1::uuid
	`, auditEventID).Scan(&sourceEventID, &payloadRaw); err != nil {
		t.Fatalf("load audit event: %v", err)
	}
	if sourceEventID != originalEventID {
		t.Fatalf("audit source_event_id = %q, want original %q", sourceEventID, originalEventID)
	}
	if !strings.Contains(payloadRaw, replayEventID) || !strings.Contains(payloadRaw, originalEventID) {
		t.Fatalf("audit payload = %s, want original and replay IDs", payloadRaw)
	}
}

func assertAgentReplayPersistence(t *testing.T, db *sql.DB, originalEventID, replayEventID, auditEventID, agentID string, wantOriginalDeliveries int) {
	t.Helper()
	if got := countEventDeliveries(t, db, replayEventID); got != 1 {
		t.Fatalf("agent replay event deliveries = %d, want 1", got)
	}
	if got := countEventDeliveries(t, db, originalEventID); got != wantOriginalDeliveries {
		t.Fatalf("original event deliveries = %d, want preserved %d", got, wantOriginalDeliveries)
	}
	if got := countEventsByName(t, db, "event.replayed"); got != 1 {
		t.Fatalf("event.replayed count = %d, want 1", got)
	}
	var sourceEventID, payloadRaw string
	if err := db.QueryRow(`
		SELECT COALESCE(source_event_id::text, ''), payload::text
		FROM events
		WHERE event_id = $1::uuid
	`, replayEventID).Scan(&sourceEventID, &payloadRaw); err != nil {
		t.Fatalf("load agent replay event lineage: %v", err)
	}
	if sourceEventID != originalEventID {
		t.Fatalf("agent replay source_event_id = %q, want original %q", sourceEventID, originalEventID)
	}
	if err := db.QueryRow(`
		SELECT COALESCE(source_event_id::text, ''), payload::text
		FROM events
		WHERE event_id = $1::uuid
	`, auditEventID).Scan(&sourceEventID, &payloadRaw); err != nil {
		t.Fatalf("load agent replay audit event: %v", err)
	}
	if sourceEventID != originalEventID {
		t.Fatalf("agent replay audit source_event_id = %q, want original %q", sourceEventID, originalEventID)
	}
	if !strings.Contains(payloadRaw, replayEventID) || !strings.Contains(payloadRaw, originalEventID) || !strings.Contains(payloadRaw, agentID) {
		t.Fatalf("agent replay audit payload = %s, want original/replay IDs and %s", payloadRaw, agentID)
	}
}

func countEventDeliveries(t *testing.T, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'agent'`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries: %v", err)
	}
	return count
}

func latestEventIDByName(t *testing.T, db *sql.DB, eventName, excludeEventID string) string {
	t.Helper()
	var eventID string
	if err := db.QueryRow(`
		SELECT event_id::text
		FROM events
		WHERE event_name = $1 AND event_id::text <> $2
		ORDER BY created_at DESC
		LIMIT 1
	`, eventName, excludeEventID).Scan(&eventID); err != nil {
		t.Fatalf("latest event by name %s: %v", eventName, err)
	}
	return eventID
}

func stringSliceValue(t *testing.T, value any, field string) []string {
	t.Helper()
	items := asSlice(t, value)
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("%s item = %#v, want string", field, item)
		}
		out = append(out, text)
	}
	return out
}

func assertStringSet(t *testing.T, got, want []string) {
	t.Helper()
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("strings = %#v, want %#v", got, want)
	}
}
