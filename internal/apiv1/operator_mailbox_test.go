package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimeingress "swarm/internal/runtime/ingress"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestOperatorMailboxHandlersSupportedRPCPath(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		PayloadValidator: mailboxItemDecidedPayloadValidator(t, mailboxItemDecidedStrictPayloadSchema(true)),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	base := time.Date(2026, 5, 10, 11, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'empire/review', 'review_case', 'review-case-1', 'Review Case 1',
			'awaiting_decision', '{"ready":true}'::jsonb, '{"score":9}'::jsonb, '{"notes":["needs approval"]}'::jsonb, 7,
			$3, $3, $3
		)
	`, runID, entityID, base); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   runID,
		Payload: json.RawMessage(`{"request":true}`),
	}.WithEntityID(entityID).WithFlowInstance("empire/review")); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   sourceEventID,
		EntityID:  entityID,
		FromAgent: "empire-agent",
		Type:      "review_request",
		Priority:  "high",
		Context:   []byte(`{"title":"review this"}`),
		Summary:   "needs approval",
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET flow_instance = 'empire/review' WHERE item_id = $1::uuid`, mailboxID); err != nil {
		t.Fatalf("set flow instance: %v", err)
	}
	bundle := runStartTestBundle("mailbox.item_decided")
	bundle.Events["mailbox.item_decided"] = runtimecontracts.EventCatalogEntry{
		Consumer: []string{"approval-agent", "review-agent"},
	}
	source := semanticview.Wrap(bundle)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now: func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready: func() bool {
				return true
			},
			Database:    fakePinger{},
			Entities:    pg,
			Mailbox:     pg,
			Idempotency: pg,
			Events:      bus,
			Source:      source,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "empire",
				WorkflowVersion: "1.0.0",
				Fingerprint:     "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"mailbox.list","params":{"status":"pending","run_id":"`+runID+`","entity_id":"`+entityID+`","type":"review_request","priority":"high","limit":1}}`)
	if list.Error != nil {
		t.Fatalf("mailbox.list error = %#v", list.Error)
	}
	items := asMap(t, list.Result)["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("mailbox.list items = %#v", items)
	}
	item := asMap(t, items[0])
	if item["mailbox_id"] != mailboxID || item["source_flow"] != "empire/review" || item["priority"] != "high" {
		t.Fatalf("mailbox.list item = %#v", item)
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"mailbox.get","params":{"mailbox_id":"`+mailboxID+`"}}`)
	if get.Error != nil {
		t.Fatalf("mailbox.get error = %#v", get.Error)
	}
	if got := asMap(t, asMap(t, get.Result)["item"])["mailbox_id"]; got != mailboxID {
		t.Fatalf("mailbox.get mailbox_id = %v", got)
	}
	if history := asMap(t, get.Result)["history"].([]any); len(history) != 1 {
		t.Fatalf("mailbox.get history = %#v", history)
	}
	decisionSheet := asMap(t, asMap(t, get.Result)["decision_sheet"])
	entityContext := asMap(t, decisionSheet["entity_context"])
	if entityContext["available"] != true {
		t.Fatalf("mailbox.get entity_context = %#v", entityContext)
	}
	entityFull := asMap(t, entityContext["entity"])
	entity := asMap(t, entityFull["entity"])
	if entity["entity_id"] != entityID || entity["run_id"] != runID || entity["current_state"] != "awaiting_decision" {
		t.Fatalf("mailbox.get decision_sheet entity = %#v", entityFull)
	}
	if fields := asMap(t, entityFull["fields"]); fields["score"] != float64(9) {
		t.Fatalf("mailbox.get decision_sheet fields = %#v", fields)
	}
	downstream := asMap(t, decisionSheet["downstream_preview"])
	if downstream["available"] != true || downstream["event_name"] != "mailbox.item_decided" || downstream["subscriber_source"] != "event_catalog" {
		t.Fatalf("mailbox.get downstream_preview = %#v", downstream)
	}
	subscribers := downstream["subscribers"].([]any)
	if len(subscribers) != 2 || subscribers[0] != "approval-agent" || subscribers[1] != "review-agent" {
		t.Fatalf("mailbox.get downstream subscribers = %#v", subscribers)
	}

	approveBody := `{"jsonrpc":"2.0","id":"approve","method":"mailbox.approve","params":{"mailbox_id":"` + mailboxID + `","decision_payload":{"approved":true},"idempotency_key":"idem-approve"}}`
	approved := rpcCall(t, handler, approveBody)
	if approved.Error != nil {
		t.Fatalf("mailbox.approve error = %#v", approved.Error)
	}
	approvedResult := asMap(t, approved.Result)
	downstreamID, _ := approvedResult["downstream_event_id"].(string)
	if downstreamID == "" || approvedResult["status"] != "decided" {
		t.Fatalf("mailbox.approve result = %#v", approvedResult)
	}
	if approvedResult["idempotency_replayed"] != false || approvedResult["downstream_event_name"] != "mailbox.item_decided" || approvedResult["downstream_subscriber_source"] != "event_catalog" {
		t.Fatalf("mailbox.approve promoted result fields = %#v", approvedResult)
	}
	approvedSubscribers := approvedResult["downstream_subscribers"].([]any)
	if len(approvedSubscribers) != 2 || approvedSubscribers[0] != "approval-agent" || approvedSubscribers[1] != "review-agent" {
		t.Fatalf("mailbox.approve downstream_subscribers = %#v", approvedSubscribers)
	}
	replay := rpcCall(t, handler, approveBody)
	if replay.Error != nil {
		t.Fatalf("mailbox.approve replay error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	if got := replayResult["downstream_event_id"]; got != downstreamID {
		t.Fatalf("idempotency replay downstream_event_id = %v, want %s", got, downstreamID)
	}
	if replayResult["idempotency_replayed"] != true || replayResult["downstream_event_name"] != "mailbox.item_decided" || replayResult["downstream_subscriber_source"] != "event_catalog" {
		t.Fatalf("mailbox.approve replay promoted result fields = %#v", replayResult)
	}
	replaySubscribers := replayResult["downstream_subscribers"].([]any)
	if len(replaySubscribers) != 2 || replaySubscribers[0] != "approval-agent" || replaySubscribers[1] != "review-agent" {
		t.Fatalf("mailbox.approve replay downstream_subscribers = %#v", replaySubscribers)
	}
	legacyReplayResponse, err := json.Marshal(map[string]any{
		"ok":                  true,
		"mailbox_decision_id": approvedResult["mailbox_decision_id"],
		"downstream_event_id": downstreamID,
		"status":              "decided",
	})
	if err != nil {
		t.Fatalf("marshal legacy replay response: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE api_idempotency
		SET response = $1::jsonb
		WHERE method = 'mailbox.approve'
		  AND idempotency_key = 'idem-approve'
	`, string(legacyReplayResponse)); err != nil {
		t.Fatalf("rewrite legacy idempotency response: %v", err)
	}
	legacyReplay := rpcCall(t, handler, approveBody)
	if legacyReplay.Error != nil {
		t.Fatalf("mailbox.approve legacy replay error = %#v", legacyReplay.Error)
	}
	legacyReplayResult := asMap(t, legacyReplay.Result)
	if legacyReplayResult["idempotency_replayed"] != true ||
		legacyReplayResult["downstream_event_id"] != downstreamID ||
		legacyReplayResult["downstream_event_name"] != "mailbox.item_decided" ||
		legacyReplayResult["downstream_subscriber_source"] != "unavailable" {
		t.Fatalf("mailbox.approve legacy replay result = %#v", legacyReplayResult)
	}
	legacyReplaySubscribers := legacyReplayResult["downstream_subscribers"].([]any)
	if len(legacyReplaySubscribers) != 0 {
		t.Fatalf("mailbox.approve legacy replay downstream_subscribers = %#v, want empty", legacyReplaySubscribers)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 1 {
		t.Fatalf("mailbox.item_decided event count = %d, want 1", count)
	}
	approvalPayload := loadEventPayload(t, db, downstreamID)
	assertMailboxItemDecidedPayloadShape(t, approvalPayload, "review this", true)

	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"conflict","method":"mailbox.approve","params":{"mailbox_id":"`+mailboxID+`","decision_payload":{"approved":false},"idempotency_key":"idem-approve"}}`)
	if conflict.Error == nil {
		t.Fatal("mailbox.approve conflicting idempotency error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("idempotency conflict data = %#v", data)
	}

	rejectAlready := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"reject","method":"mailbox.reject","params":{"mailbox_id":"`+mailboxID+`","reason":"too late"}}`)
	if rejectAlready.Error == nil {
		t.Fatal("mailbox.reject already-decided error = nil")
	}
	if data := asMap(t, rejectAlready.Error.Data); data["code"] != MailboxAlreadyDecidedCode {
		t.Fatalf("already decided data = %#v", data)
	}

	deferID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		Type:    "approval",
		Summary: "defer me",
		Context: []byte(`{"title":"defer"}`),
	})
	if err != nil {
		t.Fatalf("insert defer mailbox: %v", err)
	}
	past := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"past","method":"mailbox.defer","params":{"mailbox_id":"`+deferID+`","until":"2026-05-10T11:59:59Z","idempotency_key":"idem-past"}}`)
	if past.Error == nil {
		t.Fatal("mailbox.defer in past error = nil")
	}
	if data := asMap(t, past.Error.Data); data["code"] != InvalidDeferUntilCode {
		t.Fatalf("invalid defer data = %#v", data)
	}
	deferred := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"defer","method":"mailbox.defer","params":{"mailbox_id":"`+deferID+`","until":"2026-05-10T13:00:00Z","idempotency_key":"idem-defer"}}`)
	if deferred.Error != nil {
		t.Fatalf("mailbox.defer error = %#v", deferred.Error)
	}
	deferredResult := asMap(t, deferred.Result)
	if status := deferredResult["status"]; status != "deferred" {
		t.Fatalf("mailbox.defer status = %v, want deferred", status)
	}
	if deferredResult["idempotency_replayed"] != false {
		t.Fatalf("mailbox.defer idempotency_replayed = %v, want false", deferredResult["idempotency_replayed"])
	}
	laterHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now: func() time.Time { return time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC) },
			Ready: func() bool {
				return true
			},
			Database:    fakePinger{},
			Mailbox:     pg,
			Idempotency: pg,
			Events:      bus,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "empire",
				WorkflowVersion: "1.0.0",
				Fingerprint:     "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}),
	})
	deferReplay := rpcCall(t, laterHandler, `{"jsonrpc":"2.0","id":"defer-replay","method":"mailbox.defer","params":{"mailbox_id":"`+deferID+`","until":"2026-05-10T13:00:00Z","idempotency_key":"idem-defer"}}`)
	if deferReplay.Error != nil {
		t.Fatalf("mailbox.defer replay after until passed error = %#v", deferReplay.Error)
	}
	deferReplayResult := asMap(t, deferReplay.Result)
	if status := deferReplayResult["status"]; status != "deferred" {
		t.Fatalf("mailbox.defer replay status = %v, want deferred", status)
	}
	if deferReplayResult["idempotency_replayed"] != true {
		t.Fatalf("mailbox.defer replay idempotency_replayed = %v, want true", deferReplayResult["idempotency_replayed"])
	}

	rejectID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		Type:    "approval",
		Summary: "reject me",
		Context: []byte(`{"title":"reject"}`),
	})
	if err != nil {
		t.Fatalf("insert reject mailbox: %v", err)
	}
	rejectBody := `{"jsonrpc":"2.0","id":"reject-fresh","method":"mailbox.reject","params":{"mailbox_id":"` + rejectID + `","reason":"not enough evidence","idempotency_key":"idem-reject"}}`
	rejected := rpcCall(t, handler, rejectBody)
	if rejected.Error != nil {
		t.Fatalf("mailbox.reject error = %#v", rejected.Error)
	}
	rejectedResult := asMap(t, rejected.Result)
	if rejectedResult["status"] != "decided" || rejectedResult["idempotency_replayed"] != false {
		t.Fatalf("mailbox.reject result = %#v", rejectedResult)
	}
	rejectReplay := rpcCall(t, handler, rejectBody)
	if rejectReplay.Error != nil {
		t.Fatalf("mailbox.reject replay error = %#v", rejectReplay.Error)
	}
	rejectReplayResult := asMap(t, rejectReplay.Result)
	if rejectReplayResult["status"] != "decided" || rejectReplayResult["idempotency_replayed"] != true {
		t.Fatalf("mailbox.reject replay result = %#v", rejectReplayResult)
	}

	empty := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"empty","method":"mailbox.list","params":{"status":"pending","type":"review_request"}}`)
	if empty.Error != nil {
		t.Fatalf("empty mailbox.list error = %#v", empty.Error)
	}
	if items := asMap(t, empty.Result)["items"].([]any); len(items) != 0 {
		t.Fatalf("empty mailbox.list items = %#v", items)
	}
}

func TestOperatorMailboxGetDegradesEntityContextReadFailure(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	mailbox := newReadOnlyMailboxProbeStore(now)
	detail := mailbox.details["mailbox-1"]
	detail.Item.SourceEntityID = "entity-1"
	detail.Item.SourceRunID = "run-1"
	mailbox.details["mailbox-1"] = detail
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Mailbox:  mailbox,
			Entities: &fakeEntityReadStore{getErr: errors.New("entity reader temporarily unavailable")},
		}),
	})

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"mailbox.get","params":{"mailbox_id":"mailbox-1"}}`)
	if get.Error != nil {
		t.Fatalf("mailbox.get error = %#v", get.Error)
	}
	sheet := asMap(t, asMap(t, get.Result)["decision_sheet"])
	entityContext := asMap(t, sheet["entity_context"])
	if entityContext["available"] != false || entityContext["reason"] != "entity_reader_unavailable" {
		t.Fatalf("entity_context = %#v, want unavailable entity_reader_unavailable", entityContext)
	}
}

func TestOperatorMailboxApproveRejectsUndeclaredMailboxPayloadSchemaAndRollsBack(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		PayloadValidator: mailboxItemDecidedPayloadValidator(t, mailboxItemDecidedStrictPayloadSchema(false)),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	ctx := context.Background()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   uuid.NewString(),
		Payload: json.RawMessage(`{"request":true}`),
	}.WithEntityID(entityID)); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:  sourceEventID,
		EntityID: entityID,
		Type:     "review_request",
		Summary:  "needs approval",
		Context:  []byte(`{"title":"review this"}`),
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:         func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:       func() bool { return true },
			Database:    fakePinger{},
			Mailbox:     pg,
			Idempotency: pg,
			Events:      bus,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
		}),
	})

	approveBody := `{"jsonrpc":"2.0","id":"approve","method":"mailbox.approve","params":{"mailbox_id":"` + mailboxID + `","decision_payload":{"approved":true},"idempotency_key":"idem-schema-reject"}}`
	rejected := rpcCall(t, handler, approveBody)
	if rejected.Error == nil {
		t.Fatal("mailbox.approve schema rejection error = nil")
	}
	detail, err := pg.GetV1MailboxItem(ctx, mailboxID)
	if err != nil {
		t.Fatalf("get mailbox after schema rejection: %v", err)
	}
	if detail.Item.Status != "pending" {
		t.Fatalf("mailbox status after schema rejection = %s, want pending", detail.Item.Status)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 0 {
		t.Fatalf("mailbox.item_decided event count after schema rejection = %d, want 0", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("api_idempotency rows after schema rejection = %d, want 0", count)
	}

	oldShape := validMailboxItemDecidedPayload()
	oldShape["payload"] = map[string]any{"title": "review this"}
	if err := runtimetools.ValidatePayloadAgainstSchema(mailboxItemDecidedStrictPayloadSchema(true), oldShape); err == nil {
		t.Fatal("old mailbox.item_decided payload field unexpectedly passed strict schema")
	}
}

func TestOperatorMailboxApprovePublishFailureLeavesItemRetryable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	sourceEventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   uuid.NewString(),
		Payload: json.RawMessage(`{"request":true}`),
	}.WithEntityID(entityID)); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:  sourceEventID,
		EntityID: entityID,
		Type:     "review_request",
		Summary:  "needs approval",
		Context:  []byte(`{"title":"review this"}`),
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	failingHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:      func() time.Time { return now },
			Ready:    func() bool { return true },
			Database: fakePinger{},
			Mailbox:  pg,
			Events:   failingTxPublisher{},
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
		}),
	})
	approveBody := `{"jsonrpc":"2.0","id":"approve","method":"mailbox.approve","params":{"mailbox_id":"` + mailboxID + `","decision_payload":{"approved":true},"idempotency_key":"idem-publish-fails"}}`
	failed := rpcCall(t, failingHandler, approveBody)
	if failed.Error == nil {
		t.Fatal("mailbox.approve with failing publisher error = nil")
	}
	detail, err := pg.GetV1MailboxItem(ctx, mailboxID)
	if err != nil {
		t.Fatalf("get mailbox after failed publish: %v", err)
	}
	if detail.Item.Status != "pending" {
		t.Fatalf("mailbox status after failed publish = %s, want pending", detail.Item.Status)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 0 {
		t.Fatalf("mailbox.item_decided event count after failed publish = %d, want 0", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("api_idempotency rows after failed publish = %d, want 0", count)
	}

	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		PayloadValidator: mailboxItemDecidedPayloadValidator(t, mailboxItemDecidedStrictPayloadSchema(true)),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	successHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:      func() time.Time { return now },
			Ready:    func() bool { return true },
			Database: fakePinger{},
			Mailbox:  pg,
			Events:   bus,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
		}),
	})
	succeeded := rpcCall(t, successHandler, approveBody)
	if succeeded.Error != nil {
		t.Fatalf("mailbox.approve retry after failed publish error = %#v", succeeded.Error)
	}
	if status := asMap(t, succeeded.Result)["status"]; status != "decided" {
		t.Fatalf("mailbox.approve retry status = %v, want decided", status)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 1 {
		t.Fatalf("mailbox.item_decided event count after retry = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after retry = %d, want 1", count)
	}
}

func TestOperatorMailboxApproveQueuesTransactionalPublishWhileRuntimePaused(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		PayloadValidator: mailboxItemDecidedPayloadValidator(t, mailboxItemDecidedStrictPayloadSchema(true)),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	ctx := context.Background()
	agentID := "mailbox-approval-agent"
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, agentID)
	ch := bus.Subscribe(agentID, events.EventType("mailbox.item_decided"))
	defer bus.Unsubscribe(agentID)

	interceptorCalls := 0
	bus.SetInterceptors(interceptorFunc(func(_ context.Context, evt events.Event) (bool, []events.Event, error) {
		if evt.Type == events.EventType("mailbox.item_decided") {
			interceptorCalls++
		}
		return true, nil, nil
	}))

	now := time.Now().UTC()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   runID,
		Payload: json.RawMessage(`{"request":true}`),
	}.WithEntityID(entityID).WithFlowInstance("empire/review")); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   sourceEventID,
		EntityID:  entityID,
		FromAgent: "empire-agent",
		Type:      "review_request",
		Priority:  "high",
		Context:   []byte(`{"title":"review this"}`),
		Summary:   "needs approval",
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET flow_instance = 'empire/review' WHERE item_id = $1::uuid`, mailboxID); err != nil {
		t.Fatalf("set flow instance: %v", err)
	}
	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
		Now:          now,
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:      func() time.Time { return now },
			Ready:    func() bool { return true },
			Database: fakePinger{},
			Mailbox:  pg,
			Events:   bus,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "empire",
				WorkflowVersion: "1.0.0",
				Fingerprint:     "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}),
	})

	approveBody := `{"jsonrpc":"2.0","id":"approve","method":"mailbox.approve","params":{"mailbox_id":"` + mailboxID + `","decision_payload":{"approved":true},"idempotency_key":"idem-paused-approve"}}`
	approved := rpcCall(t, handler, approveBody)
	if approved.Error != nil {
		t.Fatalf("mailbox.approve while paused error = %#v", approved.Error)
	}
	downstreamID, _ := asMap(t, approved.Result)["downstream_event_id"].(string)
	if downstreamID == "" {
		t.Fatalf("mailbox.approve downstream event id = %#v", approved.Result)
	}
	if interceptorCalls != 0 {
		t.Fatalf("interceptor calls while paused = %d, want 0", interceptorCalls)
	}
	select {
	case got := <-ch:
		t.Fatalf("paused mailbox approval delivered event %s before resume", got.ID)
	case <-time.After(150 * time.Millisecond):
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, downstreamID); got != 1 {
		t.Fatalf("mailbox approval event deliveries while paused = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, downstreamID); got != 0 {
		t.Fatalf("mailbox approval pipeline receipts while paused = %d, want 0", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 1 {
		t.Fatalf("api_idempotency rows after paused approve = %d, want 1", got)
	}

	replay := rpcCall(t, handler, approveBody)
	if replay.Error != nil {
		t.Fatalf("mailbox.approve paused replay error = %#v", replay.Error)
	}
	if got := asMap(t, replay.Result)["downstream_event_id"]; got != downstreamID {
		t.Fatalf("paused replay downstream_event_id = %v, want %s", got, downstreamID)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 1 {
		t.Fatalf("mailbox.item_decided event count after replay = %d, want 1", count)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	select {
	case got := <-ch:
		if got.ID != downstreamID {
			t.Fatalf("released mailbox approval event = %s, want %s", got.ID, downstreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued mailbox approval release")
	}
	if interceptorCalls != 0 {
		t.Fatalf("interceptor calls after resume release = %d, want 0", interceptorCalls)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, downstreamID); got != 1 {
		t.Fatalf("mailbox approval pipeline receipts after resume = %d, want 1", got)
	}
	approvalPayload := loadEventPayload(t, db, downstreamID)
	assertMailboxItemDecidedPayloadShape(t, approvalPayload, "review this", true)
}

func TestOperatorMailboxApproveRunsPublishDispatchAfterDecisionCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}

	ctx := context.Background()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   runID,
		Payload: json.RawMessage(`{"request":true}`),
	}.WithEntityID(entityID).WithFlowInstance("empire/review")); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   sourceEventID,
		EntityID:  entityID,
		FromAgent: "empire-agent",
		Type:      "review_request",
		Priority:  "high",
		Context:   []byte(`{"title":"review this"}`),
		Summary:   "needs approval",
	})
	if err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET flow_instance = 'empire/review' WHERE item_id = $1::uuid`, mailboxID); err != nil {
		t.Fatalf("set flow instance: %v", err)
	}

	interceptorCalls := make(chan string, 1)
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		PayloadValidator: mailboxItemDecidedPayloadValidator(t, mailboxItemDecidedStrictPayloadSchema(true)),
		Interceptors: []runtimebus.EventInterceptor{interceptorFunc(func(interceptCtx context.Context, evt events.Event) (bool, []events.Event, error) {
			if evt.Type != events.EventType("mailbox.item_decided") {
				return true, nil, nil
			}
			if tx, ok := runtimepipeline.PipelineSQLTxFromContext(interceptCtx); ok && tx != nil {
				t.Fatal("mailbox approval dispatch ran with mailbox decision sql tx still in context")
			}
			var status string
			if err := db.QueryRowContext(interceptCtx, `SELECT status FROM mailbox WHERE item_id = $1::uuid`, mailboxID).Scan(&status); err != nil {
				t.Fatalf("query mailbox status from interceptor: %v", err)
			}
			if status != "decided" {
				t.Fatalf("mailbox status visible to interceptor = %s, want decided", status)
			}
			var idempotencyRows int
			if err := db.QueryRowContext(interceptCtx, `
				SELECT COUNT(*)
				FROM api_idempotency
				WHERE method = 'mailbox.approve'
				  AND idempotency_key = 'idem-post-commit-approve'
			`).Scan(&idempotencyRows); err != nil {
				t.Fatalf("query idempotency from interceptor: %v", err)
			}
			if idempotencyRows != 1 {
				t.Fatalf("api_idempotency rows visible to interceptor = %d, want 1", idempotencyRows)
			}
			select {
			case interceptorCalls <- evt.ID:
			default:
			}
			return true, nil, nil
		})},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:      func() time.Time { return now },
			Ready:    func() bool { return true },
			Database: fakePinger{},
			Mailbox:  pg,
			Events:   bus,
			MailboxApprovalRoutes: map[string]string{
				"review_request": "mailbox.item_decided",
			},
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "empire",
				WorkflowVersion: "1.0.0",
				Fingerprint:     "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}),
	})

	approveBody := `{"jsonrpc":"2.0","id":"approve","method":"mailbox.approve","params":{"mailbox_id":"` + mailboxID + `","decision_payload":{"approved":true},"idempotency_key":"idem-post-commit-approve"}}`
	approved := rpcCall(t, handler, approveBody)
	if approved.Error != nil {
		t.Fatalf("mailbox.approve error = %#v", approved.Error)
	}
	downstreamID, _ := asMap(t, approved.Result)["downstream_event_id"].(string)
	if downstreamID == "" {
		t.Fatalf("mailbox.approve downstream event id = %#v", approved.Result)
	}
	select {
	case got := <-interceptorCalls:
		if got != downstreamID {
			t.Fatalf("interceptor event id = %s, want %s", got, downstreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mailbox approval post-commit dispatch")
	}
	if got := countEventsByName(t, db, "mailbox.item_decided"); got != 1 {
		t.Fatalf("mailbox.item_decided event count = %d, want 1", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, downstreamID); got != 1 {
		t.Fatalf("mailbox approval pipeline receipts = %d, want 1", got)
	}
	approvalPayload := loadEventPayload(t, db, downstreamID)
	assertMailboxItemDecidedPayloadShape(t, approvalPayload, "review this", true)

	replay := rpcCall(t, handler, approveBody)
	if replay.Error != nil {
		t.Fatalf("mailbox.approve replay error = %#v", replay.Error)
	}
	if got := asMap(t, replay.Result)["downstream_event_id"]; got != downstreamID {
		t.Fatalf("replay downstream_event_id = %v, want %s", got, downstreamID)
	}
	select {
	case got := <-interceptorCalls:
		t.Fatalf("idempotency replay re-dispatched mailbox event %s", got)
	default:
	}
	if got := countEventsByName(t, db, "mailbox.item_decided"); got != 1 {
		t.Fatalf("mailbox.item_decided event count after replay = %d, want 1", got)
	}
}

func countEventsByName(t *testing.T, db *sql.DB, eventName string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, eventName).Scan(&count); err != nil {
		t.Fatalf("count events %s: %v", eventName, err)
	}
	return count
}

func loadEventPayload(t *testing.T, db *sql.DB, eventID string) map[string]any {
	t.Helper()
	var raw []byte
	if err := db.QueryRow(`SELECT payload FROM events WHERE event_id = $1::uuid`, eventID).Scan(&raw); err != nil {
		t.Fatalf("load event payload %s: %v", eventID, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode event payload %s: %v", eventID, err)
	}
	return payload
}

func countEventDeliveriesForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	return count
}

func countPipelineReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts for %s: %v", eventID, err)
	}
	return count
}

func countAPIIdempotencyRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency`).Scan(&count); err != nil {
		t.Fatalf("count api_idempotency rows: %v", err)
	}
	return count
}

func mailboxItemDecidedPayloadValidator(t *testing.T, schema map[string]any) runtimebus.PayloadValidator {
	t.Helper()
	return func(eventType string, payload []byte) error {
		if eventType != "mailbox.item_decided" {
			return nil
		}
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		decoded := map[string]any{}
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return err
		}
		return runtimetools.ValidatePayloadAgainstSchema(schema, decoded)
	}
}

func mailboxItemDecidedStrictPayloadSchema(includeMailboxPayload bool) map[string]any {
	properties := map[string]any{
		"mailbox_id": map[string]any{
			"type":   "string",
			"format": "uuid",
		},
		"mailbox_decision_id": map[string]any{
			"type":   "string",
			"format": "uuid",
		},
		"decision": map[string]any{
			"type": "string",
		},
		"decision_payload": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"approved": map[string]any{"type": "boolean"},
			},
			"required":             []any{"approved"},
			"additionalProperties": false,
		},
		"item_type": map[string]any{
			"type": "string",
		},
		"source_event_id": map[string]any{
			"type":   "string",
			"format": "uuid",
		},
		"source_flow": map[string]any{
			"type": "string",
		},
		"source_entity_id": map[string]any{
			"type":   "string",
			"format": "uuid",
		},
		"decided_by": map[string]any{
			"type": "string",
		},
		"decided_at": map[string]any{
			"type": "string",
		},
	}
	required := []any{
		"mailbox_id",
		"mailbox_decision_id",
		"decision",
		"decision_payload",
		"item_type",
		"source_event_id",
		"source_flow",
		"source_entity_id",
		"decided_by",
		"decided_at",
	}
	if includeMailboxPayload {
		properties["mailbox_payload"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"title": map[string]any{"type": "string"},
			},
			"required":             []any{"title"},
			"additionalProperties": false,
		}
		required = append(required, "mailbox_payload")
	}
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}

func validMailboxItemDecidedPayload() map[string]any {
	return map[string]any{
		"mailbox_id":          uuid.NewString(),
		"mailbox_decision_id": uuid.NewString(),
		"decision":            "approved",
		"decision_payload":    map[string]any{"approved": true},
		"item_type":           "review_request",
		"mailbox_payload":     map[string]any{"title": "review this"},
		"source_event_id":     uuid.NewString(),
		"source_flow":         "empire/review",
		"source_entity_id":    uuid.NewString(),
		"decided_by":          "agent-d-run-token",
		"decided_at":          "2026-05-10T12:00:00Z",
	}
}

func assertMailboxItemDecidedPayloadShape(t *testing.T, payload map[string]any, wantTitle string, wantApproved bool) {
	t.Helper()
	if _, ok := payload["payload"]; ok {
		t.Fatalf("mailbox.item_decided retained retired payload field: %#v", payload)
	}
	mailboxPayload, ok := payload["mailbox_payload"].(map[string]any)
	if !ok {
		t.Fatalf("mailbox_payload = %#v, want object", payload["mailbox_payload"])
	}
	if mailboxPayload["title"] != wantTitle {
		t.Fatalf("mailbox_payload.title = %#v, want %q", mailboxPayload["title"], wantTitle)
	}
	decisionPayload, ok := payload["decision_payload"].(map[string]any)
	if !ok {
		t.Fatalf("decision_payload = %#v, want object", payload["decision_payload"])
	}
	if decisionPayload["approved"] != wantApproved {
		t.Fatalf("decision_payload.approved = %#v, want %v", decisionPayload["approved"], wantApproved)
	}
}

type failingTxPublisher struct{}

func (failingTxPublisher) Publish(context.Context, events.Event) error {
	return nil
}

func (failingTxPublisher) PublishTx(context.Context, *sql.Tx, events.Event) error {
	return errors.New("publish failed")
}

type interceptorFunc func(context.Context, events.Event) (bool, []events.Event, error)

func (f interceptorFunc) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	return f(ctx, evt)
}

func seedActiveAPIV1RuntimeBusAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:     agentID,
			Role:   "observer",
			Mode:   "global",
			Type:   "stub",
			Config: []byte(`{}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}
