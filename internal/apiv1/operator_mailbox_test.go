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
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestOperatorMailboxHandlersSupportedRPCPath(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := context.Background()
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
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now: func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
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
	replay := rpcCall(t, handler, approveBody)
	if replay.Error != nil {
		t.Fatalf("mailbox.approve replay error = %#v", replay.Error)
	}
	if got := asMap(t, replay.Result)["downstream_event_id"]; got != downstreamID {
		t.Fatalf("idempotency replay downstream_event_id = %v, want %s", got, downstreamID)
	}
	if count := countEventsByName(t, db, "mailbox.item_decided"); count != 1 {
		t.Fatalf("mailbox.item_decided event count = %d, want 1", count)
	}

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
	if status := asMap(t, deferred.Result)["status"]; status != "deferred" {
		t.Fatalf("mailbox.defer status = %v, want deferred", status)
	}

	empty := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"empty","method":"mailbox.list","params":{"status":"pending","type":"review_request"}}`)
	if empty.Error != nil {
		t.Fatalf("empty mailbox.list error = %#v", empty.Error)
	}
	if items := asMap(t, empty.Result)["items"].([]any); len(items) != 0 {
		t.Fatalf("empty mailbox.list items = %#v", items)
	}
}

func TestOperatorMailboxApprovePublishFailureLeavesItemRetryable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	sourceEventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:      sourceEventID,
		Type:    "review.requested",
		RunID:   uuid.NewString(),
		Payload: json.RawMessage(`{"request":true}`),
	}); err != nil {
		t.Fatalf("append source event: %v", err)
	}
	mailboxID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID: sourceEventID,
		Type:    "review_request",
		Summary: "needs approval",
		Context: []byte(`{"title":"review this"}`),
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

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
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

func countEventsByName(t *testing.T, db *sql.DB, eventName string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, eventName).Scan(&count); err != nil {
		t.Fatalf("count events %s: %v", eventName, err)
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

type failingTxPublisher struct{}

func (failingTxPublisher) Publish(context.Context, events.Event) error {
	return nil
}

func (failingTxPublisher) PublishTx(context.Context, *sql.Tx, events.Event) error {
	return errors.New("publish failed")
}
