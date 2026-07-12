package apiv1

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
)

func TestOperatorMailboxHandlersSupportedRPCPath(t *testing.T) {
	state := newMutatingRuntimeProbeState(t, "mailbox.decide")
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorReadHandlers(state.options(t))})

	listed := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"mailbox.list","params":{}}`)
	if listed.Error != nil {
		t.Fatalf("mailbox.list error = %#v", listed.Error)
	}
	items := asSlice(t, asMap(t, listed.Result)["items"])
	if len(items) != 2 {
		t.Fatalf("mailbox.list items = %#v, want one notice and one decision card", items)
	}

	got := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"mailbox.get","params":{"mailbox_id":"card-1"}}`)
	if got.Error != nil {
		t.Fatalf("mailbox.get error = %#v", got.Error)
	}
	if result := asMap(t, got.Result); result["kind"] != "decision_card" {
		t.Fatalf("mailbox.get result = %#v, want decision_card", result)
	}

	beginBody := `{"jsonrpc":"2.0","id":"begin","method":"mailbox.begin_input","params":{"card_id":"card-1","verdict":"reject","observed_content_hash":"content-1","idempotency_key":"idem-begin"}}`
	begin := rpcCall(t, handler, beginBody)
	if begin.Error != nil || asMap(t, begin.Result)["status"] != "active" {
		t.Fatalf("mailbox.begin_input = result %#v error %#v", begin.Result, begin.Error)
	}
	beginReplay := rpcCall(t, handler, beginBody)
	if beginReplay.Error != nil || asMap(t, beginReplay.Result)["idempotency_replayed"] != true {
		t.Fatalf("mailbox.begin_input replay = result %#v error %#v", beginReplay.Result, beginReplay.Error)
	}

	cancel := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"cancel","method":"mailbox.cancel_input","params":{"card_id":"card-1","input_draft_id":"draft-1","idempotency_key":"idem-cancel"}}`)
	if cancel.Error != nil || asMap(t, cancel.Result)["status"] != "cancelled" {
		t.Fatalf("mailbox.cancel_input = result %#v error %#v", cancel.Result, cancel.Error)
	}

	deferred := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"defer","method":"mailbox.defer","params":{"card_id":"card-1","until":"2023-11-14T23:13:20Z","idempotency_key":"idem-defer"}}`)
	if deferred.Error != nil || asMap(t, deferred.Result)["status"] != "pending" {
		t.Fatalf("mailbox.defer = result %#v error %#v", deferred.Result, deferred.Error)
	}

	decideBody := `{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":"card-1","verdict":"approve","fields":{},"observed_content_hash":"content-1","idempotency_key":"idem-decide"}}`
	decided := rpcCall(t, handler, decideBody)
	if decided.Error != nil || asMap(t, decided.Result)["status"] != "decided" {
		t.Fatalf("mailbox.decide = result %#v error %#v", decided.Result, decided.Error)
	}
	decideReplay := rpcCall(t, handler, decideBody)
	if decideReplay.Error != nil || asMap(t, decideReplay.Result)["idempotency_replayed"] != true {
		t.Fatalf("mailbox.decide replay = result %#v error %#v", decideReplay.Result, decideReplay.Error)
	}

	for _, retired := range []string{"mailbox." + "approve", "mailbox." + "reject"} {
		resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"retired","method":"`+retired+`","params":{}}`)
		if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
			t.Fatalf("%s error = %#v, want method not found", retired, resp.Error)
		}
	}
}

func TestMailboxListTaggedCursorAdvancesOwnersIndependently(t *testing.T) {
	state := newMutatingRuntimeProbeState(t, "mailbox.list")
	opts := state.options(t)
	firstRaw, err := listMailboxProjection(context.Background(), Request{Params: map[string]any{"limit": 1}}, opts)
	if err != nil {
		t.Fatalf("first list: %v", err)
	}
	first := firstRaw.(mailboxProjectionListResult)
	if len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, want one item and cursor", first)
	}
	firstItem := first.Items[0].(map[string]any)
	if firstItem["kind"] != decisioncard.KindDecisionCard {
		t.Fatalf("first page kind = %#v, want deterministic decision_card tie-break", firstItem["kind"])
	}

	secondRaw, err := listMailboxProjection(context.Background(), Request{Params: map[string]any{"limit": 1, "cursor": first.NextCursor}}, opts)
	if err != nil {
		t.Fatalf("second list: %v", err)
	}
	second := secondRaw.(mailboxProjectionListResult)
	if len(second.Items) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %#v, want final singleton page", second)
	}
	secondItem := second.Items[0].(map[string]any)
	if secondItem["kind"] != decisioncard.KindNotice {
		t.Fatalf("second page kind = %#v, want notice without duplicate card", secondItem["kind"])
	}
}

func TestMailboxListOptionsAcceptsSupersededDecisionCardStatus(t *testing.T) {
	opts, err := mailboxListOptionsFromParams(map[string]any{"status": "SuPeRsEdEd"})
	if err != nil {
		t.Fatalf("mailboxListOptionsFromParams: %v", err)
	}
	if opts.Status != decisioncard.StatusSuperseded {
		t.Fatalf("status = %q, want %q", opts.Status, decisioncard.StatusSuperseded)
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

func countEventDeliveriesForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'agent'`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	return count
}

func countPipelineReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {
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

func seedActiveAPIV1RuntimeBusAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore, agentID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: agentID, Role: "observer", Mode: "global", Type: "stub", Model: "regular", Config: []byte(`{}`)},
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}
