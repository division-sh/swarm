package apiv1

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/testutil"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestOperatorMailboxHandlersSQLiteReadsMaterializedMailboxWrite(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteMailboxMaterializationAPIStore(t, ctx, testutil.SQLiteDefaultTemp())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := sqliteStore.AppendEvent(ctx, eventtest.PersistedProjection(
		eventID,
		"mailbox.review_requested",
		"",
		"",
		json.RawMessage(`{"kind":"review"}`),
		0,
		runID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "validation/case-1"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	itemID := uuid.NewString()
	tx, err := sqliteStore.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	for i := 0; i < 2; i++ {
		if err := sqliteStore.MaterializeMailboxWrite(txctx, runtimepipeline.MailboxWriteMaterialization{
			ItemID:        itemID,
			EntityID:      entityID,
			FlowInstance:  "validation/case-1",
			Scope:         "entity",
			ItemType:      "review_request",
			SourceEventID: eventID,
			FromAgent:     "system_node:mailbox-node",
			Severity:      "urgent",
			Summary:       "Review validation package",
			Payload:       json.RawMessage(`{"review_kind":"validation"}`),
		}); err != nil {
			_ = tx.Rollback()
			t.Fatalf("MaterializeMailboxWrite iteration %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Ready:    func() bool { return true },
			Database: fakePinger{},
			Mailbox:  sqliteStore,
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
	got := asMap(t, items[0])
	if got["mailbox_id"] != itemID || got["source_event_id"] != eventID || got["source_flow"] != "validation/case-1" || got["priority"] != "high" {
		t.Fatalf("mailbox.list item = %#v, want materialized mailbox_write row", got)
	}
}

func newSQLiteMailboxMaterializationAPIStore(t *testing.T, ctx context.Context, requirement testutil.DatabaseRequirement) *storepkg.SQLiteRuntimeStore {
	t.Helper()
	return storetest.StartSQLiteRuntimeStoreWithContext(t, ctx, requirement)
}
