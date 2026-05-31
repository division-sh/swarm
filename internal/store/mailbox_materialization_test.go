package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/testutil"
)

func TestPostgresStore_MaterializeMailboxWriteUsesTransactionAndV1ReadOwner(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := &PostgresStore{DB: db}
	ctx := context.Background()
	if _, err := store.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := store.AppendEvent(ctx, (events.Event{
		ID:        eventID,
		RunID:     runID,
		Type:      "mailbox.review_requested",
		Payload:   json.RawMessage(`{"kind":"review"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(entityID).WithFlowInstance("validation/case-1")); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	item := runtimepipeline.MailboxWriteMaterialization{
		ItemID:        uuid.NewString(),
		EntityID:      entityID,
		FlowInstance:  "validation/case-1",
		Scope:         "entity",
		ItemType:      "review_request",
		SourceEventID: eventID,
		FromAgent:     "system_node:mailbox-node",
		Severity:      "urgent",
		Summary:       "Review validation package",
		Payload:       json.RawMessage(`{"review_kind":"validation"}`),
	}

	rollbackTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx rollback: %v", err)
	}
	if err := store.MaterializeMailboxWrite(runtimepipeline.WithPipelineSQLTxContext(ctx, rollbackTx), item); err != nil {
		t.Fatalf("MaterializeMailboxWrite rollback: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	assertPostgresMailboxMaterializationCount(t, ctx, db, item.ItemID, 0)

	commitTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx commit: %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, commitTx)
	for i := 0; i < 2; i++ {
		if err := store.MaterializeMailboxWrite(txctx, item); err != nil {
			_ = commitTx.Rollback()
			t.Fatalf("MaterializeMailboxWrite commit iteration %d: %v", i, err)
		}
	}
	if err := commitTx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	assertPostgresMailboxMaterializationCount(t, ctx, db, item.ItemID, 1)
	assertV1MailboxMaterializationRead(t, ctx, store, item, runID)
}

func TestSQLiteRuntimeStore_MaterializeMailboxWriteUsesTransactionAndV1ReadOwner(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := store.AppendEvent(ctx, (events.Event{
		ID:        eventID,
		RunID:     runID,
		Type:      "mailbox.review_requested",
		Payload:   json.RawMessage(`{"kind":"review"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(entityID).WithFlowInstance("validation/case-1")); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	item := runtimepipeline.MailboxWriteMaterialization{
		ItemID:        uuid.NewString(),
		EntityID:      entityID,
		FlowInstance:  "validation/case-1",
		Scope:         "entity",
		ItemType:      "review_request",
		SourceEventID: eventID,
		FromAgent:     "system_node:mailbox-node",
		Severity:      "urgent",
		Summary:       "Review validation package",
		Payload:       json.RawMessage(`{"review_kind":"validation"}`),
	}

	rollbackTx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx rollback: %v", err)
	}
	if err := store.MaterializeMailboxWrite(runtimepipeline.WithPipelineSQLTxContext(ctx, rollbackTx), item); err != nil {
		t.Fatalf("MaterializeMailboxWrite rollback: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	assertSQLiteMailboxMaterializationCount(t, ctx, store, item.ItemID, 0)

	commitTx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx commit: %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, commitTx)
	for i := 0; i < 2; i++ {
		if err := store.MaterializeMailboxWrite(txctx, item); err != nil {
			_ = commitTx.Rollback()
			t.Fatalf("MaterializeMailboxWrite commit iteration %d: %v", i, err)
		}
	}
	if err := commitTx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	assertSQLiteMailboxMaterializationCount(t, ctx, store, item.ItemID, 1)
	assertV1MailboxMaterializationRead(t, ctx, store, item, runID)
}

type mailboxV1ReadStore interface {
	ListV1MailboxItems(context.Context, MailboxV1ListOptions) ([]MailboxV1Item, string, error)
	GetV1MailboxItem(context.Context, string) (MailboxV1ItemDetail, error)
}

func assertV1MailboxMaterializationRead(t *testing.T, ctx context.Context, store mailboxV1ReadStore, item runtimepipeline.MailboxWriteMaterialization, runID string) {
	t.Helper()
	items, cursor, err := store.ListV1MailboxItems(ctx, MailboxV1ListOptions{
		Status:   "pending",
		RunID:    runID,
		EntityID: item.EntityID,
		Type:     item.ItemType,
		Priority: item.Severity,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ListV1MailboxItems: %v", err)
	}
	if cursor != "" {
		t.Fatalf("ListV1MailboxItems cursor = %q, want empty", cursor)
	}
	if len(items) != 1 {
		t.Fatalf("ListV1MailboxItems len = %d, want 1: %#v", len(items), items)
	}
	got := items[0]
	wantPriority := item.Severity
	if wantPriority == "urgent" {
		wantPriority = "high"
	}
	if got.MailboxID != item.ItemID || got.SourceEventID != item.SourceEventID || got.SourceRunID != runID || got.SourceEntityID != item.EntityID || got.SourceFlow != item.FlowInstance || got.Priority != wantPriority {
		t.Fatalf("v1 mailbox item = %#v, want materialized identity", got)
	}
	detail, err := store.GetV1MailboxItem(ctx, item.ItemID)
	if err != nil {
		t.Fatalf("GetV1MailboxItem: %v", err)
	}
	if detail.Item.MailboxID != item.ItemID || detail.Payload["review_kind"] != "validation" {
		t.Fatalf("v1 mailbox detail = %#v, want materialized payload", detail)
	}
}

func assertPostgresMailboxMaterializationCount(t *testing.T, ctx context.Context, db execQueryer, itemID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE item_id = $1::uuid`, itemID).Scan(&count); err != nil {
		t.Fatalf("count postgres mailbox materializations: %v", err)
	}
	if count != want {
		t.Fatalf("postgres mailbox materialization count = %d, want %d", count, want)
	}
}

func assertSQLiteMailboxMaterializationCount(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, itemID string, want int) {
	t.Helper()
	var count int
	if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE item_id = ?`, itemID).Scan(&count); err != nil {
		t.Fatalf("count sqlite mailbox materializations: %v", err)
	}
	if count != want {
		t.Fatalf("sqlite mailbox materialization count = %d, want %d", count, want)
	}
}
