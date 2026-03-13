package store

import (
	"context"
	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/actors"
	llm "empireai/internal/runtime/llm"
	runtimellm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"encoding/json"
	"github.com/google/uuid"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresStore_AppendEvent_NormalizesInvalidOptionalUUIDs(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	eventID := uuid.NewString()
	err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        events.EventType("review.requested"),
		SourceAgent: "control-plane",
		TaskID:      "legacy-task-key",
		Payload:     []byte(`{"name":"Telemedicine Platform"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID("pry_hc_telemedicine_001"))
	if err != nil {
		t.Fatalf("AppendEvent should not fail on non-UUID optional refs: %v", err)
	}

	var gotTaskID, gotVerticalID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(task_id::text, ''), COALESCE(vertical_id::text, '')
		FROM events
		WHERE id = $1::uuid
	`, eventID).Scan(&gotTaskID, &gotVerticalID); err != nil {
		t.Fatalf("query event row: %v", err)
	}
	if gotTaskID != "" {
		t.Fatalf("expected normalized empty task_id, got %q", gotTaskID)
	}
	if gotVerticalID != "" {
		t.Fatalf("expected normalized empty vertical_id, got %q", gotVerticalID)
	}
}

func TestPostgresStore_PipelineReceipts_MissingEventsQuery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pipeline_receipts (
			event_id UUID PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
			result TEXT NOT NULL DEFAULT 'processed',
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_receipts: %v", err)
	}

	eventProcessed := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}
	eventMissing := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"x"}`),
		CreatedAt:   time.Now().Add(-1 * time.Minute),
	}
	if err := pg.AppendEvent(ctx, eventProcessed); err != nil {
		t.Fatalf("append processed event: %v", err)
	}
	if err := pg.AppendEvent(ctx, eventMissing); err != nil {
		t.Fatalf("append missing event: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventProcessed.ID, "processed", ""); err != nil {
		t.Fatalf("upsert processed receipt: %v", err)
	}

	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-1*time.Hour), 20)
	if err != nil {
		t.Fatalf("list missing pipeline receipts: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing event, got %d", len(missing))
	}
	if missing[0].ID != eventMissing.ID {
		t.Fatalf("expected missing event id=%s got=%s", eventMissing.ID, missing[0].ID)
	}
}

func TestPostgresStore_BeginEventTx_AppendAndDeliveriesTx(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	for _, id := range []string{"control-plane", "reviewer"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', $1, 'global', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
	}

	tx, err := pg.BeginEventTx(ctx)
	if err != nil {
		t.Fatalf("BeginEventTx: %v", err)
	}

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().UTC(),
	}
	if err := pg.AppendEventTx(ctx, tx, evt); err != nil {
		_ = tx.Rollback()
		t.Fatalf("AppendEventTx: %v", err)
	}
	if err := pg.InsertEventDeliveriesTx(ctx, tx, eventID, []string{"control-plane", "reviewer"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("InsertEventDeliveriesTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit event tx: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if nEvents != 1 || nDeliveries != 2 {
		t.Fatalf("expected event+2 deliveries persisted, got events=%d deliveries=%d", nEvents, nDeliveries)
	}
}

func TestPostgresStore_PersistEventWithDeliveries_SuccessAndRollbackOnFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('control-plane', 'stub', 'control-plane', 'global', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	eventID := uuid.NewString()
	if err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          eventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"SaaS in Argentina"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{" control-plane ", "", "control-plane"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries success path: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events success: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count deliveries success: %v", err)
	}
	if nEvents != 1 || nDeliveries != 1 {
		t.Fatalf("expected deduped delivery insertion, got events=%d deliveries=%d", nEvents, nDeliveries)
	}

	failedEventID := uuid.NewString()
	err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          failedEventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"fail path"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{"missing-agent"})
	if err == nil {
		t.Fatal("expected error for unknown delivery agent")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "insert event delivery") {
		t.Fatalf("unexpected error: %v", err)
	}

	var rolledBackCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, failedEventID).Scan(&rolledBackCount); err != nil {
		t.Fatalf("count rolled back event: %v", err)
	}
	if rolledBackCount != 0 {
		t.Fatalf("expected failed tx to roll back event row, count=%d", rolledBackCount)
	}
}

func TestIsMissingPipelineReceiptsTable(t *testing.T) {
	if isMissingPipelineReceiptsTable(nil) {
		t.Fatal("nil error should not be treated as missing table")
	}
	if !isMissingPipelineReceiptsTable(assertErr("pq: relation \"pipeline_receipts\" does not exist")) {
		t.Fatal("expected missing table error to match")
	}
	if isMissingPipelineReceiptsTable(assertErr("some other db error")) {
		t.Fatal("unexpected positive match on unrelated error")
	}
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func TestPostgresStore_Inbound_ValidationAndNotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := s.RecordInboundEvent(ctx, "", "v", "p"); err == nil {
		t.Fatal("expected provider_event_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "", "p"); err == nil {
		t.Fatal("expected entity_id required")
	}
	if _, err := s.RecordInboundEvent(ctx, "e", "v", ""); err == nil {
		t.Fatal("expected provider required")
	}

	if _, err := s.ResolveInboundTarget(ctx, "", "p"); err == nil {
		t.Fatal("expected entity key required")
	}
	if _, err := s.ResolveInboundTarget(ctx, "k", ""); err == nil {
		t.Fatal("expected provider required")
	}
	if _, err := s.ResolveInboundTarget(ctx, "missing", "chat"); err == nil || !strings.Contains(err.Error(), "entity not found") {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestPostgresStore_Inbound_SecretsLegacyFlatAndDecrypt(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	key := "k"
	os.Setenv("MAS_CREDENTIALS_KEY", key)
	defer os.Unsetenv("MAS_CREDENTIALS_KEY")

	var enc string
	if err := db.QueryRowContext(ctx, `SELECT encode(pgp_sym_encrypt('senc', $1::text), 'base64')`, key).Scan(&enc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	verticalID1 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, jsonb_build_object('slug', 'legacy', 'credentials', $2::jsonb), now(), now()
		)
	`, verticalID1, `{"chat": {"token":"legacy"}}`); err != nil {
		t.Fatalf("seed workflow instance legacy: %v", err)
	}
	target, err := s.ResolveInboundTarget(ctx, "legacy", "chat")
	if err != nil {
		t.Fatalf("resolve legacy: %v", err)
	}
	if target.WebhookSecret != "legacy" {
		t.Fatalf("expected legacy token, got %q", target.WebhookSecret)
	}

	verticalID2 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, jsonb_build_object('slug', 'flat', 'credentials', $2::jsonb), now(), now()
		)
	`, verticalID2, `{"chat_webhook_secret":" flat "}`); err != nil {
		t.Fatalf("seed workflow instance flat: %v", err)
	}
	target2, err := s.ResolveInboundTarget(ctx, "flat", "chat")
	if err != nil {
		t.Fatalf("resolve flat: %v", err)
	}
	if target2.WebhookSecret != "flat" {
		t.Fatalf("expected flat secret, got %q", target2.WebhookSecret)
	}

	verticalID3 := uuid.NewString()
	b, _ := json.Marshal(map[string]any{
		"webhooks": map[string]any{
			"chat": map[string]any{
				"secret": "enc::" + enc,
			},
		},
	})
	credsEnc := string(b)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, jsonb_build_object('slug', 'enc', 'credentials', $2::jsonb), now(), now()
		)
	`, verticalID3, credsEnc); err != nil {
		t.Fatalf("seed workflow instance enc: %v", err)
	}
	target3, err := s.ResolveInboundTarget(ctx, "enc", "chat")
	if err != nil {
		t.Fatalf("resolve enc: %v", err)
	}
	if target3.WebhookSecret != "senc" {
		t.Fatalf("expected decrypted secret, got %q", target3.WebhookSecret)
	}

	if got := s.extractWebhookSecret(ctx, []byte("{"), "chat"); got != "" {
		t.Fatalf("expected empty secret for invalid json, got %q", got)
	}

	os.Unsetenv("MAS_CREDENTIALS_KEY")
	if got := s.decryptCredentialValue(ctx, "enc::AAAA"); got != "enc::AAAA" {
		t.Fatalf("expected passthrough without key, got %#v", got)
	}
	os.Setenv("MAS_CREDENTIALS_KEY", key)
	if got := s.decryptCredentialValue(ctx, "enc::"); got != "" {
		t.Fatalf("expected empty encoded to return empty string, got %#v", got)
	}
}

func TestPostgresStore_Inbound_PurgeDeletes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'Purge','purge','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed compatibility entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{"slug":"purge","credentials":{}}'::jsonb, now(), now()
		)
	`, verticalID); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	if ok, err := s.RecordInboundEvent(ctx, "evt-old", verticalID, "chat"); err != nil || !ok {
		t.Fatalf("record old ok=%v err=%v", ok, err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE inbound_events SET received_at = now() - interval '2 days' WHERE provider_event_id = 'evt-old'`); err != nil {
		t.Fatalf("age event: %v", err)
	}

	n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-24*time.Hour), 0)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 purged row, got %d", n)
	}
}

func TestPostgresStore_Inbound_RecordResolveAndSecrets(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed compatibility entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, jsonb_build_object('slug', 'testco', 'credentials', $2::jsonb), now(), now()
		)
	`, verticalID, `{
		"webhooks": { "chat": { "secret": "s1" } },
		"chat": { "token": "legacy" },
		"chat_webhook_secret": "flat"
	}`); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	target, err := s.ResolveInboundTarget(ctx, "testco", "chat")
	if err != nil {
		t.Fatalf("resolve inbound: %v", err)
	}
	if target.EntityID != verticalID || target.EntitySlug != "testco" {
		t.Fatalf("unexpected target: %+v", target)
	}
	if target.WebhookSecret != "s1" {
		t.Fatalf("expected preferred webhook secret, got %q", target.WebhookSecret)
	}

	ok, err := s.RecordInboundEvent(ctx, "evt-1", verticalID, "chat")
	if err != nil || !ok {
		t.Fatalf("record inbound ok=%v err=%v", ok, err)
	}
	ok, err = s.RecordInboundEvent(ctx, "evt-1", verticalID, "chat")
	if err != nil || ok {
		t.Fatalf("expected duplicate record to be no-op ok=%v err=%v", ok, err)
	}

	if n, err := s.PurgeInboundEventsBefore(ctx, time.Now().Add(-1*time.Hour), 10); err != nil || n != 0 {
		t.Fatalf("purge n=%d err=%v", n, err)
	}
}

func TestPostgresStore_Mailbox_CRUD_Expire_Notify(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	id, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  verticalID,
		FromAgent: "control-plane",
		Type:      "spend_request",
		Summary:   "need approval",
	})
	if err != nil || id == "" {
		t.Fatalf("insert mailbox: id=%q err=%v", id, err)
	}

	got, err := s.GetMailboxItem(ctx, id)
	if err != nil {
		t.Fatalf("get mailbox: %v", err)
	}
	if got.Status != "pending" || got.Priority != "normal" {
		t.Fatalf("unexpected defaults: %+v", got)
	}

	if n, err := s.CountMailboxItems(ctx, "pending"); err != nil || n < 1 {
		t.Fatalf("count pending n=%d err=%v", n, err)
	}
	items, err := s.ListMailboxItems(ctx, "pending", 10)
	if err != nil || len(items) == 0 {
		t.Fatalf("list pending: n=%d err=%v", len(items), err)
	}

	if err := s.DecideMailboxItem(ctx, id, "approved", "approve", "ok"); err != nil {
		t.Fatalf("decide: %v", err)
	}
	if err := s.DecideMailboxItem(ctx, id, "approved", "approve", "again"); err == nil {
		t.Fatal("expected decide on non-pending to fail")
	}
	if err := s.DecideMailboxItem(ctx, uuid.NewString(), "nope", "approve", ""); err == nil {
		t.Fatal("expected invalid status error")
	}

	expID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  verticalID,
		FromAgent: "control-plane",
		Type:      "review",
		Priority:  "critical",
		Status:    "pending",
		Context:   []byte(`{"x":1}`),
		TimeoutAt: time.Now().Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("insert expiring mailbox: %v", err)
	}
	expired, err := s.ExpireMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	found := false
	for _, it := range expired {
		if it.ID == expID {
			found = true
			if it.Status != "timed_out" {
				t.Fatalf("expected timed_out, got %+v", it)
			}
		}
	}
	if !found {
		t.Fatalf("expected expired item in result")
	}

	critID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  verticalID,
		FromAgent: "control-plane",
		Type:      "spend_request",
		Priority:  "critical",
		Status:    "pending",
		Summary:   "critical",
		TimeoutAt: time.Now().Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("insert critical mailbox: %v", err)
	}
	crit, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil || len(crit) == 0 {
		t.Fatalf("list unnotified critical: n=%d err=%v", len(crit), err)
	}
	if err := s.MarkMailboxItemNotified(ctx, critID); err != nil {
		t.Fatalf("mark notified: %v", err)
	}
	crit2, err := s.ListUnnotifiedCriticalMailboxItems(ctx, 10)
	if err != nil {
		t.Fatalf("list unnotified critical 2: %v", err)
	}
	for _, it := range crit2 {
		if it.ID == critID {
			t.Fatalf("expected item to be notified and excluded")
		}
	}
}

func TestExtractSubscriptions(t *testing.T) {
	if got := extractSubscriptions(nil); got != nil {
		t.Fatalf("expected nil")
	}
	if got := extractSubscriptions([]byte("nope")); got != nil {
		t.Fatalf("expected nil for invalid json")
	}
	raw := []byte(`{"subscriptions":["a","b"]}`)
	got := extractSubscriptions(raw)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected subscriptions: %#v", got)
	}
}

func TestNormalizeJSONPayload_RedactsSensitiveText(t *testing.T) {

	out := normalizeJSONPayload([]byte("email me at x@example.com or call +1 (555) 123-4567"))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json wrapper, got %q", out)
	}
	if strings.Contains(out, "x@example.com") || strings.Contains(out, "555") {
		t.Fatalf("expected email/phone redacted in wrapper: %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"name":"Alice Smith","notes":"reach me at y@example.com","nested":{"full_name":"Bob Jones"}}`))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json, got %q", out)
	}
	if strings.Contains(out, "Alice") || strings.Contains(out, "Bob") {
		t.Fatalf("expected names redacted, got %q", out)
	}
	if strings.Contains(out, "y@example.com") {
		t.Fatalf("expected email redacted, got %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"payment_ref":"pi_1234567890ABCDEF","notes":"charge ch_abcdef123456 done"}`))
	if strings.Contains(out, "pi_1234567890ABCDEF") || strings.Contains(out, "ch_abcdef123456") {
		t.Fatalf("expected payment refs redacted, got %q", out)
	}
	if !strings.Contains(out, "[PAYMENT_REF]") {
		t.Fatalf("expected [PAYMENT_REF] marker, got %q", out)
	}

	out = normalizeJSONPayload([]byte(`{"timestamp":"2026-02-21T02:47:05Z","notes":"at 2026-02-21T02:47:05Z"}`))
	if strings.Contains(out, "[PHONE]") {
		t.Fatalf("expected timestamp not redacted as phone, got %q", out)
	}
}

func TestSubscriptionMatchPatterns(t *testing.T) {
	if !subscriptionMatch("", "x.y") {
		t.Fatalf("empty should match all")
	}
	if !subscriptionMatch("*", "x.y") {
		t.Fatalf("* should match all")
	}
	if !subscriptionMatch("inbound.*", "inbound.a") {
		t.Fatalf("prefix star should match")
	}
	if subscriptionMatch("inbound.*", "board.chat") {
		t.Fatalf("prefix star should not match other prefix")
	}
	if !subscriptionMatch("board.chat", "board.chat") {
		t.Fatalf("exact should match")
	}
	if subscriptionMatch("board.chat", "board.chats") {
		t.Fatalf("exact should not match different")
	}
	if !matchesAnySubscription("inbound.a", []events.EventType{"review.*", "inbound.*"}) {
		t.Fatalf("matchesAnySubscription expected true")
	}
}

func TestSchedules_UpsertLoadCancelAndMarkFired(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'control-plane', 'global', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	once := runtimepipeline.Schedule{
		AgentID:   "a1",
		EventType: "system.directive",
		Mode:      "once",
		At:        time.Now().Add(1 * time.Hour).UTC(),
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, once); err != nil {
		t.Fatalf("upsert once: %v", err)
	}
	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	if len(active) == 0 {
		t.Fatalf("expected active schedule")
	}

	if err := pg.MarkScheduleFired(ctx, once); err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules after fired: %v", err)
	}
	for _, sc := range active {
		if sc.AgentID == "a1" && sc.EventType == "system.directive" && sc.Mode == "once" {
			t.Fatalf("expected once schedule to deactivate after fired")
		}
	}

	recurring := runtimepipeline.Schedule{
		AgentID:   "a1",
		EventType: "system.started",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   nil,
	}
	if err := pg.UpsertSchedule(ctx, recurring); err != nil {
		t.Fatalf("upsert recurring: %v", err)
	}
	if err := pg.MarkScheduleFired(ctx, recurring); err != nil {
		t.Fatalf("mark recurring fired: %v", err)
	}

	if err := pg.CancelSchedule(ctx, "a1", "system.started"); err != nil {
		t.Fatalf("cancel schedule: %v", err)
	}
}

func TestSchedules_ExactIdentityUsesTaskID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'approved', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('validation-orchestrator', 'stub', 'validation-orchestrator', 'global', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	first := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(30 * time.Minute).UTC(),
		EntityID:  verticalID,
		TaskID:    "timer-a",
		Payload:   []byte(`{"timer_id":"timer-a"}`),
	}
	second := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(60 * time.Minute).UTC(),
		EntityID:  verticalID,
		TaskID:    "timer-b",
		Payload:   []byte(`{"timer_id":"timer-b"}`),
	}
	if err := pg.UpsertSchedule(ctx, first); err != nil {
		t.Fatalf("upsert first exact schedule: %v", err)
	}
	if err := pg.UpsertSchedule(ctx, second); err != nil {
		t.Fatalf("upsert second exact schedule: %v", err)
	}

	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	var exact []runtimepipeline.Schedule
	for _, sc := range active {
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == verticalID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 2 {
		t.Fatalf("expected two exact schedules to coexist, got %+v", exact)
	}
	seen := map[string]string{}
	for _, sc := range exact {
		seen[sc.TaskID] = string(sc.Payload)
	}
	if seen["timer-a"] != `{"timer_id":"timer-a"}` {
		t.Fatalf("first exact schedule payload/task mismatch: %+v", seen)
	}
	if seen["timer-b"] != `{"timer_id":"timer-b"}` {
		t.Fatalf("second exact schedule payload/task mismatch: %+v", seen)
	}

	if err := pg.CancelScheduleExact(ctx, first); err != nil {
		t.Fatalf("cancel first exact schedule: %v", err)
	}
	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules after exact cancel: %v", err)
	}
	exact = exact[:0]
	for _, sc := range active {
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == verticalID {
			exact = append(exact, sc)
		}
	}
	if len(exact) != 1 || exact[0].TaskID != "timer-b" {
		t.Fatalf("expected only timer-b to remain active, got %+v", exact)
	}
}

func TestEventReceipts_RetryToDeadLetter_AndPendingQueries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'control-plane', 'global', NULL, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'inbound.test', 'inbound', $2::uuid, '{}'::jsonb, now())
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'a1', now())
		ON CONFLICT DO NOTHING
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	pending, err := pg.ListPendingEventsForAgent(ctx, "a1", time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}

	for i := 0; i < 4; i++ {
		if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "error", "boom"); err != nil {
			t.Fatalf("upsert receipt: %v", err)
		}
	}
	r, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil || !ok {
		t.Fatalf("GetEventReceipt ok=%v err=%v", ok, err)
	}
	if strings.TrimSpace(r.Status) != "dead_letter" {
		t.Fatalf("expected dead_letter, got %q retry=%d", r.Status, r.RetryCount)
	}

	subscribed, err := pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no subscribed pending events after dead_letter, got %d", len(subscribed))
	}

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "processed", ""); err != nil {
		t.Fatalf("upsert processed: %v", err)
	}
	subscribed, err = pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents processed: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no pending after processed, got %d", len(subscribed))
	}
}

func TestListPendingSubscribedEvents_RespectsDirectDeliveryScope(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES
			('a1', 'stub', 'worker', 'factory', 'active', '{}'::jsonb, now(), now()),
			('a2', 'stub', 'worker', 'factory', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	broadcastID := uuid.NewString()
	directOtherID := uuid.NewString()
	directSelfID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '3 minute'),
			($2::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '2 minute'),
			($3::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '1 minute')
	`, broadcastID, directOtherID, directSelfID); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES
			($1::uuid, 'a2', now()),
			($2::uuid, 'a1', now())
	`, directOtherID, directSelfID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	got, err := pg.ListPendingSubscribedEvents(
		ctx,
		"a1",
		[]events.EventType{"inbound.*"},
		time.Now().Add(-1*time.Hour),
		20,
	)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	gotSet := map[string]struct{}{}
	for _, evt := range got {
		gotSet[strings.TrimSpace(evt.ID)] = struct{}{}
	}
	if _, ok := gotSet[broadcastID]; !ok {
		t.Fatalf("expected broadcast event %s in subscribed backlog, got=%v", broadcastID, gotSet)
	}
	if _, ok := gotSet[directSelfID]; !ok {
		t.Fatalf("expected direct-self event %s in subscribed backlog, got=%v", directSelfID, gotSet)
	}
	if _, ok := gotSet[directOtherID]; ok {
		t.Fatalf("did not expect direct-other event %s in subscribed backlog, got=%v", directOtherID, gotSet)
	}
}

func TestManagerStore_EventReceiptBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'a1', 'global', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'system.started', 'runtime', $2::uuid, '{}'::jsonb, now())
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, "", "a1", "processed", ""); err != nil {
		t.Fatalf("UpsertEventReceipt empty event: %v", err)
	}

	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "", ""); err != nil {
		t.Fatalf("UpsertEventReceipt default: %v", err)
	}
	r, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil || !ok || r.Status != "processed" {
		t.Fatalf("GetEventReceipt got ok=%v err=%v rec=%+v", ok, err, r)
	}

	if _, _, err := pg.GetEventReceipt(ctx, "", "a1"); err == nil {
		t.Fatalf("expected required args error")
	}
	if _, _, err := pg.GetEventReceipt(ctx, eventID, ""); err == nil {
		t.Fatalf("expected required args error")
	}

	if _, ok, err := pg.GetEventReceipt(ctx, uuid.NewString(), "a1"); err != nil || ok {
		t.Fatalf("expected not found ok=false err=%v", err)
	}
}

func TestManagerStore_LoadRoutingRules_AndDeactivateValidation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('sub', 'stub', 'sub', 'global', 'active', '{}'::jsonb, now(), now()),
		       ('inst', 'stub', 'inst', 'global', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     verticalID,
		EventPattern: "x.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "active",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     verticalID,
		EventPattern: "y.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "deactivated",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule deactivated: %v", err)
	}
	rules, err := pg.LoadRoutingRules(ctx)
	if err != nil {
		t.Fatalf("LoadRoutingRules: %v", err)
	}
	if len(rules) != 1 || rules[0].EventPattern != "x.*" {
		t.Fatalf("expected only active/proposed rules, got %#v", rules)
	}
	if err := pg.DeactivateRoutingRulesByEntity(ctx, ""); err == nil {
		t.Fatalf("expected entity_id required")
	}

	if err := pg.MarkAgentTerminated(ctx, " "); err == nil {
		t.Fatalf("expected agent_id required")
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, agent_id, event_type, mode, next_fire_at, created_at)
		VALUES ($1::uuid, 'sub', 'timer.recurring_digest', 'cron', now(), now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed schedule: %v", err)
	}
	if err := pg.CancelSchedule(ctx, "sub", "timer.recurring_digest"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	_ = time.Second
}

func TestManagerStore_EnsureEntitySchema_AndTemplates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed compatibility entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{"slug":"vslug"}'::jsonb, now(), now()
		)
	`, verticalID); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	if err := pg.EnsureEntitySchema(ctx, verticalID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}

	if _, err := pg.LoadOrgTemplate(ctx, ""); err == nil {
		t.Fatalf("expected template version required")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, created_at)
		VALUES ('t1','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test', now())
	`); err != nil {
		t.Fatalf("seed template: %v", err)
	}
	if rec, err := pg.LoadLatestOrgTemplate(ctx); err != nil || rec.Version != "t1" {
		t.Fatalf("LoadLatestOrgTemplate err=%v rec=%+v", err, rec)
	}
	if rec, err := pg.LoadOrgTemplate(ctx, "t1"); err != nil || rec.Version != "t1" {
		t.Fatalf("LoadOrgTemplate err=%v rec=%+v", err, rec)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bootstrap_versions (version, routes, proposed_by, approved_by, evidence, created_at)
		VALUES (3, '[]'::jsonb, 'initial', 'factory-cto', 'test', now())
	`); err != nil {
		t.Fatalf("seed bootstrap_versions: %v", err)
	}
	if v, err := pg.ResolveBootstrapVersion(ctx, "t1"); err != nil || v != 3 {
		t.Fatalf("ResolveBootstrapVersion err=%v v=%d", err, v)
	}
}

func TestManagerStore_RoutingRules_DeactivateAndBootstrapVersion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vslug', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('sub', 'stub', 'sub', 'operating', 'active', '{}'::jsonb, now(), now()),
		       ('inst', 'stub', 'inst', 'operating', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	r := runtimemanager.PersistedRoutingRule{
		EntityID:         verticalID,
		EventPattern:     "inbound.*",
		SubscriberID:     "sub",
		InstalledBy:      "inst",
		Reason:           "r",
		Status:           "active",
		Source:           "bootstrap",
		BootstrapVersion: 2,
	}
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}

	r.Status = "deactivated"
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule deactivate: %v", err)
	}
	var deact *time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT deactivated_at
		FROM routing_rules
		WHERE vertical_id=$1::uuid AND event_pattern='inbound.*' AND subscriber_id='sub'
	`, verticalID).Scan(&deact); err != nil {
		t.Fatalf("load deactivated_at: %v", err)
	}
	if deact == nil {
		t.Fatalf("expected deactivated_at set")
	}

	if err := pg.DeactivateRoutingRulesByEntity(ctx, verticalID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}
}

func TestManagerStore_Conversations_AndAgentTurns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'a1', 'global', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		AgentID: "a1",
		Messages: []llm.Message{
			{Role: "user", Content: "reach me at a@example.com"},
		},
		TurnCount: 2,
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "", "")
	if err != nil || !ok {
		t.Fatalf("LoadActiveConversation ok=%v err=%v", ok, err)
	}
	if rec.AgentID != "a1" || rec.Mode != "session" || rec.Status != "active" || rec.TurnCount != 2 {
		t.Fatalf("unexpected conversation: %+v", rec)
	}
	if len(rec.Messages) != 1 || strings.Contains(rec.Messages[0].Content, "a@example.com") || !strings.Contains(rec.Messages[0].Content, "[EMAIL]") {
		t.Fatalf("expected redacted email, got %#v", rec.Messages)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{AgentID: "a1", RuntimeMode: "cli_test", SessionID: "s1"}); err == nil {
		t.Fatalf("expected missing session row error")
	}
	var sessionRowID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at)
		VALUES ('a1', 'cli_test', 'cli', 's1', 'active', 0, now())
		RETURNING id::text
	`).Scan(&sessionRowID); err != nil {
		t.Fatalf("seed agent_sessions: %v", err)
	}
	_ = sessionRowID
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "cli_test",
		SessionID:      "s1",
		TaskID:         uuid.NewString(),
		RequestPayload: []byte(`{"x":1}`),
		ResponseRaw:    []byte(`{"y":2}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}
}

func TestManagerStore_UpsertAgent_MergesSubscriptions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "a1",
			Type:          "sonnet",
			Role:          "a1",
			Mode:          "global",
			Subscriptions: []string{"inbound.*"},
			Config:        []byte(`{"system_prompt":"x"}`),
		},
		Status:  "active",
		HiredBy: "test",
	}
	if err := pg.UpsertAgent(ctx, rec); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	found := false
	for _, a := range agents {
		if a.Config.ID == "a1" {
			found = true
			if len(a.Config.Subscriptions) != 1 || a.Config.Subscriptions[0] != "inbound.*" {
				t.Fatalf("expected subscriptions merged, got %#v", a.Config.Subscriptions)
			}
		}
	}
	if !found {
		t.Fatalf("expected agent loaded")
	}

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{}}); err == nil {
		t.Fatalf("expected agent id required error")
	}

	if !matchesAnySubscription("inbound.a", []events.EventType{"inbound.*"}) {
		t.Fatalf("expected matchesAnySubscription true")
	}
}

func TestPostgresStore_Manager_MoreCoverage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES
			('v1','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test','v1', now() - interval '2 hour'),
			('v2','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,'test','v2', now())
	`); err != nil {
		t.Fatalf("seed templates: %v", err)
	}
	if rec, err := pg.LoadLatestOrgTemplate(ctx); err != nil || rec.Version != "v2" {
		t.Fatalf("LoadLatestOrgTemplate rec=%+v err=%v", rec, err)
	}
	if _, err := pg.LoadOrgTemplate(ctx, ""); err == nil {
		t.Fatal("expected LoadOrgTemplate error for empty version")
	}
	if rec, err := pg.LoadOrgTemplate(ctx, "v1"); err != nil || rec.Version != "v1" {
		t.Fatalf("LoadOrgTemplate rec=%+v err=%v", rec, err)
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed compatibility entity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, workflow_version, current_state,
			entered_stage_at, accumulator_state, transition_history, timer_state, metadata, created_at, updated_at
		) VALUES (
			$1::uuid, 'test', 'v1', 'operating',
			now(), '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{"slug":"testco","template_version":"v1"}'::jsonb, now(), now()
		)
	`, verticalID); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	if err := pg.EnsureEntitySchema(ctx, verticalID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}
	if err := pg.SetEntityTemplateVersion(ctx, verticalID, "v2"); err != nil {
		t.Fatalf("SetEntityTemplateVersion: %v", err)
	}

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "a1",
			Role:     "role",
			Mode:     "global",
			Type:     "sonnet",
			EntityID: "",
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["system.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := pg.MarkAgentTerminated(ctx, "a1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       "ephemeral-shard-1",
			Role:     "worker",
			Mode:     "factory",
			Type:     "sonnet",
			EntityID: "",
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["review.ready"]}`),
		},
		Status:          "ephemeral",
		HiredBy:         "runtime",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	}); err != nil {
		t.Fatalf("UpsertAgent ephemeral: %v", err)
	}
	agents, err := pg.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	for _, a := range agents {
		if a.Config.ID == "a1" {
			t.Fatal("expected terminated agent to be excluded from LoadAgents")
		}
		if a.Config.ID == "ephemeral-shard-1" {
			t.Fatal("expected ephemeral agent to be excluded from LoadAgents")
		}
	}

	ceoID := "operator-" + verticalID
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       ceoID,
			Role:     "operator",
			Mode:     "operating",
			Type:     "sonnet",
			EntityID: verticalID,
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["review.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	})
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     verticalID,
		EventPattern: "review.*",
		SubscriberID: ceoID,
		InstalledBy:  ceoID,
		Reason:       "tests",
		Status:       "active",
		Source:       "seeded",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.DeactivateRoutingRulesByEntity(ctx, verticalID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}

	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "review.requested",
		SourceAgent: "human",
		Payload:     json.RawMessage(`{"x":1}`),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}).WithEntityID(verticalID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{ceoID}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	pending, err := pg.ListPendingEventsForAgent(ctx, ceoID, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(pending) == 0 {
		t.Fatalf("ListPendingEventsForAgent err=%v len=%d", err, len(pending))
	}

	subPending, err := pg.ListPendingSubscribedEvents(ctx, ceoID, []events.EventType{"review.*"}, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(subPending) == 0 {
		t.Fatalf("ListPendingSubscribedEvents err=%v len=%d", err, len(subPending))
	}

	if err := pg.UpsertEventReceipt(ctx, evt.ID, ceoID, "error", "boom"); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
	if rec, ok, err := pg.GetEventReceipt(ctx, evt.ID, ceoID); err != nil || !ok || rec.Status == "" {
		t.Fatalf("GetEventReceipt ok=%v err=%v rec=%+v", ok, err, rec)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1, 'cli_test', 'anthropic_cli', 's1', 'active', 0, now(), now())
	`, ceoID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        ceoID,
		RuntimeMode:    "cli_test",
		SessionID:      "s1",
		TaskID:         "",
		RequestPayload: []byte(`{"in":1}`),
		ResponseRaw:    []byte(`{"out":1}`),
		ParseOK:        true,
		Latency:        123 * time.Millisecond,
		RetryCount:     0,
		Error:          "",
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		AgentID:   ceoID,
		TaskID:    "",
		Mode:      "session",
		Messages:  []llm.Message{{Role: "user", Content: "hi"}},
		Summary:   "sum",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation: %v", err)
	}
	if rec, ok, err := pg.LoadActiveConversation(ctx, ceoID, "session", ""); err != nil || !ok || rec.AgentID != ceoID {
		t.Fatalf("LoadActiveConversation ok=%v err=%v rec=%+v", ok, err, rec)
	}

	sc := runtimepipeline.Schedule{
		AgentID:   ceoID,
		EventType: "timer.test",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, sc); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if got, err := pg.LoadActiveSchedules(ctx); err != nil || len(got) == 0 {
		t.Fatalf("LoadActiveSchedules err=%v len=%d", err, len(got))
	}
	if err := pg.MarkScheduleFired(ctx, sc); err != nil {
		t.Fatalf("MarkScheduleFired: %v", err)
	}
	if err := pg.CancelSchedule(ctx, ceoID, "timer.test"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
}

func TestPostgresStore_MarkAgentTerminated_CleansRuntimeState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:   "agent-cleanup-1",
			Role: "worker",
			Mode: "factory",
			Type: "sonnet",
			Config: json.RawMessage(`{
			"system_prompt":"x",
			"subscriptions":["review.ready"]
		}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO conversations (agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ($1, 'session', '[{"role":"user","content":"hello"}]'::jsonb, 'x', 1, 'active', now(), now())
	`, "agent-cleanup-1"); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1, 'cli_test', 'anthropic_cli', 'sess-cleanup-1', 'active', 2, now(), now())
	`, "agent-cleanup-1"); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	if err := pg.MarkAgentTerminated(ctx, "agent-cleanup-1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}

	var (
		agentStatus string
		convStatus  string
		sessStatus  string
	)
	if err := db.QueryRowContext(ctx, `SELECT status FROM agents WHERE id = $1`, "agent-cleanup-1").Scan(&agentStatus); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM conversations WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&convStatus); err != nil {
		t.Fatalf("read conversation status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&sessStatus); err != nil {
		t.Fatalf("read session status: %v", err)
	}
	if agentStatus != "terminated" {
		t.Fatalf("expected terminated agent status, got %q", agentStatus)
	}
	if convStatus != "terminated" {
		t.Fatalf("expected terminated conversation status, got %q", convStatus)
	}
	if sessStatus != "rotated" {
		t.Fatalf("expected rotated session status, got %q", sessStatus)
	}
}

func TestManagerHelpers_MatchingAndRedaction(t *testing.T) {
	got := extractSubscriptions([]byte(`{"subscriptions":["a","b"," "],"tools":[]}`))

	hasA, hasB := false, false
	for _, v := range got {
		if v == "a" {
			hasA = true
		}
		if v == "b" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Fatalf("expected a and b subscriptions, got %v", got)
	}
	if normalizeJSONPayload([]byte(`{"b":1,"a":2}`)) == "" {
		t.Fatal("expected normalized json")
	}
	if !matchesAnySubscription("review.chat", []events.EventType{"review.*"}) {
		t.Fatal("expected subscription match")
	}
	if subscriptionMatch("review.*", "budget.alert") {
		t.Fatal("unexpected match")
	}
	if nullable("", "x") != "x" {
		t.Fatal("nullable fallback mismatch")
	}
	if sanitizeSchemaIdent("Test-Co!!") != "testco" {
		t.Fatalf("sanitizeSchemaIdent mismatch")
	}
	if quoteIdent("x") != `"x"` {
		t.Fatal("quoteIdent mismatch")
	}

	obj := map[string]any{"api_key": "secret", "name": "John Doe", "nested": map[string]any{"token": "t"}}
	redacted := redactPayloadValue("root", obj)
	b, _ := json.Marshal(redacted)
	if string(b) == "" {
		t.Fatal("expected redacted json")
	}
	_ = redactText("sk-ant-foo")
	_ = redactName("John Doe")
	_ = isNameKey("name")
}
