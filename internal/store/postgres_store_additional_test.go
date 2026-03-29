package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/google/uuid"
	"strings"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/testutil"
	"testing"
	"time"
)

func resetAgentSessionsSpecTable(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE`); err != nil {
		t.Fatalf("drop legacy agent_sessions: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec.PlatformTables.Tables = map[string]struct {
		Description string `yaml:"description"`
		DDL         string `yaml:"ddl"`
	}{
		"agent_sessions": {
			DDL: "CREATE TABLE agent_sessions (\n    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n    agent_id TEXT NOT NULL,\n    entity_id UUID,\n    flow_instance TEXT,\n    scope_key TEXT NOT NULL,\n    scope TEXT NOT NULL DEFAULT 'entity',\n    conversation JSONB NOT NULL DEFAULT '[]',\n    turn_count INTEGER NOT NULL DEFAULT 0,\n    runtime_mode TEXT NOT NULL DEFAULT 'task',\n    runtime_state JSONB NOT NULL DEFAULT '{}',\n    lease_holder TEXT,\n    lease_expires_at TIMESTAMPTZ,\n    status TEXT NOT NULL DEFAULT 'active',\n    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    UNIQUE (agent_id, scope_key)\n);",
		},
	}
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs(agent_sessions): %v", err)
	}
	if err := pg.EnsureSchemaTables(ctx, plans); err != nil {
		t.Fatalf("EnsureSchemaTables(agent_sessions): %v", err)
	}
}

func seedSpecEntityState(t *testing.T, ctx context.Context, db execer, entityID, flowInstance, slug, name, state string) {
	t.Helper()
	if strings.TrimSpace(flowInstance) == "" {
		flowInstance = strings.TrimSpace(slug)
	}
	if flowInstance == "" {
		flowInstance = "entity-" + entityID
	}
	if state == "" {
		state = "operating"
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
		ON CONFLICT (instance_id) DO NOTHING
	`, flowInstance); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2, 'default', NULLIF($3,''), NULLIF($4,''), $5,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
		ON CONFLICT (entity_id) DO NOTHING
	`, entityID, flowInstance, slug, name, state); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
}

func seedSpecAgent(t *testing.T, ctx context.Context, pg *PostgresStore, agentID string, entityID string, subscriptions ...string) {
	t.Helper()
	cfg := runtimeactors.AgentConfig{
		ID:            agentID,
		Role:          agentID,
		Mode:          "global",
		Type:          "stub",
		EntityID:      strings.TrimSpace(entityID),
		Subscriptions: subscriptions,
		Config:        []byte(`{"system_prompt":"x"}`),
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config:    cfg,
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent %s: %v", agentID, err)
	}
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

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

	var gotTaskID, gotEntityID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(payload->>'task_id', ''), COALESCE(entity_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&gotTaskID, &gotEntityID); err != nil {
		t.Fatalf("query event row: %v", err)
	}
	if gotTaskID != "" {
		t.Fatalf("expected normalized empty task_id, got %q", gotTaskID)
	}
	if gotEntityID != "" {
		t.Fatalf("expected normalized empty entity_id, got %q", gotEntityID)
	}
}

func TestPostgresStore_PipelineReceipts_MissingEventsQuery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

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
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
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
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
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
	if err != nil {
		t.Fatalf("PersistEventWithDeliveries unknown subscriber should still persist: %v", err)
	}
	var persistedCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, failedEventID).Scan(&persistedCount); err != nil {
		t.Fatalf("count persisted event: %v", err)
	}
	if persistedCount != 1 {
		t.Fatalf("expected persisted event row, count=%d", persistedCount)
	}
}

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
}

func TestPostgresStore_Inbound_PurgeDeletes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	if ok, err := s.RecordInboundEvent(ctx, "evt-old", entityID, "chat"); err != nil || !ok {
		t.Fatalf("record old ok=%v err=%v", ok, err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE events
		SET created_at = now() - interval '2 days'
		WHERE event_name = 'platform.inbound_recorded'
		  AND payload->>'provider_event_id' = 'evt-old'
	`); err != nil {
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

func TestPostgresStore_Inbound_RecordAndPurge(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	ok, err := s.RecordInboundEvent(ctx, "evt-1", entityID, "chat")
	if err != nil || !ok {
		t.Fatalf("record inbound ok=%v err=%v", ok, err)
	}
	ok, err = s.RecordInboundEvent(ctx, "evt-1", entityID, "chat")
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

	entityID := uuid.NewString()

	id, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
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

	if err := s.DecideMailboxItem(ctx, id, "decided", "approve", "ok"); err != nil {
		t.Fatalf("decide: %v", err)
	}
	if err := s.DecideMailboxItem(ctx, id, "decided", "approve", "again"); err == nil {
		t.Fatal("expected decide on non-pending to fail")
	}
	if err := s.DecideMailboxItem(ctx, uuid.NewString(), "nope", "approve", ""); err == nil {
		t.Fatal("expected invalid status error")
	}

	expID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
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
			if it.Status != "expired" || it.Decision != "" {
				t.Fatalf("expected expired/empty-decision, got %+v", it)
			}
		}
	}
	if !found {
		t.Fatalf("expected expired item in result")
	}

	critID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EntityID:  entityID,
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
	if subscriptionMatch("inbound.*", "room.message") {
		t.Fatalf("prefix star should not match other prefix")
	}
	if !subscriptionMatch("room.message", "room.message") {
		t.Fatalf("exact should match")
	}
	if subscriptionMatch("room.message", "room.messages") {
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
	entityID := uuid.NewString()

	first := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(30 * time.Minute).UTC(),
		EntityID:  entityID,
		TaskID:    "timer-a",
		Payload:   []byte(`{"timer_id":"timer-a"}`),
	}
	second := runtimepipeline.Schedule{
		AgentID:   "validation-orchestrator",
		EventType: "timer.validation_timeout",
		Mode:      "once",
		At:        time.Now().Add(60 * time.Minute).UTC(),
		EntityID:  entityID,
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
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == entityID {
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
		if sc.AgentID == "validation-orchestrator" && sc.EventType == "timer.validation_timeout" && sc.EntityID == entityID {
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

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "inbound.*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "inbound.test",
		SourceAgent: "inbound",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"a1"}); err != nil {
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
	if strings.TrimSpace(string(r.Status)) != "dead_letter" {
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

	broadcastID := uuid.NewString()
	directOtherID := uuid.NewString()
	directSelfID := uuid.NewString()
	for idx, id := range []string{broadcastID, directOtherID, directSelfID} {
		if err := pg.AppendEvent(ctx, events.Event{
			ID:          id,
			Type:        "inbound.alert",
			SourceAgent: "runtime",
			Payload:     []byte(`{}`),
			CreatedAt:   time.Now().Add(time.Duration(-3+idx) * time.Minute),
		}); err != nil {
			t.Fatalf("seed events: %v", err)
		}
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, created_at)
		VALUES
			($1::uuid, 'agent', 'a2', now()),
			($2::uuid, 'agent', 'a1', now())
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

	entityID := uuid.NewString()
	seedSpecAgent(t, ctx, pg, "a1", "", "*")
	eventID := uuid.NewString()
	if err := pg.AppendEvent(ctx, (events.Event{
		ID:          eventID,
		Type:        "system.started",
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID)); err != nil {
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

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "v-flow", "v", "V", "operating")

	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
		EventPattern: "x.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "active",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
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

	if err := pg.CancelSchedule(ctx, "sub", "timer.recurring_digest"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	_ = time.Second
}

func TestManagerStore_EnsureEntitySchema(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "entity-schema-flow", "vslug", "TestCo", "operating")
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
	}

}

func TestManagerStore_RoutingRules_DeactivateAndBootstrapVersion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "vslug-flow", "vslug", "V", "operating")

	r := runtimemanager.PersistedRoutingRule{
		EntityID:         entityID,
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

	r.Status = "inactive"
	if err := pg.UpsertRoutingRule(ctx, r); err != nil {
		t.Fatalf("UpsertRoutingRule deactivate: %v", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM routing_rules
		WHERE event_pattern='inbound.*' AND subscriber_id='sub'
	`).Scan(&status); err != nil {
		t.Fatalf("load routing rule status: %v", err)
	}
	if status != "inactive" {
		t.Fatalf("expected inactive status, got %q", status)
	}

	if err := pg.DeactivateRoutingRulesByEntity(ctx, entityID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}
}

func TestManagerStore_Conversations_AndAgentTurns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

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

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{AgentID: "a1", RuntimeMode: "session", SessionID: uuid.NewString()}); err == nil {
		t.Fatalf("expected missing session row error")
	}
	var sessionID string
	if err := db.QueryRowContext(ctx, `
		SELECT session_id::text
		FROM agent_sessions
		WHERE agent_id = 'a1' AND scope_key = 'global'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&sessionID); err != nil {
		t.Fatalf("load seeded agent_session: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "session",
		SessionID:      sessionID,
		TaskID:         uuid.NewString(),
		RequestPayload: []byte(`{"x":1}`),
		ResponseRaw:    []byte(`{"y":2}`),
		ParseOK:        true,
		Latency:        10 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn: %v", err)
	}
}

func TestManagerStore_StatelessConversationPersistsAuditRowWithoutReload(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	resetAgentSessionsSpecTable(t, ctx, pg)
	seedSpecAgent(t, ctx, pg, "a1", "", "")

	sessionID := uuid.NewString()
	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		SessionID: sessionID,
		AgentID:   "a1",
		Mode:      "task",
		Messages: []llm.Message{
			{Role: "user", Content: "one-shot"},
			{Role: "assistant", Content: "done"},
		},
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("UpsertConversation(task): %v", err)
	}

	if rec, ok, err := pg.LoadActiveConversation(ctx, "a1", "task", ""); err != nil || ok || rec.AgentID != "" {
		t.Fatalf("LoadActiveConversation(task) ok=%v err=%v rec=%+v", ok, err, rec)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'a1' AND runtime_mode = 'task'`).Scan(&count); err != nil {
		t.Fatalf("count task sessions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one persisted task audit row, got %d", count)
	}

	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        "a1",
		RuntimeMode:    "task",
		SessionID:      sessionID,
		RequestPayload: []byte(`{"kind":"task"}`),
		ResponseRaw:    []byte(`{"ok":true}`),
		ParseOK:        true,
		Latency:        5 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AppendAgentTurn(task): %v", err)
	}

	var parseOK bool
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE((runtime_state->'last_turn'->>'parse_ok')::boolean, false)
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&parseOK); err != nil {
		t.Fatalf("load task runtime_state: %v", err)
	}
	if !parseOK {
		t.Fatal("expected task-mode last_turn telemetry to be persisted")
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
	resetAgentSessionsSpecTable(t, ctx, pg)

	entityID := uuid.NewString()
	seedSpecEntityState(t, ctx, db, entityID, "testco", "testco", "TestCo", "operating")
	if err := pg.EnsureEntitySchema(ctx, entityID); err != nil {
		t.Fatalf("EnsureEntitySchema: %v", err)
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
			Mode:     "worker",
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

	ceoID := "operator-" + entityID
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       ceoID,
			Role:     "operator",
			Mode:     "operating",
			Type:     "sonnet",
			EntityID: entityID,
			Config:   json.RawMessage(`{"system_prompt":"x","subscriptions":["review.*"]}`),
		},
		Status:          "active",
		HiredBy:         "test",
		StartedAt:       time.Now().UTC(),
		TemplateVersion: "v2",
	})
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{
		EntityID:     entityID,
		EventPattern: "review.*",
		SubscriberID: ceoID,
		InstalledBy:  ceoID,
		Reason:       "tests",
		Status:       "active",
		Source:       "seeded",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.DeactivateRoutingRulesByEntity(ctx, entityID); err != nil {
		t.Fatalf("DeactivateRoutingRulesByEntity: %v", err)
	}

	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "review.requested",
		SourceAgent: "human",
		Payload:     json.RawMessage(`{"x":1}`),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}).WithEntityID(entityID)
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

	sessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, agent_id, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at)
		VALUES ($1::uuid, $2, 'global', 'global', '[]'::jsonb, 0, 'session', '{}'::jsonb, 'active', now(), now())
	`, sessionID, ceoID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := pg.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID:        ceoID,
		RuntimeMode:    "session",
		SessionID:      sessionID,
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
	resetAgentSessionsSpecTable(t, ctx, pg)

	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:   "agent-cleanup-1",
			Role: "worker",
			Mode: "worker",
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

	if err := pg.UpsertConversation(ctx, runtimellm.ConversationRecord{
		AgentID:   "agent-cleanup-1",
		Mode:      "session",
		Messages:  []llm.Message{{Role: "user", Content: "hello"}},
		Summary:   "x",
		TurnCount: 1,
		Status:    "active",
	}); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	if err := pg.MarkAgentTerminated(ctx, "agent-cleanup-1"); err != nil {
		t.Fatalf("MarkAgentTerminated: %v", err)
	}

	var (
		agentStatus string
		sessStatus  string
	)
	if err := db.QueryRowContext(ctx, `SELECT status FROM agents WHERE agent_id = $1`, "agent-cleanup-1").Scan(&agentStatus); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE agent_id = $1 ORDER BY created_at DESC LIMIT 1`, "agent-cleanup-1").Scan(&sessStatus); err != nil {
		t.Fatalf("read session status: %v", err)
	}
	if agentStatus != "terminated" {
		t.Fatalf("expected terminated agent status, got %q", agentStatus)
	}
	if sessStatus != "terminated" {
		t.Fatalf("expected terminated session status, got %q", sessStatus)
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
