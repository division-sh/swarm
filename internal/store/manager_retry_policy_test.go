package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/store"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestUpsertEventReceipt_DeadLettersAfterThreeRetries_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	verticalID, agentID := seedVerticalAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, verticalID, "test.retry_upsert")

	for i := 1; i <= 4; i++ {
		if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, "error", "boom"); err != nil {
			t.Fatalf("upsert receipt error #%d: %v", i, err)
		}

		var status string
		var retryCount int
		if err := pg.DB.QueryRowContext(ctx, `
			SELECT status, retry_count
			FROM event_receipts
			WHERE event_id = $1::uuid AND agent_id = $2
		`, evt.ID, agentID).Scan(&status, &retryCount); err != nil {
			t.Fatalf("query receipt after #%d: %v", i, err)
		}

		wantStatus := "error"
		if i == 4 {
			wantStatus = "dead_letter"
		}
		if status != wantStatus || retryCount != i {
			t.Fatalf("after %d errors: got status=%q retry_count=%d, want status=%q retry_count=%d", i, status, retryCount, wantStatus, i)
		}
	}
}

func TestListPendingEventsForAgent_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	verticalID, agentID := seedVerticalAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, verticalID, "test.pending_direct")
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	since := time.Now().Add(-2 * time.Hour)

	// No receipt: should be immediately pending.
	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 1, time.Now().Add(-30*time.Second))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=1 not ready): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 1, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 2, time.Now().Add(-4*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=2 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=2 not ready): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 2, time.Now().Add(-6*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=2 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (retry=2 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 3, time.Now().Add(-29*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=3 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=3 not ready): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 3, time.Now().Add(-31*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=3 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (retry=3 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	// After retries are exhausted, the event should not be pending.
	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 4, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=4): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=4): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "dead_letter", 4, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (dead_letter): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (dead_letter): got %d events, want 0", len(evts))
	}
}

func TestListPendingSubscribedEvents_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	verticalID, agentID := seedVerticalAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, verticalID, "test.pending_subscribed")

	since := time.Now().Add(-2 * time.Hour)
	subs := []events.EventType{evt.Type}

	evts, err := pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 3, time.Now().Add(-29*time.Minute))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (retry=3 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list subscribed pending (retry=3 not ready): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 3, time.Now().Add(-31*time.Minute))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (retry=3 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (retry=3 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "dead_letter", 4, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (dead_letter): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list subscribed pending (dead_letter): got %d events, want 0", len(evts))
	}
}

func insertOrUpdateReceipt(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID, status string, retryCount int, processedAt time.Time) {
	t.Helper()
	// Upsert-style helper for tests; the production upsert also mutates retry_count which isn't what we want
	// for time-window filtering tests.
	const q = `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, $3, $4, $5, 'boom')
		ON CONFLICT (event_id, agent_id) DO UPDATE SET
			processed_at = EXCLUDED.processed_at,
			status = EXCLUDED.status,
			retry_count = EXCLUDED.retry_count,
			error = EXCLUDED.error
	`
	if _, err := pg.DB.ExecContext(ctx, q, eventID, agentID, processedAt, status, retryCount); err != nil {
		t.Fatalf("upsert receipt: %v", err)
	}
}

func newTestPostgresStore(t *testing.T) (*store.PostgresStore, func()) {
	t.Helper()

	adminDSN := os.Getenv("EMPIREAI_PG_ADMIN_DSN")
	if strings.TrimSpace(adminDSN) == "" {
		t.Skip("set EMPIREAI_PG_ADMIN_DSN to run postgres store retry policy tests")
	}

	ctx := context.Background()
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatalf("open admin dsn: %v", err)
	}
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Skipf("postgres not reachable from EMPIREAI_PG_ADMIN_DSN: %v", err)
	}

	var owner string
	if err := admin.QueryRowContext(ctx, `SELECT current_user`).Scan(&owner); err != nil {
		_ = admin.Close()
		t.Fatalf("resolve current_user: %v", err)
	}

	dbName := fmt.Sprintf("empireai_store_test_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s" OWNER "%s"`, dbName, owner)); err != nil {
		_ = admin.Close()
		t.Fatalf("create test db: %v", err)
	}

	appDSN := withDBName(adminDSN, dbName)
	pg, err := store.NewPostgresStore(appDSN)
	if err != nil {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		_ = admin.Close()
		t.Fatalf("new postgres store: %v", err)
	}
	if err := pg.Ping(ctx); err != nil {
		_ = pg.DB.Close()
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		_ = admin.Close()
		t.Fatalf("ping app db: %v", err)
	}

	if err := pg.ApplyMigrationFile(ctx, migrationPath(t)); err != nil {
		_ = pg.DB.Close()
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		_ = admin.Close()
		t.Fatalf("apply migration: %v", err)
	}

	cleanup := func() {
		_ = pg.DB.Close()
		_, _ = admin.ExecContext(ctx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		_ = admin.Close()
	}

	return pg, cleanup
}

func seedVerticalAndAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore) (verticalID, agentID string) {
	t.Helper()

	verticalID = uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO verticals (id, name, geography, stage, mode)
		VALUES ($1::uuid, 'Store Retry Policy Test', 'US', 'approved', 'factory')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	agentID = "agent-" + uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, config)
		VALUES ($1, 'test', 'test', 'factory', $2::uuid, '{}'::jsonb)
	`, agentID, verticalID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	return verticalID, agentID
}

func seedEvent(t *testing.T, ctx context.Context, pg *store.PostgresStore, verticalID, eventType string) events.Event {
	t.Helper()

	payload, _ := json.Marshal(map[string]any{"k": "v"})
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: "store-test",
		Payload:     payload,
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}).WithEntityID(verticalID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	return evt
}

func withDBName(dsn, dbName string) string {
	fields := strings.Fields(dsn)
	out := make([]string, 0, len(fields)+1)
	replaced := false
	for _, f := range fields {
		if strings.HasPrefix(f, "dbname=") {
			out = append(out, "dbname="+dbName)
			replaced = true
			continue
		}
		out = append(out, f)
	}
	if !replaced {
		out = append(out, "dbname="+dbName)
	}
	return strings.Join(out, " ")
}

func migrationPath(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations", "001_initial.sql"))
}
