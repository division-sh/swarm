package runtime_test

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
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestPostgresRecoverySmoke(t *testing.T) {
	adminDSN := os.Getenv("EMPIREAI_PG_ADMIN_DSN")
	if strings.TrimSpace(adminDSN) == "" {
		t.Skip("set EMPIREAI_PG_ADMIN_DSN to run postgres recovery smoke test")
	}

	ctx := context.Background()
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatalf("open admin dsn: %v", err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Skipf("postgres not reachable from EMPIREAI_PG_ADMIN_DSN: %v", err)
	}
	var owner string
	if err := admin.QueryRowContext(ctx, `SELECT current_user`).Scan(&owner); err != nil {
		t.Fatalf("resolve current_user: %v", err)
	}

	dbName := fmt.Sprintf("empireai_smoke_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s" OWNER "%s"`, dbName, owner)); err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer func() {
		_, _ = admin.ExecContext(ctx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
	}()

	appDSN := withDBName(adminDSN, dbName)
	pg, err := store.NewPostgresStore(appDSN)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	defer pg.DB.Close()
	if err := pg.Ping(ctx); err != nil {
		t.Fatalf("ping app db: %v", err)
	}

	if err := pg.ApplyMigrationFile(ctx, migrationPath(t)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	verticalID := uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO verticals (id, name, geography, stage, mode)
		VALUES ($1::uuid, 'Recovery Smoke', 'US', 'approved', 'factory')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus1 := rt.NewEventBus(pg)
	manager1 := rt.NewAgentManager(bus1, nil, pg)
	if err := manager1.SpawnOpCo(verticalID, models.MandateDocument{VerticalID: verticalID}); err != nil {
		t.Fatalf("spawn opco: %v", err)
	}
	manager1.Run(ctx)

	e1 := mustEvent("product_report", verticalID)
	if err := bus1.Publish(ctx, e1); err != nil {
		t.Fatalf("publish e1: %v", err)
	}
	ceoID := fmt.Sprintf("opco-ceo-%s", verticalID)
	waitForReceipt(t, pg.DB, e1.ID, ceoID, 3*time.Second)
	if err := manager1.Shutdown(); err != nil {
		t.Fatalf("shutdown manager1: %v", err)
	}

	e2 := mustEvent("growth_report", verticalID)
	if err := pg.AppendEvent(ctx, e2); err != nil {
		t.Fatalf("append e2: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, e2.ID, []string{ceoID}); err != nil {
		t.Fatalf("insert e2 deliveries: %v", err)
	}

	bus2 := rt.NewEventBus(pg)
	manager2 := rt.NewAgentManager(bus2, nil, pg)
	if err := manager2.Recover(ctx); err != nil {
		t.Fatalf("recover manager2: %v", err)
	}
	waitForReceipt(t, pg.DB, e2.ID, ceoID, 3*time.Second)
}

func mustEvent(eventType, verticalID string) events.Event {
	payload, _ := json.Marshal(map[string]any{"k": "v"})
	return events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: "smoke-test",
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
}

func waitForReceipt(t *testing.T, db *sql.DB, eventID, agentID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := db.QueryRow(`
			SELECT status
			FROM event_receipts
			WHERE event_id = $1::uuid AND agent_id = $2
		`, eventID, agentID).Scan(&status)
		if err == nil && (status == "processed" || status == "skipped") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("receipt not found for event=%s agent=%s within %s", eventID, agentID, timeout)
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
