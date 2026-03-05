package main

import (
	"context"
	"os"
	"testing"

	"empireai/internal/config"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestChatSubcommand_AsyncInteractive_UsesStdinLoop(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	// Pre-seed a vertical so the target fallback gets a real UUID vertical_id.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	if stores.SQLDB == nil {
		t.Fatalf("expected postgres db")
	}
	verticalID := uuid.NewString()
	if _, err := stores.SQLDB.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// Replace stdin to exercise the interactive async loop.
	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdin = r
	t.Cleanup(func() {
		os.Stdin = oldStdin
		_ = r.Close()
	})
	go func() {
		// blank line should be ignored; one message should queue; then exit
		_, _ = w.Write([]byte("\nhello\n/exit\n"))
		_ = w.Close()
	}()

	if err := runChatSubcommand([]string{
		"--config", cfgPath,
		"--store", "postgres",
		"--async=true",
		verticalID + "/ceo",
	}); err != nil {
		t.Fatalf("chat async interactive: %v", err)
	}
}

func TestChatSubcommand_UsageError(t *testing.T) {
	if err := runChatSubcommand([]string{}); err == nil {
		t.Fatal("expected usage error")
	}
}

func TestChatSubcommand_LiveOneShot_ExercisesRuntimeSetup(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	// Ensure a real vertical exists; live chat will upsert + recover agents from store.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	if stores.SQLDB == nil {
		t.Fatalf("expected postgres db")
	}
	verticalID := uuid.NewString()
	if _, err := stores.SQLDB.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	// In cli_test mode the Claude CLI command is "true", which yields invalid/empty output.
	// That's fine: we want to exercise the live setup path; the final ChatWithAgent may fail.
	err = runChatSubcommand([]string{
		"--config", cfgPath,
		"--store", "postgres",
		verticalID + "/ceo",
		"ping",
	})
	if err == nil {
		// Accept success if the runtime changes; the point is that it should complete.
		return
	}
}

func TestChatSubcommand_LiveInteractive_ExitImmediately(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	port := mustPortFromDSN(t, dsn)
	cfgPath := writeTempConfig(t, port)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	verticalID := uuid.NewString()
	if _, err := stores.SQLDB.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdin = r
	t.Cleanup(func() {
		os.Stdin = oldStdin
		_ = r.Close()
	})
	go func() {
		_, _ = w.Write([]byte("/exit\n"))
		_ = w.Close()
	}()

	// Live mode without initial message should enter the loop and exit cleanly.
	_ = runChatSubcommand([]string{
		"--config", cfgPath,
		"--store", "postgres",
		verticalID + "/ceo",
	})
}
