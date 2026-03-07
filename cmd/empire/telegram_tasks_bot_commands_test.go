package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestTelegramTaskCommands_EndToEndAgainstPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	stores := storeBundle{SQLDB: db, EventStore: pg, MailboxStore: pg}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{Command: "true", OutputFormat: "json"},
		},
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{BudgetReset: "monday", MaxTasksPerWeek: 3, AutoExpireHours: 168},
		},
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// Ensure coordinator exists so event delivery FK doesn't fail.
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator','system','empire-coordinator','holding','active','{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	taskID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid,'empire-coordinator',$2::uuid,'verification','call someone','approved', now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	msg := &telegramMessage{}
	msg.Chat.ID = 1
	msg.From.Username = "me"

	// details
	msg.Text = "/details " + taskID[:8]
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); !strings.Contains(out, "[TASK details]") {
		t.Fatalf("details output: %q", out)
	}

	// claim
	msg.Text = "/claim " + taskID[:8]
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); !strings.Contains(out, "Claimed task") {
		t.Fatalf("claim output: %q", out)
	}

	// complete requires result text
	msg.Text = "/complete_success " + taskID[:8]
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); !strings.Contains(out, "Result text required") {
		t.Fatalf("complete missing result output: %q", out)
	}

	// complete success
	msg.Text = "/complete_success " + taskID[:8] + " done"
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); !strings.Contains(out, "Completed task") {
		t.Fatalf("complete output: %q", out)
	}

	// reject on a new task
	task2 := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid,'empire-coordinator',$2::uuid,'verification','call again','approved', now())
	`, task2, verticalID); err != nil {
		t.Fatalf("seed task2: %v", err)
	}
	msg.Text = "/reject " + task2[:8] + " not now"
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); !strings.Contains(out, "Rejected") {
		t.Fatalf("reject output: %q", out)
	}

	// unknown command returns empty.
	msg.Text = "/unknown " + taskID[:8]
	if out := handleTelegramTaskCommand(context.Background(), stores, cfg, msg); out != "" {
		t.Fatalf("expected empty for unknown command, got %q", out)
	}

	// render helpers: talking points.
	if renderTalkingPoints([]byte(`["a","b"]`)) == "" {
		t.Fatalf("expected list talking points")
	}
	if renderTalkingPoints([]byte(`{"k":"v"}`)) == "" {
		t.Fatalf("expected map talking points")
	}
	// renderTaskTelegramMessage should include commands.
	row := humanTaskRow{ID: taskID, Category: "verification", VerticalSlug: "testco", Description: "x", Priority: "high"}
	if msg := renderTaskTelegramMessage(row); !strings.Contains(msg, "/claim") {
		t.Fatalf("expected commands in message: %q", msg)
	}

	// coverage for early return: empty payload.
	if extractString([]byte(`{}`), "nope") != "" {
		t.Fatalf("expected empty extractString")
	}

	// Touch runtime symbol for imports (storeBundle uses runtime interfaces).
	_ = runtimetools.MailboxItem{}
}
